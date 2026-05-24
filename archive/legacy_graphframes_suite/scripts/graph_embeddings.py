"""
graph_embeddings.py
===================
Node2Vec Graph Embeddings for Social Network Analysis
Learns dense vector representations of nodes using
biased random walks + Word2Vec.

Dynamic: auto-detects any SNAP dataset in data_lake/.

Author  : BDA Social Graph Project
"""

import os, sys, glob
import pyspark

# -- Windows Environment Setup --------------------------
os.environ['SPARK_HOME'] = os.path.dirname(pyspark.__file__)
os.environ["PYSPARK_PIN_THREAD"] = "true"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
hadoop_home = r'C:\hadoop'
if os.path.isdir(hadoop_home):
    os.environ['HADOOP_HOME'] = hadoop_home
# --------------------------------------------------------

import random
import collections
import math
import numpy as np

# PySpark for data loading
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

# Gensim for Word2Vec (pip install gensim)
from gensim.models import Word2Vec

# Sklearn for downstream tasks
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
from sklearn.preprocessing import LabelEncoder
from sklearn.svm import SVC
from sklearn.model_selection import cross_val_score
from sklearn.metrics import silhouette_score


# ---------------------------------------------
#  1. Node2Vec Random Walk Generator
# ---------------------------------------------
class Node2Vec:
    """
    Node2Vec biased random walk generator.
    
    Parameters
    ----------
    p : float
        Return parameter. Higher p -> less likely to backtrack.
    q : float  
        In-out parameter. q < 1 -> DFS (community); q > 1 -> BFS (role).
    walk_length : int
        Length of each random walk.
    num_walks : int
        Number of walks per node.
    """

    def __init__(
        self,
        edges: list,
        nodes: list,
        p: float = 1.0,
        q: float = 0.5,
        walk_length: int = 80,
        num_walks: int = 10,
        seed: int = 42,
    ):
        self.nodes = nodes
        self.p = p
        self.q = q
        self.walk_length = walk_length
        self.num_walks = num_walks
        self.seed = seed

        # Build weighted adjacency (uniform weights for unweighted graph)
        self.adj = {n: [] for n in nodes}
        for src, dst in edges:
            self.adj[src].append(dst)
            self.adj[dst].append(src)

        # Precompute transition probabilities
        self._precompute_probs()

    def _precompute_probs(self):
        """
        Precompute unnormalized transition probabilities for each (prev, curr) pair.
        Uses Node2Vec's biased walk formula.
        """
        print("  [Node2Vec] Precomputing transition probabilities...")
        self.probs = {}  # (prev_node, curr_node) -> [prob per neighbor]

        for curr in self.nodes:
            for prev in self.adj[curr]:
                unnorm = []
                for next_node in self.adj[curr]:
                    if next_node == prev:
                        unnorm.append(1.0 / self.p)
                    elif next_node in set(self.adj[prev]):
                        unnorm.append(1.0)
                    else:
                        unnorm.append(1.0 / self.q)

                total = sum(unnorm)
                self.probs[(prev, curr)] = [u / total for u in unnorm]

        print("  [Node2Vec] Precomputation complete.")

    def _walk_from(self, start: int) -> list:
        """Generate a single random walk starting from `start`."""
        walk = [start]
        if not self.adj[start]:
            return walk

        # First step: uniform
        walk.append(random.choice(self.adj[start]))

        for _ in range(self.walk_length - 2):
            curr = walk[-1]
            prev = walk[-2]
            neighbors = self.adj[curr]
            if not neighbors:
                break

            key = (prev, curr)
            if key in self.probs:
                weights = self.probs[key]
                # Weighted random choice
                r = random.random()
                cumsum = 0.0
                chosen = neighbors[0]
                for nb, w in zip(neighbors, weights):
                    cumsum += w
                    if r <= cumsum:
                        chosen = nb
                        break
                walk.append(chosen)
            else:
                walk.append(random.choice(neighbors))

        return walk

    def generate_walks(self) -> list:
        """
        Generate all random walks.
        Returns list of walks (each walk = list of node IDs as strings).
        """
        random.seed(self.seed)
        walks = []
        shuffled_nodes = list(self.nodes)

        print(f"  [Node2Vec] Generating {self.num_walks} walks × "
              f"{len(self.nodes)} nodes (length={self.walk_length})...")

        for walk_num in range(self.num_walks):
            random.shuffle(shuffled_nodes)
            for node in shuffled_nodes:
                walk = self._walk_from(node)
                walks.append([str(n) for n in walk])

            if (walk_num + 1) % max(1, self.num_walks // 5) == 0:
                print(f"  [Node2Vec] Walk {walk_num + 1}/{self.num_walks} done, "
                      f"total walks: {len(walks)}")

        return walks


# ---------------------------------------------
#  2. Train Word2Vec on Walks
# ---------------------------------------------
def train_word2vec(
    walks: list,
    dimensions: int = 128,
    window: int = 10,
    min_count: int = 1,
    workers: int = 4,
    epochs: int = 5,
    seed: int = 42,
) -> Word2Vec:
    """
    Train Word2Vec (skip-gram) on random walks.
    Each walk = sentence, each node = word.
    """
    print(f"\n  [Word2Vec] Training on {len(walks)} walks, "
          f"dim={dimensions}, window={window}...")
    model = Word2Vec(
        sentences=walks,
        vector_size=dimensions,
        window=window,
        min_count=min_count,
        sg=1,             # Skip-gram (better for rare nodes)
        workers=workers,
        epochs=epochs,
        seed=seed,
    )
    print(f"  [Word2Vec] Vocabulary size: {len(model.wv.key_to_index)}")
    return model


# ---------------------------------------------
#  3. Embedding Matrix Extraction
# ---------------------------------------------
def extract_embeddings(
    model: Word2Vec,
    nodes: list,
) -> tuple:
    """
    Returns (node_ids, embedding_matrix) as numpy arrays.
    node_ids shape: (N,), embeddings shape: (N, dim)
    """
    valid_nodes = [n for n in nodes if str(n) in model.wv]
    node_ids = np.array(valid_nodes)
    embeddings = np.array([model.wv[str(n)] for n in valid_nodes])
    print(f"  Extracted embeddings: {embeddings.shape}")
    return node_ids, embeddings


# ---------------------------------------------
#  4. Node Clustering on Embeddings
# ---------------------------------------------
def cluster_nodes_kmeans(
    node_ids: np.ndarray,
    embeddings: np.ndarray,
    k_range: range = range(5, 25),
) -> tuple:
    """
    Applies K-Means clustering to node embeddings.
    Selects optimal K using silhouette score.
    Returns (cluster_labels, optimal_k, silhouette_scores).
    """
    print("\n  [KMeans] Finding optimal number of clusters...")
    silhouette_scores = {}

    for k in k_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(embeddings)
        score = silhouette_score(embeddings, labels)
        silhouette_scores[k] = score
        print(f"    k={k:>3}: silhouette={score:.4f}")

    best_k = max(silhouette_scores, key=silhouette_scores.get)
    print(f"\n  Optimal k={best_k} (silhouette={silhouette_scores[best_k]:.4f})")

    kmeans_best = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    labels = kmeans_best.fit_predict(embeddings)
    return labels, best_k, silhouette_scores


# ---------------------------------------------
#  5. t-SNE Visualization Data
# ---------------------------------------------
def compute_tsne(
    embeddings: np.ndarray,
    perplexity: float = 30.0,
    n_iter: int = 1000,
    seed: int = 42,
) -> np.ndarray:
    """
    Reduces embeddings to 2D using t-SNE for visualization.
    Returns (N, 2) array.
    """
    print("\n  [t-SNE] Computing 2D projection...")
    # Reduce to 50D with PCA first for speed
    from sklearn.decomposition import PCA
    n_pca = min(50, embeddings.shape[1], embeddings.shape[0] - 1)
    pca = PCA(n_components=n_pca, random_state=seed)
    reduced = pca.fit_transform(embeddings)

    tsne = TSNE(
        n_components=2,
        perplexity=perplexity,
        n_iter=n_iter,
        random_state=seed,
        learning_rate="auto",
        init="pca",
    )
    coords_2d = tsne.fit_transform(reduced)
    print(f"  t-SNE complete: shape={coords_2d.shape}")
    return coords_2d


# ---------------------------------------------
#  6. Similarity Search
# ---------------------------------------------
def find_similar_nodes(
    model: Word2Vec,
    query_node: int,
    top_k: int = 10,
) -> list:
    """
    Finds top-K most similar nodes using cosine similarity
    in the embedding space (Word2Vec's built-in most_similar).
    """
    key = str(query_node)
    if key not in model.wv:
        return []
    similar = model.wv.most_similar(key, topn=top_k)
    return [(int(node_str), float(score)) for node_str, score in similar]


# ---------------------------------------------
#  7. Save Embeddings to Spark DataFrame
# ---------------------------------------------
def save_embeddings_to_spark(
    spark: SparkSession,
    node_ids: np.ndarray,
    embeddings: np.ndarray,
    cluster_labels: np.ndarray,
    tsne_coords: np.ndarray,
    out_path: str,
):
    """Saves node embeddings + cluster + 2D coords as CSV."""
    from pyspark.sql.types import (
        StructType, StructField, LongType, IntegerType, DoubleType
    )

    dim = embeddings.shape[1]
    rows = []
    for i, (nid, emb, cl, xy) in enumerate(
        zip(node_ids, embeddings, cluster_labels, tsne_coords)
    ):
        row = [int(nid), int(cl), float(xy[0]), float(xy[1])]
        # Store first 32 dims to keep CSV manageable
        row += [float(x) for x in emb[:32]]
        rows.append(tuple(row))

    schema = (
        ["node_id", "cluster", "tsne_x", "tsne_y"]
        + [f"emb_{i}" for i in range(32)]
    )

    df = spark.createDataFrame(rows, schema)
    df.coalesce(1).write.mode("overwrite").csv(out_path, header=True)
    print(f"  Embeddings saved to: {out_path}")
    return df


# ---------------------------------------------
#  MAIN RUNNER
# ---------------------------------------------
if __name__ == "__main__":
    BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR   = os.path.join(os.path.dirname(BASE_DIR), "data_lake")

    def discover_dataset(explicit_path=None):
        if explicit_path and os.path.exists(explicit_path):
            return explicit_path
        candidates = (
            glob.glob(os.path.join(DATA_DIR, "*.txt"))
            + glob.glob(os.path.join(DATA_DIR, "*.txt.gz"))
            + glob.glob(os.path.join(DATA_DIR, "*.csv"))
            + glob.glob(os.path.join(DATA_DIR, "*.csv.gz"))
        )
        if not candidates:
            print("[ERROR] No dataset found in data_lake/")
            sys.exit(1)
        candidates.sort(key=lambda f: os.path.getsize(f))
        return candidates[0]

    explicit = sys.argv[1] if len(sys.argv) > 1 else None
    if explicit and not os.path.isabs(explicit):
        explicit = os.path.join(BASE_DIR, explicit)
    DATA_PATH  = discover_dataset(explicit)
    OUT_DIR    = os.path.join(BASE_DIR, "outputs", "embeddings")
    MODEL_SAVE = os.path.join(BASE_DIR, "models", "node2vec_w2v")
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(os.path.join(os.path.dirname(BASE_DIR), "models"), exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Node2VecEmbeddings")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("  NODE2VEC GRAPH EMBEDDINGS")
    print("=" * 60)

    # Load graph
    print("\n[1] Loading graph...")
    raw = spark.read.text(DATA_PATH)
    edges_df = (
        raw.select(F.split("value", r"\s+").alias("p"))
        .select(
            F.col("p")[0].cast(LongType()).alias("src"),
            F.col("p")[1].cast(LongType()).alias("dst"),
        )
    )
    edges_local = [(r["src"], r["dst"]) for r in edges_df.collect()]
    nodes_local = list(set([e[0] for e in edges_local] + [e[1] for e in edges_local]))

    # -- DFS Mode: community structure (q=0.5) --
    print("\n[2] Node2Vec with DFS bias (q=0.5, community structure)...")
    n2v = Node2Vec(
        edges=edges_local, nodes=nodes_local,
        p=1.0, q=0.5,
        walk_length=80, num_walks=10, seed=42
    )
    walks = n2v.generate_walks()

    print("\n[3] Training Word2Vec...")
    w2v_model = train_word2vec(
        walks, dimensions=128, window=10, epochs=5
    )
    w2v_model.save(MODEL_SAVE)
    print(f"  Model saved to: {MODEL_SAVE}")

    print("\n[4] Extracting embeddings...")
    node_ids, embeddings = extract_embeddings(w2v_model, nodes_local)

    print("\n[5] Clustering nodes (K-Means on embeddings)...")
    cluster_labels, best_k, sil_scores = cluster_nodes_kmeans(
        node_ids, embeddings, k_range=range(5, 20, 3)
    )

    print(f"\n  Cluster size distribution (k={best_k}):")
    cluster_sizes = collections.Counter(cluster_labels)
    for cid, size in sorted(cluster_sizes.items()):
        print(f"    Cluster {cid:>3}: {size:>5} nodes")

    print("\n[6] Computing t-SNE 2D projection...")
    tsne_coords = compute_tsne(embeddings, perplexity=40.0)

    print("\n[7] Similarity search (top-10 neighbors for node 0)...")
    similar = find_similar_nodes(w2v_model, query_node=0, top_k=10)
    print("  Nodes most similar to Node 0 in embedding space:")
    for node, score in similar:
        print(f"    Node {node:>5}: cosine_similarity={score:.4f}")

    print("\n[8] Saving embeddings to Spark/CSV...")
    save_embeddings_to_spark(
        spark, node_ids, embeddings, cluster_labels, tsne_coords,
        out_path=os.path.join(OUT_DIR, "node_embeddings")
    )

    spark.stop()
    print("\n  Done! Use t-SNE coordinates to visualize in gephi_exporter.py")
