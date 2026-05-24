"""
link_prediction.py
==================
ML-Based Link Prediction for Social Graph
Uses graph topology features + Spark MLlib to predict
which future connections are most likely.

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

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType, IntegerType
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import (
    RandomForestClassifier,
    GBTClassifier,
    LogisticRegression,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
import random
import collections
import math


# ---------------------------------------------
#  1. Graph Feature Engineering
# ---------------------------------------------
class GraphFeatureExtractor:
    """
    Extracts 8 link prediction features for node pairs.
    Operates on local (collected) graph for efficiency.
    """

    def __init__(self, edges: list, nodes: list, community: dict = None):
        self.nodes = nodes
        self.node_set = set(nodes)
        self.community = community or {}

        # Adjacency sets
        self.adj = {n: set() for n in nodes}
        for src, dst in edges:
            self.adj[src].add(dst)
            self.adj[dst].add(src)

        self.degree = {n: len(self.adj[n]) for n in nodes}
        self.N = len(nodes)

    def common_neighbors(self, u, v) -> int:
        return len(self.adj[u] & self.adj[v])

    def jaccard_coefficient(self, u, v) -> float:
        union = len(self.adj[u] | self.adj[v])
        return self.common_neighbors(u, v) / union if union > 0 else 0.0

    def adamic_adar(self, u, v) -> float:
        common = self.adj[u] & self.adj[v]
        score = sum(
            1.0 / math.log(self.degree[w] + 1e-9)
            for w in common if self.degree[w] > 1
        )
        return score

    def resource_allocation(self, u, v) -> float:
        common = self.adj[u] & self.adj[v]
        return sum(
            1.0 / self.degree[w]
            for w in common if self.degree[w] > 0
        )

    def preferential_attachment(self, u, v) -> float:
        return float(self.degree[u] * self.degree[v])

    def same_community(self, u, v) -> int:
        if not self.community:
            return 0
        return int(self.community.get(u, -1) == self.community.get(v, -2))

    def bfs_distance(self, u, v, max_hops: int = 4) -> float:
        """BFS shortest path distance (capped at max_hops, returns max+1 if unreachable)."""
        if u == v:
            return 0.0
        visited = {u}
        queue = collections.deque([(u, 0)])
        while queue:
            node, dist = queue.popleft()
            if dist >= max_hops:
                return float(max_hops + 1)
            for nb in self.adj[node]:
                if nb == v:
                    return float(dist + 1)
                if nb not in visited:
                    visited.add(nb)
                    queue.append((nb, dist + 1))
        return float(max_hops + 1)

    def katz_contribution(self, u, v, beta: float = 0.005) -> float:
        """
        Approximate Katz score (paths of length 2 and 3 only).
        Full Katz requires matrix inversion; this is a fast approximation.
        """
        # Length-2 paths: common neighbors
        l2 = len(self.adj[u] & self.adj[v])

        # Length-3 paths: nodes reachable in 2 hops from u that neighbor v
        l3 = 0
        for w in self.adj[u]:
            l3 += len(self.adj[w] & self.adj[v]) - (1 if v in self.adj[w] else 0)

        return beta * l2 + beta ** 2 * l3

    def extract_features(self, u, v) -> tuple:
        """Returns (cn, jaccard, aa, ra, pa, same_comm, dist, katz)"""
        return (
            float(self.common_neighbors(u, v)),
            self.jaccard_coefficient(u, v),
            self.adamic_adar(u, v),
            self.resource_allocation(u, v),
            self.preferential_attachment(u, v),
            float(self.same_community(u, v)),
            self.bfs_distance(u, v),
            self.katz_contribution(u, v),
        )


# ---------------------------------------------
#  2. Dataset Construction
# ---------------------------------------------
def build_link_prediction_dataset(
    extractor: GraphFeatureExtractor,
    edges: list,
    neg_ratio: float = 1.0,
    seed: int = 42,
) -> list:
    """
    Builds balanced positive/negative training dataset.
    Positive samples: existing edges (held-out 20%)
    Negative samples: random non-edges

    Returns list of (u, v, cn, jaccard, aa, ra, pa, same_comm, dist, katz, label)
    """
    random.seed(seed)
    edge_set = set()
    for src, dst in edges:
        edge_set.add((min(src, dst), max(src, dst)))

    nodes = extractor.nodes
    total_edges = len(edge_set)
    n_positive = int(total_edges * 0.20)  # 20% as positive test set

    # Sample positive edges
    pos_edges = random.sample(list(edge_set), n_positive)

    # Sample negative edges (non-existent pairs)
    neg_edges = []
    attempts = 0
    max_attempts = n_positive * 10
    while len(neg_edges) < int(n_positive * neg_ratio) and attempts < max_attempts:
        u = random.choice(nodes)
        v = random.choice(nodes)
        pair = (min(u, v), max(u, v))
        if u != v and pair not in edge_set:
            neg_edges.append((u, v))
        attempts += 1

    dataset = []
    for u, v in pos_edges:
        feats = extractor.extract_features(u, v)
        dataset.append((int(u), int(v)) + feats + (1,))

    for u, v in neg_edges:
        feats = extractor.extract_features(u, v)
        dataset.append((int(u), int(v)) + feats + (0,))

    random.shuffle(dataset)
    print(f"  Dataset: {len(pos_edges)} positive, {len(neg_edges)} negative samples")
    return dataset


# ---------------------------------------------
#  3. Spark ML Pipeline
# ---------------------------------------------
FEATURE_COLS = [
    "common_neighbors", "jaccard", "adamic_adar",
    "resource_allocation", "preferential_attachment",
    "same_community", "path_distance", "katz_score"
]

SCHEMA = StructType([
    StructField("src",                    LongType(),   False),
    StructField("dst",                    LongType(),   False),
    StructField("common_neighbors",        DoubleType(), False),
    StructField("jaccard",                 DoubleType(), False),
    StructField("adamic_adar",             DoubleType(), False),
    StructField("resource_allocation",     DoubleType(), False),
    StructField("preferential_attachment", DoubleType(), False),
    StructField("same_community",          DoubleType(), False),
    StructField("path_distance",           DoubleType(), False),
    StructField("katz_score",              DoubleType(), False),
    StructField("label",                   IntegerType(),False),
])


def train_and_evaluate(spark: SparkSession, dataset: list):
    """
    Trains 3 models, evaluates AUC-ROC, returns best model.
    """
    df = spark.createDataFrame(dataset, SCHEMA)
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    print(f"  Train: {train.count()}, Test: {test.count()}")

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
    scaler    = StandardScaler(inputCol="raw_features", outputCol="features",
                               withMean=True, withStd=True)

    evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    results = {}

    # -- Random Forest --------------------------
    rf = RandomForestClassifier(
        labelCol="label", featuresCol="features",
        numTrees=100, maxDepth=8, seed=42
    )
    rf_pipeline = Pipeline(stages=[assembler, scaler, rf])
    rf_model = rf_pipeline.fit(train)
    rf_preds = rf_model.transform(test)
    rf_auc = evaluator.evaluate(rf_preds)
    results["RandomForest"] = (rf_model, rf_preds, rf_auc)
    print(f"  RandomForest  AUC-ROC: {rf_auc:.4f}")

    # Feature importance
    rf_stage = rf_model.stages[-1]
    importances = rf_stage.featureImportances
    print("  Feature Importances (RF):")
    feat_imp = sorted(
        zip(FEATURE_COLS, importances.toArray()),
        key=lambda x: x[1], reverse=True
    )
    for fname, imp in feat_imp:
        print(f"    {fname:<30}: {imp:.4f}")

    # -- Gradient Boosted Trees -----------------
    gbt = GBTClassifier(
        labelCol="label", featuresCol="features",
        maxIter=50, maxDepth=5, seed=42
    )
    gbt_pipeline = Pipeline(stages=[assembler, scaler, gbt])
    gbt_model = gbt_pipeline.fit(train)
    gbt_preds = gbt_model.transform(test)
    gbt_auc = evaluator.evaluate(gbt_preds)
    results["GBT"] = (gbt_model, gbt_preds, gbt_auc)
    print(f"  GBT           AUC-ROC: {gbt_auc:.4f}")

    # -- Logistic Regression (Baseline) ---------
    lr = LogisticRegression(
        labelCol="label", featuresCol="features",
        maxIter=100, regParam=0.01
    )
    lr_pipeline = Pipeline(stages=[assembler, scaler, lr])
    lr_model = lr_pipeline.fit(train)
    lr_preds = lr_model.transform(test)
    lr_auc = evaluator.evaluate(lr_preds)
    results["LogisticRegression"] = (lr_model, lr_preds, lr_auc)
    print(f"  LogRegression AUC-ROC: {lr_auc:.4f}")

    # Best model
    best_name = max(results, key=lambda k: results[k][2])
    print(f"\n  Best Model: {best_name} (AUC={results[best_name][2]:.4f})")
    return results, best_name


# ---------------------------------------------
#  4. Predict Top-K Future Links
# ---------------------------------------------
def predict_future_links(
    spark: SparkSession,
    best_model,
    extractor: GraphFeatureExtractor,
    edges: list,
    top_k: int = 50,
    sample_candidates: int = 5000,
    seed: int = 42,
) -> "DataFrame":
    """
    Scores random non-edge candidate pairs and returns
    the top-K most likely future connections.
    """
    random.seed(seed)
    edge_set = {(min(s, d), max(s, d)) for s, d in edges}
    nodes = extractor.nodes

    candidates = []
    attempts = 0
    while len(candidates) < sample_candidates and attempts < sample_candidates * 5:
        u = random.choice(nodes)
        v = random.choice(nodes)
        pair = (min(u, v), max(u, v))
        if u != v and pair not in edge_set:
            candidates.append((u, v))
        attempts += 1

    rows = []
    for u, v in candidates:
        feats = extractor.extract_features(u, v)
        rows.append((int(u), int(v)) + feats + (0,))  # label=0 (unknown)

    candidate_df = spark.createDataFrame(rows, SCHEMA)
    predictions = best_model.transform(candidate_df)

    # Extract probability of class 1
    get_prob = F.udf(lambda v: float(v[1]), DoubleType())
    result = (
        predictions
        .withColumn("link_probability", get_prob(F.col("probability")))
        .select("src", "dst",
                "common_neighbors", "jaccard", "adamic_adar",
                "link_probability")
        .orderBy(F.col("link_probability").desc())
        .limit(top_k)
    )
    return result


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
    COMM_PATH  = os.path.join(BASE_DIR, "outputs", "communities", "community_assignment")
    OUT_DIR    = os.path.join(BASE_DIR, "outputs", "link_prediction")
    MODEL_SAVE = os.path.join(BASE_DIR, "models", "link_pred_rf")
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(os.path.join(os.path.dirname(BASE_DIR), "models"), exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("LinkPrediction")
        .master("local[*]")
        .config("spark.jars.packages",
                "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("  LINK PREDICTION ENGINE")
    print(f"  Dataset: {os.path.basename(DATA_PATH)}")
    print("=" * 60)

    # -- Load graph -----------------------------
    print("\n[1] Loading graph...")
    raw = spark.read.text(DATA_PATH)
    edges_df = (
        raw.select(F.split("value", r"\s+").alias("p"))
        .select(
            F.col("p")[0].cast(LongType()).alias("src"),
            F.col("p")[1].cast(LongType()).alias("dst"),
        )
    )
    edges_local = [(row["src"], row["dst"]) for row in edges_df.collect()]
    nodes_local = list(set(
        [e[0] for e in edges_local] + [e[1] for e in edges_local]
    ))
    print(f"  Nodes: {len(nodes_local)}, Edges: {len(edges_local)}")

    # -- Load community assignment --------------
    print("\n[2] Loading community labels...")
    try:
        comm_df = spark.read.csv(COMM_PATH, header=True, inferSchema=True)
        community = {row["node_id"]: row["community_id"]
                     for row in comm_df.collect()}
        print(f"  Loaded communities for {len(community)} nodes")
    except Exception:
        community = {}
        print("  Community data not found; skipping community feature")

    # -- Feature Extraction ---------------------
    print("\n[3] Extracting graph features...")
    extractor = GraphFeatureExtractor(edges_local, nodes_local, community)

    # -- Build Dataset --------------------------
    print("\n[4] Building training dataset...")
    dataset = build_link_prediction_dataset(extractor, edges_local, neg_ratio=1.0)

    # -- Train & Evaluate -----------------------
    print("\n[5] Training ML models...")
    results, best_name = train_and_evaluate(spark, dataset)

    # -- Predict Future Links -------------------
    print("\n[6] Predicting top-50 future links...")
    best_model = results[best_name][0]
    future_links = predict_future_links(
        spark, best_model, extractor, edges_local,
        top_k=50, sample_candidates=5000
    )
    print("  Top 20 predicted future links:")
    future_links.show(20)

    # -- Save -----------------------------------
    future_links.coalesce(1).write.mode("overwrite").csv(
        os.path.join(OUT_DIR, "predicted_links"), header=True
    )
    best_model.write().overwrite().save(MODEL_SAVE)
    print(f"\n  Outputs saved to: {OUT_DIR}")
    print(f"  Model saved  to: {MODEL_SAVE}")

    spark.stop()
