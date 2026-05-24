"""
advanced_centrality.py
======================
Advanced Graph Centrality & Metrics Engine
Extends base GraphFrames with Betweenness, Closeness,
Eigenvector Centrality, K-Core Decomposition, and
Structural Hole Analysis using Apache Spark.

Dynamic: auto-detects any SNAP dataset in data_lake/.

Author  : BDA Social Graph Project
"""

import os, sys, glob, collections
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
from graphframes import GraphFrame
import math

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), "data_lake")


# ---------------------------------------------
#  Spark Session Factory
# ---------------------------------------------
def get_spark(app_name: str = "AdvancedCentrality") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages",
                "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def discover_dataset(explicit_path=None):
    """Auto-detect SNAP edge-list dataset in data_lake/."""
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


# ---------------------------------------------
#  Load Facebook SNAP Graph
# ---------------------------------------------
def load_facebook_graph(spark: SparkSession, path: str) -> GraphFrame:
    """
    Load the Facebook combined edge list into a GraphFrame.
    Each line: src_id  dst_id (space separated).
    Returns an UNDIRECTED GraphFrame (edges added in both directions).
    """
    raw = spark.read.text(path)
    edges_df = (
        raw.select(
            F.split(F.col("value"), r"\s+").alias("parts")
        )
        .select(
            F.col("parts")[0].cast(LongType()).alias("src"),
            F.col("parts")[1].cast(LongType()).alias("dst"),
        )
        .filter(F.col("src").isNotNull())
    )

    # Undirected: add reverse edges
    reverse = edges_df.select(
        F.col("dst").alias("src"),
        F.col("src").alias("dst"),
    )
    all_edges = edges_df.union(reverse).distinct()

    # Vertices: all unique node IDs
    v1 = all_edges.select(F.col("src").alias("id"))
    v2 = all_edges.select(F.col("dst").alias("id"))
    vertices = v1.union(v2).distinct()

    return GraphFrame(vertices, all_edges)


# ---------------------------------------------
#  1. Degree Centrality (Normalized)
# ---------------------------------------------
def degree_centrality(g: GraphFrame) -> "DataFrame":
    """
    Normalized Degree Centrality = degree(v) / (N-1)
    Also returns in-degree and out-degree.
    """
    N = g.vertices.count()
    degrees = g.degrees.withColumnRenamed("degree", "raw_degree")
    return degrees.withColumn(
        "degree_centrality", F.col("raw_degree") / (N - 1)
    ).orderBy(F.col("degree_centrality").desc())


# ---------------------------------------------
#  2. Approximate Betweenness Centrality
#     (Brandes approximation via sampled BFS)
# ---------------------------------------------
def approx_betweenness_centrality(
    g: GraphFrame,
    sample_fraction: float = 0.05,
    seed: int = 42,
) -> "DataFrame":
    """
    Approximate betweenness centrality using sampled BFS.
    Full Brandes on 4K nodes in pure Spark is expensive;
    we sample `sample_fraction` of source nodes and extrapolate.

    Returns DataFrame(id, betweenness_score).
    """
    spark = SparkSession.getActiveSession()

    # Collect edge list for BFS (fits in memory for 4K nodes)
    edges_local = g.edges.select("src", "dst").collect()
    nodes_local = [row["id"] for row in g.vertices.collect()]
    N = len(nodes_local)
    node_index = {n: i for i, n in enumerate(nodes_local)}

    # Build adjacency list
    adj = {n: [] for n in nodes_local}
    for row in edges_local:
        adj[row["src"]].append(row["dst"])

    import random, collections
    random.seed(seed)
    sample_size = max(1, int(N * sample_fraction))
    sources = random.sample(nodes_local, sample_size)

    betweenness = {n: 0.0 for n in nodes_local}

    def bfs_betweenness(s):
        # Standard Brandes BFS from source s
        stack = []
        pred = {n: [] for n in nodes_local}
        sigma = {n: 0 for n in nodes_local}
        sigma[s] = 1
        dist = {n: -1 for n in nodes_local}
        dist[s] = 0
        queue = collections.deque([s])

        while queue:
            v = queue.popleft()
            stack.append(v)
            for w in adj[v]:
                if dist[w] < 0:
                    queue.append(w)
                    dist[w] = dist[v] + 1
                if dist[w] == dist[v] + 1:
                    sigma[w] += sigma[v]
                    pred[w].append(v)

        delta = {n: 0.0 for n in nodes_local}
        while stack:
            w = stack.pop()
            for v in pred[w]:
                if sigma[w] != 0:
                    delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])
            if w != s:
                betweenness[w] += delta[w]

    for s in sources:
        bfs_betweenness(s)

    # Extrapolate: scale by N / sample_size, normalize by (N-1)(N-2)
    scale = (N / sample_size) / ((N - 1) * (N - 2))
    result = [
        (int(n), float(betweenness[n] * scale))
        for n in nodes_local
    ]

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("betweenness_centrality", DoubleType(), False),
    ])
    return (
        spark.createDataFrame(result, schema)
        .orderBy(F.col("betweenness_centrality").desc())
    )


# ---------------------------------------------
#  3. Closeness Centrality (Sampled BFS)
# ---------------------------------------------
def approx_closeness_centrality(
    g: GraphFrame,
    sample_fraction: float = 0.10,
    seed: int = 42,
) -> "DataFrame":
    """
    Closeness Centrality = (N-1) / sum_of_shortest_path_distances.
    Approximated via sampled BFS sources.
    """
    spark = SparkSession.getActiveSession()
    edges_local = g.edges.select("src", "dst").collect()
    nodes_local = [row["id"] for row in g.vertices.collect()]
    N = len(nodes_local)

    adj = {n: [] for n in nodes_local}
    for row in edges_local:
        adj[row["src"]].append(row["dst"])

    import random, collections
    random.seed(seed)
    sample_size = max(1, int(N * sample_fraction))
    sources = set(random.sample(nodes_local, sample_size))

    total_dist = {n: 0 for n in nodes_local}
    reach_count = {n: 0 for n in nodes_local}

    for s in sources:
        dist = {n: -1 for n in nodes_local}
        dist[s] = 0
        queue = collections.deque([s])
        while queue:
            v = queue.popleft()
            for w in adj[v]:
                if dist[w] < 0:
                    dist[w] = dist[v] + 1
                    queue.append(w)
        for n in nodes_local:
            if dist[n] > 0:
                total_dist[n] += dist[n]
                reach_count[n] += 1

    result = []
    for n in nodes_local:
        if total_dist[n] > 0:
            # Wasserman–Faust normalization
            cc = (reach_count[n] ** 2) / ((N - 1) * total_dist[n])
        else:
            cc = 0.0
        result.append((int(n), float(cc)))

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("closeness_centrality", DoubleType(), False),
    ])
    return (
        spark.createDataFrame(result, schema)
        .orderBy(F.col("closeness_centrality").desc())
    )


# ---------------------------------------------
#  4. Eigenvector Centrality (Power Iteration)
# ---------------------------------------------
def eigenvector_centrality(
    g: GraphFrame,
    max_iter: int = 100,
    tol: float = 1e-6,
) -> "DataFrame":
    """
    Eigenvector Centrality via power iteration on Spark.
    Uses Pregel-like message passing via GraphFrame aggregateMessages.
    Converges in ~30-50 iterations for Facebook graph.
    """
    spark = SparkSession.getActiveSession()
    N = g.vertices.count()

    # Initialize scores = 1/N
    scores = g.vertices.withColumn("score", F.lit(1.0 / N))

    edges = g.edges.cache()

    for iteration in range(max_iter):
        # Propagate: new_score(v) = sum of scores of neighbors
        new_scores = (
            edges.join(scores.withColumnRenamed("id", "src")
                              .withColumnRenamed("score", "src_score"),
                       on="src", how="left")
            .groupBy("dst")
            .agg(F.sum("src_score").alias("new_score"))
            .withColumnRenamed("dst", "id")
        )

        # Add isolated nodes with zero
        new_scores = (
            scores.select("id")
            .join(new_scores, on="id", how="left")
            .fillna(0.0, subset=["new_score"])
        )

        # Normalize L2
        norm = new_scores.agg(
            F.sqrt(F.sum(F.col("new_score") ** 2)).alias("l2")
        ).first()["l2"]

        if norm == 0:
            break

        new_scores = new_scores.withColumn(
            "score", F.col("new_score") / norm
        ).select("id", "score")

        # Check convergence
        delta = (
            scores.join(new_scores.withColumnRenamed("score", "new_s"), on="id")
            .agg(F.max(F.abs(F.col("score") - F.col("new_s"))).alias("delta"))
            .first()["delta"]
        )

        scores = new_scores

        if delta < tol:
            print(f"  [EVC] Converged at iteration {iteration + 1}, delta={delta:.2e}")
            break

    return scores.withColumnRenamed("score", "eigenvector_centrality") \
                 .orderBy(F.col("eigenvector_centrality").desc())


# ---------------------------------------------
#  5. K-Core Decomposition
# ---------------------------------------------
def k_core_decomposition(g: GraphFrame) -> "DataFrame":
    """
    Computes the coreness (k-shell) of each vertex.
    Iteratively removes vertices with degree < k.
    Returns DataFrame(id, coreness).
    """
    spark = SparkSession.getActiveSession()
    edges_local = g.edges.select("src", "dst").collect()
    nodes_local = [row["id"] for row in g.vertices.collect()]

    # Build degree dict
    degree = {n: 0 for n in nodes_local}
    adj = {n: set() for n in nodes_local}
    for row in edges_local:
        adj[row["src"]].add(row["dst"])
        degree[row["src"]] += 1

    coreness = {}
    removed = set()
    k = 1

    while len(removed) < len(nodes_local):
        changed = True
        while changed:
            changed = False
            for n in nodes_local:
                if n not in removed and degree[n] < k:
                    coreness[n] = k - 1
                    removed.add(n)
                    for neighbor in adj[n]:
                        if neighbor not in removed:
                            degree[neighbor] -= 1
                    changed = True
        k += 1

    result = [(int(n), int(coreness.get(n, k - 1))) for n in nodes_local]
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("coreness", IntegerType(), False),
    ])
    return (
        spark.createDataFrame(result, schema)
        .orderBy(F.col("coreness").desc())
    )


# ---------------------------------------------
#  6. Structural Holes (Burt's Constraint)
# ---------------------------------------------
def structural_holes_constraint(g: GraphFrame) -> "DataFrame":
    """
    Burt's Constraint C(i) measures how constrained a node is
    by its network neighborhood. Low constraint = bridges many
    communities (structural hole spanner = influencer/broker).

    C(i) = sum_j [ p_ij + sum_q p_iq * p_qj ]^2
    where p_ij = proportion of i's edges going to j.
    """
    spark = SparkSession.getActiveSession()
    edges_local = g.edges.select("src", "dst").collect()
    nodes_local = [row["id"] for row in g.vertices.collect()]

    adj = {n: set() for n in nodes_local}
    for row in edges_local:
        adj[row["src"]].add(row["dst"])

    result = []
    for i in nodes_local:
        neighbors = list(adj[i])
        deg_i = len(neighbors)
        if deg_i == 0:
            result.append((int(i), 1.0))
            continue

        # p_ij = 1 / deg_i (unweighted)
        p = {j: 1.0 / deg_i for j in neighbors}

        constraint = 0.0
        for j in neighbors:
            # shared neighbors between i and j
            mutual = adj[i].intersection(adj[j])
            indirect = sum(p.get(q, 0) * (1.0 / max(len(adj[q]), 1))
                           for q in mutual if q != i and q != j)
            constraint += (p[j] + indirect) ** 2

        result.append((int(i), float(constraint)))

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("burt_constraint", DoubleType(), False),
    ])
    # Low constraint = structural hole = influential broker
    return (
        spark.createDataFrame(result, schema)
        .withColumn(
            "broker_score", F.lit(1.0) - F.col("burt_constraint")
        )
        .orderBy(F.col("broker_score").desc())
    )


# ---------------------------------------------
#  7. Unified Influence Score
# ---------------------------------------------
def unified_influence_score(
    degree_df, betweenness_df, closeness_df, evc_df, coreness_df
) -> "DataFrame":
    """
    Combines all centrality measures into a single
    composite Influence Score using min-max normalization
    and equal weighting (can be tuned).

    Score = 0.25*deg + 0.25*btw + 0.2*cls + 0.15*evc + 0.15*core
    """
    def minmax_norm(df, col_name):
        stats = df.agg(
            F.min(col_name).alias("mn"),
            F.max(col_name).alias("mx")
        ).first()
        mn, mx = stats["mn"], stats["mx"]
        rng = mx - mn if mx != mn else 1.0
        return df.withColumn(
            col_name + "_norm",
            (F.col(col_name) - mn) / rng
        )

    d = minmax_norm(degree_df.select("id", "degree_centrality"), "degree_centrality")
    b = minmax_norm(betweenness_df.select("id", "betweenness_centrality"), "betweenness_centrality")
    c = minmax_norm(closeness_df.select("id", "closeness_centrality"), "closeness_centrality")
    e = minmax_norm(evc_df.select("id", "eigenvector_centrality"), "eigenvector_centrality")
    k = minmax_norm(coreness_df.select("id", F.col("coreness").cast(DoubleType())), "coreness")

    combined = (
        d.join(b, "id").join(c, "id").join(e, "id").join(k, "id")
        .withColumn(
            "influence_score",
            0.25 * F.col("degree_centrality_norm") +
            0.25 * F.col("betweenness_centrality_norm") +
            0.20 * F.col("closeness_centrality_norm") +
            0.15 * F.col("eigenvector_centrality_norm") +
            0.15 * F.col("coreness_norm")
        )
        .select(
            "id",
            "degree_centrality_norm",
            "betweenness_centrality_norm",
            "closeness_centrality_norm",
            "eigenvector_centrality_norm",
            "coreness_norm",
            "influence_score",
        )
        .orderBy(F.col("influence_score").desc())
    )
    return combined


# ---------------------------------------------
#  MAIN RUNNER
# ---------------------------------------------
if __name__ == "__main__":
    explicit = sys.argv[1] if len(sys.argv) > 1 else None
    if explicit and not os.path.isabs(explicit):
        explicit = os.path.join(BASE_DIR, explicit)
    DATA_PATH = discover_dataset(explicit)
    OUT_DIR   = os.path.join(BASE_DIR, "outputs", "centrality")
    os.makedirs(OUT_DIR, exist_ok=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("  ADVANCED CENTRALITY ENGINE")
    print(f"  Dataset: {os.path.basename(DATA_PATH)}")
    print("=" * 60)

    g = load_facebook_graph(spark, DATA_PATH)
    print(f"Graph loaded: {g.vertices.count()} nodes, "
          f"{g.edges.count() // 2} undirected edges\n")

    print("[1/6] Degree Centrality ...")
    deg_df = degree_centrality(g)
    deg_df.show(10)

    print("[2/6] Approximate Betweenness Centrality (5% sample) ...")
    btw_df = approx_betweenness_centrality(g, sample_fraction=0.05)
    btw_df.show(10)

    print("[3/6] Approximate Closeness Centrality (10% sample) ...")
    cls_df = approx_closeness_centrality(g, sample_fraction=0.10)
    cls_df.show(10)

    print("[4/6] Eigenvector Centrality (power iteration) ...")
    evc_df = eigenvector_centrality(g, max_iter=100)
    evc_df.show(10)

    print("[5/6] K-Core Decomposition ...")
    core_df = k_core_decomposition(g)
    core_df.show(10)
    print("Core distribution:")
    core_df.groupBy("coreness").count().orderBy("coreness").show(20)

    print("[6/6] Structural Holes (Burt's Constraint) ...")
    struct_df = structural_holes_constraint(g)
    struct_df.show(10)

    print("\n[FINAL] Unified Influence Score ...")
    influence_df = unified_influence_score(deg_df, btw_df, cls_df, evc_df, core_df)
    influence_df.show(20)

    # Save outputs
    influence_df.coalesce(1).write.mode("overwrite").csv(
        os.path.join(OUT_DIR, "influence_scores"), header=True
    )
    core_df.coalesce(1).write.mode("overwrite").csv(
        os.path.join(OUT_DIR, "k_core"), header=True
    )
    struct_df.coalesce(1).write.mode("overwrite").csv(
        os.path.join(OUT_DIR, "structural_holes"), header=True
    )
    print(f"\nOutputs saved to: {OUT_DIR}")
    spark.stop()
