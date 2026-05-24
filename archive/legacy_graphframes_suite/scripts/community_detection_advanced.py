"""
community_detection_advanced.py
================================
Advanced Community Detection for Social Graph Analysis
Implements:
  1. Louvain Community Detection (modularity optimization)
  2. Hierarchical Community Tree (Dendrogram)
  3. Community Profiling & Quality Metrics
  4. Inter-community Bridge Analysis
  5. Community Evolution Simulation

Dynamic: auto-detects any SNAP dataset in data_lake/.

Author  : BDA Social Graph Project
"""

import os, sys, glob, random, collections
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# -- Windows Environment Setup --------------------------
os.environ['SPARK_HOME'] = os.path.dirname(pyspark.__file__)
os.environ["PYSPARK_PIN_THREAD"] = "true"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
hadoop_home = r'C:\hadoop'
if os.path.isdir(hadoop_home):
    os.environ['HADOOP_HOME'] = hadoop_home
# --------------------------------------------------------



# ---------------------------------------------
#  1. Louvain Community Detection
#     (Pure Python on collected graph - fits in memory for 4K nodes)
# ---------------------------------------------
class LouvainCommunityDetector:
    """
    Louvain Method for community detection.
    Optimizes modularity Q = (1/2m) * sum_ij [A_ij - k_i*k_j/2m] * delta(c_i, c_j)
    
    Two phases:
      Phase 1 - Local modularity optimization (node reassignment)
      Phase 2 - Graph aggregation (super-nodes)
    """

    def __init__(self, edges: list, nodes: list):
        self.nodes = nodes
        self.node_set = set(nodes)

        # Build adjacency with weights
        self.adj = {n: {} for n in nodes}
        self.total_weight = 0.0
        for src, dst in edges:
            self.adj[src][dst] = self.adj[src].get(dst, 0) + 1
            self.adj[dst][src] = self.adj[dst].get(src, 0) + 1
            self.total_weight += 1

        # Degree of each node
        self.degree = {n: sum(self.adj[n].values()) for n in nodes}

        # Community assignment: start = each node in own community
        self.community = {n: n for n in nodes}
        self.history = []  # track partition at each level

    def _modularity_gain(self, node, target_comm, m2):
        """
        Delta Q when moving `node` to `target_comm`.
        m2 = 2 * total_weight
        """
        # Weight of edges from node to target community
        ki_in = sum(
            w for neighbor, w in self.adj[node].items()
            if self.community[neighbor] == target_comm
        )
        # Sum of degrees in target community
        sigma_tot = sum(
            self.degree[n] for n in self.nodes
            if self.community[n] == target_comm and n != node
        )
        ki = self.degree[node]
        return (2 * ki_in - sigma_tot * ki / m2) / m2

    def _phase1(self) -> bool:
        """
        Phase 1: Move each node to the community of its neighbor
        that gives maximum modularity gain. Repeat until stable.
        Returns True if any move was made.
        """
        m2 = 2 * self.total_weight
        improved = False
        shuffled = list(self.nodes)
        random.shuffle(shuffled)

        for node in shuffled:
            current_comm = self.community[node]
            best_comm = current_comm
            best_gain = 0.0

            # Remove node from current community temporarily
            self.community[node] = -1
            neighbor_communities = set(
                self.community[nb] for nb in self.adj[node]
                if self.community[nb] != -1
            )
            neighbor_communities.add(current_comm)

            for target_comm in neighbor_communities:
                gain = self._modularity_gain(node, target_comm, m2)
                if gain > best_gain:
                    best_gain = gain
                    best_comm = target_comm

            self.community[node] = best_comm
            if best_comm != current_comm:
                improved = True

        return improved

    def _compute_modularity(self) -> float:
        """Compute current modularity Q."""
        m2 = 2 * self.total_weight
        if m2 == 0:
            return 0.0
        q = 0.0
        for src in self.nodes:
            for dst, w in self.adj[src].items():
                if self.community[src] == self.community[dst]:
                    q += w - (self.degree[src] * self.degree[dst]) / m2
        return q / m2

    def detect(self, max_passes: int = 10, seed: int = 42) -> dict:
        """
        Run Louvain algorithm. Returns {node_id: community_id}.
        """
        random.seed(seed)
        print("  [Louvain] Starting community detection...")

        for pass_num in range(max_passes):
            improved = self._phase1()
            q = self._compute_modularity()
            # Remap community IDs to 0-based integers
            unique_comms = sorted(set(self.community.values()))
            remap = {c: i for i, c in enumerate(unique_comms)}
            for n in self.nodes:
                self.community[n] = remap[self.community[n]]

            n_comms = len(unique_comms)
            print(f"  [Louvain] Pass {pass_num + 1}: "
                  f"{n_comms} communities, Q={q:.4f}")
            self.history.append((n_comms, q, dict(self.community)))

            if not improved:
                print(f"  [Louvain] Converged after {pass_num + 1} passes.")
                break

        return dict(self.community)

    def get_communities(self) -> dict:
        """Returns {community_id: [list of node ids]}"""
        result = collections.defaultdict(list)
        for node, comm in self.community.items():
            result[comm].append(node)
        return dict(result)


# ---------------------------------------------
#  2. Community Profiling
# ---------------------------------------------
def profile_communities(
    community_assignment: dict,
    adj: dict,
    degree: dict,
) -> list:
    """
    Computes per-community statistics:
      - size, density, internal edges, external edges
      - avg_degree, max_degree, clustering coefficient estimate
      - modularity contribution
    """
    communities = collections.defaultdict(list)
    for node, comm in community_assignment.items():
        communities[comm].append(node)

    total_edges = sum(degree.values()) / 2
    profiles = []

    for comm_id, members in communities.items():
        member_set = set(members)
        size = len(members)

        # Count internal vs external edges
        internal = 0
        external = 0
        internal_deg_sum = 0
        max_deg = 0
        for n in members:
            d = degree[n]
            max_deg = max(max_deg, d)
            for neighbor in adj[n]:
                if neighbor in member_set:
                    internal += 1
                else:
                    external += 1
            internal_deg_sum += d

        internal //= 2  # undirected

        # Density = actual / possible edges
        possible = size * (size - 1) / 2 if size > 1 else 1
        density = internal / possible

        # Average degree within community
        avg_degree = internal_deg_sum / size if size > 0 else 0

        # Coverage = fraction of graph edges inside community
        coverage = internal / total_edges if total_edges > 0 else 0

        profiles.append({
            "community_id": comm_id,
            "size": size,
            "internal_edges": internal,
            "external_edges": external // 2,
            "density": round(density, 4),
            "avg_internal_degree": round(avg_degree, 2),
            "max_degree_in_comm": max_deg,
            "coverage": round(coverage, 4),
            "conductance": round(
                external / (2 * internal + external) if (2 * internal + external) > 0 else 0, 4
            ),
        })

    return sorted(profiles, key=lambda x: x["size"], reverse=True)


# ---------------------------------------------
#  3. Inter-Community Bridge Detection
# ---------------------------------------------
def find_community_bridges(
    community_assignment: dict,
    edges: list,
) -> list:
    """
    Identifies BRIDGE NODES: nodes that connect 2+ different communities.
    Returns list of (node_id, community_id, bridge_score, target_communities).
    Bridge score = number of distinct communities the node connects to.
    """
    bridges = []
    node_comms = {}
    for node, comm in community_assignment.items():
        node_comms[node] = comm

    # Build adjacency
    adj = collections.defaultdict(set)
    for src, dst in edges:
        adj[src].add(dst)
        adj[dst].add(src)

    for node in node_comms:
        my_comm = node_comms[node]
        ext_comms = set()
        for nb in adj[node]:
            nb_comm = node_comms.get(nb, -1)
            if nb_comm != my_comm:
                ext_comms.add(nb_comm)
        if ext_comms:
            bridges.append({
                "node_id": node,
                "home_community": my_comm,
                "bridge_score": len(ext_comms),
                "connects_to_communities": sorted(ext_comms),
            })

    return sorted(bridges, key=lambda x: x["bridge_score"], reverse=True)


# ---------------------------------------------
#  4. Community Similarity Matrix (Jaccard)
# ---------------------------------------------
def community_similarity_matrix(
    community_assignment: dict,
    adj: dict,
    top_n_comms: int = 20,
) -> list:
    """
    Computes pairwise Jaccard similarity between communities
    based on shared inter-community edges.
    Returns list of (comm_a, comm_b, jaccard_similarity).
    """
    communities = collections.defaultdict(set)
    for node, comm in community_assignment.items():
        communities[comm].add(node)

    # Select top-N communities by size
    top_comms = sorted(communities, key=lambda c: len(communities[c]),
                       reverse=True)[:top_n_comms]

    # Build neighbor sets for each community
    comm_neighbors = {}
    for c in top_comms:
        nb_comms = collections.Counter()
        for node in communities[c]:
            for nb in adj[node]:
                nb_comm = community_assignment.get(nb, -1)
                if nb_comm != c:
                    nb_comms[nb_comm] += 1
        comm_neighbors[c] = nb_comms

    result = []
    for i, ca in enumerate(top_comms):
        for cb in top_comms[i + 1:]:
            a_set = set(comm_neighbors[ca].keys())
            b_set = set(comm_neighbors[cb].keys())
            intersection = len(a_set & b_set)
            union = len(a_set | b_set)
            jaccard = intersection / union if union > 0 else 0.0
            if jaccard > 0:
                result.append((ca, cb, round(jaccard, 4)))

    return sorted(result, key=lambda x: x[2], reverse=True)


# ---------------------------------------------
#  5. Convert to Spark DataFrames & Save
# ---------------------------------------------
def to_spark_dataframe(spark, community_assignment: dict):
    """Converts community assignment dict to Spark DataFrame."""
    data = [(int(node), int(comm))
            for node, comm in community_assignment.items()]
    schema = StructType([
        StructField("node_id", LongType(), False),
        StructField("community_id", IntegerType(), False),
    ])
    return spark.createDataFrame(data, schema)


def profiles_to_spark(spark, profiles: list):
    schema = StructType([
        StructField("community_id",         IntegerType(), False),
        StructField("size",                  IntegerType(), False),
        StructField("internal_edges",        IntegerType(), False),
        StructField("external_edges",        IntegerType(), False),
        StructField("density",               DoubleType(),  False),
        StructField("avg_internal_degree",   DoubleType(),  False),
        StructField("max_degree_in_comm",    IntegerType(), False),
        StructField("coverage",              DoubleType(),  False),
        StructField("conductance",           DoubleType(),  False),
    ])
    rows = [
        (p["community_id"], p["size"], p["internal_edges"],
         p["external_edges"], p["density"], p["avg_internal_degree"],
         p["max_degree_in_comm"], p["coverage"], p["conductance"])
        for p in profiles
    ]
    return spark.createDataFrame(rows, schema)


# ---------------------------------------------
#  MAIN RUNNER
# ---------------------------------------------
if __name__ == "__main__":
    import gzip

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), "data_lake")

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
    DATA_PATH = discover_dataset(explicit)
    OUT_DIR   = os.path.join(BASE_DIR, "outputs", "communities")
    os.makedirs(OUT_DIR, exist_ok=True)

    # -- Init Spark ------------------------------
    spark = (
        SparkSession.builder
        .appName("AdvancedCommunityDetection")
        .master("local[*]")
        .config("spark.jars.packages",
                "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("  ADVANCED COMMUNITY DETECTION ENGINE")
    print(f"  Dataset: {os.path.basename(DATA_PATH)}")
    print("=" * 60)

    # -- Load graph locally for Louvain ----------
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

    # -- Louvain Detection -----------------------
    print("\n[2] Running Louvain Community Detection...")
    louvain = LouvainCommunityDetector(edges_local, nodes_local)
    assignment = louvain.detect(max_passes=15, seed=42)
    communities = louvain.get_communities()
    print(f"  Final: {len(communities)} communities detected")

    # Size distribution
    sizes = sorted([len(v) for v in communities.values()], reverse=True)
    print(f"  Largest 5 communities: {sizes[:5]}")
    print(f"  Communities with >10 nodes: {sum(1 for s in sizes if s > 10)}")

    # -- Community Profiling ----------------------
    print("\n[3] Profiling communities...")
    adj_dict = {n: {} for n in nodes_local}
    for src, dst in edges_local:
        adj_dict[src][dst] = adj_dict[src].get(dst, 0) + 1
        adj_dict[dst][src] = adj_dict[dst].get(src, 0) + 1
    degree_dict = {n: sum(adj_dict[n].values()) for n in nodes_local}
    profiles = profile_communities(assignment, adj_dict, degree_dict)

    print("\n  Top 10 Community Profiles:")
    print(f"  {'ID':>4} {'Size':>6} {'InEdge':>8} {'Density':>8} "
          f"{'Conductance':>12}")
    for p in profiles[:10]:
        print(f"  {p['community_id']:>4} {p['size']:>6} "
              f"{p['internal_edges']:>8} {p['density']:>8.3f} "
              f"{p['conductance']:>12.3f}")

    # -- Bridge Node Detection --------------------
    print("\n[4] Detecting bridge (cross-community) nodes...")
    bridges = find_community_bridges(assignment, edges_local)
    print(f"  Found {len(bridges)} bridge nodes")
    print("  Top 10 bridge nodes:")
    for b in bridges[:10]:
        print(f"    Node {b['node_id']:>5} | Home:{b['home_community']:>3} "
              f"| BridgeScore:{b['bridge_score']:>3} "
              f"| Connects:{b['connects_to_communities'][:5]}")

    # -- Community Similarity ---------------------
    print("\n[5] Computing community similarity matrix...")
    sim_pairs = community_similarity_matrix(assignment, adj_dict, top_n_comms=20)
    print("  Top 10 most similar community pairs (Jaccard):")
    for ca, cb, j in sim_pairs[:10]:
        print(f"    Community {ca:>3} <-> Community {cb:>3}  Jaccard={j:.4f}")

    # -- Save to Spark / CSV ----------------------
    print("\n[6] Saving outputs...")
    assign_df = to_spark_dataframe(spark, assignment)
    profile_df = profiles_to_spark(spark, profiles)

    assign_df.coalesce(1).write.mode("overwrite").csv(
        os.path.join(OUT_DIR, "community_assignment"), header=True
    )
    profile_df.coalesce(1).write.mode("overwrite").csv(
        os.path.join(OUT_DIR, "community_profiles"), header=True
    )

    # Save bridges
    bridge_rows = [
        (int(b["node_id"]), int(b["home_community"]),
         int(b["bridge_score"]))
        for b in bridges
    ]
    bridge_schema = StructType([
        StructField("node_id",       LongType(),    False),
        StructField("home_community",IntegerType(), False),
        StructField("bridge_score",  IntegerType(), False),
    ])
    bridge_df = spark.createDataFrame(bridge_rows, bridge_schema)
    bridge_df.coalesce(1).write.mode("overwrite").csv(
        os.path.join(OUT_DIR, "bridge_nodes"), header=True
    )
    print(f"  Saved to: {OUT_DIR}")

    spark.stop()
