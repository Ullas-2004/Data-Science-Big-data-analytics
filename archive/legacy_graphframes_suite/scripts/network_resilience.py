"""
network_resilience.py
=====================
Network Resilience & Robustness Analysis
Simulates targeted and random attacks to measure
how the social graph degrades under failures.

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
    StructType, StructField, LongType, DoubleType, IntegerType, StringType
)
import random
import collections
import math


# ---------------------------------------------
#  Graph Utility Functions
# ---------------------------------------------
def build_adj(nodes, edges):
    adj = {n: set() for n in nodes}
    for src, dst in edges:
        if src in adj and dst in adj:
            adj[src].add(dst)
            adj[dst].add(src)
    return adj


def giant_component_size(adj: dict, active: set) -> int:
    """BFS to find largest connected component among active nodes."""
    visited = set()
    max_size = 0
    for start in active:
        if start not in visited:
            size = 0
            queue = collections.deque([start])
            visited.add(start)
            while queue:
                node = queue.popleft()
                size += 1
                for nb in adj[node]:
                    if nb in active and nb not in visited:
                        visited.add(nb)
                        queue.append(nb)
            max_size = max(max_size, size)
    return max_size


def global_efficiency(adj: dict, active: set, sample: int = 200) -> float:
    """
    Global efficiency = avg(1/d_ij) for all pairs (i,j).
    Sampled for speed.
    """
    active_list = list(active)
    if len(active_list) < 2:
        return 0.0

    random.seed(99)
    sample_nodes = random.sample(
        active_list, min(sample, len(active_list))
    )

    total = 0.0
    count = 0
    for src in sample_nodes:
        # BFS from src
        dist = {src: 0}
        queue = collections.deque([src])
        while queue:
            v = queue.popleft()
            for nb in adj[v]:
                if nb in active and nb not in dist:
                    dist[nb] = dist[v] + 1
                    queue.append(nb)
        for dst in active_list:
            if dst != src and dst in dist:
                total += 1.0 / dist[dst]
                count += 1

    return total / count if count > 0 else 0.0


def avg_clustering(adj: dict, active: set, sample: int = 300) -> float:
    """Estimate average clustering coefficient."""
    active_list = list(active)
    if len(active_list) < 3:
        return 0.0

    random.seed(99)
    sample_nodes = random.sample(
        active_list, min(sample, len(active_list))
    )

    total_cc = 0.0
    for node in sample_nodes:
        neighbors = [n for n in adj[node] if n in active]
        k = len(neighbors)
        if k < 2:
            total_cc += 0.0
            continue
        triangles = sum(
            1 for i in range(len(neighbors))
            for j in range(i + 1, len(neighbors))
            if neighbors[j] in adj[neighbors[i]]
        )
        total_cc += 2 * triangles / (k * (k - 1))

    return total_cc / len(sample_nodes)


# ---------------------------------------------
#  1. Random Attack Simulation
# ---------------------------------------------
def simulate_random_attack(
    adj: dict,
    nodes: list,
    steps: int = 20,
    seed: int = 42,
) -> list:
    """
    Randomly removes nodes in steps and measures resilience.
    Returns list of {fraction_removed, gcs_fraction, efficiency}.
    """
    random.seed(seed)
    N = len(nodes)
    active = set(nodes)
    remaining = list(nodes)
    random.shuffle(remaining)

    step_size = max(1, N // steps)
    results = []

    # Baseline
    gcs = giant_component_size(adj, active)
    eff = global_efficiency(adj, active)
    results.append({
        "strategy": "random",
        "fraction_removed": 0.0,
        "gcs_fraction": gcs / N,
        "efficiency": eff,
        "nodes_remaining": N,
    })

    ptr = 0
    while ptr < len(remaining) and len(active) > 0:
        batch = remaining[ptr: ptr + step_size]
        for n in batch:
            active.discard(n)
        ptr += step_size

        fraction = 1 - len(active) / N
        gcs = giant_component_size(adj, active)
        eff = global_efficiency(adj, active, sample=100)
        results.append({
            "strategy": "random",
            "fraction_removed": round(fraction, 3),
            "gcs_fraction": round(gcs / N, 4),
            "efficiency": round(eff, 4),
            "nodes_remaining": len(active),
        })

    return results


# ---------------------------------------------
#  2. Targeted Degree Attack
# ---------------------------------------------
def simulate_targeted_degree_attack(
    adj: dict,
    nodes: list,
    steps: int = 20,
) -> list:
    """
    Removes highest-degree nodes first (worst-case attack).
    Recalculates degree after each removal (adaptive).
    """
    N = len(nodes)
    active = set(nodes)
    results = []

    step_size = max(1, N // steps)

    # Baseline
    gcs = giant_component_size(adj, active)
    eff = global_efficiency(adj, active)
    results.append({
        "strategy": "degree_targeted",
        "fraction_removed": 0.0,
        "gcs_fraction": gcs / N,
        "efficiency": eff,
        "nodes_remaining": N,
    })

    while len(active) > 0:
        # Recalculate degrees (adaptive attack)
        degree_active = {n: sum(1 for nb in adj[n] if nb in active)
                         for n in active}
        # Sort and remove top step_size nodes
        sorted_nodes = sorted(active, key=lambda n: degree_active[n],
                               reverse=True)
        batch = sorted_nodes[:step_size]
        for n in batch:
            active.discard(n)

        if len(active) == 0:
            break

        fraction = 1 - len(active) / N
        if fraction > 0.80:
            break

        gcs = giant_component_size(adj, active)
        eff = global_efficiency(adj, active, sample=100)
        results.append({
            "strategy": "degree_targeted",
            "fraction_removed": round(fraction, 3),
            "gcs_fraction": round(gcs / N, 4),
            "efficiency": round(eff, 4),
            "nodes_remaining": len(active),
        })

    return results


# ---------------------------------------------
#  3. Cascading Failure Simulation
# ---------------------------------------------
def simulate_cascading_failure(
    adj: dict,
    nodes: list,
    initial_fraction: float = 0.05,
    load_threshold: float = 2.0,
    seed: int = 42,
) -> dict:
    """
    Models cascading failures:
    1. Remove initial_fraction of random nodes
    2. Surviving nodes absorb load from removed nodes
    3. Nodes exceeding load threshold also fail
    4. Cascade continues until stable

    Uses degree as proxy for load capacity.
    """
    random.seed(seed)
    N = len(nodes)
    active = set(nodes)
    degree = {n: len(adj[n]) for n in nodes}

    # Initial capacity proportional to degree
    capacity = {n: degree[n] * load_threshold for n in nodes}
    current_load = {n: float(degree[n]) for n in nodes}

    # Initial failures
    initial_fails = random.sample(nodes, max(1, int(N * initial_fraction)))
    failed_set = set(initial_fails)
    active -= failed_set

    cascade_rounds = 0
    total_failed = len(failed_set)
    history = [{"round": 0, "newly_failed": len(failed_set),
                 "total_failed": total_failed,
                 "gcs_fraction": giant_component_size(adj, active) / N}]

    while True:
        # Redistribute load from failed to active neighbors
        new_loads = dict(current_load)
        for failed in failed_set:
            active_neighbors = [nb for nb in adj[failed] if nb in active]
            if active_neighbors:
                extra = degree[failed] / len(active_neighbors)
                for nb in active_neighbors:
                    new_loads[nb] = new_loads.get(nb, 0) + extra

        # Identify newly overloaded nodes
        newly_failed = {n for n in active if new_loads.get(n, 0) > capacity[n]}
        if not newly_failed:
            break  # Stable state

        failed_set |= newly_failed
        active -= newly_failed
        current_load = new_loads
        cascade_rounds += 1
        total_failed = len(failed_set)

        gcs = giant_component_size(adj, active) if active else 0
        history.append({
            "round": cascade_rounds,
            "newly_failed": len(newly_failed),
            "total_failed": total_failed,
            "gcs_fraction": gcs / N
        })

        if cascade_rounds > 50 or not active:
            break

    return {
        "initial_failures": len(initial_fails),
        "total_failed": total_failed,
        "cascade_rounds": cascade_rounds,
        "final_gcs_fraction": giant_component_size(adj, active) / N if active else 0.0,
        "survival_rate": len(active) / N,
        "history": history
    }


# ---------------------------------------------
#  4. Node Vulnerability Score
# ---------------------------------------------
def compute_vulnerability_scores(
    adj: dict,
    nodes: list,
    sample: int = 200,
    seed: int = 42,
) -> list:
    """
    Vulnerability(v) = drop in GCS when node v is removed.
    High vulnerability = critical node for connectivity.
    Sampled to avoid O(N^2) cost.
    """
    random.seed(seed)
    N = len(nodes)
    active = set(nodes)
    baseline_gcs = giant_component_size(adj, active)

    sample_nodes = random.sample(nodes, min(sample, N))
    results = []

    for node in sample_nodes:
        test_active = active - {node}
        test_gcs = giant_component_size(adj, test_active)
        vulnerability = (baseline_gcs - test_gcs) / N
        results.append({
            "node_id": node,
            "vulnerability": round(vulnerability, 4),
            "degree": len(adj[node]),
        })

    return sorted(results, key=lambda x: x["vulnerability"], reverse=True)


# ---------------------------------------------
#  5. Convert Results to Spark DataFrame
# ---------------------------------------------
def resilience_to_spark(spark, random_results, degree_results):
    schema = StructType([
        StructField("strategy",        StringType(),  False),
        StructField("fraction_removed",DoubleType(),  False),
        StructField("gcs_fraction",    DoubleType(),  False),
        StructField("efficiency",      DoubleType(),  False),
        StructField("nodes_remaining", IntegerType(), False),
    ])
    rows = []
    for r in random_results + degree_results:
        rows.append((
            r["strategy"],
            float(r["fraction_removed"]),
            float(r["gcs_fraction"]),
            float(r["efficiency"]),
            int(r["nodes_remaining"]),
        ))
    return spark.createDataFrame(rows, schema)


# ---------------------------------------------
#  MAIN RUNNER
# ---------------------------------------------
if __name__ == "__main__":
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
    OUT_DIR   = os.path.join(BASE_DIR, "outputs", "resilience")
    os.makedirs(OUT_DIR, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("NetworkResilience")
        .master("local[*]")
        .config("spark.jars.packages",
                "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("  NETWORK RESILIENCE ANALYSIS")
    print(f"  Dataset: {os.path.basename(DATA_PATH)}")
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
    adj = build_adj(nodes_local, edges_local)
    print(f"  Nodes: {len(nodes_local)}, Edges: {len(edges_local)}")

    # Baseline stats
    baseline_gcs = giant_component_size(adj, set(nodes_local))
    baseline_eff = global_efficiency(adj, set(nodes_local), sample=300)
    baseline_cc  = avg_clustering(adj, set(nodes_local), sample=300)
    print(f"\n  Baseline GCS:        {baseline_gcs} / {len(nodes_local)}")
    print(f"  Baseline Efficiency: {baseline_eff:.4f}")
    print(f"  Baseline Clustering: {baseline_cc:.4f}")

    # Attack simulations
    print("\n[2] Random node removal simulation (20 steps)...")
    rand_results = simulate_random_attack(adj, nodes_local, steps=20)
    for r in rand_results[::4]:
        print(f"  Removed {r['fraction_removed']*100:.0f}% -> "
              f"GCS={r['gcs_fraction']:.3f}, Eff={r['efficiency']:.4f}")

    print("\n[3] Targeted degree attack simulation (adaptive, 20 steps)...")
    deg_results = simulate_targeted_degree_attack(adj, nodes_local, steps=20)
    for r in deg_results[::4]:
        print(f"  Removed {r['fraction_removed']*100:.0f}% -> "
              f"GCS={r['gcs_fraction']:.3f}, Eff={r['efficiency']:.4f}")

    print("\n[4] Cascading failure simulation (5% initial failure)...")
    cascade = simulate_cascading_failure(
        adj, nodes_local, initial_fraction=0.05, load_threshold=2.0
    )
    print(f"  Initial failures:    {cascade['initial_failures']}")
    print(f"  Total failed:        {cascade['total_failed']}")
    print(f"  Cascade rounds:      {cascade['cascade_rounds']}")
    print(f"  Final GCS fraction:  {cascade['final_gcs_fraction']:.4f}")
    print(f"  Survival rate:       {cascade['survival_rate']:.4f}")

    print("\n[5] Computing node vulnerability scores (200 sample nodes)...")
    vuln = compute_vulnerability_scores(adj, nodes_local, sample=200)
    print("  Top 10 most vulnerable nodes:")
    for v in vuln[:10]:
        print(f"    Node {v['node_id']:>5} | Vulnerability={v['vulnerability']:.4f} "
              f"| Degree={v['degree']}")

    # Save to Spark
    print("\n[6] Saving outputs...")
    resilience_df = resilience_to_spark(spark, rand_results, deg_results)
    resilience_df.coalesce(1).write.mode("overwrite").csv(
        os.path.join(OUT_DIR, "attack_simulations"), header=True
    )

    vuln_schema = StructType([
        StructField("node_id",       LongType(),    False),
        StructField("vulnerability", DoubleType(),  False),
        StructField("degree",        IntegerType(), False),
    ])
    vuln_rows = [(int(v["node_id"]), v["vulnerability"], v["degree"])
                 for v in vuln]
    vuln_df = spark.createDataFrame(vuln_rows, vuln_schema)
    vuln_df.coalesce(1).write.mode("overwrite").csv(
        os.path.join(OUT_DIR, "vulnerability_scores"), header=True
    )
    print(f"  Saved to: {OUT_DIR}")

    spark.stop()
