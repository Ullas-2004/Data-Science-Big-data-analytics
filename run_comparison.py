"""
run_comparison.py
=================
Unified comparison runner for GraphFrames and GraphX.

This version produces:
  1. plain-text comparison output
  2. machine-readable JSON proof
  3. a polished HTML dashboard for project presentation
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
from typing import Dict, List, Optional


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
GF_DIR = os.path.join(ROOT_DIR, "GraphFrames")
GX_DIR = os.path.join(ROOT_DIR, "GraphX")
DATA_DIR = os.path.join(ROOT_DIR, "data_lake")
REPORT_DIR = os.path.join(ROOT_DIR, "Comparison_Report")
GF_OUTPUT_DIR = os.path.join(GF_DIR, "outputs", "graph_analytics")
GX_OUTPUT_DIR = os.path.join(GX_DIR, "outputs")

VENV_PY = os.path.join(ROOT_DIR, "venv", "Scripts", "python.exe")
if not os.path.exists(VENV_PY):
    VENV_PY = sys.executable

os.makedirs(REPORT_DIR, exist_ok=True)


RESEARCH_DIFFERENCES = [
    {
        "aspect": "Graph theory model",
        "graphframes": "Represents the graph as property-graph DataFrames for vertices and edges, which keeps graph data close to tabular analytics.",
        "graphx": "Represents the graph as an RDD-backed property graph with vertex and edge collections plus a triplet view.",
        "social_media": "Both can model users and relationships, but GraphFrames is easier when social graph analysis must stay connected to profile, activity, or text metadata tables.",
        "source_label": "Spark GraphX programming guide",
        "source_url": "https://spark.apache.org/docs/3.5.7/graphx-programming-guide.html",
    },
    {
        "aspect": "DSA orientation",
        "graphframes": "Closer to relational data structures and algorithms because joins, filters, and aggregations are the main execution building blocks.",
        "graphx": "Closer to classical graph DSA because it exposes graph-native structures, edge triplets, message passing, and Pregel-style iteration.",
        "social_media": "This is a clean theoretical difference to explain in a viva: GraphFrames is graph analytics on tables, GraphX is graph computation inside Spark.",
        "source_label": "GraphFrames architecture",
        "source_url": "https://graphframes.io/01-about/02-architecture.html",
    },
    {
        "aspect": "Core execution model",
        "graphframes": "DataFrame-based graph processing with Spark SQL planning and relational operators.",
        "graphx": "RDD-based property-graph processing with graph-parallel primitives.",
        "social_media": "GraphFrames fits pipelines where user metadata, hashtags, posts, and graph edges all need to be joined together.",
        "source_label": "GraphFrames paper (2016)",
        "source_url": "https://www.microsoft.com/en-us/research/publication/graphframes-an-integrated-api-for-mixing-graph-and-relational-queries/",
    },
    {
        "aspect": "Custom iterative algorithms",
        "graphframes": "Supports Pregel-style APIs, but the natural development style is still higher-level DataFrame analytics.",
        "graphx": "Better fit for custom iterative algorithms, edge-centric computation, and message-passing research prototypes.",
        "social_media": "GraphX is easier to defend when the project includes custom influence propagation, diffusion, or routing logic.",
        "source_label": "Spark GraphX programming guide",
        "source_url": "https://spark.apache.org/docs/3.5.7/graphx-programming-guide.html",
    },
    {
        "aspect": "Pattern and motif search",
        "graphframes": "Native motif finding is built into the API.",
        "graphx": "No equivalent high-level motif DSL; patterns require lower-level graph logic.",
        "social_media": "Motif queries are useful for reply loops, mutual follows, suspicious engagement rings, and bot-like triads.",
        "source_label": "GraphFrames user guide",
        "source_url": "https://graphframes.io/04-user-guide/04-motif-finding.html",
    },
    {
        "aspect": "Algorithm surface",
        "graphframes": "Broader high-level toolbox: BFS, motif finding, cycles, k-core, maximal independent set, typed degrees, and DataFrame-friendly graph workflows.",
        "graphx": "Stronger low-level control and graph-native primitives such as PageRank, SCC, SVD++, and triplet-centric custom algorithms.",
        "social_media": "GraphFrames is easier to demo broadly; GraphX is better when you want to explain graph-computation internals.",
        "source_label": "GraphFrames Quick Start",
        "source_url": "https://graphframes.io/02-quick-start/02-quick-start.html",
    },
    {
        "aspect": "Language and ecosystem fit",
        "graphframes": "Works naturally with Python, Scala, Java, and Spark Connect workflows.",
        "graphx": "Primarily Scala-centric and not designed around PySpark workflows.",
        "social_media": "This matters in classroom and analytics teams where most experimentation happens in Python notebooks.",
        "source_label": "GraphFrames docs",
        "source_url": "https://graphframes.io/",
    },
    {
        "aspect": "Memory and optimizer behavior",
        "graphframes": "Benefits from DataFrame storage, Spark SQL planning, and modern optimizer features such as Catalyst and Adaptive Query Execution.",
        "graphx": "Uses RDD-based graph structures, which can be faster in graph-native loops but usually carry more object overhead.",
        "social_media": "For mixed analytics workloads, GraphFrames can be easier to scale and reason about when many joins or filters are involved.",
        "source_label": "GraphFrames About",
        "source_url": "https://graphframes.io/01-about/01-index.html",
    },
    {
        "aspect": "Performance tradeoff",
        "graphframes": "Often more memory-friendly and easier to integrate with SQL optimization.",
        "graphx": "Often faster on raw iterative workloads for small and medium graphs.",
        "social_media": "If the goal is fastest graph-only execution, GraphX often wins; if the goal is mixed analytics plus explainability, GraphFrames is usually more practical.",
        "source_label": "GraphFrames benchmarks",
        "source_url": "https://graphframes.io/01-about/03-benchmarks.html",
    },
    {
        "aspect": "Research maturity",
        "graphframes": "Has fewer recent university papers, but the research tends to focus on workflow integration and community analysis.",
        "graphx": "Has stronger long-term academic presence in systems papers, benchmarks, and custom algorithm engineering.",
        "social_media": "This helps explain why GraphX still appears in benchmark papers even while GraphFrames has stronger product momentum.",
        "source_label": "Revisiting Graph Analytics Benchmark (SIGMOD 2025)",
        "source_url": "https://doi.org/10.1145/3725345",
    },
    {
        "aspect": "Research evidence in social graphs",
        "graphframes": "Pattern-driven community discovery can be very effective when the suspicious structure is already known.",
        "graphx": "General iterative graph algorithms remain useful when the pattern is unknown and must emerge from the graph.",
        "social_media": "This is a strong talking point for Twitter-style community mining and suspicious-behavior detection.",
        "source_label": "GraphFrames community-detection paper (2024)",
        "source_url": "https://arxiv.org/abs/2408.03966",
    },
    {
        "aspect": "Ecosystem direction",
        "graphframes": "Actively evolving, including new algorithms and an internal GraphX fork for current Spark support.",
        "graphx": "Still usable, but marked deprecated upstream in Spark 4.0 planning.",
        "social_media": "For a new student project frontend, GraphFrames gives a more future-facing story while GraphX still provides a valuable low-level benchmark baseline.",
        "source_label": "SPARK-50857",
        "source_url": "https://issues.apache.org/jira/browse/SPARK-50857",
    },
]


def discover_dataset(explicit_path: Optional[str] = None) -> str:
    """Find the dataset to analyze."""
    if explicit_path:
        if not os.path.isabs(explicit_path):
            explicit_path = os.path.join(ROOT_DIR, explicit_path)
        if os.path.exists(explicit_path):
            return explicit_path
        print(f"[ERROR] File not found: {explicit_path}")
        sys.exit(1)

    candidates = (
        glob.glob(os.path.join(DATA_DIR, "*.txt"))
        + glob.glob(os.path.join(DATA_DIR, "*.txt.gz"))
        + glob.glob(os.path.join(DATA_DIR, "*.csv"))
        + glob.glob(os.path.join(DATA_DIR, "*.csv.gz"))
        + glob.glob(os.path.join(DATA_DIR, "*.tsv"))
        + glob.glob(os.path.join(DATA_DIR, "*.tsv.gz"))
    )
    if not candidates:
        print("[ERROR] No dataset found in data_lake/")
        print(f"  Place a SNAP edge-list file into: {DATA_DIR}")
        sys.exit(1)

    candidates.sort(key=lambda path: os.path.getsize(path))
    if len(candidates) == 1:
        return candidates[0]

    print("\n  Available datasets in data_lake/:")
    print("  " + "-" * 52)
    for index, path in enumerate(candidates, start=1):
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"    {index}. {os.path.basename(path):<40} ({size_mb:.2f} MB)")
    print("  " + "-" * 52)

    while True:
        try:
            choice = int(input(f"\n  Select dataset [1-{len(candidates)}]: ").strip())
        except KeyboardInterrupt:
            print("\n  Cancelled.")
            sys.exit(0)
        except ValueError:
            choice = -1

        if 1 <= choice <= len(candidates):
            return candidates[choice - 1]
        print(f"  Please enter a number between 1 and {len(candidates)}")


def get_dataset_name(path: str) -> str:
    """Extract human-readable dataset name."""
    name = os.path.basename(path)
    for ext in [".txt.gz", ".csv.gz", ".tsv.gz", ".txt", ".csv", ".tsv"]:
        if name.endswith(ext):
            name = name[: -len(ext)]
            break
    return name.replace("_", " ").replace("-", " ").title()


def get_dataset_slug(path: str) -> str:
    """Return a filesystem-safe dataset slug."""
    name = os.path.basename(path)
    for ext in [".txt.gz", ".csv.gz", ".tsv.gz", ".txt", ".csv", ".tsv"]:
        if name.endswith(ext):
            name = name[: -len(ext)]
            break
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "dataset"


def resolve_report_dir(output_dir: Optional[str] = None) -> str:
    """Resolve and create the output report directory."""
    report_dir = output_dir or REPORT_DIR
    if not os.path.isabs(report_dir):
        report_dir = os.path.join(ROOT_DIR, report_dir)
    os.makedirs(report_dir, exist_ok=True)
    return report_dir


def build_artifact_paths(report_dir: str) -> Dict[str, str]:
    """Return the standard artifact paths for a given report directory."""
    return {
        "text_report": os.path.join(report_dir, "comparison_results.txt"),
        "json_report": os.path.join(report_dir, "comparison_data.json"),
        "timing_csv": os.path.join(report_dir, "latency_comparison.csv"),
        "run_log_csv": os.path.join(report_dir, "run_logs.csv"),
        "dashboard": os.path.join(report_dir, "dashboard.html"),
        "graphframes_log": os.path.join(report_dir, "graphframes_output.log"),
        "graphframes_error": os.path.join(report_dir, "graphframes_error.log"),
        "graphx_log": os.path.join(report_dir, "graphx_output.log"),
        "graphx_error": os.path.join(report_dir, "graphx_error.log"),
    }


def find_part_csv(directory: str) -> Optional[str]:
    """Return the first Spark-generated part CSV inside a directory."""
    if not os.path.isdir(directory):
        return None
    matches = sorted(
        path for path in glob.glob(os.path.join(directory, "*.csv"))
        if os.path.isfile(path) and os.path.basename(path).startswith("part-")
    )
    return matches[0] if matches else None


def read_metric_csv(path: str) -> Dict[str, str]:
    """Read metric,value CSV into a dictionary."""
    if not os.path.exists(path):
        return {}
    metrics: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            metric = (row.get("metric") or "").strip()
            value = (row.get("value") or "").strip()
            if metric:
                metrics[metric] = value
    return metrics


def read_benchmark_csv(path: str) -> Dict[str, float]:
    """Read algorithm,time_seconds CSV into a dictionary."""
    if not os.path.exists(path):
        return {}
    metrics: Dict[str, float] = {}
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            name = (row.get("algorithm") or "").strip()
            raw = (row.get("time_seconds") or "").strip()
            try:
                metrics[name] = float(raw)
            except ValueError:
                continue
    return metrics


def read_table_rows(path: Optional[str], limit: int = 10) -> List[Dict[str, str]]:
    """Read a small table preview from CSV."""
    if not path or not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            rows.append({key: value for key, value in row.items()})
            if len(rows) >= limit:
                break
    return rows


def copy_path(source: str, destination: str) -> str:
    """Copy a file or directory into the report archive."""
    if os.path.isdir(source):
        if os.path.exists(destination):
            shutil.rmtree(destination)
        shutil.copytree(source, destination)
    else:
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.copy2(source, destination)
    return destination


def archive_framework_outputs(report_dir: str) -> Dict[str, Dict[str, str]]:
    """Archive framework outputs into the dataset-specific report folder."""
    proof_root = os.path.join(report_dir, "proof")
    gf_root = os.path.join(proof_root, "graphframes")
    gx_root = os.path.join(proof_root, "graphx")

    copy_path(GF_OUTPUT_DIR, gf_root)
    copy_path(GX_OUTPUT_DIR, gx_root)

    gf_pagerank_dir = os.path.join(gf_root, "pagerank_top100")
    gf_components_dir = os.path.join(gf_root, "connected_components")
    gf_degrees_dir = os.path.join(gf_root, "degree_distribution")
    gf_triangles_dir = os.path.join(gf_root, "triangle_counts")

    return {
        "graphframes": {
            "root": gf_root,
            "summary_csv": os.path.join(gf_root, "graph_summary.csv"),
            "timings_csv": os.path.join(gf_root, "benchmark_timings.csv"),
            "pagerank_csv": find_part_csv(gf_pagerank_dir) or gf_pagerank_dir,
            "components_csv": find_part_csv(gf_components_dir) or gf_components_dir,
            "degrees_csv": find_part_csv(gf_degrees_dir) or gf_degrees_dir,
            "triangles_csv": find_part_csv(gf_triangles_dir) or gf_triangles_dir,
        },
        "graphx": {
            "root": gx_root,
            "summary_csv": os.path.join(gx_root, "graph_summary.csv"),
            "timings_csv": os.path.join(gx_root, "benchmark_timings.csv"),
            "pagerank_csv": os.path.join(gx_root, "pagerank_top100.csv"),
            "communities_csv": os.path.join(gx_root, "community_sizes.csv"),
            "degrees_csv": os.path.join(gx_root, "top_hubs.csv"),
        },
    }


def build_metric_proof(
    key: str,
    label: str,
    gf_value: str,
    gx_value: str,
    match: Optional[bool],
    proof_files: Dict[str, Dict[str, str]],
) -> Dict[str, object]:
    """Attach dataset-specific evidence and an explanation to a metric row."""
    summary_paths = {
        "graphframes": proof_files["graphframes"]["summary_csv"],
        "graphx": proof_files["graphx"]["summary_csv"],
    }

    note = "Both frameworks exported this metric from their generated summary files."
    if key == "communities_lpa":
        note = (
            "Label Propagation is heuristic and implementation-sensitive, so differing community counts "
            "can occur even when the underlying graph is the same."
        )
    elif key in {"density", "global_clustering_coeff", "avg_local_clustering_coeff"}:
        note = (
            "This metric is currently exported only by the GraphX pipeline in this project, so the "
            "GraphFrames side is intentionally marked N/A."
        )

    status = "matched" if match is True else "different" if match is False else "framework-specific"
    return {
        "status": status,
        "note": note,
        "files": summary_paths,
        "values": {
            "graphframes": gf_value,
            "graphx": gx_value,
        },
        "metric_key": key,
        "label": label,
    }


def parse_gf_output(output: str) -> Dict[str, str]:
    """Fallback parser for GraphFrames stdout."""
    metrics: Dict[str, str] = {}
    patterns = {
        "nodes": r"Nodes:\s+([\d,]+)",
        "edges": r"Edges:\s+([\d,]+)",
        "triangles": r"Total.*?Triangles:\s+([\d,]+)",
        "communities_lpa": r"Communities \(LPA\):\s+(\d+)|Total Communities Detected:\s+(\d+)",
        "connected_components": r"Connected Components:\s+(\d+)|Total Connected Components:\s+(\d+)",
        "max_degree": r"Max Degree:\s+(\d+)",
        "avg_degree": r"Average Degree:\s+([\d.]+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, output)
        if not match:
            continue
        value = next((group for group in match.groups() if group), "")
        if value:
            metrics[key] = value
    return metrics


def parse_gx_output(output: str) -> Dict[str, str]:
    """Fallback parser for GraphX stdout."""
    metrics: Dict[str, str] = {}
    patterns = {
        "nodes": r"Nodes:\s+([\d,]+)",
        "edges": r"Edges:\s+([\d,]+)",
        "triangles": r"Triangles:\s+([\d,]+)|Total Triangles:\s+([\d,]+)",
        "communities_lpa": r"Communities \(LPA\):\s+(\d+)|Total Communities:\s+(\d+)",
        "connected_components": r"Connected Components:\s+(\d+)|Total Components:\s+(\d+)",
        "max_degree": r"Max Degree:\s+(\d+)",
        "avg_degree": r"Avg Degree:\s+([\d.]+)",
        "density": r"Density:\s+([\d.]+)|Graph Density:\s+([\d.]+)",
        "global_clustering_coeff": r"Global Clustering Coeff:\s+([\d.]+)|Global Clustering Coefficient:\s+([\d.]+)",
        "avg_local_clustering_coeff": r"Avg Local Clustering:\s+([\d.]+)|Average Local Clustering Coefficient:\s+([\d.]+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, output)
        if not match:
            continue
        value = next((group for group in match.groups() if group), "")
        if value:
            metrics[key] = value
    return metrics


def format_number(value: object) -> str:
    """Pretty format numbers for display."""
    if value in (None, "", "N/A"):
        return "N/A"
    try:
        if isinstance(value, str) and value.strip().isdigit():
            return f"{int(value):,}"
        number = float(str(value).replace(",", ""))
    except ValueError:
        return str(value)

    if abs(number - round(number)) < 1e-9:
        return f"{int(round(number)):,}"
    return f"{number:,.4f}".rstrip("0").rstrip(".")


def to_number(value: object) -> Optional[float]:
    """Convert formatted value to float when possible."""
    if value in (None, "", "N/A"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def values_match(key: str, left: object, right: object) -> Optional[bool]:
    """Compare numeric values with metric-aware tolerance."""
    left_num = to_number(left)
    right_num = to_number(right)
    if left_num is None or right_num is None:
        return None

    tolerance = 1e-6
    if key in {"avg_degree", "density", "global_clustering_coeff", "avg_local_clustering_coeff"}:
        tolerance = 0.02
    return abs(left_num - right_num) <= tolerance


def extract_log_snippets(output: str, keywords: List[str], limit: int = 12) -> List[str]:
    """Pull user-friendly evidence lines out of a log."""
    if not output:
        return []
    snippets: List[str] = []
    lowered = [keyword.lower() for keyword in keywords]
    for line in output.splitlines():
        probe = line.lower()
        if any(keyword in probe for keyword in lowered):
            cleaned = line.strip()
            if cleaned and cleaned not in snippets:
                snippets.append(cleaned)
        if len(snippets) >= limit:
            break
    return snippets


def format_time_value(value: object) -> str:
    """Format seconds for display."""
    if value in (None, "", "N/A"):
        return "N/A"
    try:
        return f"{float(value):.2f} s"
    except (TypeError, ValueError):
        return str(value)


def find_spark_shell() -> str:
    """Locate spark-shell on Windows."""
    found = shutil.which("spark-shell") or shutil.which("spark-shell.cmd")
    if found:
        return found

    spark_home = os.environ.get("SPARK_HOME", "")
    if spark_home:
        candidate = os.path.join(spark_home, "bin", "spark-shell.cmd")
        if os.path.exists(candidate):
            return candidate

    try:
        import pyspark  # type: ignore

        candidate = os.path.join(os.path.dirname(pyspark.__file__), "bin", "spark-shell.cmd")
        if os.path.exists(candidate):
            return candidate
    except ImportError:
        pass

    return "spark-shell"


def run_graphframes(dataset_path: str, artifacts: Dict[str, str]) -> Dict[str, object]:
    """Execute the GraphFrames pipeline and capture logs."""
    print("\n" + "=" * 64)
    print("  PHASE 1: GRAPHFRAMES (Python / DataFrame Pipeline)")
    print("=" * 64)

    script = os.path.join(GF_DIR, "graph_analytics.py")
    cmd = [VENV_PY, script, dataset_path]

    print(f"  CMD: {' '.join(cmd)}")
    print(f"  Dataset: {os.path.basename(dataset_path)}")
    print("  Running...\n")

    start = time.time()
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=ROOT_DIR,
    )
    elapsed = time.time() - start

    log_file = artifacts["graphframes_log"]
    with open(log_file, "w", encoding="utf-8") as handle:
        handle.write(result.stdout)

    err_file = artifacts["graphframes_error"]
    if result.stderr:
        with open(err_file, "w", encoding="utf-8") as handle:
            handle.write(result.stderr)
    elif os.path.exists(err_file):
        os.remove(err_file)

    success = result.returncode == 0
    print(f"  [{'OK' if success else 'FAILED'}] GraphFrames completed in {elapsed:.1f}s")
    if not success:
        print(f"  Error log: {err_file}")

    return {
        "success": success,
        "returncode": result.returncode,
        "time": elapsed,
        "output": result.stdout,
        "stderr": result.stderr,
        "log_file": log_file,
        "error_file": err_file if result.stderr else "",
        "command": " ".join(cmd),
    }


def run_graphx(dataset_path: str, artifacts: Dict[str, str]) -> Dict[str, object]:
    """Execute the GraphX pipeline via spark-shell with a writable home directory."""
    print("\n" + "=" * 64)
    print("  PHASE 2: GRAPHX (Scala / Native Spark Graph Engine)")
    print("=" * 64)

    scala_script = os.path.join(GX_DIR, "GraphAnalytics.scala")
    spark_shell = find_spark_shell()

    config_file = os.path.join(GX_DIR, ".dataset_config")
    dataset_rel = os.path.relpath(dataset_path, ROOT_DIR).replace("\\", "/")
    with open(config_file, "w", encoding="utf-8") as handle:
        handle.write(dataset_rel)

    shell_home = os.path.join(ROOT_DIR, ".spark_shell_home")
    shell_tmp = os.path.join(shell_home, "tmp")
    os.makedirs(shell_tmp, exist_ok=True)
    java_home = shell_home.replace("\\", "/")

    env = os.environ.copy()
    env["HOME"] = java_home
    env["USERPROFILE"] = shell_home
    env["TMP"] = shell_tmp
    env["TEMP"] = shell_tmp

    raw_driver_memory = str(env.get("BDA_SPARK_DRIVER_GB", "12")).strip().lower()
    driver_memory = raw_driver_memory if raw_driver_memory.endswith("g") else f"{raw_driver_memory}g"
    partitions = str(env.get("BDA_SPARK_PARTITIONS", "80")).strip()
    java_opts = f'-Duser.home={java_home}'
    command = (
        f'"{spark_shell}" '
        f'--driver-java-options "{java_opts}" '
        f'--driver-memory {driver_memory} '
        f'--conf spark.executor.memory={driver_memory} '
        f'--conf spark.default.parallelism={partitions} '
        f'--conf spark.sql.shuffle.partitions={partitions}'
    )
    with open(scala_script, "r", encoding="utf-8") as handle:
        scala_source = handle.read()

    print(f"  CMD: {command}")
    print(f"  Dataset: {os.path.basename(dataset_path)}")
    print("  Running...\n")

    start = time.time()
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE,
        cwd=ROOT_DIR,
        shell=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )

    graphx_timeout = int(os.environ.get("BDA_GRAPHX_TIMEOUT_SECONDS", "900"))
    try:
        stdout_data, _ = proc.communicate(input=scala_source + "\n:quit\n", timeout=graphx_timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout_data, _ = proc.communicate()

    elapsed = time.time() - start

    log_file = artifacts["graphx_log"]
    with open(log_file, "w", encoding="utf-8") as handle:
        handle.write(stdout_data)

    error_file = artifacts["graphx_error"]
    success_markers = [
        "GraphX Deep Analytics Complete.",
        "GRAPHX DEEP ANALYTICS - SUMMARY",
    ]
    success = any(marker in stdout_data for marker in success_markers)

    if not success:
        with open(error_file, "w", encoding="utf-8") as handle:
            handle.write(stdout_data)
    elif os.path.exists(error_file):
        os.remove(error_file)

    if success and proc.returncode not in (0, None):
        print("  Note: GraphX produced the expected summary despite a non-zero shell exit code.")

    print(f"  [{'OK' if success else 'FAILED'}] GraphX completed in {elapsed:.1f}s")
    if not success:
        print(f"  Log: {log_file}")

    return {
        "success": success,
        "returncode": proc.returncode,
        "time": elapsed,
        "output": stdout_data,
        "log_file": log_file,
        "error_file": error_file if os.path.exists(error_file) else "",
        "command": command,
    }


def load_graphframes_artifacts(output: str) -> Dict[str, object]:
    """Load the GraphFrames summary, benchmarks, and proof tables."""
    summary = read_metric_csv(os.path.join(GF_OUTPUT_DIR, "graph_summary.csv"))
    if not summary:
        summary = parse_gf_output(output)

    benchmarks = read_benchmark_csv(os.path.join(GF_OUTPUT_DIR, "benchmark_timings.csv"))
    pagerank = read_table_rows(find_part_csv(os.path.join(GF_OUTPUT_DIR, "pagerank_top100")), limit=10)
    components = read_table_rows(find_part_csv(os.path.join(GF_OUTPUT_DIR, "connected_components")), limit=10)
    degrees = read_table_rows(find_part_csv(os.path.join(GF_OUTPUT_DIR, "degree_distribution")), limit=10)
    triangles = read_table_rows(find_part_csv(os.path.join(GF_OUTPUT_DIR, "triangle_counts")), limit=10)

    return {
        "summary": summary,
        "benchmarks": benchmarks,
        "pagerank_top": pagerank,
        "components": components,
        "degrees": degrees,
        "triangles": triangles,
    }


def load_graphx_artifacts(output: str) -> Dict[str, object]:
    """Load the GraphX summary, benchmarks, and proof tables."""
    summary = read_metric_csv(os.path.join(GX_OUTPUT_DIR, "graph_summary.csv"))
    if not summary:
        summary = parse_gx_output(output)

    benchmarks = read_benchmark_csv(os.path.join(GX_OUTPUT_DIR, "benchmark_timings.csv"))
    pagerank = read_table_rows(os.path.join(GX_OUTPUT_DIR, "pagerank_top100.csv"), limit=10)
    communities = read_table_rows(os.path.join(GX_OUTPUT_DIR, "community_sizes.csv"), limit=10)
    degrees = read_table_rows(os.path.join(GX_OUTPUT_DIR, "top_hubs.csv"), limit=10)

    return {
        "summary": summary,
        "benchmarks": benchmarks,
        "pagerank_top": pagerank,
        "communities": communities,
        "degrees": degrees,
    }


def build_metric_rows(
    gf_summary: Dict[str, str],
    gx_summary: Dict[str, str],
    proof_files: Dict[str, Dict[str, str]],
) -> List[Dict[str, object]]:
    """Build the side-by-side metric comparison table."""
    labels = [
        ("nodes", "Nodes"),
        ("edges", "Edges (undirected)"),
        ("triangles", "Total Triangles"),
        ("communities_lpa", "Communities (LPA)"),
        ("connected_components", "Connected Components"),
        ("max_degree", "Max Degree"),
        ("avg_degree", "Average Degree"),
        ("density", "Graph Density"),
        ("global_clustering_coeff", "Global Clustering Coefficient"),
        ("avg_local_clustering_coeff", "Average Local Clustering Coefficient"),
    ]

    rows: List[Dict[str, object]] = []
    for key, label in labels:
        gf_value = gf_summary.get(key, "N/A")
        gx_value = gx_summary.get(key, "N/A")
        rows.append(
            {
                "key": key,
                "label": label,
                "graphframes": format_number(gf_value),
                "graphx": format_number(gx_value),
                "match": values_match(key, gf_value, gx_value),
                "proof": build_metric_proof(
                    key,
                    label,
                    format_number(gf_value),
                    format_number(gx_value),
                    values_match(key, gf_value, gx_value),
                    proof_files,
                ),
            }
        )
    return rows


def build_timing_rows(gf_benchmarks: Dict[str, float], gx_benchmarks: Dict[str, float]) -> List[Dict[str, object]]:
    """Align per-algorithm timings from both frameworks."""
    preferred_order = [
        "Graph Loading",
        "PageRank",
        "Label Propagation",
        "Triangle Count",
        "Connected Components",
        "Degree Analysis",
        "Shortest Paths",
        "TOTAL",
    ]
    all_names = list(dict.fromkeys(preferred_order + list(gf_benchmarks.keys()) + list(gx_benchmarks.keys())))

    rows: List[Dict[str, object]] = []
    for name in all_names:
        gf_time = gf_benchmarks.get(name)
        gx_time = gx_benchmarks.get(name)
        speedup = None
        best_framework = "N/A"
        if gf_time and gx_time and gx_time > 0:
            speedup = gf_time / gx_time
        if gf_time is not None and gx_time is not None:
            if abs(gf_time - gx_time) <= 1e-9:
                best_framework = "Tie"
            elif gf_time < gx_time:
                best_framework = "GraphFrames"
            else:
                best_framework = "GraphX"
        rows.append(
            {
                "algorithm": name,
                "graphframes": gf_time,
                "graphx": gx_time,
                "speedup": speedup,
                "best_framework": best_framework,
            }
        )
    return rows


def compute_pagerank_overlap(
    gf_rows: List[Dict[str, str]],
    gx_rows: List[Dict[str, str]],
) -> Dict[str, object]:
    """Measure node overlap in the top PageRank outputs."""
    gf_ids = [row.get("id") for row in gf_rows if row.get("id")]
    gx_ids = [row.get("node_id") for row in gx_rows if row.get("node_id")]
    overlap = [node_id for node_id in gf_ids if node_id in gx_ids]
    return {
        "count": len(overlap),
        "total": min(len(gf_ids), len(gx_ids)),
        "nodes": overlap[:10],
    }


def build_comparison_payload(
    dataset_path: str,
    dataset_name: str,
    gf_result: Dict[str, object],
    gx_result: Dict[str, object],
    artifacts: Dict[str, str],
) -> Dict[str, object]:
    """Collect measured proof into one serializable structure."""
    report_dir = os.path.dirname(artifacts["dashboard"])
    proof_files = archive_framework_outputs(report_dir)
    gf_artifacts = load_graphframes_artifacts(str(gf_result.get("output", "")))
    gx_artifacts = load_graphx_artifacts(str(gx_result.get("output", "")))

    gf_summary = gf_artifacts["summary"]  # type: ignore[assignment]
    gx_summary = gx_artifacts["summary"]  # type: ignore[assignment]
    metric_rows = build_metric_rows(gf_summary, gx_summary, proof_files)  # type: ignore[arg-type]
    timing_rows = build_timing_rows(
        gf_artifacts["benchmarks"],  # type: ignore[arg-type]
        gx_artifacts["benchmarks"],  # type: ignore[arg-type]
    )

    comparable = [row for row in metric_rows if row["match"] is not None]
    matched = [row for row in comparable if row["match"] is True]

    total_gf = gf_artifacts["benchmarks"].get("TOTAL") or gf_result.get("time")  # type: ignore[index]
    total_gx = gx_artifacts["benchmarks"].get("TOTAL") or gx_result.get("time")  # type: ignore[index]
    speedup = None
    if total_gf and total_gx:
        try:
            if float(total_gx) > 0:
                speedup = float(total_gf) / float(total_gx)
        except (TypeError, ValueError):
            speedup = None

    gf_pagerank = [
        {"node": row.get("id", "N/A"), "score": format_number(row.get("pagerank", "N/A"))}
        for row in gf_artifacts["pagerank_top"]  # type: ignore[index]
    ]
    gx_pagerank = [
        {"node": row.get("node_id", "N/A"), "score": format_number(row.get("pagerank_score", "N/A"))}
        for row in gx_artifacts["pagerank_top"]  # type: ignore[index]
    ]
    overlap = compute_pagerank_overlap(
        gf_artifacts["pagerank_top"],  # type: ignore[arg-type]
        gx_artifacts["pagerank_top"],  # type: ignore[arg-type]
    )

    return {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "dataset": {
            "name": dataset_name,
            "slug": get_dataset_slug(dataset_path),
            "path": dataset_path,
            "file_name": os.path.basename(dataset_path),
            "size_mb": os.path.getsize(dataset_path) / (1024 * 1024),
        },
        "artifacts": {
            "text_report": artifacts["text_report"],
            "json_report": artifacts["json_report"],
            "timing_csv": artifacts["timing_csv"],
            "run_log_csv": artifacts["run_log_csv"],
            "dashboard": artifacts["dashboard"],
        },
        "proof_files": proof_files,
        "hero_cards": [
            {
                "label": "Comparable Metrics Matched",
                "value": f"{len(matched)} / {len(comparable)}",
                "note": "Direct result agreement across shared graph metrics.",
            },
            {
                "label": "Measured Runtime Gap",
                "value": f"{speedup:.2f}x" if speedup else "N/A",
                "note": "How much slower GraphFrames was than GraphX on this run.",
            },
            {
                "label": "GraphFrames Status",
                "value": "Success" if gf_result.get("success") else "Needs attention",
                "note": "Based on runner exit state and generated files.",
            },
            {
                "label": "GraphX Status",
                "value": "Success" if gx_result.get("success") else "Needs attention",
                "note": "Based on runner exit state and generated files.",
            },
        ],
        "runs": [
            {
                "name": "GraphFrames",
                "success": bool(gf_result.get("success")),
                "returncode": gf_result.get("returncode"),
                "runtime": format_time_value(total_gf or gf_result.get("time")),
                "log_file": gf_result.get("log_file"),
                "error_file": gf_result.get("error_file"),
                "command": gf_result.get("command"),
            },
            {
                "name": "GraphX",
                "success": bool(gx_result.get("success")),
                "returncode": gx_result.get("returncode"),
                "runtime": format_time_value(total_gx or gx_result.get("time")),
                "log_file": gx_result.get("log_file"),
                "error_file": gx_result.get("error_file"),
                "command": gx_result.get("command"),
            },
        ],
        "metrics": metric_rows,
        "timings": timing_rows,
        "pagerank": {
            "graphframes": gf_pagerank,
            "graphx": gx_pagerank,
            "overlap": overlap,
        },
        "callouts": [
            f"{len(matched)} of {len(comparable)} comparable structural metrics matched exactly or within tolerance.",
            (
                f"GraphX completed the full pipeline about {speedup:.2f}x faster on this dataset."
                if speedup
                else "A total-runtime speedup could not be computed because one framework did not produce a complete benchmark."
            ),
            "GraphFrames provides richer high-level graph analytics and DataFrame integration, while GraphX stays closer to graph-engine internals.",
            "The dashboard includes both measured output files and research-backed architectural differences, so you can defend implementation and theory together.",
        ],
        "research_facts": RESEARCH_DIFFERENCES,
        "log_paths": {
            "graphframes": gf_result.get("log_file"),
            "graphx": gx_result.get("log_file"),
        },
        "log_snippets": {
            "graphframes": extract_log_snippets(
                str(gf_result.get("output", "")),
                ["Nodes:", "Edges:", "Triangles", "Connected Components", "Average Degree", "ANALYSIS COMPLETE"],
            ),
            "graphx": extract_log_snippets(
                str(gx_result.get("output", "")),
                ["Nodes:", "Edges:", "Triangles", "Connected Components", "Avg Degree", "SUMMARY", "Complete"],
            ),
        },
    }


def render_dashboard(data: Dict[str, object], output_path: str) -> None:
    """Render a self-contained HTML dashboard."""
    dashboard_json = json.dumps(data, indent=2)
    html_page = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>GraphFrames vs GraphX Dashboard</title>
  <style>
    :root {{
      --bg: #0b1220;
      --panel: rgba(15, 22, 35, 0.88);
      --panel-strong: rgba(21, 29, 45, 0.96);
      --text: #eef2ff;
      --muted: #96a4c0;
      --line: rgba(145, 175, 255, 0.18);
      --gf: #49d3a7;
      --gx: #78a2ff;
      --accent: #ffb66d;
      --ok: #46d086;
      --warn: #f6c463;
      --bad: #ff7d86;
      --radius: 22px;
      --shadow: 0 20px 60px rgba(0, 0, 0, 0.34);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at 12% 12%, rgba(73, 211, 167, 0.16), transparent 28%),
        radial-gradient(circle at 85% 8%, rgba(120, 162, 255, 0.16), transparent 30%),
        linear-gradient(150deg, #09101b 0%, #0d1625 45%, #131a2d 100%);
      color: var(--text);
      font-family: "Trebuchet MS", "Lucida Sans Unicode", sans-serif;
    }}
    .wrap {{ width: min(1240px, calc(100% - 28px)); margin: 22px auto 38px; }}
    .hero, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 28px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }}
    .hero {{ padding: 30px; overflow: hidden; position: relative; }}
    .hero::after {{
      content: "";
      position: absolute;
      width: 300px; height: 300px; right: -80px; bottom: -100px;
      background: radial-gradient(circle, rgba(255, 182, 109, 0.18), transparent 65%);
    }}
    .eyebrow {{
      color: var(--gf);
      text-transform: uppercase;
      letter-spacing: 0.18em;
      font-size: 0.76rem;
      margin-bottom: 10px;
    }}
    h1, h2, h3 {{ margin: 0; font-family: "Century Gothic", "Trebuchet MS", sans-serif; }}
    h1 {{ font-size: clamp(2rem, 5vw, 3.3rem); max-width: 10ch; line-height: 1.04; }}
    h2 {{ font-size: 1.28rem; margin-bottom: 8px; }}
    .sub {{ color: var(--muted); margin: 0 0 16px; }}
    .hero-grid {{
      display: grid; grid-template-columns: 1.4fr 1fr; gap: 24px; align-items: start;
    }}
    .hero p {{ color: var(--muted); max-width: 58ch; margin: 14px 0 0; }}
    .cards, .grid, .status-grid {{ display: grid; gap: 16px; }}
    .cards {{ grid-template-columns: repeat(2, minmax(0, 1fr)); margin-top: 22px; }}
    .grid.two, .status-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .panel {{ padding: 22px; margin-top: 18px; }}
    .mini {{
      background: var(--panel-strong);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 16px;
    }}
    .mini small {{
      display: block;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
      font-size: 0.68rem;
    }}
    .mini strong {{ display: block; font-size: 1.45rem; }}
    .mini span {{ color: var(--muted); font-size: 0.92rem; }}
    .kv {{
      display: grid; grid-template-columns: 1fr auto; gap: 8px 12px; color: var(--muted); font-size: 0.92rem;
    }}
    .kv b {{ color: var(--text); text-align: right; font-weight: 700; }}
    .pill {{
      display: inline-flex; align-items: center; justify-content: center;
      border-radius: 999px; padding: 6px 12px; font-size: 0.78rem; font-weight: 700;
    }}
    .ok {{ background: rgba(70, 208, 134, 0.16); color: var(--ok); }}
    .warn {{ background: rgba(246, 196, 99, 0.16); color: var(--warn); }}
    .bad {{ background: rgba(255, 125, 134, 0.16); color: var(--bad); }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 11px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; font-size: 0.76rem; }}
    .tag {{
      display: inline-flex; border-radius: 999px; padding: 5px 10px; font-size: 0.78rem; font-weight: 700;
    }}
    .bars {{ display: grid; gap: 12px; }}
    .bar-head {{ display: flex; justify-content: space-between; gap: 14px; color: var(--muted); font-size: 0.9rem; }}
    .track {{ height: 14px; border-radius: 999px; background: rgba(255, 255, 255, 0.06); overflow: hidden; }}
    .fill.gf {{ height: 100%; background: linear-gradient(90deg, var(--gf), #35e0d7); }}
    .fill.gx {{ height: 100%; background: linear-gradient(90deg, var(--gx), #8cb1ff); }}
    .facts {{ display: grid; gap: 14px; }}
    .fact {{
      background: linear-gradient(180deg, rgba(18, 27, 44, 0.96), rgba(10, 16, 26, 0.96));
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
    }}
    .fact p {{ color: var(--muted); margin: 0 0 10px; }}
    .fact a {{ color: var(--gf); text-decoration: none; }}
    .log {{
      background: #08101a;
      border: 1px solid rgba(120, 162, 255, 0.22);
      border-radius: 18px;
      padding: 14px;
      max-height: 300px;
      overflow: auto;
      white-space: pre-wrap;
      font-family: Consolas, "Courier New", monospace;
      font-size: 0.82rem;
      color: #dbe4ff;
    }}
    ul.notes {{ margin: 0; padding-left: 18px; color: var(--muted); }}
    ul.notes li::marker {{ color: var(--accent); }}
    code {{ font-family: Consolas, "Courier New", monospace; background: rgba(255,255,255,0.06); padding: 2px 7px; border-radius: 8px; }}
    @media (max-width: 900px) {{
      .hero-grid, .grid.two, .status-grid, .cards {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">Social Graph Analytics Evidence Board</div>
      <div class="hero-grid">
        <div>
          <h1>GraphFrames vs GraphX</h1>
          <p id="hero-summary"></p>
          <div class="cards" id="hero-cards"></div>
        </div>
        <div class="grid">
          <div class="mini">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
              <h2>Dataset</h2>
              <span class="pill warn" id="dataset-size"></span>
            </div>
            <div class="kv" id="dataset-meta"></div>
          </div>
          <div class="mini">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
              <h2>Proof Files</h2>
              <span class="pill ok">Generated</span>
            </div>
            <div class="kv" id="proof-files"></div>
          </div>
        </div>
      </div>
    </section>

    <section class="panel grid two">
      <div>
        <h2>Run Status</h2>
        <p class="sub">Shows whether each framework actually ran and where the evidence logs were written.</p>
        <div class="status-grid" id="run-status"></div>
      </div>
      <div>
        <h2>Presentation Notes</h2>
        <p class="sub">Quick points you can say during your demo or viva.</p>
        <ul class="notes" id="notes"></ul>
      </div>
    </section>

    <section class="panel">
      <h2>Metric Agreement</h2>
      <p class="sub">Direct structural proof from the generated summary files.</p>
      <table>
        <thead><tr><th>Metric</th><th>GraphFrames</th><th>GraphX</th><th>Result</th></tr></thead>
        <tbody id="metric-table"></tbody>
      </table>
    </section>

    <section class="panel grid two">
      <div>
        <h2>Per-Algorithm Timings</h2>
        <p class="sub">Measured from benchmark CSV exports. Longer bars mean slower runtime.</p>
        <div class="bars" id="timing-bars"></div>
      </div>
      <div>
        <h2>Top PageRank Proof</h2>
        <p class="sub">These tables come from the exported top-PageRank result files.</p>
        <div class="grid two">
          <div>
            <h3 style="margin-bottom:8px">GraphFrames</h3>
            <table><thead><tr><th>Node</th><th>Score</th></tr></thead><tbody id="gf-pr"></tbody></table>
          </div>
          <div>
            <h3 style="margin-bottom:8px">GraphX</h3>
            <table><thead><tr><th>Node</th><th>Score</th></tr></thead><tbody id="gx-pr"></tbody></table>
          </div>
        </div>
        <p class="sub" id="pr-overlap" style="margin-top:14px"></p>
      </div>
    </section>

    <section class="panel">
      <h2>Research-Backed Differences</h2>
      <p class="sub">Each card links to docs or papers so you can justify architectural claims with sources.</p>
      <div class="facts" id="facts"></div>
    </section>

    <section class="panel grid two">
      <div>
        <h2>GraphFrames Log Evidence</h2>
        <p class="sub" id="gf-log-path"></p>
        <div class="log" id="gf-log"></div>
      </div>
      <div>
        <h2>GraphX Log Evidence</h2>
        <p class="sub" id="gx-log-path"></p>
        <div class="log" id="gx-log"></div>
      </div>
    </section>
  </div>

  <script>
    const DATA = {dashboard_json};
    const badge = value => value === true ? '<span class="tag ok">Matched</span>' : value === false ? '<span class="tag bad">Different</span>' : '<span class="tag warn">N/A</span>';
    const klass = value => value ? 'ok' : 'bad';
    const timeText = value => value === null || value === undefined ? 'N/A' : `${{value.toFixed(2)}} s`;
    const width = (value, max) => !value || !max ? '0%' : `${{Math.max((value / max) * 100, 1)}}%`;

    document.getElementById('hero-summary').textContent =
      `Dataset: ${{DATA.dataset.name}}. This dashboard combines benchmark CSVs, result tables, and runtime logs so you can prove implementation-level differences instead of only describing them.`;
    document.getElementById('dataset-size').textContent = `${{DATA.dataset.size_mb.toFixed(2)}} MB`;
    document.getElementById('dataset-meta').innerHTML = `
      <span>File</span><b>${{DATA.dataset.file_name}}</b>
      <span>Generated</span><b>${{DATA.generated_at}}</b>
      <span>Path</span><b><code>${{DATA.dataset.path}}</code></b>
    `;
    document.getElementById('proof-files').innerHTML = `
      <span>Text summary</span><b><code>${{DATA.artifacts.text_report}}</code></b>
      <span>JSON proof</span><b><code>${{DATA.artifacts.json_report}}</code></b>
      <span>Dashboard</span><b><code>${{DATA.artifacts.dashboard}}</code></b>
    `;
    document.getElementById('hero-cards').innerHTML = DATA.hero_cards.map(card => `
      <article class="mini">
        <small>${{card.label}}</small>
        <strong>${{card.value}}</strong>
        <span>${{card.note}}</span>
      </article>
    `).join('');
    document.getElementById('run-status').innerHTML = DATA.runs.map(run => `
      <article class="mini">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
          <h3>${{run.name}}</h3>
          <span class="pill ${{klass(run.success)}}">${{run.success ? 'Success' : 'Failed / Partial'}}</span>
        </div>
        <div class="kv">
          <span>Runtime</span><b>${{run.runtime}}</b>
          <span>Return code</span><b>${{run.returncode}}</b>
          <span>Command</span><b><code>${{run.command}}</code></b>
          <span>Log file</span><b><code>${{run.log_file}}</code></b>
        </div>
      </article>
    `).join('');
    document.getElementById('notes').innerHTML = DATA.callouts.map(item => `<li>${{item}}</li>`).join('');
    document.getElementById('metric-table').innerHTML = DATA.metrics.map(row => `
      <tr>
        <td>${{row.label}}</td>
        <td>${{row.graphframes}}</td>
        <td>${{row.graphx}}</td>
        <td>${{badge(row.match)}}</td>
      </tr>
    `).join('');
    const maxTime = Math.max(1, ...DATA.timings.map(item => item.graphframes || 0), ...DATA.timings.map(item => item.graphx || 0));
    document.getElementById('timing-bars').innerHTML = DATA.timings.map(item => {{
      const speedupText = item.speedup ? ' | ' + item.speedup.toFixed(2) + 'x GF slower' : '';
      return `
        <div>
          <div class="bar-head"><span>${{item.algorithm}}</span><span>GF: ${{timeText(item.graphframes)}} | GX: ${{timeText(item.graphx)}}${{speedupText}}</span></div>
          <div class="track" style="margin-top:6px"><div class="fill gf" style="width:${{width(item.graphframes, maxTime)}}"></div></div>
          <div class="track" style="margin-top:6px"><div class="fill gx" style="width:${{width(item.graphx, maxTime)}}"></div></div>
        </div>
      `;
    }}).join('');
    document.getElementById('gf-pr').innerHTML = DATA.pagerank.graphframes.map(row => `<tr><td>${{row.node}}</td><td>${{row.score}}</td></tr>`).join('');
    document.getElementById('gx-pr').innerHTML = DATA.pagerank.graphx.map(row => `<tr><td>${{row.node}}</td><td>${{row.score}}</td></tr>`).join('');
    document.getElementById('pr-overlap').textContent = `Top-node overlap: ${{DATA.pagerank.overlap.count}} / ${{DATA.pagerank.overlap.total}}. Shared nodes: ${{DATA.pagerank.overlap.nodes.join(', ') || 'None'}}.`;
    document.getElementById('facts').innerHTML = DATA.research_facts.map(fact => `
      <article class="fact">
        <h3 style="margin-bottom:8px">${{fact.aspect}}</h3>
        <p><strong>GraphFrames:</strong> ${{fact.graphframes}}</p>
        <p><strong>GraphX:</strong> ${{fact.graphx}}</p>
        <p><strong>Social-media angle:</strong> ${{fact.social_media}}</p>
        <p><a href="${{fact.source_url}}" target="_blank" rel="noreferrer">${{fact.source_label}}</a></p>
      </article>
    `).join('');
    document.getElementById('gf-log-path').innerHTML = `<code>${{DATA.log_paths.graphframes}}</code>`;
    document.getElementById('gx-log-path').innerHTML = `<code>${{DATA.log_paths.graphx}}</code>`;
    document.getElementById('gf-log').textContent = DATA.log_snippets.graphframes.join('\\n');
    document.getElementById('gx-log').textContent = DATA.log_snippets.graphx.join('\\n');
  </script>
</body>
</html>
"""
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(html_page)


def write_text_summary(payload: Dict[str, object]) -> str:
    """Write the plain-text comparison summary."""
    report_file = payload["artifacts"]["text_report"]  # type: ignore[index]
    metrics = payload["metrics"]  # type: ignore[index]
    runs = payload["runs"]  # type: ignore[index]

    with open(report_file, "w", encoding="utf-8") as handle:
        handle.write("GraphFrames vs GraphX - Comparison Results\n")
        handle.write(f"Dataset: {payload['dataset']['name']}\n")
        handle.write("=" * 70 + "\n\n")
        handle.write(f"{'Metric':<36} {'GraphFrames':<16} {'GraphX':<16} {'Result'}\n")
        handle.write("-" * 70 + "\n")
        for row in metrics:
            result = "Matched" if row["match"] is True else "Different" if row["match"] is False else "N/A"
            handle.write(
                f"{row['label']:<36} {row['graphframes']:<16} {row['graphx']:<16} {result}\n"
            )

        handle.write("\nRun Status\n")
        handle.write("-" * 70 + "\n")
        for run in runs:
            handle.write(
                f"{run['name']:<12} Success={run['success']}  Runtime={run['runtime']:<10}  Log={run['log_file']}\n"
            )

        handle.write("\nPresentation Notes\n")
        handle.write("-" * 70 + "\n")
        for item in payload["callouts"]:  # type: ignore[index]
            handle.write(f"- {item}\n")

        handle.write("\nDashboard\n")
        handle.write("-" * 70 + "\n")
        handle.write(f"{payload['artifacts']['dashboard']}\n")

    return report_file


def write_json_summary(payload: Dict[str, object]) -> str:
    """Write the machine-readable JSON proof file."""
    json_file = payload["artifacts"]["json_report"]  # type: ignore[index]
    with open(json_file, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return json_file


def write_latency_csv(payload: Dict[str, object]) -> str:
    """Write per-algorithm latency, speedup, and winner rows."""
    csv_file = payload["artifacts"]["timing_csv"]  # type: ignore[index]
    rows = payload["timings"]  # type: ignore[index]
    with open(csv_file, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "algorithm",
                "graphframes_seconds",
                "graphx_seconds",
                "graphframes_over_graphx_speedup",
                "best_framework",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "algorithm": row.get("algorithm", ""),
                    "graphframes_seconds": row.get("graphframes", ""),
                    "graphx_seconds": row.get("graphx", ""),
                    "graphframes_over_graphx_speedup": row.get("speedup", ""),
                    "best_framework": row.get("best_framework", "N/A"),
                }
            )
    return csv_file


def write_run_log_csv(payload: Dict[str, object]) -> str:
    """Write framework-level runtime and log path metadata."""
    csv_file = payload["artifacts"]["run_log_csv"]  # type: ignore[index]
    runs = payload["runs"]  # type: ignore[index]
    with open(csv_file, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "framework",
                "success",
                "returncode",
                "runtime",
                "command",
                "stdout_log",
                "error_log",
            ],
        )
        writer.writeheader()
        for run in runs:
            writer.writerow(
                {
                    "framework": run.get("name", ""),
                    "success": run.get("success", ""),
                    "returncode": run.get("returncode", ""),
                    "runtime": run.get("runtime", ""),
                    "command": run.get("command", ""),
                    "stdout_log": run.get("log_file", ""),
                    "error_log": run.get("error_file", ""),
                }
            )
    return csv_file


def generate_outputs(
    dataset_path: str,
    dataset_name: str,
    gf_result: Dict[str, object],
    gx_result: Dict[str, object],
    artifacts: Dict[str, str],
) -> Dict[str, object]:
    """Build and write all comparison artifacts."""
    payload = build_comparison_payload(dataset_path, dataset_name, gf_result, gx_result, artifacts)
    write_text_summary(payload)
    write_json_summary(payload)
    write_latency_csv(payload)
    write_run_log_csv(payload)
    render_dashboard(payload, payload["artifacts"]["dashboard"])  # type: ignore[index]

    print("\n" + "=" * 64)
    print("  COMPARISON ARTIFACTS GENERATED")
    print("=" * 64)
    print(f"  Text summary : {payload['artifacts']['text_report']}")
    print(f"  JSON proof   : {payload['artifacts']['json_report']}")
    print(f"  Latency CSV  : {payload['artifacts']['timing_csv']}")
    print(f"  Run log CSV  : {payload['artifacts']['run_log_csv']}")
    print(f"  Dashboard    : {payload['artifacts']['dashboard']}")

    return payload


def run_full_comparison(dataset_path: str, output_dir: Optional[str] = None) -> Dict[str, object]:
    """Execute both frameworks and write all output artifacts."""
    dataset_name = get_dataset_name(dataset_path)
    artifacts = build_artifact_paths(resolve_report_dir(output_dir))
    gf_result = run_graphframes(dataset_path, artifacts)
    gx_result = run_graphx(dataset_path, artifacts)
    return generate_outputs(dataset_path, dataset_name, gf_result, gx_result, artifacts)


def build_reused_graphframes_result(artifacts: Dict[str, str]) -> Dict[str, object]:
    """Represent an already completed GraphFrames run without executing it again."""
    summary_path = os.path.join(GF_OUTPUT_DIR, "graph_summary.csv")
    timings_path = os.path.join(GF_OUTPUT_DIR, "benchmark_timings.csv")
    if not os.path.exists(summary_path) or not os.path.exists(timings_path):
        print("[ERROR] Cannot reuse GraphFrames output because required files are missing.")
        print(f"  Missing/expected summary : {summary_path}")
        print(f"  Missing/expected timings : {timings_path}")
        sys.exit(1)

    benchmarks = read_benchmark_csv(timings_path)
    runtime = benchmarks.get("TOTAL", 0.0)
    log_file = artifacts.get("graphframes_log", "")
    output = ""
    if log_file and os.path.exists(log_file):
        with open(log_file, "r", encoding="utf-8", errors="replace") as handle:
            output = handle.read()

    print("\n" + "=" * 64)
    print("  PHASE 1: GRAPHFRAMES (reusing completed output)")
    print("=" * 64)
    print(f"  Summary: {summary_path}")
    print(f"  Timings: {timings_path}")
    if runtime:
        print(f"  Runtime: {runtime:.1f}s")

    return {
        "success": True,
        "returncode": 0,
        "time": runtime,
        "output": output,
        "stderr": "",
        "log_file": log_file,
        "error_file": "",
        "command": "reused existing GraphFrames output",
    }


def main() -> None:
    """Program entry point."""
    explicit = None
    output_dir = None
    skip_graphframes = False

    args = iter(sys.argv[1:])
    for arg in args:
        if arg == "--output-dir":
            output_dir = next(args, None)
        elif arg == "--skip-graphframes":
            skip_graphframes = True
        elif explicit is None:
            explicit = arg

    dataset_path = discover_dataset(explicit)
    dataset_name = get_dataset_name(dataset_path)
    file_size_mb = os.path.getsize(dataset_path) / (1024 * 1024)
    report_dir = resolve_report_dir(output_dir)
    artifacts = build_artifact_paths(report_dir)

    print("+" + "=" * 66 + "+")
    print("|   BDA Social Graph -- Unified Comparison + Dashboard Runner       |")
    print("|   GraphFrames (Python) vs GraphX (Scala) with proof artifacts     |")
    print("+" + "=" * 66 + "+")
    print(f"\n  Dataset:  {dataset_name}")
    print(f"  File:     {os.path.basename(dataset_path)}")
    print(f"  Size:     {file_size_mb:.2f} MB")
    print(f"  Path:     {dataset_path}")
    print(f"  Report:   {report_dir}")

    total_start = time.time()
    if skip_graphframes:
        gf_result = build_reused_graphframes_result(artifacts)
    else:
        gf_result = run_graphframes(dataset_path, artifacts)
    gx_result = run_graphx(dataset_path, artifacts)
    generate_outputs(dataset_path, dataset_name, gf_result, gx_result, artifacts)

    total_time = time.time() - total_start
    print(f"\n  Total comparison time: {total_time:.1f}s")
    print("\n  Presentation shortcut:")
    print(f"    Open: {artifacts['dashboard']}")


if __name__ == "__main__":
    main()
