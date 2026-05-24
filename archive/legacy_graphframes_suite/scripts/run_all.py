"""
run_all.py
==========
Master Pipeline Runner for Dynamic Social Graph Analysis.
Auto-detects the SNAP dataset in data_lake/ and runs
all modules in sequence.

Usage:
  python run_all.py                        # auto-detect dataset
  python run_all.py data_lake/myfile.txt   # explicit dataset

Author  : BDA Social Graph Project
"""

import subprocess
import sys
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VENV_PY  = os.path.join(BASE_DIR, "venv", "Scripts", "python.exe")

# Fallback to system Python if venv not found
if not os.path.exists(VENV_PY):
    VENV_PY = sys.executable

MODULES = [
    ("Core GraphFrames Analytics", "graph_analytics.py"),
    ("Advanced Centrality",        "advanced_centrality.py"),
    ("Community Detection",        "community_detection_advanced.py"),
    ("Link Prediction ML",         "link_prediction.py"),
    ("Network Resilience",         "network_resilience.py"),
    ("Graph Embeddings Node2Vec",  "graph_embeddings.py"),
    ("Visualization Export",       "visualization_engine.py"),
]


def run_module(name, script_path, dataset_arg=None):
    print(f"\n  >>> Starting: {name}...")
    start_time = time.time()

    cmd = [VENV_PY, script_path]
    if dataset_arg:
        cmd.append(dataset_arg)

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8"
    )
    elapsed = time.time() - start_time
    
    log_dir = os.path.join(BASE_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    script_base_name = os.path.basename(script_path)

    if result.returncode == 0:
        status = "[OK]"
        print(f"\n  {status} {name} ({elapsed:.1f}s)")
        log_file = os.path.join(log_dir, f"{script_base_name}.log")
        print(f"      Log saved to {log_file}")
        with open(log_file, "w", encoding="utf-8") as f:
            f.write(result.stdout)
        return True
    else:
        status = "[FAILED]"
        print(f"\n  {status} {name} ({elapsed:.1f}s)")
        error_log_file = os.path.join(log_dir, f"{script_base_name}.error.log")
        print(f"      Error Log saved to {error_log_file}")
        with open(error_log_file, "w", encoding="utf-8") as f:
            f.write(result.stderr)
        return False


if __name__ == "__main__":
    # Optional: pass dataset path as CLI argument
    dataset_arg = sys.argv[1] if len(sys.argv) > 1 else None

    print("+======================================================+")
    print("|    BDA Social Graph — Dynamic Analysis Pipeline     |")
    print("|         Auto-detects SNAP Dataset in data_lake/     |")
    print("+======================================================+")

    if dataset_arg:
        print(f"\n  Using dataset: {dataset_arg}")
    else:
        print(f"\n  Auto-detecting dataset in: {os.path.join(BASE_DIR, 'data_lake')}")

    # Create output directories
    for subdir in ["graph_analytics", "centrality", "communities",
                   "link_prediction", "resilience", "embeddings",
                   "visualization"]:
        os.makedirs(os.path.join(BASE_DIR, "outputs", subdir), exist_ok=True)
    os.makedirs(os.path.join(os.path.dirname(BASE_DIR), "models"), exist_ok=True)

    results = {}
    total_start = time.time()

    for name, script in MODULES:
        script_full = os.path.join(BASE_DIR, script)
        success = run_module(name, script_full, dataset_arg)
        results[name] = success

    total_time = time.time() - total_start
    print(f"\n{'=' * 60}")
    print("  PIPELINE SUMMARY")
    print(f"{'=' * 60}")
    for name, ok in results.items():
        icon = "[OK]" if ok else "[FAILED]"
        print(f"  {icon}  {name}")
    print(f"\n  Total time: {total_time:.1f}s")
    print(f"\n  View visualization: "
          f"{os.path.join(BASE_DIR, 'outputs', 'visualization', 'graph_interactive.html')}")
