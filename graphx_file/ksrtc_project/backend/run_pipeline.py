#!/usr/bin/env python3
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
RESULTS = ROOT / "results" / "csv"
SPARK_JOBS = BACKEND / "spark_jobs"
MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0"


def run(cmd: list[str], step: str) -> bool:
    try:
        subprocess.run(cmd, check=True, cwd=ROOT)
        print(f"{step}: completed")
        return True
    except FileNotFoundError:
        print(f"{step}: skipped (command not found: {cmd[0]})")
    except subprocess.CalledProcessError as exc:
        print(f"{step}: failed ({exc})")
    return False


def ensure_seed_outputs() -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    pagerank = RESULTS / "pagerank.csv"
    shortest = RESULTS / "shortest_paths.csv"
    routes = RESULTS / "predictions.csv"

    if not pagerank.exists():
        pagerank.write_text("stop_id,pagerank\n101,0.02\n102,0.03\n103,0.025\n", encoding="utf-8")
    if not shortest.exists():
        shortest.write_text("source_stop,destination_stop,estimated_cost\n101,102,1\n102,103,1\n", encoding="utf-8")
    if not routes.exists():
        routes.write_text("stop,prediction\nStop-101,52.5\nStop-102,48.1\n", encoding="utf-8")


def main() -> int:
    ensure_seed_outputs()

    spark_shell = shutil.which("spark-shell")
    if spark_shell:
        run(
            [
                spark_shell,
                "--packages",
                MONGO_SPARK_PACKAGE,
                "-i",
                str(SPARK_JOBS / "data_processing.scala"),
            ],
            "Spark preprocessing",
        )
        run(
            [
                spark_shell,
                "--packages",
                MONGO_SPARK_PACKAGE,
                "-i",
                str(SPARK_JOBS / "graph_route_engine.scala"),
            ],
            "GraphX route engine",
        )
        run(
            [
                spark_shell,
                "--packages",
                MONGO_SPARK_PACKAGE,
                "-i",
                str(SPARK_JOBS / "store_to_mongodb.scala"),
            ],
            "MongoDB storage job",
        )
    else:
        print("Spark jobs skipped (spark-shell not found).")

    run([sys.executable, str(BACKEND / "api" / "app.py")], "Flask API startup")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
