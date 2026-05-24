"""Compatibility Spark entrypoint.

Uses existing Scala GraphX pipeline artifacts in backend/spark_jobs.
"""
from pathlib import Path
import subprocess
import sys


def main() -> int:
    scala_job = Path(__file__).with_name("graph_route_engine.scala")
    print(f"Primary GraphX job is Scala: {scala_job}")
    print("Run with spark-shell or use backend/run_pipeline.py for orchestrated execution.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
