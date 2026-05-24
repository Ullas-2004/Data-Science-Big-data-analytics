#!/usr/bin/env bash
set -e
python backend/spark_jobs/graph_analysis.py
python backend/spark_jobs/route_optimization.py
python backend/spark_jobs/traffic_analysis.py
