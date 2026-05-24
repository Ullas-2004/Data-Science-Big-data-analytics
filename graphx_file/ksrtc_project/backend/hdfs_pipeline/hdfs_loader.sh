#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOCAL_DATA_FILE="${1:-$PROJECT_ROOT/data/ksrtc_data.csv}"
HDFS_TARGET_DIR="${2:-/ksrtc/raw}"

if [[ ! -f "$LOCAL_DATA_FILE" ]]; then
  echo "Input file not found: $LOCAL_DATA_FILE"
  exit 1
fi

echo "Creating HDFS directory: $HDFS_TARGET_DIR"
hdfs dfs -mkdir -p "$HDFS_TARGET_DIR"

echo "Uploading data to HDFS..."
hdfs dfs -put -f "$LOCAL_DATA_FILE" "$HDFS_TARGET_DIR/"

echo "HDFS load complete: $HDFS_TARGET_DIR/$(basename "$LOCAL_DATA_FILE")"
