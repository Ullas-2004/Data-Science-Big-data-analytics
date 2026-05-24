# GraphFrames Module

This folder now contains the active GraphFrames implementation used by the current GraphFrames vs GraphX comparison project.

## Active File

- `graph_analytics.py`
  - loads a SNAP dataset from `data_lake/`
  - builds a GraphFrame from vertices and edges
  - runs PageRank, Label Propagation, Triangle Count, Connected Components, Degree Analysis, and Shortest Paths
  - exports:
    - `outputs/graph_analytics/graph_summary.csv`
    - `outputs/graph_analytics/benchmark_timings.csv`
    - algorithm result CSV folders used by the comparison dashboard

## Current Output Folder

```text
outputs/graph_analytics/
```

This is the only GraphFrames output path used by the active comparison runner in `run_comparison.py`.

## Archived Legacy Work

Older side modules such as:

- advanced centrality
- community profiling
- link prediction
- resilience simulation
- graph embeddings
- standalone visualization export

were moved to:

```text
../archive/legacy_graphframes_suite/
```

They are preserved for reference, but they are not part of the active comparison workflow or dashboard.
