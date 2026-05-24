# Archive

This folder stores files that are not part of the active GraphFrames vs GraphX comparison pipeline but were kept for reference instead of being permanently deleted.

## `legacy_graphframes_suite/`

Contains older GraphFrames-only side modules and their generated outputs/logs:

- `scripts/`
  - `advanced_centrality.py`
  - `community_detection_advanced.py`
  - `graph_embeddings.py`
  - `link_prediction.py`
  - `network_resilience.py`
  - `run_all.py`
  - `visualization_engine.py`
- `outputs/`
  - centrality
  - communities
  - embeddings
  - link prediction
  - resilience
  - visualization
- `logs/`
  - log files from the legacy batch runner

These files were archived because the active project now centers on:

1. `GraphFrames/graph_analytics.py`
2. `GraphX/GraphAnalytics.scala`
3. `run_comparison.py`
4. `dashboard_server.py`
5. `webapp/`
