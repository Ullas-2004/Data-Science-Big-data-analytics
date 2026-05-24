# BDA Social Graph Analytics
## GraphFrames vs GraphX Performance Comparison

This repository is now organized around the active comparison pipeline:

1. `run_comparison.py` for the main head-to-head execution
2. `GraphFrames/graph_analytics.py` for the active GraphFrames implementation
3. `GraphX/GraphAnalytics.scala` for the active GraphX implementation
4. `dashboard_server.py` and `webapp/` for the interactive frontend
5. `Comparison_Report/` for generated evidence, reports, and presentation scripts

Older experimental GraphFrames modules were moved to `archive/legacy_graphframes_suite/` so the main tree stays focused on the current project.

**Course:** Big Data Analytics (4th Semester)
**Dataset:** Any SNAP edge-list (auto-detected from data_lake/)
**Current:** Facebook SNAP Combined (4,039 nodes, 88,234 edges)

---

## Quick Start — Unified Runner

Run both frameworks head-to-head on any dataset with a single command:

```cmd
cd C:\DSAI\4th_sem\BDA\BigData_SocialGraph

:: Auto-detect dataset in data_lake/
python run_comparison.py

:: Or specify a dataset explicitly
python run_comparison.py data_lake/facebook_combined.txt.gz
python run_comparison.py data_lake/twitter_combined.txt
python run_comparison.py data_lake/any_snap_file.txt.gz
```

This now generates three presentation-ready proof artifacts in `Comparison_Report/`:

- `comparison_results.txt` — plain-text side-by-side summary
- `comparison_data.json` — structured proof data for the project
- `dashboard.html` — polished frontend for class/demo presentation

### Interactive Web App

Launch the local multi-dataset frontend with:

```cmd
cd C:\DSAI\4th_sem\BDA\BigData_SocialGraph
python dashboard_server.py
```

Then open:

```text
http://127.0.0.1:8000
```

The web app lets you:

- pick any SNAP dataset from `data_lake/`
- run GraphFrames and GraphX from the browser
- switch between saved dataset runs
- inspect latency, metric differences, proof previews, and research-backed architecture notes
- keep each dataset in `Comparison_Report/runs/<dataset-slug>/` so nothing is overwritten

### Run Individually

```cmd
:: GraphFrames only (Python)
venv\Scripts\python.exe GraphFrames\graph_analytics.py

:: GraphX only (Scala)
spark-shell -i GraphX\GraphAnalytics.scala
```

### Archived Legacy Modules

Older GraphFrames side experiments such as advanced centrality, link prediction, resilience, embeddings, and visualization exports were moved to:

```text
archive/legacy_graphframes_suite/
```

They are preserved for reference, but they are not part of the active GraphFrames vs GraphX comparison flow.

---

## Project Structure

```text
BigData_SocialGraph/
|-- run_comparison.py                    # Unified comparison runner
|-- dashboard_server.py                  # Local API/server for the dashboard
|-- README.md
|
|-- GraphFrames/
|   |-- graph_analytics.py               # Active GraphFrames analytics pipeline
|   |-- README.md
|   +-- outputs/
|       +-- graph_analytics/             # Current GraphFrames output exports
|
|-- GraphX/
|   |-- GraphAnalytics.scala             # Active GraphX analytics pipeline
|   +-- outputs/                         # Current GraphX output exports
|
|-- data_lake/                           # SNAP datasets
|   |-- facebook_combined.txt.gz
|   +-- twitter_combined.txt.gz
|
|-- webapp/
|   |-- index.html
|   |-- styles.css
|   +-- app.js
|
|-- Comparison_Report/
|   |-- comparison_results.txt           # Latest plain-text comparison
|   |-- comparison_data.json             # Latest JSON proof bundle
|   |-- dashboard.html                   # Latest generated standalone dashboard
|   |-- reports/                         # Final report drafts
|   |-- presentations/                   # Presentation scripts
|   +-- runs/                            # Saved per-dataset evidence bundles
|
+-- archive/
    +-- legacy_graphframes_suite/        # Older side modules and outputs
```

## Algorithms Implemented

| # | Algorithm | GraphFrames | GraphX | Result Match? |
|:-:|:----------|:-----------:|:------:|:-------------:|
| 1 | PageRank | Yes | Yes | YES |
| 2 | Label Propagation | Yes | Yes | ~diff (non-deterministic) |
| 3 | Triangle Count | Yes | Yes | YES (1,612,010) |
| 4 | Connected Components | Yes | Yes | YES (1) |
| 5 | Degree Analysis | Yes | Yes | YES |
| 6 | Shortest Paths | Yes | Yes | YES |
| 7 | Clustering Coefficient | - | Yes | - |
| 8 | Graph Density | - | Yes | - |
| 9 | Community Profiling | - | Yes | - |

## Key Findings

- Both frameworks produce identical analytical results
- GraphX runs **2.4x faster** (~18s vs ~44s)
- GraphFrames is more accessible for Python teams
- GraphX has zero Python overhead and no platform bugs
- Fully dynamic: works on any SNAP edge-list dataset
- Dashboard shows proof from generated logs, metrics, timings, and source-backed differences

See `Comparison_Report/reports/report.md` for the focused report and `Comparison_Report/reports/final_combined_research_report.md` for the expanded final document.

## Supported Datasets

Drop any SNAP edge-list file into `data_lake/` and it auto-detects:
- `.txt` / `.txt.gz` (space/tab separated)
- `.csv` / `.csv.gz` (comma separated)
- `.tsv` / `.tsv.gz` (tab separated)

Download datasets from: https://snap.stanford.edu/data/
