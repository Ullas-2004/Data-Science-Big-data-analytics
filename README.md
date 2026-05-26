# Big Data Social Graph Analytics

Professional Big Data Analytics project comparing **GraphFrames** and **GraphX** for large-scale social graph processing. The project includes a unified runner, Spark-based analytics pipelines, generated proof artifacts, a local dashboard, reports, and a demo video.

## Project Highlights

- Compares GraphFrames and GraphX on SNAP-style edge-list datasets.
- Implements PageRank, connected components, triangle count, degree analysis, shortest paths, and community-related analytics.
- Produces repeatable proof artifacts for timings, metrics, and framework comparison.
- Includes an interactive dashboard for running datasets and reviewing results.
- Keeps older experiments archived while the main tree stays focused on the active comparison pipeline.

## Tech Stack

| Area | Tools |
| --- | --- |
| Big Data | Apache Spark, GraphFrames, GraphX |
| Programming | Python, Scala |
| Analytics | PageRank, triangle count, connected components, shortest paths, degree analysis |
| Frontend | HTML, CSS, JavaScript |
| Reporting | PDF report, PowerPoint, generated dashboards, JSON/CSV proof outputs |

## Repository Structure

```text
.
|-- run_comparison.py              # Unified GraphFrames vs GraphX runner
|-- dashboard_server.py            # Local dashboard/API server
|-- GraphFrames/                   # Python GraphFrames pipeline
|-- GraphX/                        # Scala GraphX pipeline
|-- Comparison_Report/             # Reports, dashboards, metrics, proof outputs
|-- webapp/                        # Interactive browser frontend
|-- archive/                       # Legacy experiments kept for reference
|-- Report.pdf                     # Final report
|-- PPT.pptx                       # Project presentation
```

## Demo

Project demo video: [Watch on Google Drive](https://drive.google.com/file/d/1vcN0Eg701Q6x2vrqdl858Dp3PG9it0w3/view?usp=sharing)

## How to Run

Run the complete comparison pipeline:

```cmd
python run_comparison.py
```

Run with a specific dataset:

```cmd
python run_comparison.py data_lake/facebook_combined.txt.gz
python run_comparison.py data_lake/twitter_combined.txt.gz
```

Start the interactive dashboard:

```cmd
python dashboard_server.py
```

Then open:

```text
http://127.0.0.1:8000
```

## Outputs

The project generates evidence in `Comparison_Report/`, including:

- `comparison_results.txt` for side-by-side summary results.
- `comparison_data.json` for structured proof data.
- `dashboard.html` for presentation-ready visual review.
- Per-run evidence folders under `Comparison_Report/runs/`.

## Key Findings

- GraphFrames is easier to use from Python and works well for data science workflows.
- GraphX provides stronger execution performance because it runs natively on Scala/Spark.
- On the tested social graph workflow, GraphX was measured faster while both frameworks produced matching analytical results for key graph metrics.

## Notes

Large datasets and generated temporary files are intentionally excluded from GitHub. Add SNAP edge-list datasets locally under `data_lake/` before running full experiments.
