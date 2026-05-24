from __future__ import annotations

import csv
import subprocess
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from pymongo import MongoClient


ROOT = Path(__file__).resolve().parents[2]
RESULTS_ROOT = ROOT / "results"
RESULTS = RESULTS_ROOT / "csv"
REPORTS = RESULTS_ROOT / "reports"
VISUALS = ROOT / "visualizations"
HDFS_CMD = Path(r"C:\hadoop-3.3.6\bin\hdfs.cmd")
HDFS_RESULTS = "/ksrtc/results"
INF_DISTANCE = 9223372036854775807


def export_hdfs_csv(result_dir: str, local_file: Path) -> None:
    local_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [str(HDFS_CMD), "dfs", "-cat", f"{HDFS_RESULTS}/{result_dir}/part-*.csv"]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True)
    local_file.write_text(out.stdout, encoding="utf-8")


def load_results() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    pagerank_path = RESULTS / "pagerank_results.csv"
    shortest_path = RESULTS / "shortest_paths.csv"
    components_path = RESULTS / "connected_components.csv"

    export_hdfs_csv("pagerank", pagerank_path)
    export_hdfs_csv("shortest_paths", shortest_path)
    export_hdfs_csv("connected_components", components_path)

    pagerank = pd.read_csv(pagerank_path)
    shortest = pd.read_csv(shortest_path)
    components = pd.read_csv(components_path)
    return pagerank, shortest, components


def load_stops() -> pd.DataFrame:
    stops = pd.read_csv(ROOT / "data" / "raw" / "synthetic_bus_stops.csv")
    stops["stop_id"] = pd.to_numeric(stops["stop_id"], errors="coerce").astype("Int64")
    return stops[["stop_id", "stop_name"]]


def top_hubs_table(pagerank: pd.DataFrame, stops: pd.DataFrame) -> pd.DataFrame:
    merged = pagerank.merge(stops, on="stop_id", how="left")
    merged = merged.sort_values("pagerank", ascending=False).head(10).copy()
    merged["rank_score"] = merged["pagerank"].round(6)
    return merged[["stop_id", "stop_name", "rank_score"]]


def shortest_path_example(stops: pd.DataFrame, source: int = 101, target: int = 130) -> list[int]:
    stop_ids = sorted(stops["stop_id"].dropna().astype(int).tolist())
    graph = nx.DiGraph()
    for i in range(len(stop_ids) - 1):
        graph.add_edge(stop_ids[i], stop_ids[i + 1], weight=1)
    if source not in graph:
        source = stop_ids[0]
    if target not in graph:
        target = stop_ids[min(len(stop_ids) - 1, 20)]
    return nx.shortest_path(graph, source=source, target=target, weight="weight")


def component_summary(components: pd.DataFrame) -> pd.DataFrame:
    out = (
        components.groupby("component_id", dropna=False)["stop_id"]
        .count()
        .reset_index(name="stop_count")
        .sort_values("stop_count", ascending=False)
    )
    return out


def mongo_counts() -> dict[str, int]:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ksrtc_db"]
    names = [
        "bus_stops",
        "routes",
        "gps_data",
        "weather_data",
        "schedule_data",
        "route_analysis",
    ]
    return {name: db[name].count_documents({}) for name in names}


def plot_pagerank(top_hubs: pd.DataFrame) -> Path:
    VISUALS.mkdir(parents=True, exist_ok=True)
    fig_path = VISUALS / "top_pagerank_hubs.png"
    labels = [
        f"{int(row.stop_id)}\n{(row.stop_name or 'Unknown')[:12]}"
        for row in top_hubs.itertuples(index=False)
    ]

    plt.figure(figsize=(12, 6))
    plt.bar(labels, top_hubs["rank_score"], color="#1565C0")
    plt.title("Top Bus Stops by PageRank")
    plt.xlabel("Stop")
    plt.ylabel("Rank Score")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(fig_path, dpi=180)
    plt.close()
    return fig_path


def plot_components(summary: pd.DataFrame) -> Path:
    fig_path = VISUALS / "connected_component_sizes.png"
    plt.figure(figsize=(8, 5))
    plt.bar(summary["component_id"].astype(str), summary["stop_count"], color="#2E7D32")
    plt.title("Connected Component Sizes")
    plt.xlabel("Component ID")
    plt.ylabel("Number of Stops")
    plt.tight_layout()
    plt.savefig(fig_path, dpi=180)
    plt.close()
    return fig_path


def plot_shortest_distribution(shortest: pd.DataFrame) -> Path:
    fig_path = VISUALS / "shortest_path_distances.png"
    finite = shortest[shortest["distance_from_source"] < INF_DISTANCE].copy()
    finite = finite.sort_values("distance_from_source")
    plt.figure(figsize=(10, 5))
    plt.plot(
        finite["stop_id"].astype(int).astype(str),
        finite["distance_from_source"],
        marker="o",
        color="#EF6C00",
    )
    plt.title("Shortest Path Distances to Landmark Stop")
    plt.xlabel("Stop ID")
    plt.ylabel("Distance")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(fig_path, dpi=180)
    plt.close()
    return fig_path


def plot_network(stops: pd.DataFrame) -> Path:
    fig_path = VISUALS / "route_network_graph.png"
    nodes = sorted(stops["stop_id"].dropna().astype(int).tolist())[:25]
    labels = {
        int(row.stop_id): row.stop_name
        for row in stops[stops["stop_id"].isin(nodes)].itertuples(index=False)
    }
    graph = nx.Graph()
    for node in nodes:
        graph.add_node(node)
    for i in range(len(nodes) - 1):
        graph.add_edge(nodes[i], nodes[i + 1])

    pos = nx.spring_layout(graph, seed=42)
    plt.figure(figsize=(12, 8))
    nx.draw_networkx_nodes(graph, pos, node_size=450, node_color="#80CBC4")
    nx.draw_networkx_edges(graph, pos, width=1.5, edge_color="#37474F")
    nx.draw_networkx_labels(
        graph,
        pos,
        labels={k: f"{k}" for k in graph.nodes()},
        font_size=8,
    )
    plt.title("KSRTC Route Network (Sampled)")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(fig_path, dpi=180)
    plt.close()
    return fig_path


def plot_architecture() -> Path:
    fig_path = VISUALS / "architecture_diagram.png"
    blocks = [
        "Raw Dataset",
        "HDFS Storage",
        "Apache Spark ETL",
        "GraphX Analytics",
        "MongoDB",
        "API / Results",
    ]

    fig, ax = plt.subplots(figsize=(8, 10))
    ax.set_axis_off()
    y = 0.92
    for i, label in enumerate(blocks):
        rect = plt.Rectangle((0.2, y - 0.08), 0.6, 0.08, fc="#E3F2FD", ec="#1565C0", lw=1.5)
        ax.add_patch(rect)
        ax.text(0.5, y - 0.04, label, ha="center", va="center", fontsize=11)
        if i < len(blocks) - 1:
            ax.annotate(
                "",
                xy=(0.5, y - 0.10),
                xytext=(0.5, y - 0.16),
                arrowprops=dict(arrowstyle="->", lw=1.5, color="#263238"),
            )
        y -= 0.16
    plt.tight_layout()
    plt.savefig(fig_path, dpi=180)
    plt.close()
    return fig_path


def write_analysis_markdown(
    top_hubs: pd.DataFrame,
    shortest: pd.DataFrame,
    path_example: list[int],
    component_sizes: pd.DataFrame,
    counts: dict[str, int],
) -> Path:
    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / "final_analysis_summary.md"
    finite = shortest[shortest["distance_from_source"] < INF_DISTANCE]

    with out.open("w", encoding="utf-8", newline="\n") as f:
        f.write("# KSRTC Final Analysis Summary\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("## Top Bus Hubs (PageRank)\n\n")
        f.write("| Stop ID | Stop Name | Rank Score |\n")
        f.write("|---|---|---|\n")
        for row in top_hubs.itertuples(index=False):
            f.write(f"| {int(row.stop_id)} | {row.stop_name} | {row.rank_score} |\n")
        f.write("\nInterpretation: Higher PageRank indicates more central bus hubs.\n\n")

        f.write("## Shortest Path Analysis\n\n")
        f.write(
            f"- Reachable stops from landmark (finite distance): {len(finite)} / {len(shortest)}\n"
        )
        f.write(f"- Example path: {' -> '.join(map(str, path_example))}\n\n")

        f.write("## Connected Components\n\n")
        f.write("| Component ID | Stop Count |\n")
        f.write("|---|---|\n")
        for row in component_sizes.itertuples(index=False):
            f.write(f"| {int(row.component_id)} | {int(row.stop_count)} |\n")
        f.write("\nInterpretation: Components represent route clusters in the bus network.\n\n")

        f.write("## MongoDB Collection Counts\n\n")
        f.write("| Collection | Count |\n")
        f.write("|---|---|\n")
        for name, count in counts.items():
            f.write(f"| {name} | {count} |\n")
        f.write("\n")

        f.write("## Generated Visual Files\n\n")
        for file_name in [
            "top_pagerank_hubs.png",
            "shortest_path_distances.png",
            "connected_component_sizes.png",
            "route_network_graph.png",
            "architecture_diagram.png",
        ]:
            f.write(f"- visualizations/{file_name}\n")
    return out


def write_auxiliary_csvs(top_hubs: pd.DataFrame, component_sizes: pd.DataFrame, path_example: list[int]) -> None:
    top_hubs.to_csv(RESULTS / "top_pagerank_hubs.csv", index=False)
    component_sizes.to_csv(RESULTS / "connected_component_summary.csv", index=False)
    with (RESULTS / "shortest_path_example.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["step", "stop_id"])
        for idx, stop_id in enumerate(path_example, start=1):
            writer.writerow([idx, stop_id])


def main() -> int:
    pagerank, shortest, components = load_results()
    stops = load_stops()

    top_hubs = top_hubs_table(pagerank, stops)
    path_example = shortest_path_example(stops)
    component_sizes = component_summary(components)
    counts = mongo_counts()

    write_auxiliary_csvs(top_hubs, component_sizes, path_example)
    plot_pagerank(top_hubs)
    plot_shortest_distribution(shortest)
    plot_components(component_sizes)
    plot_network(stops)
    plot_architecture()
    summary = write_analysis_markdown(top_hubs, shortest, path_example, component_sizes, counts)

    print("Generated report assets successfully:")
    print(f"- {summary}")
    print(f"- {VISUALS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
