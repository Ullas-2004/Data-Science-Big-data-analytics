"""
visualization_engine.py
========================
Multi-Format Graph Visualization & Export Engine

Exports graph data in formats suitable for:
  1. Gephi (GEXF format) - Full graph with community colors
  2. Interactive D3.js HTML - Self-contained browser visualization
  3. NetworkX matplotlib plots - Static analysis charts
  4. GeoJSON-style JSON - For web dashboards

All visualizations include:
  - Community coloring
  - Node size = degree centrality
  - Edge weight = connection strength
  - Influence score overlay

Author  : BDA Social Graph Project
Dataset : Facebook SNAP (4039 nodes, 88234 edges)
"""

import json
import math
import random
import collections
import os
from datetime import datetime


# ---------------------------------------------
#  Color Palette for Communities
# ---------------------------------------------
COMMUNITY_COLORS = [
    "#E74C3C", "#3498DB", "#2ECC71", "#F39C12", "#9B59B6",
    "#1ABC9C", "#E67E22", "#34495E", "#E91E63", "#00BCD4",
    "#8BC34A", "#FF5722", "#607D8B", "#795548", "#CDDC39",
    "#FF9800", "#03A9F4", "#4CAF50", "#9C27B0", "#F44336",
]

def get_color(community_id: int) -> str:
    return COMMUNITY_COLORS[community_id % len(COMMUNITY_COLORS)]


# ---------------------------------------------
#  1. GEXF Export (for Gephi)
# ---------------------------------------------
def export_gexf(
    nodes: list,
    edges: list,
    community: dict,
    degree: dict,
    influence: dict = None,
    out_path: str = "graph.gexf",
):
    """
    Exports graph as GEXF 1.2 XML for Gephi.
    Includes community, degree, and influence as node attributes.
    """
    inf = influence or {}
    lines = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append('<gexf xmlns="http://gexf.net/1.2" version="1.2">')
    lines.append(f'  <meta lastmodifieddate="{datetime.now().strftime("%Y-%m-%d")}">')
    lines.append('    <creator>BDA Social Graph Project</creator>')
    lines.append('    <description>Facebook SNAP Social Network</description>')
    lines.append('  </meta>')
    lines.append('  <graph defaultedgetype="undirected">')

    # Node attribute declarations
    lines.append('    <attributes class="node">')
    lines.append('      <attribute id="0" title="community" type="integer"/>')
    lines.append('      <attribute id="1" title="degree" type="integer"/>')
    lines.append('      <attribute id="2" title="influence_score" type="float"/>')
    lines.append('      <attribute id="3" title="color_r" type="integer"/>')
    lines.append('      <attribute id="4" title="color_g" type="integer"/>')
    lines.append('      <attribute id="5" title="color_b" type="integer"/>')
    lines.append('    </attributes>')

    # Nodes
    lines.append('    <nodes>')
    for n in nodes:
        comm = community.get(n, 0)
        deg = degree.get(n, 1)
        inf_score = inf.get(n, 0.0)
        color = get_color(comm)
        r = int(color[1:3], 16)
        g = int(color[3:5], 16)
        b = int(color[5:7], 16)
        # Size proportional to degree (range 2-20)
        size = max(2.0, min(20.0, 2.0 + deg * 0.15))
        lines.append(f'      <node id="{n}" label="User_{n}">')
        lines.append(f'        <viz:size value="{size:.1f}"/>')
        lines.append(f'        <viz:color r="{r}" g="{g}" b="{b}" a="1"/>')
        lines.append(f'        <attvalues>')
        lines.append(f'          <attvalue for="0" value="{comm}"/>')
        lines.append(f'          <attvalue for="1" value="{deg}"/>')
        lines.append(f'          <attvalue for="2" value="{inf_score:.4f}"/>')
        lines.append(f'          <attvalue for="3" value="{r}"/>')
        lines.append(f'          <attvalue for="4" value="{g}"/>')
        lines.append(f'          <attvalue for="5" value="{b}"/>')
        lines.append(f'        </attvalues>')
        lines.append(f'      </node>')
    lines.append('    </nodes>')

    # Edges
    lines.append('    <edges>')
    edge_set = set()
    eid = 0
    for src, dst in edges:
        pair = (min(src, dst), max(src, dst))
        if pair not in edge_set:
            edge_set.add(pair)
            lines.append(
                f'      <edge id="{eid}" source="{src}" target="{dst}" weight="1.0"/>'
            )
            eid += 1
    lines.append('    </edges>')
    lines.append('  </graph>')
    lines.append('</gexf>')

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"  GEXF exported: {out_path} ({eid} edges, {len(nodes)} nodes)")


# ---------------------------------------------
#  2. Interactive D3.js HTML Visualization
# ---------------------------------------------
def export_interactive_html(
    nodes: list,
    edges: list,
    community: dict,
    degree: dict,
    influence: dict = None,
    out_path: str = "graph_viz.html",
    max_nodes: int = 500,   # Limit for browser performance
    max_edges: int = 2000,
):
    """
    Generates a self-contained interactive D3.js force-directed graph.
    Click nodes to highlight their neighborhood.
    Hover for stats.
    """
    inf = influence or {}

    # Sample for performance if graph is too large
    if len(nodes) > max_nodes:
        # Keep highest-degree nodes
        sorted_nodes = sorted(nodes, key=lambda n: degree.get(n, 0), reverse=True)
        viz_nodes = set(sorted_nodes[:max_nodes])
    else:
        viz_nodes = set(nodes)

    # Filter edges to only viz_nodes
    viz_edges = [
        (s, d) for s, d in edges
        if s in viz_nodes and d in viz_nodes
    ][:max_edges]

    # Build JSON data
    nodes_json = []
    for n in viz_nodes:
        nodes_json.append({
            "id": n,
            "label": f"User_{n}",
            "community": community.get(n, 0),
            "degree": degree.get(n, 1),
            "influence": round(inf.get(n, 0.0), 4),
            "color": get_color(community.get(n, 0)),
            "size": max(4, min(18, 4 + degree.get(n, 1) * 0.2)),
        })

    edges_json = [{"source": s, "target": d} for s, d in viz_edges]

    # Get unique communities for legend
    unique_comms = sorted(set(community.get(n, 0) for n in viz_nodes))[:15]
    legend_items = [
        {"id": c, "color": get_color(c),
         "label": f"Community {c}"}
        for c in unique_comms
    ]

    nodes_data = json.dumps(nodes_json, indent=2)
    edges_data = json.dumps(edges_json, indent=2)
    legend_data = json.dumps(legend_items)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Facebook Social Graph — BDA Analysis</title>
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', sans-serif; }}
    #header {{
      padding: 16px 24px;
      background: #161b22;
      border-bottom: 1px solid #30363d;
      display: flex;
      align-items: center;
      gap: 16px;
    }}
    #header h1 {{ font-size: 1.2rem; color: #58a6ff; }}
    #header .stats {{ font-size: 0.8rem; color: #8b949e; margin-left: auto; }}
    #main {{ display: flex; height: calc(100vh - 60px); }}
    #sidebar {{
      width: 260px;
      background: #161b22;
      border-right: 1px solid #30363d;
      padding: 16px;
      overflow-y: auto;
      flex-shrink: 0;
    }}
    #sidebar h3 {{ font-size: 0.75rem; text-transform: uppercase;
                   color: #8b949e; margin-bottom: 12px; }}
    .legend-item {{
      display: flex; align-items: center; gap: 8px;
      margin-bottom: 6px; cursor: pointer;
      padding: 4px 6px; border-radius: 4px;
    }}
    .legend-item:hover {{ background: #21262d; }}
    .legend-dot {{ width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; }}
    .legend-label {{ font-size: 0.75rem; }}
    #tooltip {{
      position: fixed;
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 8px;
      padding: 10px 14px;
      font-size: 0.75rem;
      pointer-events: none;
      display: none;
      z-index: 100;
      min-width: 160px;
    }}
    #tooltip .t-name {{ color: #58a6ff; font-weight: bold; margin-bottom: 4px; }}
    #tooltip .t-row {{ display: flex; justify-content: space-between; gap: 16px; margin: 2px 0; }}
    #tooltip .t-val {{ color: #c9d1d9; }}
    #canvas {{ flex: 1; cursor: grab; }}
    #canvas:active {{ cursor: grabbing; }}
    .node {{ cursor: pointer; transition: opacity 0.2s; }}
    .link {{ stroke: #30363d; stroke-opacity: 0.6; }}
    .link.highlighted {{ stroke: #58a6ff; stroke-opacity: 1; stroke-width: 2; }}
    #controls {{
      position: absolute;
      bottom: 20px; right: 20px;
      display: flex; gap: 8px;
    }}
    button {{
      background: #21262d; border: 1px solid #30363d;
      color: #c9d1d9; padding: 6px 12px;
      border-radius: 6px; cursor: pointer; font-size: 0.75rem;
    }}
    button:hover {{ background: #30363d; }}
    #info-panel {{
      background: #161b22; border: 1px solid #30363d;
      border-radius: 8px; padding: 12px 16px;
      margin-top: 16px; font-size: 0.75rem;
    }}
    #info-panel h4 {{ color: #58a6ff; margin-bottom: 8px; }}
    #info-panel .metric {{ display: flex; justify-content: space-between;
                           margin: 3px 0; color: #8b949e; }}
    #info-panel .metric span {{ color: #c9d1d9; }}
  </style>
</head>
<body>
<div id="header">
  <div>
    <h1>🕸️ Facebook Social Graph — BDA Project</h1>
  </div>
  <div class="stats">
    Showing {min(len(nodes), max_nodes)} nodes · {min(len(viz_edges), max_edges)} edges
    (Full: {len(nodes)} nodes, {len(edges)//2} undirected edges)
  </div>
</div>
<div id="main">
  <div id="sidebar">
    <h3>Communities</h3>
    <div id="legend"></div>
    <div id="info-panel">
      <h4>Graph Statistics</h4>
      <div class="metric">Total Nodes <span>{len(nodes)}</span></div>
      <div class="metric">Total Edges <span>{len(edges)//2:,}</span></div>
      <div class="metric">Communities <span>{len(unique_comms)}</span></div>
      <div class="metric">Avg Degree <span>{sum(degree.values())//len(nodes) if nodes else 0}</span></div>
    </div>
    <div id="info-panel" style="margin-top:8px">
      <h4>Selected Node</h4>
      <div id="node-details" style="color:#8b949e">Click a node to inspect</div>
    </div>
  </div>
  <svg id="canvas"></svg>
</div>
<div id="tooltip"></div>
<div id="controls">
  <button onclick="resetZoom()">Reset View</button>
  <button onclick="togglePhysics()">Pause/Resume</button>
</div>

<script>
const nodesData = {nodes_data};
const linksData = {edges_data};
const legendData = {legend_data};

// Legend
const legend = document.getElementById('legend');
legendData.forEach(item => {{
  const div = document.createElement('div');
  div.className = 'legend-item';
  div.innerHTML = `<div class="legend-dot" style="background:${{item.color}}"></div>
                   <span class="legend-label">${{item.label}}</span>`;
  div.onclick = () => highlightCommunity(item.id);
  legend.appendChild(div);
}});

// D3 Setup
const svg = d3.select('#canvas');
const container = svg.append('g');
let simRunning = true;

const width  = document.getElementById('canvas').clientWidth  || 900;
const height = document.getElementById('canvas').clientHeight || 700;

// Zoom
const zoom = d3.zoom()
  .scaleExtent([0.1, 8])
  .on('zoom', e => container.attr('transform', e.transform));
svg.call(zoom);

// Simulation
const simulation = d3.forceSimulation(nodesData)
  .force('link', d3.forceLink(linksData).id(d => d.id).distance(40).strength(0.5))
  .force('charge', d3.forceManyBody().strength(-120).distanceMax(200))
  .force('center', d3.forceCenter(width / 2, height / 2))
  .force('collision', d3.forceCollide().radius(d => d.size + 2));

// Links
const link = container.append('g')
  .selectAll('line')
  .data(linksData)
  .join('line')
  .attr('class', 'link');

// Nodes
const node = container.append('g')
  .selectAll('circle')
  .data(nodesData)
  .join('circle')
  .attr('class', 'node')
  .attr('r', d => d.size)
  .attr('fill', d => d.color)
  .attr('stroke', '#161b22')
  .attr('stroke-width', 1.5)
  .call(d3.drag()
    .on('start', dragStart)
    .on('drag', dragged)
    .on('end', dragEnd)
  )
  .on('mouseover', showTooltip)
  .on('mousemove', moveTooltip)
  .on('mouseout', hideTooltip)
  .on('click', selectNode);

// Tick
simulation.on('tick', () => {{
  link
    .attr('x1', d => d.source.x)
    .attr('y1', d => d.source.y)
    .attr('x2', d => d.target.x)
    .attr('y2', d => d.target.y);
  node
    .attr('cx', d => d.x)
    .attr('cy', d => d.y);
}});

function dragStart(event, d) {{
  if (!event.active) simulation.alphaTarget(0.3).restart();
  d.fx = d.x; d.fy = d.y;
}}
function dragged(event, d) {{ d.fx = event.x; d.fy = event.y; }}
function dragEnd(event, d) {{
  if (!event.active) simulation.alphaTarget(0);
  d.fx = null; d.fy = null;
}}

// Tooltip
const tooltip = document.getElementById('tooltip');
function showTooltip(event, d) {{
  tooltip.style.display = 'block';
  tooltip.innerHTML = `
    <div class="t-name">👤 User ${{d.id}}</div>
    <div class="t-row"><span>Community</span><span class="t-val">${{d.community}}</span></div>
    <div class="t-row"><span>Degree</span><span class="t-val">${{d.degree}}</span></div>
    <div class="t-row"><span>Influence</span><span class="t-val">${{d.influence}}</span></div>
  `;
}}
function moveTooltip(event) {{
  tooltip.style.left = (event.clientX + 12) + 'px';
  tooltip.style.top  = (event.clientY - 20) + 'px';
}}
function hideTooltip() {{ tooltip.style.display = 'none'; }}

// Node selection
let selectedNode = null;
function selectNode(event, d) {{
  selectedNode = d;
  const neighborIds = new Set();
  linksData.forEach(l => {{
    const s = typeof l.source === 'object' ? l.source.id : l.source;
    const t = typeof l.target === 'object' ? l.target.id : l.target;
    if (s === d.id) neighborIds.add(t);
    if (t === d.id) neighborIds.add(s);
  }});
  node.style('opacity', n => (n.id === d.id || neighborIds.has(n.id)) ? 1.0 : 0.15);
  link.classed('highlighted', l => {{
    const s = typeof l.source === 'object' ? l.source.id : l.source;
    const t = typeof l.target === 'object' ? l.target.id : l.target;
    return s === d.id || t === d.id;
  }});
  document.getElementById('node-details').innerHTML = `
    <div class="metric">Node ID <span>${{d.id}}</span></div>
    <div class="metric">Community <span>${{d.community}}</span></div>
    <div class="metric">Connections <span>${{d.degree}}</span></div>
    <div class="metric">Influence <span>${{d.influence}}</span></div>
    <div class="metric">Neighbors <span>${{neighborIds.size}}</span></div>
  `;
}}

function highlightCommunity(commId) {{
  node.style('opacity', d => d.community === commId ? 1.0 : 0.1);
  link.style('opacity', l => {{
    const s = typeof l.source === 'object' ? l.source : {{ community: -1 }};
    const t = typeof l.target === 'object' ? l.target : {{ community: -1 }};
    return (s.community === commId && t.community === commId) ? 0.8 : 0.05;
  }});
}}

function resetZoom() {{
  svg.transition().duration(500)
    .call(zoom.transform, d3.zoomIdentity.translate(width/2, height/2).scale(0.8));
  node.style('opacity', 1);
  link.style('opacity', 0.6).classed('highlighted', false);
}}

function togglePhysics() {{
  simRunning = !simRunning;
  if (simRunning) {{ simulation.alphaTarget(0.1).restart(); }}
  else {{ simulation.stop(); }}
}}

// Reset on background click
svg.on('click', function(event) {{
  if (event.target === this || event.target.tagName === 'svg') {{
    node.style('opacity', 1);
    link.style('opacity', 0.6).classed('highlighted', false);
    document.getElementById('node-details').innerHTML = 'Click a node to inspect';
  }}
}});
</script>
</body>
</html>"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  Interactive HTML exported: {out_path}")
    print(f"  Open in browser: file:///{os.path.abspath(out_path)}")


# ---------------------------------------------
#  3. Matplotlib Static Plots
# ---------------------------------------------
def plot_degree_distribution(degree: dict, out_path: str):
    """Log-log degree distribution plot (power law check)."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import numpy as np

        degrees = list(degree.values())
        deg_counts = collections.Counter(degrees)
        k_vals = sorted(deg_counts.keys())
        p_vals = [deg_counts[k] / len(degrees) for k in k_vals]

        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        fig.patch.set_facecolor('#0d1117')

        for ax in axes:
            ax.set_facecolor('#161b22')
            ax.tick_params(colors='#8b949e')
            ax.xaxis.label.set_color('#c9d1d9')
            ax.yaxis.label.set_color('#c9d1d9')
            ax.title.set_color('#58a6ff')
            for spine in ax.spines.values():
                spine.set_edgecolor('#30363d')

        # Linear
        axes[0].bar(k_vals[:50], p_vals[:50], color='#58a6ff', alpha=0.8, width=0.8)
        axes[0].set_title('Degree Distribution (Linear)')
        axes[0].set_xlabel('Degree k')
        axes[0].set_ylabel('P(k)')

        # Log-log
        axes[1].loglog(k_vals, p_vals, 'o', color='#E74C3C', alpha=0.7, markersize=4)
        axes[1].set_title('Degree Distribution (Log-Log)')
        axes[1].set_xlabel('Degree k')
        axes[1].set_ylabel('P(k)')

        # Fit power law
        log_k = np.log(k_vals[1:])
        log_p = np.log([p for p in p_vals[1:] if p > 0])
        if len(log_k) > 2:
            coeffs = np.polyfit(log_k[:len(log_p)], log_p, 1)
            gamma = -coeffs[0]
            axes[1].set_title(f'Log-Log Degree Dist. (γ≈{gamma:.2f})')

        plt.tight_layout()
        plt.savefig(out_path, dpi=150, bbox_inches='tight',
                    facecolor='#0d1117')
        plt.close()
        print(f"  Degree distribution plot saved: {out_path}")
    except ImportError:
        print("  matplotlib not available; skipping degree distribution plot")


# ---------------------------------------------
#  MAIN RUNNER
# ---------------------------------------------
if __name__ == "__main__":
    import os, sys, glob
    import pyspark

    os.environ['SPARK_HOME'] = os.path.dirname(pyspark.__file__)
    os.environ["PYSPARK_PIN_THREAD"] = "true"
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    hadoop_home = r'C:\hadoop'
    if os.path.isdir(hadoop_home):
        os.environ['HADOOP_HOME'] = hadoop_home

    BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR   = os.path.join(os.path.dirname(BASE_DIR), "data_lake")

    def discover_dataset(explicit_path=None):
        if explicit_path and os.path.exists(explicit_path):
            return explicit_path
        candidates = (
            glob.glob(os.path.join(DATA_DIR, "*.txt"))
            + glob.glob(os.path.join(DATA_DIR, "*.txt.gz"))
            + glob.glob(os.path.join(DATA_DIR, "*.csv"))
            + glob.glob(os.path.join(DATA_DIR, "*.csv.gz"))
        )
        if not candidates:
            print("[ERROR] No dataset found in data_lake/")
            sys.exit(1)
        candidates.sort(key=lambda f: os.path.getsize(f))
        return candidates[0]

    explicit = sys.argv[1] if len(sys.argv) > 1 else None
    if explicit and not os.path.isabs(explicit):
        explicit = os.path.join(BASE_DIR, explicit)
    DATA_PATH  = discover_dataset(explicit)
    COMM_PATH  = os.path.join(BASE_DIR, "outputs", "communities", "community_assignment")
    INF_PATH   = os.path.join(BASE_DIR, "outputs", "centrality", "influence_scores")
    OUT_DIR    = os.path.join(BASE_DIR, "outputs", "visualization")
    os.makedirs(OUT_DIR, exist_ok=True)

    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pyspark.sql.types import *
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType

    spark = (
        SparkSession.builder
        .appName("VisualizationEngine")
        .master("local[*]")
        .config("spark.jars.packages",
                "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("  VISUALIZATION & EXPORT ENGINE")
    print(f"  Dataset: {os.path.basename(DATA_PATH)}")
    print("=" * 60)

    # Load graph
    print("\n[1] Loading graph...")
    raw = spark.read.text(DATA_PATH)
    edges_df = (
        raw.select(F.split("value", r"\s+").alias("p"))
        .select(
            F.col("p")[0].cast(LongType()).alias("src"),
            F.col("p")[1].cast(LongType()).alias("dst"),
        )
    )
    edges_local = [(r["src"], r["dst"]) for r in edges_df.collect()]
    nodes_local = list(set([e[0] for e in edges_local] + [e[1] for e in edges_local]))
    degree = collections.Counter()
    for s, d in edges_local:
        degree[s] += 1
        degree[d] += 1

    # Load community
    print("[2] Loading community data...")
    try:
        comm_df = spark.read.csv(COMM_PATH, header=True, inferSchema=True)
        community = {r["node_id"]: r["community_id"] for r in comm_df.collect()}
    except Exception:
        community = {n: 0 for n in nodes_local}
        print("  Using default community=0")

    # Load influence scores
    print("[3] Loading influence scores...")
    try:
        inf_df = spark.read.csv(INF_PATH, header=True, inferSchema=True)
        influence = {r["id"]: r["influence_score"] for r in inf_df.collect()}
    except Exception:
        influence = {}
        print("  Using default influence=0")

    # Export GEXF
    print("\n[4] Exporting GEXF for Gephi...")
    export_gexf(
        nodes_local, edges_local, community, dict(degree), influence,
        out_path=os.path.join(OUT_DIR, "facebook_graph.gexf")
    )

    # Export Interactive HTML
    print("\n[5] Generating interactive D3.js visualization...")
    export_interactive_html(
        nodes_local, edges_local, community, dict(degree), influence,
        out_path=os.path.join(OUT_DIR, "graph_interactive.html"),
        max_nodes=400, max_edges=1500
    )

    # Degree distribution plot
    print("\n[6] Plotting degree distribution...")
    plot_degree_distribution(
        dict(degree),
        out_path=os.path.join(OUT_DIR, "degree_distribution.png")
    )

    print(f"\n  All outputs saved to: {OUT_DIR}")
    spark.stop()
