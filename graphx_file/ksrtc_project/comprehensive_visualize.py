import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import io

os.makedirs("visualizations", exist_ok=True)

# Read GPS data properly
with open("data/raw/gps.csv", 'r') as f:
    lines = [line.strip('"') for line in f]
df = pd.read_csv(io.StringIO('\n'.join(lines)))

# Ensure columns are correct
df.columns = df.columns.str.strip('"')  # Remove any trailing quotes

# 1 Speed Distribution
plt.figure()
sns.histplot(df["Speed_kmh"], bins=20)
plt.title("Speed Distribution")
plt.savefig("visualizations/speed_distribution.png")
plt.close()

# 2 Bus Count (using ID as bus_id)
bus_counts = df["ID"].value_counts()
plt.figure(figsize=(10,6))
bus_counts.plot(kind='bar')
plt.title("Bus Distribution")
plt.gca().set_xticks([])  # remove x-axis labels to avoid clutter
plt.gca().spines['bottom'].set_visible(False)
plt.tight_layout()
plt.savefig("visualizations/bus_distribution.png")
plt.close()

# 3 Time vs Speed
df["Timestamp"] = pd.to_datetime(df["Timestamp"])
df = df.sort_values("Timestamp")
plt.figure()
plt.plot(df["Timestamp"], df["Speed_kmh"])
plt.title("Speed over Time")
plt.xticks(rotation=45)
plt.savefig("visualizations/time_speed_trend.png")
plt.close()

# 4 Traffic Heatmap
plt.figure()
sns.kdeplot(x=df["Longitude"], y=df["Latitude"], cmap="Reds", fill=True)
plt.title("Traffic Heatmap")
plt.savefig("visualizations/traffic_heatmap.png")
plt.close()

# 5 Route Demand
route_counts = df.groupby("ID").size()
plt.figure(figsize=(10,6))
route_counts.plot(kind="bar")
plt.title("Route Demand")
plt.gca().set_xticks([])  # remove x-axis labels to avoid clutter
plt.gca().spines['bottom'].set_visible(False)
plt.tight_layout()
plt.savefig("visualizations/route_demand.png")
plt.close()

# 6 Average Speed per Bus
avg_speed = df.groupby("ID")["Speed_kmh"].mean()
plt.figure(figsize=(10,6))
avg_speed.plot(kind="bar")
plt.title("Average Speed per Bus")
plt.gca().set_xticks([])  # remove x-axis labels to avoid clutter
plt.gca().spines['bottom'].set_visible(False)
plt.tight_layout()
plt.savefig("visualizations/avg_speed_per_bus.png")
plt.close()

# 7 Stop Density
plt.figure()
plt.scatter(df["Longitude"], df["Latitude"], alpha=0.3)
plt.title("Stop Density Map")
plt.savefig("visualizations/stop_density.png")
plt.close()

# 8 Speed Categories
df["speed_category"] = pd.cut(df["Speed_kmh"], bins=[0,20,40,60], labels=["Low","Medium","High"])
counts = df["speed_category"].value_counts()
plt.figure()
counts.plot(kind="pie", autopct="%1.1f%%")
plt.title("Speed Category Distribution")
plt.savefig("visualizations/speed_category.png")
plt.close()

# 9 PageRank Visualization
pr = pd.read_csv("results/csv/pagerank.csv")
plt.figure()
plt.bar(pr["stop_id"], pr["pagerank"])
plt.title("PageRank of Stops")
plt.savefig("visualizations/pagerank_visual.png")
plt.close()

# 10 Shortest Path Visualization
sp = pd.read_csv("results/csv/shortest_paths.csv")
plt.figure()
plt.plot(sp["source_stop"], sp["estimated_cost"])
plt.title("Shortest Path Analysis")
plt.savefig("visualizations/shortest_path_visual.png")
plt.close()

print("All visualizations generated!")