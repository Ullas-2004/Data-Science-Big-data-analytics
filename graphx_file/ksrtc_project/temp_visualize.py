import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import io

os.makedirs("visualizations", exist_ok=True)

# Read the CSV properly by stripping quotes
with open("data/raw/gps.csv", 'r') as f:
    lines = [line.strip('"') for line in f]
df = pd.read_csv(io.StringIO('\n'.join(lines)))
print(df.columns)
print(df.head())

# 1 Speed Distribution
plt.figure()
sns.histplot(df["Speed_kmh"], bins=20)
plt.title("Speed Distribution")
plt.savefig("visualizations/speed_distribution.png")

# 2 Bus Count
bus_counts = df["ID"].value_counts()
plt.figure()
bus_counts.plot(kind='bar')
plt.title("Bus Distribution")
plt.savefig("visualizations/bus_distribution.png")

# 3 Time vs Speed
df["Timestamp"] = pd.to_datetime(df["Timestamp"])
df = df.sort_values("Timestamp")

plt.figure()
plt.plot(df["Timestamp"], df["Speed_kmh"])
plt.title("Speed over Time")
plt.xticks(rotation=45)
plt.savefig("visualizations/time_speed_trend.png")

print("Visualizations generated!")