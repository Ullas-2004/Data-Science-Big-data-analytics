from __future__ import annotations

import math
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ROUTE_FILE = PROJECT_ROOT / "data" / "processed" / "bus_routes.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "gps.csv"


def interpolate_position(route_points: pd.DataFrame, progress: float) -> tuple[float, float]:
    point_count = route_points.shape[0]
    segment_count = max(1, point_count - 1)
    bounded_progress = progress % segment_count
    segment_index = int(bounded_progress)
    fraction = bounded_progress - segment_index

    start = route_points.iloc[segment_index]
    end = route_points.iloc[(segment_index + 1) % point_count]
    lat = float(start["lat"]) + (float(end["lat"]) - float(start["lat"])) * fraction
    lon = float(start["lon"]) + (float(end["lon"]) - float(start["lon"])) * fraction
    return lat, lon


def build_demo_gps(route_df: pd.DataFrame, total_buses: int = 18, samples_per_bus: int = 18) -> pd.DataFrame:
    valid_routes = [
        route.sort_values("seq").reset_index(drop=True)
        for _, route in route_df.groupby("route_id", sort=True)
        if route.shape[0] >= 2
    ]
    if not valid_routes:
        raise ValueError("No routes with at least two points were found in data/processed/bus_routes.csv.")

    buses_per_route = max(1, math.ceil(total_buses / len(valid_routes)))
    base_time = datetime(2026, 3, 18, 6, 0, 0)
    rows: list[dict[str, object]] = []
    bus_index = 0

    for route_index, route_points in enumerate(valid_routes):
        route_id = str(route_points.iloc[0]["route_id"])
        segment_count = max(1, route_points.shape[0] - 1)

        for route_bus_index in range(buses_per_route):
            if bus_index >= total_buses:
                break

            bus_id = f"KSRTC-{101 + bus_index}"
            bus_start_time = base_time + timedelta(minutes=(route_index * 9) + (route_bus_index * 3))

            for sample_index in range(samples_per_bus):
                progress = (sample_index * 0.72) + (route_bus_index * 0.51) + (route_index * 0.33)
                lat, lon = interpolate_position(route_points, progress)
                speed = 30 + (8 * math.sin((sample_index + 1) / 2.6 + route_bus_index * 0.45))
                speed += 5 * math.cos((progress / segment_count) * math.pi * 2)
                speed = round(max(18.0, min(54.0, speed)), 1)
                status = "delayed" if speed < 24 else "on_time"

                rows.append(
                    {
                        "bus_id": bus_id,
                        "timestamp": (bus_start_time + timedelta(minutes=sample_index * 5)).strftime("%Y-%m-%d %H:%M:%S"),
                        "lat": round(lat, 6),
                        "lon": round(lon, 6),
                        "speed": speed,
                        "route_id": route_id,
                        "status": status,
                    }
                )

            bus_index += 1

    return pd.DataFrame(rows)


def main() -> int:
    route_df = pd.read_csv(ROUTE_FILE)
    required_columns = {"route_id", "seq", "lat", "lon"}
    if not required_columns.issubset(route_df.columns):
        missing = ", ".join(sorted(required_columns - set(route_df.columns)))
        raise ValueError(f"Missing required route columns: {missing}")

    route_df["seq"] = pd.to_numeric(route_df["seq"], errors="coerce")
    route_df["lat"] = pd.to_numeric(route_df["lat"], errors="coerce")
    route_df["lon"] = pd.to_numeric(route_df["lon"], errors="coerce")
    route_df = route_df.dropna(subset=["route_id", "seq", "lat", "lon"])

    gps_df = build_demo_gps(route_df)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    gps_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Wrote {len(gps_df)} demo GPS records to {OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
