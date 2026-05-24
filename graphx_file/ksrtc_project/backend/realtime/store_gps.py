from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime

from pymongo import MongoClient


def store_gps_data(bus_id: str, route: str, lat: float, lon: float, status: str = "on_time") -> dict:
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    mongo_db_name = os.getenv("MONGODB_DB", "ksrtc_db")

    gps_data = {
        "bus_id": bus_id,
        "route": route,
        "lat": lat,
        "lon": lon,
        "status": status,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]
    db.gps_data.insert_one(gps_data)
    return gps_data


def main() -> int:
    parser = argparse.ArgumentParser(description="Store one GPS bus position in MongoDB.")
    parser.add_argument("--bus-id", default="KA25F1234")
    parser.add_argument("--route", default="Hubli-Dharwad")
    parser.add_argument("--lat", type=float, default=15.3647)
    parser.add_argument("--lon", type=float, default=75.1240)
    parser.add_argument("--status", default="on_time")
    args = parser.parse_args()

    record = store_gps_data(args.bus_id, args.route, args.lat, args.lon, args.status)
    print(f"Inserted GPS record: {record}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
