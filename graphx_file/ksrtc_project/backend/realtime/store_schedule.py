from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime

from pymongo import MongoClient


def store_schedule_recommendation(
    route: str,
    recommended_buses: int,
    predicted_demand: int,
    decision: str,
) -> dict:
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    mongo_db_name = os.getenv("MONGODB_DB", "ksrtc_db")

    schedule = {
        "route": route,
        "recommended_buses": recommended_buses,
        "predicted_demand": predicted_demand,
        "decision": decision,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]
    db.schedule_recommendations.insert_one(schedule)
    return schedule


def main() -> int:
    parser = argparse.ArgumentParser(description="Store one schedule recommendation in MongoDB.")
    parser.add_argument("--route", default="Hubli-Dharwad")
    parser.add_argument("--recommended-buses", type=int, default=5)
    parser.add_argument("--predicted-demand", type=int, default=135)
    parser.add_argument("--decision", default="Increase frequency")
    args = parser.parse_args()

    record = store_schedule_recommendation(
        args.route,
        args.recommended_buses,
        args.predicted_demand,
        args.decision,
    )
    print(f"Inserted schedule recommendation: {record}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
