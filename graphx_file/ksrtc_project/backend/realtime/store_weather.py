from __future__ import annotations

import os
import sys
from datetime import UTC, datetime

from pymongo import MongoClient

try:
    from backend.realtime.weather_service import get_weather
except Exception:
    from weather_service import get_weather


def main() -> int:
    city = sys.argv[1] if len(sys.argv) > 1 else "Hubli"
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    mongo_db_name = os.getenv("MONGODB_DB", "ksrtc_db")

    weather = get_weather(city)
    weather["timestamp"] = datetime.now(UTC).isoformat()

    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]
    db.weather_data.insert_one(weather)

    print(f"Inserted weather record for {city} into {mongo_db_name}.weather_data")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
