from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

from flask import Flask, Response, jsonify

BASE_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = BASE_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from backend.realtime.maps_service import get_map_image
    from backend.realtime.weather_service import get_weather
except Exception:
    from realtime.maps_service import get_map_image
    from realtime.weather_service import get_weather

try:
    from pymongo import DESCENDING, MongoClient
except Exception:
    DESCENDING = -1
    MongoClient = None


RESULTS_DIR = PROJECT_ROOT / "results" / "csv"
LEGACY_OUTPUTS = BASE_DIR / "outputs"
MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGODB_DB", "ksrtc_db")

app = Flask(__name__)
mongo_client = None
mongo_db = None


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    return response


def resolve_csv_path(filename: str) -> Path:
    for directory in (RESULTS_DIR, LEGACY_OUTPUTS):
        file_path = directory / filename
        if file_path.exists():
            return file_path
    return RESULTS_DIR / filename


def read_csv_records(filename: str) -> list[dict]:
    file_path = resolve_csv_path(filename)
    if not file_path.exists():
        return []
    with file_path.open("r", newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def get_mongo_db():
    global mongo_client, mongo_db
    if mongo_db is not None:
        return mongo_db
    if MongoClient is None:
        return None

    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
        mongo_client.admin.command("ping")
        mongo_db = mongo_client[MONGO_DB_NAME]
    except Exception:
        mongo_client = None
        mongo_db = None
    return mongo_db


def mongo_records(
    collection: str,
    query: dict | None = None,
    projection: dict | None = None,
    sort_field: str | None = None,
    descending: bool = False,
    limit: int | None = None,
) -> list[dict]:
    db = get_mongo_db()
    if db is None:
        return []

    projection = projection or {"_id": 0}
    cursor = db[collection].find(query or {}, projection)
    if sort_field:
        order = DESCENDING if descending else 1
        cursor = cursor.sort(sort_field, order)
    if limit:
        cursor = cursor.limit(limit)
    return list(cursor)


@app.route("/api/routes")
def routes() -> tuple:
    records = mongo_records("route_predictions")
    if not records:
        records = read_csv_records("shortest_paths.csv")
    return jsonify(records), 200


@app.route("/api/demand")
def demand() -> tuple:
    records = mongo_records("route_predictions")
    if not records:
        records = read_csv_records("predictions.csv")
    return jsonify(records), 200


@app.route("/api/bus_location")
def bus_location() -> tuple:
    records = mongo_records("gps_data", sort_field="timestamp", descending=True, limit=50)
    if not records:
        records = [
            {"bus_id": "KA-01-F-1201", "lat": 12.9716, "lon": 77.5946, "status": "on_time"},
            {"bus_id": "KA-02-F-4432", "lat": 12.9352, "lon": 77.6245, "status": "delayed"},
        ]
    return jsonify(records), 200


@app.route("/api/buses")
def buses() -> tuple:
    return bus_location()


@app.route("/api/system_metrics")
def system_metrics() -> tuple:
    db = get_mongo_db()
    if db is not None:
        pagerank_rows = db.route_analysis.count_documents({})
        demand_rows = db.route_predictions.count_documents({})
    else:
        pagerank_rows = len(read_csv_records("pagerank.csv"))
        demand_rows = len(read_csv_records("predictions.csv"))
    return jsonify(
        {
            "status": "running",
            "pagerank_rows": pagerank_rows,
            "demand_rows": demand_rows,
        }
    ), 200


@app.route("/api/weather")
def weather_all() -> tuple:
    records = mongo_records("weather_data", sort_field="timestamp", descending=True, limit=50)
    return jsonify(records), 200


@app.route("/api/weather/<city>")
def weather(city: str) -> tuple:
    records = mongo_records(
        "weather_data",
        query={"city": {"$regex": f"^{city}$", "$options": "i"}},
        sort_field="timestamp",
        descending=True,
        limit=1,
    )
    if records:
        return jsonify(records[0]), 200

    try:
        return jsonify(get_weather(city)), 200
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@app.route("/api/map/<location>")
def map_image(location: str):
    try:
        image = get_map_image(location)
        return Response(image, mimetype="image/jpeg", status=200)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@app.route("/health")
def health() -> tuple:
    return jsonify({"status": "KSRTC backend running"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
