from __future__ import annotations

import hashlib
import json
import math
import re
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
RESULTS_DIR = PROJECT_ROOT / "results" / "csv"
LEGACY_OUTPUT_DIR = PROJECT_ROOT / "backend" / "outputs"


def resolve_results_file(filename: str) -> Path:
    for directory in (RESULTS_DIR, LEGACY_OUTPUT_DIR):
        candidate = directory / filename
        if candidate.exists():
            return candidate
    return RESULTS_DIR / filename

ROUTE_FILE = DATA_PROCESSED_DIR / "bus_routes.csv"
SCHEDULE_FILE = DATA_PROCESSED_DIR / "cleaned_ksrtc_data.csv"
CITY_COORDINATES_CACHE_FILE = DATA_PROCESSED_DIR / "city_coordinates.csv"
CITY_COORDINATES_MISSING_FILE = DATA_PROCESSED_DIR / "city_coordinates_missing.csv"
STOPS_FILE = DATA_RAW_DIR / "synthetic_bus_stops.csv"
GPS_FILE = DATA_RAW_DIR / "gps.csv"
SHORTEST_PATHS_FILE = resolve_results_file("shortest_paths.csv")
SHORTEST_PATH_EXAMPLE_FILE = resolve_results_file("shortest_path_example.csv")
PAGERANK_FILE = resolve_results_file("pagerank.csv")
PREDICTIONS_FILE = resolve_results_file("predictions.csv")
CONNECTED_COMPONENTS_SUMMARY_FILE = RESULTS_DIR / "connected_component_summary.csv"
ROUTE_NAME_SOURCE_DIR = PROJECT_ROOT / "data"
BUS_ICON_URL = "https://img.icons8.com/fluency/96/bus.png"
NOMINATIM_USER_AGENT = "ksrtc-bda-dashboard/1.0"
PRIMARY_COLOR = "#2563eb"
SUCCESS_COLOR = "#16a34a"
WARNING_COLOR = "#f59e0b"
DANGER_COLOR = "#dc2626"
BACKGROUND_COLOR = "#f8fafc"
CARD_COLOR = "#ffffff"
TEXT_COLOR = "#111827"

SERVICE_LABELS = {
    "",
    "ordinary",
    "ord",
    "exp",
    "city volvo",
    "city.volvo",
    "p",
    "t",
    "google",
    "gtttc",
    "b c",
    "b.c",
}

LOCATION_ALIASES = {
    "alnda": "Alanda",
    "ananthapura": "Anantapura",
    "anathapura": "Anantapura",
    "bangalore": "Bengaluru",
    "bangalore 3": "Bengaluru",
    "bangalore 5": "Bengaluru",
    "bangalore 6": "Bengaluru",
    "bailahongal": "Bailhongal",
    "challkere": "Challakere",
    "chanraypattana": "Channarayapatna",
    "chickballapura": "Chickballapur",
    "chickballapuradoddaballapura": "Doddaballapura",
    "chikbalapura": "Chickballapur",
    "chikkaballapura": "Chickballapur",
    "chikkamangalore": "Chikkamagaluru",
    "chinthamani": "Chintamani",
    "chitamani": "Chintamani",
    "chithamani": "Chintamani",
    "dharmastla": "Dharmasthala",
    "dhrmasthala": "Dharmasthala",
    "gowribidanuru": "Gowribidanur",
    "gurumadkal": "Gurmitkal",
    "gulbarga": "Kalaburagi",
    "hasan": "Hassan",
    "hasana": "Hassan",
    "hassan 2": "Hassan",
    "hospete": "Hosapete",
    "homnabad": "Humnabad",
    "hubbali": "Hubballi",
    "huliyaru": "Huliyar",
    "jindal jsw": "Toranagallu",
    "kalburgi": "Kalaburagi",
    "kgf": "Kolar Gold Fields",
    "kolara": "Kolar",
    "koppala": "Koppal",
    "kudlagi": "Kudligi",
    "kukkanuru": "Kukkanur",
    "kukkanur": "Kukanur",
    "kuknooru": "Kukkanur",
    "kuragodu": "Kurugodu",
    "kurgodu": "Kurugodu",
    "lingasur": "Lingasugur",
    "lingasuru": "Lingasugur",
    "maisuru": "Mysuru",
    "manthralaya": "Mantralaya",
    "muddebihala": "Muddebihal",
    "mudebihala": "Muddebihal",
    "mysore": "Mysuru",
    "nanjanagudu": "Nanjangud",
    "jalhali": "Jalahalli",
    "panchanahally": "Panchanahalli",
    "piriyapattana": "Periyapatna",
    "raichuru": "Raichur",
    "ramanathpura": "Ramanathapura",
    "ranebennuru": "Ranebennur",
    "rayadurga": "Rayadurg",
    "shapura": "Shahpur",
    "shidalaghata": "Sidlaghatta",
    "shrinivas ura": "Srinivasapura",
    "shrinivasapura": "Srinivasapura",
    "shrinivaspura": "Srinivasapura",
    "sidlghatta": "Sidlaghatta",
    "sakleshpura": "Sakleshpur",
    "sedum": "Sedam",
    "shiraguppa": "Siruguppa",
    "sindanoor": "Sindhanur",
    "sindhanuru": "Sindhanur",
    "siraguppa": "Siruguppa",
    "sirgoppa": "Siruguppa",
    "subrhamnya": "Subrahmanya",
    "tumkur": "Tumakuru",
    "umnabad": "Homnabad",
    "umnabada": "Homnabad",
    "yallpur": "Yellapur",
}


@st.cache_data(show_spinner=False)
def load_csv(path_str: str, version: float | None = None) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

    # Handle CSV files where each row is wrapped as one quoted string.
    if df.shape[1] == 1 and "," in df.columns[0]:
        raw_col = df.columns[0]
        split_df = df[raw_col].astype(str).str.split(",", expand=True)
        headers = [header.strip() for header in raw_col.split(",")]
        if split_df.shape[1] == len(headers):
            split_df.columns = headers
            return split_df

    return df


def slugify_text(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", str(value).strip().lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def normalize_location_name(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return ""

    cleaned = raw.replace("[", " ").replace("]", " ").replace("(", " ").replace(")", " ")
    cleaned = cleaned.replace("_", " ").replace(".", " ").replace("/", " ")
    cleaned = re.sub(r"-\d+$", "", cleaned.strip())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    slug = slugify_text(cleaned)
    if not slug or slug in SERVICE_LABELS:
        return ""

    alias = LOCATION_ALIASES.get(slug)
    if alias:
        return alias

    return cleaned.title()


def normalize_schedule_data(schedule_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["route_id", "route_name", "origin_city", "destination_city", "trip_count"]
    if schedule_df.empty:
        return pd.DataFrame(columns=columns)

    df = schedule_df.copy()
    df.columns = [column.strip().lower() for column in df.columns]
    if not {"origin", "destination"}.issubset(df.columns):
        return pd.DataFrame(columns=columns)

    pairs_df = pd.DataFrame(
        {
            "origin_city": df["origin"].map(normalize_location_name),
            "destination_city": df["destination"].map(normalize_location_name),
        }
    )
    valid_pairs = (
        pairs_df["origin_city"].str.len().fillna(0).ge(2)
        & pairs_df["destination_city"].str.len().fillna(0).ge(2)
        & pairs_df["origin_city"].ne(pairs_df["destination_city"])
    )
    pairs_df = pairs_df.loc[valid_pairs].copy()
    if pairs_df.empty:
        return pd.DataFrame(columns=columns)

    grouped = (
        pairs_df.groupby(["origin_city", "destination_city"], as_index=False)
        .size()
        .rename(columns={"size": "trip_count"})
        .sort_values(["trip_count", "origin_city", "destination_city"], ascending=[False, True, True])
        .reset_index(drop=True)
    )
    grouped["route_name"] = grouped["origin_city"] + " -> " + grouped["destination_city"]
    grouped["route_id"] = grouped["route_name"].map(
        lambda route_name: "OD-" + hashlib.sha1(route_name.encode("utf-8")).hexdigest()[:8].upper()
    )

    return grouped[columns]


def normalize_city_coordinate_cache(cache_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["city", "lat", "lon", "display_name", "query"]
    if cache_df.empty:
        return pd.DataFrame(columns=columns)

    df = cache_df.copy()
    df.columns = [column.strip().lower() for column in df.columns]
    if not {"city", "lat", "lon"}.issubset(df.columns):
        return pd.DataFrame(columns=columns)

    if "display_name" not in df.columns:
        df["display_name"] = df["city"].astype(str)
    if "query" not in df.columns:
        df["query"] = df["city"].astype(str)

    df["city"] = df["city"].astype(str).str.strip()
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["display_name"] = df["display_name"].astype(str)
    df["query"] = df["query"].astype(str)
    df = df.dropna(subset=["city", "lat", "lon"])
    df = df[df["city"].str.len() > 0]

    return df[columns].drop_duplicates(subset=["city"]).sort_values("city").reset_index(drop=True)


def load_unresolved_city_cache(path: Path) -> set[str]:
    if not path.exists():
        return set()

    try:
        unresolved_df = pd.read_csv(path)
    except Exception:
        return set()

    if "city" not in unresolved_df.columns:
        return set()

    return set(unresolved_df["city"].dropna().astype(str).str.strip().tolist())


def save_unresolved_city_cache(path: Path, unresolved_cities: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"city": sorted(city for city in unresolved_cities if city)}).to_csv(path, index=False)


def geocode_city(city_name: str) -> dict | None:
    normalized_city = normalize_location_name(city_name)
    if not normalized_city:
        return None

    queries = [
        f"{normalized_city}, Karnataka, India",
        f"{normalized_city}, India",
    ]
    for query in queries:
        url = "https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&q=" + quote(query)
        request = Request(url, headers={"User-Agent": NOMINATIM_USER_AGENT})
        try:
            with urlopen(request, timeout=20) as response:
                payload = json.load(response)
        except (HTTPError, URLError, TimeoutError, ConnectionError, OSError, json.JSONDecodeError):
            payload = []

        if payload:
            match = payload[0]
            return {
                "city": normalized_city,
                "lat": float(match["lat"]),
                "lon": float(match["lon"]),
                "display_name": str(match.get("display_name", normalized_city)),
                "query": query,
            }

    return None


def synthesize_city_coordinates(
    schedule_routes_df: pd.DataFrame,
    cache_df: pd.DataFrame,
    missing_cities: list[str],
) -> list[dict]:
    if schedule_routes_df.empty or not missing_cities:
        return []

    known_df = normalize_city_coordinate_cache(cache_df)
    if known_df.empty:
        return []

    coordinate_lookup = {
        str(row.city): (float(row.lat), float(row.lon))
        for row in known_df.itertuples(index=False)
    }
    pending = [city for city in missing_cities if city and city not in coordinate_lookup]
    synthesized_rows: list[dict] = []

    # Iterate so newly synthesized villages can help resolve other nearby villages.
    for _ in range(3):
        progress = False
        next_pending: list[str] = []
        for city_name in pending:
            origin_neighbors = schedule_routes_df.loc[
                schedule_routes_df["origin_city"].astype(str) == city_name, "destination_city"
            ].astype(str)
            destination_neighbors = schedule_routes_df.loc[
                schedule_routes_df["destination_city"].astype(str) == city_name, "origin_city"
            ].astype(str)
            neighbor_names = sorted(set(origin_neighbors.tolist() + destination_neighbors.tolist()))
            known_neighbors = [coordinate_lookup[name] for name in neighbor_names if name in coordinate_lookup]

            if not known_neighbors:
                next_pending.append(city_name)
                continue

            avg_lat = sum(lat for lat, _ in known_neighbors) / len(known_neighbors)
            avg_lon = sum(lon for _, lon in known_neighbors) / len(known_neighbors)

            # Spread villages around the nearby known city cluster so routes remain visible.
            city_hash = int(hashlib.sha1(city_name.encode("utf-8")).hexdigest()[:8], 16)
            angle = (city_hash % 360) * math.pi / 180.0
            radius = 0.06 + ((city_hash >> 8) % 8) * 0.01
            lat = avg_lat + math.sin(angle) * radius
            lon = avg_lon + math.cos(angle) * radius

            row = {
                "city": city_name,
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "display_name": f"{city_name} (approximate)",
                "query": "synthetic-neighbor-fallback",
            }
            synthesized_rows.append(row)
            coordinate_lookup[city_name] = (row["lat"], row["lon"])
            progress = True

        pending = next_pending
        if not progress:
            break

    return synthesized_rows


def load_or_create_city_coordinates(schedule_routes_df: pd.DataFrame, cache_path: Path) -> pd.DataFrame:
    if cache_path.exists():
        try:
            cache_df = normalize_city_coordinate_cache(pd.read_csv(cache_path))
        except Exception:
            cache_df = pd.DataFrame(columns=["city", "lat", "lon", "display_name", "query"])
    else:
        cache_df = pd.DataFrame(columns=["city", "lat", "lon", "display_name", "query"])

    unresolved_cache = load_unresolved_city_cache(CITY_COORDINATES_MISSING_FILE)
    if schedule_routes_df.empty:
        return cache_df

    requested_cities = sorted(
        set(schedule_routes_df["origin_city"].astype(str)).union(set(schedule_routes_df["destination_city"].astype(str)))
    )
    known_cities = set(cache_df["city"].astype(str).tolist())
    missing_cities = [city for city in requested_cities if city and city not in known_cities]

    if not missing_cities:
        return cache_df

    fetched_rows: list[dict] = []
    unresolved_updates = set(unresolved_cache)
    geocode_candidates = [city for city in missing_cities if city not in unresolved_cache]
    for city_index, city_name in enumerate(geocode_candidates):
        geocoded = geocode_city(city_name)
        if geocoded is not None:
            fetched_rows.append(geocoded)
        else:
            unresolved_updates.add(city_name)
        if city_index < len(geocode_candidates) - 1:
            time.sleep(1.0)

    if fetched_rows:
        cache_df = pd.concat([cache_df, pd.DataFrame(fetched_rows)], ignore_index=True)
        cache_df = normalize_city_coordinate_cache(cache_df)

    remaining_cities = [
        city for city in missing_cities if city and city not in set(cache_df["city"].astype(str).tolist())
    ]
    synthesized_rows = synthesize_city_coordinates(schedule_routes_df, cache_df, remaining_cities)
    if synthesized_rows:
        cache_df = pd.concat([cache_df, pd.DataFrame(synthesized_rows)], ignore_index=True)
        cache_df = normalize_city_coordinate_cache(cache_df)
        resolved_synthetic_cities = {row["city"] for row in synthesized_rows}
        unresolved_updates = {city for city in unresolved_updates if city not in resolved_synthetic_cities}

    resolved_city_names = set(cache_df["city"].astype(str).tolist())
    unresolved_updates = {
        city_name
        for city_name in unresolved_updates
        if normalize_location_name(city_name) not in resolved_city_names
    }

    if fetched_rows or synthesized_rows:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_df.to_csv(cache_path, index=False)
    if unresolved_updates != unresolved_cache:
        save_unresolved_city_cache(CITY_COORDINATES_MISSING_FILE, unresolved_updates)

    return cache_df


def build_route_control_point(origin_lat: float, origin_lon: float, destination_lat: float, destination_lon: float, route_index: int) -> tuple[float, float]:
    mid_lat = (origin_lat + destination_lat) / 2
    mid_lon = (origin_lon + destination_lon) / 2

    d_lat = destination_lat - origin_lat
    d_lon = destination_lon - origin_lon
    distance = math.hypot(d_lat, d_lon)
    if distance == 0:
        return mid_lat, mid_lon

    offset_scale = min(0.35, max(0.05, distance * 0.12))
    direction = -1 if route_index % 2 else 1
    control_lat = mid_lat + direction * (-d_lon / distance) * offset_scale
    control_lon = mid_lon + direction * (d_lat / distance) * offset_scale

    return float(control_lat), float(control_lon)


def build_routes_from_schedule(schedule_routes_df: pd.DataFrame, city_coordinates_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    columns = ["route_id", "route_name", "seq", "stop", "lat", "lon", "stop_id", "origin_city", "destination_city", "trip_count"]
    if schedule_routes_df.empty or city_coordinates_df.empty:
        return pd.DataFrame(columns=columns), []

    coordinate_lookup = city_coordinates_df.set_index("city")[["lat", "lon"]]
    records: list[dict] = []
    skipped_routes: list[str] = []

    for route_index, row in enumerate(schedule_routes_df.itertuples(index=False), start=1):
        if row.origin_city not in coordinate_lookup.index or row.destination_city not in coordinate_lookup.index:
            skipped_routes.append(row.route_name)
            continue

        origin = coordinate_lookup.loc[row.origin_city]
        destination = coordinate_lookup.loc[row.destination_city]
        control_lat, control_lon = build_route_control_point(
            float(origin["lat"]),
            float(origin["lon"]),
            float(destination["lat"]),
            float(destination["lon"]),
            route_index=route_index,
        )
        stop_seed = route_index * 10
        corridor_label = f"{row.origin_city} Corridor"

        records.extend(
            [
                {
                    "route_id": row.route_id,
                    "route_name": row.route_name,
                    "seq": 1,
                    "stop": row.origin_city,
                    "lat": float(origin["lat"]),
                    "lon": float(origin["lon"]),
                    "stop_id": stop_seed + 1,
                    "origin_city": row.origin_city,
                    "destination_city": row.destination_city,
                    "trip_count": int(row.trip_count),
                },
                {
                    "route_id": row.route_id,
                    "route_name": row.route_name,
                    "seq": 2,
                    "stop": corridor_label,
                    "lat": control_lat,
                    "lon": control_lon,
                    "stop_id": stop_seed + 2,
                    "origin_city": row.origin_city,
                    "destination_city": row.destination_city,
                    "trip_count": int(row.trip_count),
                },
                {
                    "route_id": row.route_id,
                    "route_name": row.route_name,
                    "seq": 3,
                    "stop": row.destination_city,
                    "lat": float(destination["lat"]),
                    "lon": float(destination["lon"]),
                    "stop_id": stop_seed + 3,
                    "origin_city": row.origin_city,
                    "destination_city": row.destination_city,
                    "trip_count": int(row.trip_count),
                },
            ]
        )

    return pd.DataFrame(records, columns=columns), skipped_routes


def normalize_route_data(route_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["route_id", "route_name", "seq", "stop", "lat", "lon", "stop_id", "origin_city", "destination_city", "trip_count"]
    if route_df.empty:
        return pd.DataFrame(columns=columns)

    df = route_df.copy()
    df.columns = [column.strip().lower() for column in df.columns]

    rename_map = {
        "latitude": "lat",
        "longitude": "lon",
        "stop_name": "stop",
        "name": "stop",
    }
    df = df.rename(columns={key: value for key, value in rename_map.items() if key in df.columns})

    if "stop" not in df.columns:
        df["stop"] = "Stop-" + (df.index + 1).astype(str)

    if "route_id" not in df.columns:
        df["route_id"] = "R101"

    if "route_name" not in df.columns:
        df["route_name"] = df["route_id"].astype(str)

    if "seq" not in df.columns:
        df["seq"] = np.arange(1, df.shape[0] + 1)

    if "stop_id" not in df.columns:
        numeric_ids = pd.to_numeric(df["stop"].astype(str).str.extract(r"(\d+)", expand=False), errors="coerce")
        df["stop_id"] = numeric_ids
    if "origin_city" not in df.columns or "destination_city" not in df.columns:
        split_names = df["route_name"].astype(str).map(split_route_name)
        if "origin_city" not in df.columns:
            df["origin_city"] = split_names.map(lambda item: item[0])
        if "destination_city" not in df.columns:
            df["destination_city"] = split_names.map(lambda item: item[1])
    if "trip_count" not in df.columns:
        df["trip_count"] = 1

    required = {"route_id", "route_name", "seq", "stop", "lat", "lon"}
    if not required.issubset(df.columns):
        return pd.DataFrame(columns=columns)

    df["seq"] = pd.to_numeric(df["seq"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["stop_id"] = pd.to_numeric(df["stop_id"], errors="coerce")
    df["trip_count"] = pd.to_numeric(df["trip_count"], errors="coerce").fillna(1)

    df = df.dropna(subset=["route_id", "route_name", "seq", "stop", "lat", "lon"])
    df["route_id"] = df["route_id"].astype(str)
    df["route_name"] = df["route_name"].astype(str)
    df["origin_city"] = df["origin_city"].fillna("").astype(str)
    df["destination_city"] = df["destination_city"].fillna("").astype(str)
    df["seq"] = df["seq"].astype(int)
    df["trip_count"] = df["trip_count"].astype(int)

    return df[columns].sort_values(["route_id", "seq"]).reset_index(drop=True)


def normalize_stops(stops_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["stop_id", "stop_name", "latitude", "longitude"]
    if stops_df.empty:
        return pd.DataFrame(columns=columns)

    df = stops_df.copy()
    df.columns = [column.strip().lower() for column in df.columns]

    rename_map = {}
    if "lat" in df.columns and "latitude" not in df.columns:
        rename_map["lat"] = "latitude"
    if "lon" in df.columns and "longitude" not in df.columns:
        rename_map["lon"] = "longitude"
    if "name" in df.columns and "stop_name" not in df.columns:
        rename_map["name"] = "stop_name"
    if rename_map:
        df = df.rename(columns=rename_map)

    required = {"stop_id", "latitude", "longitude"}
    if not required.issubset(df.columns):
        return pd.DataFrame(columns=columns)

    if "stop_name" not in df.columns:
        df["stop_name"] = "Stop-" + df["stop_id"].astype(str)

    df["stop_id"] = pd.to_numeric(df["stop_id"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    df = df.dropna(subset=["stop_id", "latitude", "longitude"])
    df["stop_id"] = df["stop_id"].astype(int)

    return df[columns].sort_values("stop_id").reset_index(drop=True)


def normalize_shortest_paths(shortest_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["source_stop", "destination_stop", "estimated_cost"]
    if shortest_df.empty:
        return pd.DataFrame(columns=columns)

    df = shortest_df.copy()
    df.columns = [column.strip().lower() for column in df.columns]
    if not {"source_stop", "destination_stop"}.issubset(df.columns):
        return pd.DataFrame(columns=columns)

    if "estimated_cost" not in df.columns:
        df["estimated_cost"] = 1

    df["source_stop"] = pd.to_numeric(df["source_stop"], errors="coerce")
    df["destination_stop"] = pd.to_numeric(df["destination_stop"], errors="coerce")
    df["estimated_cost"] = pd.to_numeric(df["estimated_cost"], errors="coerce")
    df = df.dropna(subset=["source_stop", "destination_stop"])

    df["source_stop"] = df["source_stop"].astype(int)
    df["destination_stop"] = df["destination_stop"].astype(int)

    return df[columns]


def normalize_pagerank(pagerank_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["stop_id", "pagerank"]
    if pagerank_df.empty:
        return pd.DataFrame(columns=columns)

    df = pagerank_df.copy()
    df.columns = [column.strip().lower() for column in df.columns]
    if not {"stop_id", "pagerank"}.issubset(df.columns):
        return pd.DataFrame(columns=columns)

    df["stop_id"] = pd.to_numeric(df["stop_id"], errors="coerce")
    df["pagerank"] = pd.to_numeric(df["pagerank"], errors="coerce")
    df = df.dropna(subset=["stop_id", "pagerank"])
    df["stop_id"] = df["stop_id"].astype(int)

    return df[columns]


def normalize_predictions(prediction_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["stop", "prediction"]
    if prediction_df.empty:
        return pd.DataFrame(columns=columns)

    df = prediction_df.copy()
    df.columns = [column.strip().lower() for column in df.columns]
    if not {"stop", "prediction"}.issubset(df.columns):
        return pd.DataFrame(columns=columns)

    df["prediction"] = pd.to_numeric(df["prediction"], errors="coerce")
    df = df.dropna(subset=["prediction"])

    return df[columns]


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    lat1_r = math.radians(lat1)
    lat2_r = math.radians(lat2)

    a_val = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    )
    c_val = 2 * math.atan2(math.sqrt(a_val), math.sqrt(1 - a_val))
    return float(6371.0 * c_val)


def normalize_gps(gps_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["ID", "Timestamp", "Latitude", "Longitude", "Speed_kmh"]
    if gps_df.empty:
        return pd.DataFrame(columns=columns)

    df = gps_df.copy()
    df.columns = [str(column).strip() for column in df.columns]

    alias_map = {
        "id": "ID",
        "busid": "ID",
        "vehicleid": "ID",
        "vehicleno": "ID",
        "vehicle": "ID",
        "busnumber": "ID",
        "timestamp": "Timestamp",
        "time": "Timestamp",
        "datetime": "Timestamp",
        "recordedat": "Timestamp",
        "latitude": "Latitude",
        "lat": "Latitude",
        "longitude": "Longitude",
        "lon": "Longitude",
        "lng": "Longitude",
        "long": "Longitude",
        "speedkmh": "Speed_kmh",
        "speed": "Speed_kmh",
        "speedkmph": "Speed_kmh",
        "speedkmhr": "Speed_kmh",
    }
    rename_map: dict[str, str] = {}
    seen_targets = set(df.columns)
    for column in df.columns:
        normalized = re.sub(r"[^a-z0-9]+", "", column.lower())
        target = alias_map.get(normalized)
        if target and target not in seen_targets:
            rename_map[column] = target
            seen_targets.add(target)

    if rename_map:
        df = df.rename(columns=rename_map)

    required = {"ID", "Latitude", "Longitude"}
    if not required.issubset(df.columns):
        return pd.DataFrame(columns=columns)

    if "Timestamp" not in df.columns:
        df["Timestamp"] = pd.NaT
    if "Speed_kmh" not in df.columns:
        df["Speed_kmh"] = np.nan

    df["ID"] = df["ID"].astype(str).str.strip()
    df["ID"] = df["ID"].replace({"": np.nan, "nan": np.nan, "None": np.nan})
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")
    df["Speed_kmh"] = pd.to_numeric(df["Speed_kmh"], errors="coerce")
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce", utc=True).dt.tz_localize(None)
    df = df.dropna(subset=["ID", "Latitude", "Longitude"])

    needs_speed = df["Speed_kmh"].isna()
    if needs_speed.any() and df["Timestamp"].notna().any():
        ordered = df.sort_values(["ID", "Timestamp"]).copy()
        ordered["prev_latitude"] = ordered.groupby("ID")["Latitude"].shift(1)
        ordered["prev_longitude"] = ordered.groupby("ID")["Longitude"].shift(1)
        ordered["prev_timestamp"] = ordered.groupby("ID")["Timestamp"].shift(1)

        elapsed_hours = (
            (ordered["Timestamp"] - ordered["prev_timestamp"]).dt.total_seconds() / 3600.0
        )
        distance_km = ordered.apply(
            lambda row: haversine_distance_km(
                row["prev_latitude"],
                row["prev_longitude"],
                row["Latitude"],
                row["Longitude"],
            )
            if pd.notna(row["prev_latitude"])
            and pd.notna(row["prev_longitude"])
            and pd.notna(row["Timestamp"])
            and pd.notna(row["prev_timestamp"])
            else np.nan,
            axis=1,
        )
        derived_speed = distance_km / elapsed_hours.replace(0, np.nan)
        ordered["Speed_kmh"] = ordered["Speed_kmh"].fillna(derived_speed.clip(lower=0, upper=110))
        df = ordered.drop(columns=["prev_latitude", "prev_longitude", "prev_timestamp"])

    df = df.dropna(subset=["Speed_kmh"])
    return df[columns]


def build_stop_catalog(stops_df: pd.DataFrame, route_df: pd.DataFrame) -> pd.DataFrame:
    if not stops_df.empty:
        return stops_df

    if route_df.empty:
        return pd.DataFrame(columns=["stop_id", "stop_name", "latitude", "longitude"])

    fallback = route_df.rename(
        columns={
            "stop": "stop_name",
            "lat": "latitude",
            "lon": "longitude",
        }
    )[["stop_id", "stop_name", "latitude", "longitude"]].copy()

    fallback["stop_id"] = pd.to_numeric(fallback["stop_id"], errors="coerce")
    missing_stop_ids = fallback["stop_id"].isna()
    if missing_stop_ids.any():
        fallback.loc[missing_stop_ids, "stop_id"] = np.arange(1, missing_stop_ids.sum() + 1, dtype=int)

    fallback = (
        fallback.dropna(subset=["stop_name", "latitude", "longitude"])
        .drop_duplicates(subset=["stop_name", "latitude", "longitude"])
        .reset_index(drop=True)
    )
    fallback["stop_id"] = np.arange(1, fallback.shape[0] + 1, dtype=int)

    return normalize_stops(fallback)


def get_route_order_from_graph(stops_df: pd.DataFrame, shortest_df: pd.DataFrame) -> list[int]:
    stop_ids = set(stops_df["stop_id"].tolist())
    route_order: list[int] = []

    if not shortest_df.empty:
        for row in shortest_df.itertuples(index=False):
            source = int(row.source_stop)
            destination = int(row.destination_stop)
            if source in stop_ids and source not in route_order:
                route_order.append(source)
            if destination in stop_ids and destination not in route_order:
                route_order.append(destination)

    if len(route_order) < 2:
        route_order = sorted(stop_ids)
    else:
        for stop_id in sorted(stop_ids):
            if stop_id not in route_order:
                route_order.append(stop_id)

    return route_order


def build_route_from_graph(stops_df: pd.DataFrame, shortest_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["route_id", "route_name", "seq", "stop", "lat", "lon", "stop_id"]
    if stops_df.empty:
        return pd.DataFrame(columns=columns)

    route_order = get_route_order_from_graph(stops_df, shortest_df)
    if not route_order:
        return pd.DataFrame(columns=columns)

    base = stops_df.set_index("stop_id")
    route = base.reindex(route_order).dropna().reset_index()
    route = route.rename(columns={"stop_name": "stop", "latitude": "lat", "longitude": "lon"})

    first_stop = str(route.iloc[0]["stop"])
    last_stop = str(route.iloc[-1]["stop"])
    route["route_id"] = "R101"
    route["route_name"] = f"{first_stop} -> {last_stop}"
    route["seq"] = np.arange(1, route.shape[0] + 1)

    return route[columns]


@st.cache_data(show_spinner=False)
def load_project_route_names(route_dir_str: str, limit: int | None = None) -> list[str]:
    route_dir = Path(route_dir_str)
    if not route_dir.exists() or not route_dir.is_dir():
        return []

    route_names: set[str] = set()

    for csv_path in sorted(route_dir.rglob("*.csv")):
        try:
            header = pd.read_csv(csv_path, nrows=0)
        except Exception:
            continue

        columns = {column.strip().lower(): column for column in header.columns}
        if "origin" not in columns or "destination" not in columns:
            continue

        origin_col = columns["origin"]
        destination_col = columns["destination"]

        try:
            pairs_df = pd.read_csv(csv_path, usecols=[origin_col, destination_col], dtype=str)
        except Exception:
            continue

        origins = pairs_df[origin_col].fillna("").astype(str).str.strip()
        destinations = pairs_df[destination_col].fillna("").astype(str).str.strip()
        names = origins + " -> " + destinations
        names = names[(origins.str.len() >= 2) & (destinations.str.len() >= 2)]
        names = names[names.str.len() >= 7]
        names = names[~names.str.contains(r"->\s*[A-Za-z]\.?$", regex=True)]

        for name in names.tolist():
            if "->" in name:
                route_names.add(name)

    names_sorted = sorted(route_names)
    if limit is None:
        return names_sorted
    return names_sorted[:limit]


def normalize_single_placeholder_route_id(route_df: pd.DataFrame) -> pd.DataFrame:
    if route_df.empty or route_df["route_id"].nunique() != 1:
        return route_df

    route_id_val = str(route_df.iloc[0]["route_id"]).strip().lower()
    if not route_id_val.startswith("route"):
        return route_df

    normalized = route_df.copy()
    normalized["route_id"] = "R101"
    return normalized


def split_route_name(route_name: str) -> tuple[str, str]:
    name = str(route_name).strip()
    if "->" not in name:
        return name, ""

    origin, _, destination = name.partition("->")
    return origin.strip(), destination.strip()


@st.cache_data(show_spinner=False)
def build_additional_routes_from_stops(
    _stops_df: pd.DataFrame,
    route_names: tuple[str, ...],
    route_count: int = 8,
    stops_per_route: int = 10,
    stride: int = 9,
    start_route_number: int = 201,
) -> pd.DataFrame:
    columns = ["route_id", "route_name", "seq", "stop", "lat", "lon", "stop_id"]
    if _stops_df.empty or _stops_df.shape[0] < 4:
        return pd.DataFrame(columns=columns)

    total_stops = _stops_df.shape[0]
    max_window = min(max(6, stops_per_route), total_stops)
    min_window = min(max_window, max(4, max_window - 3))
    window_span = max(1, max_window - min_window + 1)
    n_routes = max(1, route_count)

    records: list[dict] = []
    used_names: set[str] = set()

    for route_idx in range(n_routes):
        start = (route_idx * stride) % total_stops
        current_window = max_window - (route_idx % window_span)
        indices = [(start + offset) % total_stops for offset in range(current_window)]
        route_stops = _stops_df.iloc[indices].reset_index(drop=True)
        if route_idx % 2 == 1:
            route_stops = route_stops.iloc[::-1].reset_index(drop=True)

        default_name = f"{route_stops.iloc[0]['stop_name']} -> {route_stops.iloc[-1]['stop_name']}"
        chosen_name = route_names[route_idx] if route_idx < len(route_names) else default_name
        route_name = chosen_name if chosen_name and chosen_name not in used_names else default_name
        used_names.add(route_name)

        route_id = f"R{start_route_number + route_idx}"

        for seq_num, row in enumerate(route_stops.itertuples(index=False), start=1):
            records.append(
                {
                    "route_id": route_id,
                    "route_name": route_name,
                    "seq": seq_num,
                    "stop": row.stop_name,
                    "lat": float(row.latitude),
                    "lon": float(row.longitude),
                    "stop_id": int(row.stop_id),
                }
            )

    return pd.DataFrame(records, columns=columns)


def slice_route_points(route_points: pd.DataFrame, start_index: int, stop_count: int) -> pd.DataFrame:
    if route_points.shape[0] <= 1:
        return route_points.copy().reset_index(drop=True)

    total = route_points.shape[0]
    if stop_count >= total:
        return route_points.copy().reset_index(drop=True)

    indices = [(start_index + offset) % total for offset in range(stop_count)]
    return route_points.iloc[indices].reset_index(drop=True)


def interpolate_route_points(route_points: pd.DataFrame, points_per_segment: int, loop_route: bool) -> pd.DataFrame:
    if route_points.shape[0] < 2:
        return pd.DataFrame(columns=["lat", "lon", "next_stop_idx"])

    records: list[dict] = []
    stop_count = route_points.shape[0]
    segment_count = stop_count if loop_route else stop_count - 1

    for segment in range(segment_count):
        source = route_points.iloc[segment]
        destination = route_points.iloc[(segment + 1) % stop_count]
        next_stop_idx = (segment + 1) % stop_count

        lat_values = np.linspace(source["lat"], destination["lat"], points_per_segment, endpoint=False)
        lon_values = np.linspace(source["lon"], destination["lon"], points_per_segment, endpoint=False)

        for lat_value, lon_value in zip(lat_values, lon_values):
            records.append(
                {
                    "lat": float(lat_value),
                    "lon": float(lon_value),
                    "next_stop_idx": int(next_stop_idx),
                }
            )

    if not loop_route:
        last_stop = route_points.iloc[-1]
        records.append(
            {
                "lat": float(last_stop["lat"]),
                "lon": float(last_stop["lon"]),
                "next_stop_idx": int(stop_count - 1),
            }
        )

    return pd.DataFrame(records)


def build_city_columns(route_points: pd.DataFrame, density: int = 2) -> pd.DataFrame:
    if route_points.empty:
        return pd.DataFrame(columns=["lat", "lon", "elevation", "color"])

    offsets = [
        (-0.0012, -0.0010),
        (-0.0010, 0.0011),
        (0.0011, -0.0012),
        (0.0010, 0.0010),
        (0.0, -0.0014),
        (-0.0014, 0.0),
        (0.0014, 0.0),
        (0.0, 0.0014),
    ]
    selected_offsets = offsets[: max(1, min(len(offsets), density * 3))]

    records: list[dict] = []
    for stop_index, row in enumerate(route_points.itertuples(index=False)):
        for offset_index, (lat_offset, lon_offset) in enumerate(selected_offsets):
            wave = abs(math.sin((stop_index + 1) * (offset_index + 2)))
            elevation = 25 + wave * 320
            tone = int(120 + wave * 90)
            records.append(
                {
                    "lat": float(row.lat + lat_offset),
                    "lon": float(row.lon + lon_offset),
                    "elevation": float(elevation),
                    "color": [tone, tone + 8, min(255, tone + 18), 160],
                }
            )

    return pd.DataFrame(records)


def estimate_route_distance_km(route_points: pd.DataFrame, loop_route: bool) -> float:
    if route_points.shape[0] < 2:
        return 0.0

    coords = route_points[["lat", "lon"]].to_numpy(dtype=float)
    total = 0.0

    max_index = coords.shape[0] if loop_route else coords.shape[0] - 1
    for idx in range(max_index):
        lat1, lon1 = coords[idx]
        lat2, lon2 = coords[(idx + 1) % coords.shape[0]]

        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        lat1_r = math.radians(lat1)
        lat2_r = math.radians(lat2)

        a_val = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
        )
        c_val = 2 * math.atan2(math.sqrt(a_val), math.sqrt(1 - a_val))
        total += 6371.0 * c_val

    return float(total)



def build_route_segments(
    route_points: pd.DataFrame,
    frame_index: int,
    loop_route: bool,
    route_name: str | None = None,
    route_id: str | None = None,
) -> pd.DataFrame:
    columns = ["name", "path", "color", "traffic", "detail"]
    if route_points.shape[0] < 2:
        return pd.DataFrame(columns=columns)

    records: list[dict] = []
    stop_count = route_points.shape[0]
    segment_count = stop_count if loop_route else stop_count - 1

    for segment in range(segment_count):
        source = route_points.iloc[segment]
        destination = route_points.iloc[(segment + 1) % stop_count]

        traffic_wave = (math.sin((frame_index / 7.0) + (segment * 0.95)) + 1) / 2
        if traffic_wave < 0.35:
            traffic = "Low"
            color = [40, 167, 69, 230]
        elif traffic_wave < 0.7:
            traffic = "Medium"
            color = [255, 193, 7, 230]
        else:
            traffic = "High"
            color = [220, 53, 69, 230]

        records.append(
            {
                "name": route_name or f"Segment {segment + 1}",
                "path": [[float(source["lon"]), float(source["lat"])], [float(destination["lon"]), float(destination["lat"])]],
                "color": color,
                "traffic": traffic,
                "detail": (
                    f"{route_name or 'Route'}"
                    + (f" [{route_id}]" if route_id else "")
                    + f"<br/>{source['stop']} -> {destination['stop']}<br/>Traffic: {traffic}"
                ),
            }
        )

    return pd.DataFrame(records, columns=columns)


def build_bus_states(
    path_points: pd.DataFrame,
    route_points: pd.DataFrame,
    frame_index: int,
    bus_count: int,
    trail_length: int,
    bus_id_offset: int = 0,
    route_id: str = "",
    route_name: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    bus_columns = [
        "bus_id",
        "route_id",
        "route_name",
        "lat",
        "lon",
        "speed_kmh",
        "delay_min",
        "status",
        "next_stop",
        "color",
        "name",
        "detail",
        "icon",
    ]
    trail_columns = ["bus_id", "trail_path", "color"]

    if path_points.empty or bus_count <= 0:
        return pd.DataFrame(columns=bus_columns), pd.DataFrame(columns=trail_columns)

    total_points = path_points.shape[0]
    buses: list[dict] = []
    trails: list[dict] = []

    for bus_index in range(bus_count):
        phase_offset = int(bus_index * total_points / max(1, bus_count))
        point_index = (frame_index + phase_offset) % total_points
        point = path_points.iloc[point_index]

        wave = (math.sin((frame_index + bus_index * 13) / 10.0) + 1) / 2
        speed = 20 + (1 - wave) * 36
        delay = max(0.0, (wave - 0.48) * 16)

        if delay >= 4:
            status = "Delayed"
            color = [220, 53, 69, 220]
        elif delay >= 1.5:
            status = "Moderate"
            color = [255, 193, 7, 220]
        else:
            status = "On Time"
            color = [40, 167, 69, 220]

        next_stop_idx = int(point["next_stop_idx"])
        next_stop_name = route_points.iloc[next_stop_idx]["stop"]
        bus_id = f"KSRTC-{101 + bus_id_offset + bus_index}"
        route_label = route_name or route_id or "Simulated Route"
        speed_kmh = round(speed, 1)
        delay_min = round(delay, 1)

        buses.append(
            {
                "bus_id": bus_id,
                "route_id": route_id,
                "route_name": route_name,
                "lat": float(point["lat"]),
                "lon": float(point["lon"]),
                "speed_kmh": speed_kmh,
                "delay_min": delay_min,
                "status": status,
                "next_stop": str(next_stop_name),
                "color": color,
                "name": bus_id,
                "detail": (
                    f"Route: {route_label}<br/>"
                    f"Speed: {speed_kmh} km/h<br/>"
                    f"Delay: {delay_min} min<br/>"
                    f"Status: {status}<br/>"
                    f"Next stop: {next_stop_name}"
                ),
                "icon": {"url": BUS_ICON_URL, "width": 96, "height": 96, "anchorY": 96},
            }
        )

        trail_indices = [((point_index - back) % total_points) for back in range(trail_length)]
        trail_indices.reverse()
        trail_path = path_points.iloc[trail_indices][["lon", "lat"]].values.tolist()
        trails.append({"bus_id": bus_id, "trail_path": trail_path, "color": color})

    return pd.DataFrame(buses), pd.DataFrame(trails)


def build_route_layer_frame(
    route_points: pd.DataFrame,
    path_points: pd.DataFrame,
    route_id: str,
    route_name: str,
    highlighted: bool,
) -> pd.DataFrame:
    columns = ["name", "path", "detail", "color", "width"]
    if route_points.empty or path_points.empty:
        return pd.DataFrame(columns=columns)

    color = [37, 99, 235, 210] if highlighted else [148, 163, 184, 90]
    width = 7 if highlighted else 4

    return pd.DataFrame(
        [
            {
                "name": route_name,
                "path": path_points[["lon", "lat"]].values.tolist(),
                "detail": f"{route_name} [{route_id}]<br/>Stops: {route_points.shape[0]}",
                "color": color,
                "width": width,
            }
        ],
        columns=columns,
    )


def build_network_fleet_state(
    route_df: pd.DataFrame,
    selected_route_id: str,
    frame_index: int,
    points_per_segment: int,
    loop_route: bool,
    bus_count: int,
    trail_length: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    route_layer_columns = ["name", "path", "detail", "color", "width"]
    route_groups = [
        (str(route_id), group.sort_values("seq").reset_index(drop=True))
        for route_id, group in route_df.groupby("route_id", sort=True)
        if group.shape[0] >= 2
    ]

    if not route_groups:
        empty_routes = pd.DataFrame(columns=route_layer_columns)
        empty_segments = pd.DataFrame(columns=["name", "path", "color", "traffic", "detail"])
        empty_buses = pd.DataFrame(
            columns=["bus_id", "route_id", "route_name", "lat", "lon", "speed_kmh", "delay_min", "status", "next_stop", "color", "name", "detail", "icon"]
        )
        empty_trails = pd.DataFrame(columns=["bus_id", "trail_path", "color"])
        return pd.DataFrame(columns=["lat", "lon"]), empty_routes, empty_segments, empty_buses, empty_trails

    base_bus_count = bus_count // len(route_groups)
    remainder = bus_count % len(route_groups)
    bus_id_offset = 0

    map_points_frames: list[pd.DataFrame] = []
    route_layer_frames: list[pd.DataFrame] = []
    bus_frames: list[pd.DataFrame] = []
    trail_frames: list[pd.DataFrame] = []
    selected_segments_df = pd.DataFrame(columns=["name", "path", "color", "traffic", "detail"])

    for route_index, (route_id, route_points) in enumerate(route_groups):
        route_name = str(route_points.iloc[0]["route_name"])
        highlighted = route_id == selected_route_id
        path_points = interpolate_route_points(route_points, points_per_segment=points_per_segment, loop_route=loop_route)
        if path_points.empty:
            continue

        map_points_frames.append(route_points[["lat", "lon"]])
        route_layer_frames.append(
            build_route_layer_frame(
                route_points=route_points,
                path_points=path_points,
                route_id=route_id,
                route_name=route_name,
                highlighted=highlighted,
            )
        )

        if highlighted:
            selected_segments_df = build_route_segments(
                route_points,
                frame_index=frame_index,
                loop_route=loop_route,
                route_name=route_name,
                route_id=route_id,
            )

        buses_for_route = base_bus_count + (1 if route_index < remainder else 0)
        route_buses_df, route_trails_df = build_bus_states(
            path_points=path_points,
            route_points=route_points,
            frame_index=frame_index + route_index * 5,
            bus_count=buses_for_route,
            trail_length=trail_length,
            bus_id_offset=bus_id_offset,
            route_id=route_id,
            route_name=route_name,
        )
        bus_id_offset += buses_for_route

        if not route_buses_df.empty:
            bus_frames.append(route_buses_df)
        if not route_trails_df.empty:
            trail_frames.append(route_trails_df)

    map_points_df = pd.concat(map_points_frames, ignore_index=True) if map_points_frames else pd.DataFrame(columns=["lat", "lon"])
    route_layer_df = (
        pd.concat(route_layer_frames, ignore_index=True) if route_layer_frames else pd.DataFrame(columns=route_layer_columns)
    )
    buses_df = pd.concat(bus_frames, ignore_index=True) if bus_frames else pd.DataFrame(
        columns=["bus_id", "route_id", "route_name", "lat", "lon", "speed_kmh", "delay_min", "status", "next_stop", "color", "name", "detail", "icon"]
    )
    trails_df = pd.concat(trail_frames, ignore_index=True) if trail_frames else pd.DataFrame(columns=["bus_id", "trail_path", "color"])

    return map_points_df, route_layer_df, selected_segments_df, buses_df, trails_df


def resolve_map_style(style_choice: str, mapbox_token: str) -> tuple[str, str | None, str | None]:
    style_map = {
        "Road": "road",
        "Light": "light",
        "Dark": "dark",
        "Mapbox Light": "mapbox://styles/mapbox/light-v11",
        "Mapbox Streets": "mapbox://styles/mapbox/streets-v12",
    }

    style = style_map.get(style_choice, "road")
    token = mapbox_token.strip() if mapbox_token else ""

    if style.startswith("mapbox://") and not token:
        warning = "Mapbox style selected without token. Falling back to built-in Road style."
        return "road", None, warning

    if token:
        return style, token, None
    return style, None, None


def render_live_map(
    map_points_df: pd.DataFrame,
    route_layer_df: pd.DataFrame,
    route_segments_df: pd.DataFrame,
    buses_df: pd.DataFrame,
    trails_df: pd.DataFrame,
    stop_points_df: pd.DataFrame,
    city_columns_df: pd.DataFrame,
    map_style: str,
    mapbox_token: str | None,
    zoom: float,
    pitch: int,
    bearing: int,
    show_stop_labels: bool,
    use_bus_icons: bool,
    enable_touch_rotate: bool,
    map_height: int,
    chart_key: str | None,
) -> None:
    stop_points = stop_points_df.copy()
    if not stop_points.empty:
        stop_points["name"] = stop_points["stop"]
        stop_points["detail"] = stop_points.get("detail", "Route stop")

    # PyDeck + Pandas can fail to serialize DataFrames in some Python environments,
    # so we send plain JSON-friendly records to each layer.
    route_layer_records = route_layer_df.to_dict(orient="records") if not route_layer_df.empty else []
    route_segment_records = route_segments_df.to_dict(orient="records") if not route_segments_df.empty else []
    trail_records = trails_df.to_dict(orient="records") if not trails_df.empty else []
    stop_records = stop_points.to_dict(orient="records")
    bus_records = buses_df.to_dict(orient="records")
    city_records = city_columns_df.to_dict(orient="records") if not city_columns_df.empty else []

    layers: list[pdk.Layer] = []

    if not city_columns_df.empty:
        layers.append(
            pdk.Layer(
                "ColumnLayer",
                data=city_records,
                get_position="[lon, lat]",
                get_elevation="elevation",
                elevation_scale=1,
                radius=70,
                get_fill_color="color",
                extruded=True,
                pickable=False,
                auto_highlight=False,
                opacity=0.45,
            )
        )

    # Base route scaffold
    layers.append(
        pdk.Layer(
            "PathLayer",
            data=route_layer_records,
            get_path="path",
            get_color="color",
            get_width="width",
            width_min_pixels=2,
            pickable=True,
        )
    )

    # Traffic-aware route coloring
    if not route_segments_df.empty:
        layers.append(
            pdk.Layer(
                "PathLayer",
                data=route_segment_records,
                get_path="path",
                get_color="color",
                width_min_pixels=8,
                pickable=True,
            )
        )

    if not trails_df.empty:
        layers.append(
            pdk.Layer(
                "PathLayer",
                data=trail_records,
                get_path="trail_path",
                get_color="color",
                width_min_pixels=2,
                pickable=False,
                opacity=0.65,
            )
        )

    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=stop_records,
            get_position="[lon, lat]",
            get_radius=90,
            get_fill_color=[220, 20, 60, 220],
            pickable=True,
        )
    )

    if use_bus_icons:
        layers.append(
            pdk.Layer(
                "IconLayer",
                data=bus_records,
                get_icon="icon",
                get_position="[lon, lat]",
                get_size=4,
                size_scale=10,
                pickable=True,
            )
        )
    else:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=bus_records,
                get_position="[lon, lat]",
                get_radius=170,
                get_fill_color="color",
                pickable=True,
            )
        )

    layers.append(
        pdk.Layer(
            "TextLayer",
            data=bus_records,
            get_position="[lon, lat]",
            get_text="bus_id",
            get_size=13,
            get_color=[15, 15, 15, 230],
            get_alignment_baseline="'bottom'",
            get_pixel_offset=[0, -14],
        )
    )

    if show_stop_labels:
        layers.append(
            pdk.Layer(
                "TextLayer",
                data=stop_records,
                get_position="[lon, lat]",
                get_text="stop",
                get_size=12,
                get_color=[32, 32, 32, 220],
                get_alignment_baseline="'top'",
            )
        )

    view_state = pdk.ViewState(
        latitude=float(map_points_df["lat"].mean()),
        longitude=float(map_points_df["lon"].mean()),
        zoom=float(zoom),
        pitch=int(pitch),
        bearing=int(bearing),
    )

    map_controller = {
        "dragPan": True,
        "dragRotate": True,
        "touchZoom": True,
        "touchRotate": bool(enable_touch_rotate),
        "doubleClickZoom": True,
        "keyboard": True,
    }

    deck_kwargs: dict = {
        "layers": layers,
        # Explicitly enable rotate gestures for desktop and touch input.
        "views": [pdk.View(type="MapView", controller=map_controller)],
        "initial_view_state": view_state,
        "map_style": map_style,
        "tooltip": {"html": "<b>{name}</b><br/>{detail}"},
    }

    if mapbox_token:
        deck_kwargs["api_keys"] = {"mapbox": mapbox_token}

    deck = pdk.Deck(**deck_kwargs)
    st.pydeck_chart(deck, use_container_width=True, height=map_height, key=chart_key)


def render_traffic_analytics(predictions_df: pd.DataFrame, gps_df: pd.DataFrame) -> None:
    st.subheader("Traffic Analytics")

    metric_left, metric_mid, metric_right = st.columns(3)
    avg_speed = gps_df["Speed_kmh"].mean() if not gps_df.empty else 0.0
    peak_demand = predictions_df["prediction"].max() if not predictions_df.empty else 0.0
    low_speed_share = ((gps_df["Speed_kmh"] < 20).mean() * 100) if not gps_df.empty else 0.0

    metric_left.metric("Average Fleet Speed", f"{avg_speed:.1f} km/h")
    metric_mid.metric("Peak Demand Score", f"{peak_demand:.1f}")
    metric_right.metric("Low-Speed Share", f"{low_speed_share:.1f}%")

    chart_left, chart_right = st.columns(2)

    with chart_left:
        st.caption("Demand prediction by stop")
        if predictions_df.empty:
            st.info(
                "No demand predictions found in results/csv/predictions.csv. "
                "Legacy fallback was also checked in backend/outputs/predictions.csv."
            )
        else:
            demand_chart = predictions_df.sort_values("prediction", ascending=False).head(15).set_index("stop")
            st.bar_chart(demand_chart["prediction"])

    with chart_right:
        st.caption("Average speed by hour")
        if gps_df.empty or gps_df["Timestamp"].isna().all():
            st.info("No timestamped GPS values found in data/raw/gps.csv.")
        else:
            hourly = (
                gps_df.dropna(subset=["Timestamp"])
                .assign(hour=lambda frame: frame["Timestamp"].dt.hour)
                .groupby("hour", as_index=True)["Speed_kmh"]
                .mean()
            )
            st.line_chart(hourly)

    st.caption("Speed distribution")
    if gps_df.empty:
        st.info("No GPS speed values available.")
    else:
        bins = [0, 10, 20, 30, 40, 50, 60, 80, 120]
        speed_bands = pd.cut(gps_df["Speed_kmh"], bins=bins, include_lowest=True)
        distribution = speed_bands.value_counts().sort_index()

        # Altair rejects Interval values on some environments, so cast bucket labels to strings.
        distribution_chart = pd.DataFrame(
            {
                "speed_band": [str(interval) for interval in distribution.index],
                "count": distribution.values,
            }
        )
        st.bar_chart(distribution_chart.set_index("speed_band")["count"])


def render_route_optimization(
    stops_lookup_df: pd.DataFrame,
    pagerank_df: pd.DataFrame,
    shortest_df: pd.DataFrame,
    component_summary_df: pd.DataFrame,
) -> None:
    st.subheader("Route Optimization Insights")

    if not pagerank_df.empty:
        if not stops_lookup_df.empty and "stop_id" in stops_lookup_df.columns:
            merged = pagerank_df.merge(stops_lookup_df, on="stop_id", how="left")
        else:
            merged = pagerank_df.copy()
            merged["stop_name"] = "Stop-" + merged["stop_id"].astype(str)

        top_hubs = (
            merged.sort_values("pagerank", ascending=False)
            .head(12)
            .rename(columns={"pagerank": "pagerank_score"})
        )
        st.caption("Top hub stops by PageRank")
        st.dataframe(top_hubs, use_container_width=True, hide_index=True)
    else:
        st.info(
            "PageRank results are missing in results/csv/pagerank.csv. "
            "Legacy fallback was also checked in backend/outputs/pagerank.csv."
        )

    lower_left, lower_right = st.columns(2)

    with lower_left:
        st.caption("Shortest-path edges (sample)")
        if shortest_df.empty:
            st.info(
                "Shortest path results are missing in results/csv/shortest_paths.csv. "
                "Legacy fallback was also checked in backend/outputs/shortest_paths.csv."
            )
        else:
            st.dataframe(shortest_df.head(25), use_container_width=True, hide_index=True)

    with lower_right:
        st.caption("Connected component summary")
        if component_summary_df.empty:
            st.info("Component summary is missing in results/csv/connected_component_summary.csv.")
        else:
            st.dataframe(component_summary_df, use_container_width=True, hide_index=True)


def legacy_main() -> None:
    st.set_page_config(page_title="KSRTC 3D Transit Control Dashboard", page_icon=":bus:", layout="wide")
    st.title("KSRTC 3D Transit Control Dashboard")
    st.caption("Live route simulation with stops, route geometry, moving buses, and analytics")

    stops_df = normalize_stops(load_csv(str(STOPS_FILE)))
    shortest_df = normalize_shortest_paths(load_csv(str(SHORTEST_PATHS_FILE)))
    pagerank_df = normalize_pagerank(load_csv(str(PAGERANK_FILE)))
    predictions_df = normalize_predictions(load_csv(str(PREDICTIONS_FILE)))
    gps_version = GPS_FILE.stat().st_mtime if GPS_FILE.exists() else None
    gps_df = normalize_gps(load_csv(str(GPS_FILE), gps_version))
    component_summary_df = load_csv(str(CONNECTED_COMPONENTS_SUMMARY_FILE))

    provided_route_df = normalize_route_data(load_csv(str(ROUTE_FILE)))
    schedule_routes_df = normalize_schedule_data(load_csv(str(SCHEDULE_FILE)))
    city_coordinates_df = load_or_create_city_coordinates(schedule_routes_df, CITY_COORDINATES_CACHE_FILE)
    schedule_route_df, skipped_schedule_routes = build_routes_from_schedule(schedule_routes_df, city_coordinates_df)

    if not schedule_route_df.empty:
        route_df = normalize_route_data(schedule_route_df)
        route_data_source = "cleaned_ksrtc_data.csv"
    elif not provided_route_df.empty:
        route_df = normalize_route_data(normalize_single_placeholder_route_id(provided_route_df))
        route_data_source = "bus_routes.csv"
    else:
        route_df = normalize_route_data(build_route_from_graph(stops_df, shortest_df))
        route_data_source = "graph fallback"

    stops_df = build_stop_catalog(stops_df, route_df)

    # Replace placeholder names like "Route-A" with descriptive "Start -> End" names.
    route_name_map = (
        route_df.sort_values(["route_id", "seq"])
        .groupby("route_id", as_index=False)
        .agg(first_stop=("stop", "first"), last_stop=("stop", "last"))
    )
    route_name_map["fallback_name"] = route_name_map["first_stop"].astype(str) + " -> " + route_name_map["last_stop"].astype(str)
    route_df = route_df.merge(route_name_map[["route_id", "fallback_name"]], on="route_id", how="left")
    placeholder_name = (
        route_df["route_name"].astype(str).str.strip().eq("")
        | route_df["route_name"].astype(str).eq(route_df["route_id"].astype(str))
    )
    route_df.loc[placeholder_name, "route_name"] = route_df.loc[placeholder_name, "fallback_name"]
    route_df = route_df.drop(columns=["fallback_name"])

    if route_df.empty:
        st.error("No route points available. Add geocoded route data or ensure cleaned_ksrtc_data.csv can be resolved into coordinates.")
        st.stop()

    if route_data_source == "cleaned_ksrtc_data.csv":
        resolved_city_count = city_coordinates_df.shape[0]
        st.caption(
            f"Data-driven network from `{route_data_source}`: {route_df['route_id'].nunique()} routes, "
            f"{resolved_city_count} geocoded cities."
        )
        if skipped_schedule_routes:
            st.caption(f"Skipped {len(skipped_schedule_routes)} routes because their cities could not be geocoded.")

    st.sidebar.header("Dashboard Controls")
    page = st.sidebar.radio("Select Page", ["Live Transit Map", "Traffic Analytics", "Route Optimization"])

    route_catalog = (
        route_df.sort_values(["route_id", "seq"])
        .groupby("route_id", as_index=False)
        .agg(
            route_name=("route_name", "first"),
            first_stop=("stop", "first"),
            last_stop=("stop", "last"),
            stop_count=("seq", "count"),
            origin_city=("origin_city", "first"),
            destination_city=("destination_city", "first"),
            trip_count=("trip_count", "first"),
        )
    )
    route_catalog["route_id"] = route_catalog["route_id"].astype(str)
    route_catalog["route_name"] = route_catalog["route_name"].fillna(route_catalog["route_id"].astype(str))
    route_catalog["label"] = route_catalog["route_name"].astype(str) + " [" + route_catalog["route_id"].astype(str) + "]"
    route_catalog["trip_count"] = pd.to_numeric(route_catalog["trip_count"], errors="coerce").fillna(1).astype(int)
    route_catalog = route_catalog.sort_values(["trip_count", "route_name"], ascending=[False, True]).reset_index(drop=True)

    origin_choices = ["All Origins"] + sorted(
        [name for name in route_catalog["origin_city"].dropna().astype(str).str.strip().unique().tolist() if name]
    )
    selected_origin = st.sidebar.selectbox("Origin Filter", origin_choices, index=0)
    route_search = st.sidebar.text_input("Route Search", value="", placeholder="Type origin/destination or route id")

    filtered_catalog = route_catalog.copy()
    if selected_origin != "All Origins":
        filtered_catalog = filtered_catalog[filtered_catalog["origin_city"].astype(str) == selected_origin]

    if route_search.strip():
        search_token = route_search.strip()
        filtered_catalog = filtered_catalog[
            filtered_catalog["route_name"].astype(str).str.contains(search_token, case=False, na=False)
            | filtered_catalog["route_id"].astype(str).str.contains(search_token, case=False, na=False)
            | filtered_catalog["destination_city"].astype(str).str.contains(search_token, case=False, na=False)
        ]

    if filtered_catalog.empty:
        st.sidebar.warning("No routes match the current filter. Showing all routes.")
        filtered_catalog = route_catalog.copy()

    st.sidebar.caption(f"Routes available: {filtered_catalog.shape[0]} / {route_catalog.shape[0]}")
    st.sidebar.caption(f"Trips represented: {int(filtered_catalog['trip_count'].sum())}")

    selected_label = st.sidebar.selectbox("Route", filtered_catalog["label"].tolist(), index=0)
    selected_route_meta = filtered_catalog[filtered_catalog["label"] == selected_label].iloc[0]
    selected_route_id = str(selected_route_meta["route_id"])
    selected_route_name = str(selected_route_meta["route_name"])
    st.sidebar.caption(f"Route stops: {int(selected_route_meta['stop_count'])}")
    st.sidebar.caption(f"Trips in dataset: {int(selected_route_meta['trip_count'])}")

    selected_route_df = route_df[route_df["route_id"].astype(str) == selected_route_id].sort_values("seq").reset_index(drop=True)
    visible_route_ids = filtered_catalog["route_id"].astype(str).tolist()
    visible_route_df = route_df[route_df["route_id"].astype(str).isin(visible_route_ids)].copy()

    if selected_route_df.shape[0] < 2:
        st.error("Selected route does not have enough points for simulation.")
        st.stop()

    route_point_count = selected_route_df.shape[0]
    min_stop_count = min(3, route_point_count)
    max_stop_count = min(50, route_point_count)
    stop_count = min(12, max_stop_count) if max_stop_count >= min_stop_count else min_stop_count
    start_index = 0
    loop_route = True
    points_per_segment = 28
    fleet_scope_options = ["Selected Route"]
    if route_catalog.shape[0] > 1:
        fleet_scope_options.insert(0, "All Routes")
    fleet_scope = st.sidebar.radio("Bus View", fleet_scope_options, index=0)

    if fleet_scope == "All Routes":
        visible_route_count = max(1, visible_route_df["route_id"].nunique())
        default_bus_count = visible_route_count
        st.sidebar.caption(f"Fleet distributed across {visible_route_count} visible routes.")
    else:
        default_bus_count = min(24, max(15, selected_route_df.shape[0] + 4))
    bus_count = default_bus_count
    trail_length = 16
    zoom = 12.8
    pitch = 58
    bearing = 28
    auto_rotate_map = False
    rotation_speed = 3
    enable_touch_rotate = True
    show_stop_labels = False
    show_city_context = True
    use_bus_icons = True
    show_traffic_overlay = True
    expanded_map_view = True
    map_height = 920
    map_style, resolved_token, style_warning = resolve_map_style("Road", "")

    active_route = slice_route_points(selected_route_df, start_index=start_index, stop_count=stop_count)
    path_points = interpolate_route_points(active_route, points_per_segment=points_per_segment, loop_route=loop_route)

    if "simulation_frame" not in st.session_state:
        st.session_state["simulation_frame"] = 0
    if "rotation_tick" not in st.session_state:
        st.session_state["rotation_tick"] = 0
    if "manual_bearing_offset" not in st.session_state:
        st.session_state["manual_bearing_offset"] = 0

    if page == "Live Transit Map":
        controls_left, controls_mid, controls_move_l, controls_move_r = st.columns(
            [1.0, 1.6, 1.0, 1.0],
            gap="small",
        )
        auto_play = controls_left.toggle("Auto Play", value=False)
        refresh_seconds = controls_mid.slider("Refresh (seconds)", min_value=0.1, max_value=1.5, value=0.5, step=0.1)
        if controls_move_l.button("Left Move", use_container_width=True):
            st.session_state["manual_bearing_offset"] -= 15
        if controls_move_r.button("Right Move", use_container_width=True):
            st.session_state["manual_bearing_offset"] += 15

        st.session_state["manual_bearing_offset"] = (
            (int(st.session_state["manual_bearing_offset"]) + 180) % 360
        ) - 180
        st.caption(
            "Use Left Move and Right Move to rotate the map view. Desktop rotate: right-click drag (or Ctrl+drag). "
            "Touch rotate: two-finger twist gesture."
        )
        if auto_play:
            st.info("Auto Play is ON. Turn it OFF for smooth manual touch rotation.", icon="ℹ️")

        if auto_play:
            st.session_state["simulation_frame"] += 1

        frame_index = int(st.session_state["simulation_frame"])
        if auto_rotate_map:
            st.session_state["rotation_tick"] += 1
        rotation_tick = int(st.session_state["rotation_tick"])

        base_bearing = int(bearing + int(st.session_state["manual_bearing_offset"]))
        map_bearing = base_bearing
        if auto_rotate_map:
            map_bearing = int((base_bearing + rotation_tick * rotation_speed) % 360)
        if map_bearing > 180:
            map_bearing -= 360
        if map_bearing < -180:
            map_bearing += 360

        map_chart_key = f"live-map-{selected_route_id}-{fleet_scope}-{map_bearing}-{frame_index}"
        stop_points_df = active_route.copy()
        stop_points_df["detail"] = "Highlighted route stop"
        city_columns_df = build_city_columns(active_route, density=2) if show_city_context else pd.DataFrame()
        distance_km = estimate_route_distance_km(active_route, loop_route=loop_route)

        if fleet_scope == "All Routes":
            map_points_df, route_layer_df, route_segments_df, buses_df, trails_df = build_network_fleet_state(
                route_df=visible_route_df,
                selected_route_id=selected_route_id,
                frame_index=frame_index,
                points_per_segment=points_per_segment,
                loop_route=loop_route,
                bus_count=bus_count,
                trail_length=trail_length,
            )
            analytics_heading = "Fleet Analytics"
            route_caption = "Highlighted route"
            status_columns = ["bus_id", "route_id", "status", "next_stop"]
            live_panel_columns = ["bus_id", "route_id", "status", "speed_kmh", "delay_min", "next_stop"]
        else:
            map_points_df = active_route[["lat", "lon"]].copy()
            route_layer_df = build_route_layer_frame(
                route_points=active_route,
                path_points=path_points,
                route_id=selected_route_id,
                route_name=selected_route_name,
                highlighted=True,
            )
            buses_df, trails_df = build_bus_states(
                path_points=path_points,
                route_points=active_route,
                frame_index=frame_index,
                bus_count=bus_count,
                trail_length=trail_length,
                route_id=selected_route_id,
                route_name=selected_route_name,
            )
            route_segments_df = build_route_segments(
                active_route,
                frame_index=frame_index,
                loop_route=loop_route,
                route_name=selected_route_name,
                route_id=selected_route_id,
            )
            analytics_heading = "Route Analytics"
            route_caption = "Current route"
            status_columns = ["bus_id", "status", "next_stop"]
            live_panel_columns = ["bus_id", "status", "speed_kmh", "delay_min", "next_stop"]

        if not show_traffic_overlay:
            route_segments_df = pd.DataFrame(columns=["name", "path", "color", "traffic", "detail"])

        avg_speed = buses_df["speed_kmh"].mean() if not buses_df.empty else 0.0
        avg_delay = buses_df["delay_min"].mean() if not buses_df.empty else 0.0
        on_time_share = (buses_df["status"].eq("On Time").mean() * 100) if not buses_df.empty else 0.0
        delayed_bus_count = int(buses_df["status"].eq("Delayed").sum()) if not buses_df.empty else 0

        traffic_counts = route_segments_df["traffic"].value_counts() if not route_segments_df.empty else pd.Series(dtype=int)
        high_traffic = int(traffic_counts.get("High", 0))
        med_traffic = int(traffic_counts.get("Medium", 0))
        low_traffic = int(traffic_counts.get("Low", 0))

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Active Buses", f"{buses_df.shape[0]}")
        if fleet_scope == "All Routes":
            m2.metric("Visible Routes", f"{route_layer_df.shape[0]}")
            m3.metric("Delayed Buses", f"{delayed_bus_count}")
        else:
            m2.metric("Active Stops", f"{active_route.shape[0]}")
            m3.metric("Route Distance", f"{distance_km:.2f} km")
        m4.metric("On-Time Share", f"{on_time_share:.1f}%")

        if expanded_map_view:
            render_live_map(
                map_points_df=map_points_df,
                route_layer_df=route_layer_df,
                route_segments_df=route_segments_df,
                buses_df=buses_df,
                trails_df=trails_df,
                stop_points_df=stop_points_df,
                city_columns_df=city_columns_df,
                map_style=map_style,
                mapbox_token=resolved_token,
                zoom=zoom,
                pitch=pitch,
                bearing=map_bearing,
                show_stop_labels=show_stop_labels,
                use_bus_icons=use_bus_icons,
                enable_touch_rotate=enable_touch_rotate,
                map_height=map_height,
                chart_key=map_chart_key,
            )

            st.subheader(analytics_heading)
            panel_a, panel_b, panel_c = st.columns(3)
            panel_a.metric("Average Speed", f"{avg_speed:.1f} km/h")
            panel_b.metric("Average Delay", f"{avg_delay:.1f} min")
            panel_c.metric("Traffic (H/M/L)", f"{high_traffic}/{med_traffic}/{low_traffic}")

            details_col, status_col = st.columns([1.2, 2.8], gap="large")
            with details_col:
                st.caption(route_caption)
                st.write(f"{selected_route_name} [{selected_route_id}]")

            with status_col:
                if not buses_df.empty:
                    st.caption("Bus status")
                    st.dataframe(
                        buses_df[status_columns],
                        use_container_width=True,
                        hide_index=True,
                    )

        else:
            map_col, panel_col = st.columns([4.6, 1.0], gap="large")

            with map_col:
                render_live_map(
                    map_points_df=map_points_df,
                    route_layer_df=route_layer_df,
                    route_segments_df=route_segments_df,
                    buses_df=buses_df,
                    trails_df=trails_df,
                    stop_points_df=stop_points_df,
                    city_columns_df=city_columns_df,
                    map_style=map_style,
                    mapbox_token=resolved_token,
                    zoom=zoom,
                    pitch=pitch,
                    bearing=map_bearing,
                    show_stop_labels=show_stop_labels,
                    use_bus_icons=use_bus_icons,
                    enable_touch_rotate=enable_touch_rotate,
                    map_height=map_height,
                    chart_key=map_chart_key,
                )

            with panel_col:
                st.subheader(analytics_heading)
                st.metric("Average Speed", f"{avg_speed:.1f} km/h")
                st.metric("Average Delay", f"{avg_delay:.1f} min")
                st.metric("Traffic (H/M/L)", f"{high_traffic}/{med_traffic}/{low_traffic}")
                st.caption(route_caption)
                st.write(f"{selected_route_name} [{selected_route_id}]")

                if not buses_df.empty:
                    st.caption("Bus status")
                    st.dataframe(
                        buses_df[status_columns],
                        use_container_width=True,
                        hide_index=True,
                    )

        st.caption("Live bus panel")
        st.dataframe(
            buses_df[live_panel_columns],
            use_container_width=True,
            hide_index=True,
        )

        if auto_play or auto_rotate_map:
            time.sleep(refresh_seconds)
            st.rerun()

    elif page == "Traffic Analytics":
        render_traffic_analytics(predictions_df, gps_df)

    else:
        if not stops_df.empty:
            stops_lookup_df = stops_df[["stop_id", "stop_name"]]
        else:
            stops_lookup_df = (
                route_df.dropna(subset=["stop_id"])
                [["stop_id", "stop"]]
                .drop_duplicates(subset=["stop_id"])  # noqa: PD010
                .rename(columns={"stop": "stop_name"})
            )
            if not stops_lookup_df.empty:
                stops_lookup_df["stop_id"] = pd.to_numeric(stops_lookup_df["stop_id"], errors="coerce")
                stops_lookup_df = stops_lookup_df.dropna(subset=["stop_id"])
                stops_lookup_df["stop_id"] = stops_lookup_df["stop_id"].astype(int)

        render_route_optimization(
            stops_lookup_df=stops_lookup_df,
            pagerank_df=pagerank_df,
            shortest_df=shortest_df,
            component_summary_df=component_summary_df,
        )


def file_version(path: Path) -> float | None:
    return path.stat().st_mtime if path.exists() else None


def apply_light_theme() -> None:
    css = """
        <style>
            :root {
                --primary: __PRIMARY_COLOR__;
                --success: __SUCCESS_COLOR__;
                --warning: __WARNING_COLOR__;
                --danger: __DANGER_COLOR__;
                --bg: __BACKGROUND_COLOR__;
                --card: __CARD_COLOR__;
                --text: __TEXT_COLOR__;
            }
            .stApp {
                background: linear-gradient(180deg, var(--bg) 0%, #eef4ff 100%);
                color: var(--text);
            }
            [data-testid="stHeader"] {
                background: rgba(247, 250, 252, 0.85);
            }
            .block-container {
                padding-top: 1rem;
                padding-bottom: 2.5rem;
            }
            div[data-testid="stMetric"] {
                background: var(--card);
                border: 1px solid #dbeafe;
                border-radius: 16px;
                padding: 1rem 1.05rem;
                box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
            }
            div[data-testid="stMetricLabel"] {
                color: #64748b;
            }
            div[data-testid="stMetricValue"] {
                color: var(--text);
            }
            div[data-testid="stVerticalBlockBorderWrapper"] {
                background: rgba(255, 255, 255, 0.94);
                border: 1px solid #e5edf7;
                border-radius: 20px;
                box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
            }
            .ksrtc-hero {
                background: linear-gradient(135deg, #ffffff 0%, #eef4ff 100%);
                border: 1px solid #dbeafe;
                border-radius: 24px;
                padding: 1.4rem 1.5rem;
                margin-bottom: 1.15rem;
                box-shadow: 0 14px 34px rgba(15, 23, 42, 0.08);
            }
            .ksrtc-eyebrow {
                color: var(--primary);
                font-size: 0.82rem;
                font-weight: 700;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                margin-bottom: 0.35rem;
            }
            .ksrtc-title {
                color: var(--text);
                font-size: 2rem;
                font-weight: 700;
                line-height: 1.15;
                margin-bottom: 0.35rem;
            }
            .ksrtc-subtitle {
                color: #64748b;
                font-size: 1rem;
                max-width: 56rem;
            }
            .metric-card {
                background: var(--card);
                border: 1px solid #e5edf7;
                border-radius: 16px;
                box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
                padding: 1rem 1.1rem;
                margin-bottom: 0.85rem;
            }
            .metric-card.spotlight {
                border-top: 4px solid var(--primary);
            }
            .metric-card.success {
                border-top: 4px solid var(--success);
            }
            .metric-card.warning {
                border-top: 4px solid var(--warning);
            }
            .metric-card.danger {
                border-top: 4px solid var(--danger);
            }
            .metric-label {
                color: #64748b;
                font-size: 0.78rem;
                font-weight: 700;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                margin-bottom: 0.35rem;
            }
            .metric-value {
                color: var(--text);
                font-size: 1.15rem;
                font-weight: 700;
                margin-bottom: 0.2rem;
            }
            .metric-meta {
                color: #64748b;
                font-size: 0.92rem;
            }
        </style>
    """
    css = (
        css.replace("__PRIMARY_COLOR__", PRIMARY_COLOR)
        .replace("__SUCCESS_COLOR__", SUCCESS_COLOR)
        .replace("__WARNING_COLOR__", WARNING_COLOR)
        .replace("__DANGER_COLOR__", DANGER_COLOR)
        .replace("__BACKGROUND_COLOR__", BACKGROUND_COLOR)
        .replace("__CARD_COLOR__", CARD_COLOR)
        .replace("__TEXT_COLOR__", TEXT_COLOR)
    )
    st.markdown(
        css,
        unsafe_allow_html=True,
    )


def render_page_header(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <section class="ksrtc-hero">
            <div class="ksrtc-eyebrow">KSRTC Dashboard</div>
            <div class="ksrtc-title">{title}</div>
            <div class="ksrtc-subtitle">{subtitle}</div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_metric_card(label: str, value: str, meta: str, tone: str = "spotlight") -> None:
    st.markdown(
        f"""
        <div class="metric-card {tone}">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-meta">{meta}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_route_catalog(route_df: pd.DataFrame) -> pd.DataFrame:
    if route_df.empty:
        return pd.DataFrame(
            columns=[
                "route_id",
                "route_name",
                "first_stop",
                "last_stop",
                "stop_count",
                "origin_city",
                "destination_city",
                "trip_count",
                "label",
            ]
        )

    route_catalog = (
        route_df.sort_values(["route_id", "seq"])
        .groupby("route_id", as_index=False)
        .agg(
            route_name=("route_name", "first"),
            first_stop=("stop", "first"),
            last_stop=("stop", "last"),
            stop_count=("seq", "count"),
            origin_city=("origin_city", "first"),
            destination_city=("destination_city", "first"),
            trip_count=("trip_count", "first"),
        )
    )
    route_catalog["route_id"] = route_catalog["route_id"].astype(str)
    route_catalog["route_name"] = route_catalog["route_name"].fillna(route_catalog["route_id"].astype(str))
    route_catalog["trip_count"] = pd.to_numeric(route_catalog["trip_count"], errors="coerce").fillna(1).astype(int)
    route_catalog["label"] = (
        route_catalog["route_name"].astype(str) + " [" + route_catalog["route_id"].astype(str) + "]"
    )
    return route_catalog.sort_values(["trip_count", "route_name"], ascending=[False, True]).reset_index(drop=True)


def build_stops_lookup(stops_df: pd.DataFrame, route_df: pd.DataFrame) -> pd.DataFrame:
    if not stops_df.empty:
        return stops_df[["stop_id", "stop_name"]].copy()

    fallback = (
        route_df.dropna(subset=["stop_id"])
        [["stop_id", "stop"]]
        .drop_duplicates(subset=["stop_id"])
        .rename(columns={"stop": "stop_name"})
    )
    if fallback.empty:
        return pd.DataFrame(columns=["stop_id", "stop_name"])

    fallback["stop_id"] = pd.to_numeric(fallback["stop_id"], errors="coerce")
    fallback = fallback.dropna(subset=["stop_id"])
    fallback["stop_id"] = fallback["stop_id"].astype(int)
    fallback["stop_name"] = fallback["stop_name"].astype(str)
    return fallback.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_dashboard_context(
    stops_version: float | None,
    shortest_version: float | None,
    path_example_version: float | None,
    pagerank_version: float | None,
    predictions_version: float | None,
    gps_version: float | None,
    component_summary_version: float | None,
    route_version: float | None,
    schedule_version: float | None,
    city_cache_version: float | None,
    unresolved_cache_version: float | None,
) -> dict[str, object]:
    del city_cache_version, unresolved_cache_version

    stops_df = normalize_stops(load_csv(str(STOPS_FILE), stops_version))
    shortest_df = normalize_shortest_paths(load_csv(str(SHORTEST_PATHS_FILE), shortest_version))
    path_example_df = load_csv(str(SHORTEST_PATH_EXAMPLE_FILE), path_example_version)
    pagerank_df = normalize_pagerank(load_csv(str(PAGERANK_FILE), pagerank_version))
    predictions_df = normalize_predictions(load_csv(str(PREDICTIONS_FILE), predictions_version))
    gps_df = normalize_gps(load_csv(str(GPS_FILE), gps_version))
    component_summary_df = load_csv(str(CONNECTED_COMPONENTS_SUMMARY_FILE), component_summary_version)

    provided_route_df = normalize_route_data(load_csv(str(ROUTE_FILE), route_version))
    schedule_routes_df = normalize_schedule_data(load_csv(str(SCHEDULE_FILE), schedule_version))
    city_coordinates_df = load_or_create_city_coordinates(schedule_routes_df, CITY_COORDINATES_CACHE_FILE)
    schedule_route_df, skipped_schedule_routes = build_routes_from_schedule(schedule_routes_df, city_coordinates_df)

    if not schedule_route_df.empty:
        route_df = normalize_route_data(schedule_route_df)
        route_data_source = "cleaned_ksrtc_data.csv"
    elif not provided_route_df.empty:
        route_df = normalize_route_data(normalize_single_placeholder_route_id(provided_route_df))
        route_data_source = "bus_routes.csv"
    else:
        route_df = normalize_route_data(build_route_from_graph(stops_df, shortest_df))
        route_data_source = "graph fallback"

    stops_df = build_stop_catalog(stops_df, route_df)

    if not route_df.empty:
        route_name_map = (
            route_df.sort_values(["route_id", "seq"])
            .groupby("route_id", as_index=False)
            .agg(first_stop=("stop", "first"), last_stop=("stop", "last"))
        )
        route_name_map["fallback_name"] = (
            route_name_map["first_stop"].astype(str) + " -> " + route_name_map["last_stop"].astype(str)
        )
        route_df = route_df.merge(route_name_map[["route_id", "fallback_name"]], on="route_id", how="left")
        placeholder_name = (
            route_df["route_name"].astype(str).str.strip().eq("")
            | route_df["route_name"].astype(str).eq(route_df["route_id"].astype(str))
        )
        route_df.loc[placeholder_name, "route_name"] = route_df.loc[placeholder_name, "fallback_name"]
        route_df = route_df.drop(columns=["fallback_name"])

    route_catalog = build_route_catalog(route_df)
    stops_lookup_df = build_stops_lookup(stops_df, route_df)

    if route_df.empty:
        route_data_note = ""
    elif route_data_source == "cleaned_ksrtc_data.csv":
        route_data_note = (
            f"Using {route_catalog.shape[0]} geocoded routes from {route_data_source}. "
            f"Cities resolved: {city_coordinates_df.shape[0]}."
        )
        if skipped_schedule_routes:
            route_data_note += f" Skipped {len(skipped_schedule_routes)} routes with missing coordinates."
    else:
        route_data_note = f"Using {route_catalog.shape[0]} routes from {route_data_source}."

    return {
        "stops_df": stops_df,
        "stops_lookup_df": stops_lookup_df,
        "shortest_df": shortest_df,
        "path_example_df": path_example_df,
        "pagerank_df": pagerank_df,
        "predictions_df": predictions_df,
        "gps_df": gps_df,
        "component_summary_df": component_summary_df,
        "route_df": route_df,
        "route_catalog": route_catalog,
        "route_data_note": route_data_note,
    }


def estimate_map_zoom(route_points: pd.DataFrame) -> float:
    if route_points.empty:
        return 6.2

    lat_span = float(route_points["lat"].max() - route_points["lat"].min())
    lon_span = float(route_points["lon"].max() - route_points["lon"].min())
    max_span = max(lat_span, lon_span)

    if max_span > 6:
        return 5.2
    if max_span > 4:
        return 5.8
    if max_span > 2:
        return 6.6
    if max_span > 1:
        return 7.4
    if max_span > 0.5:
        return 8.4
    if max_span > 0.2:
        return 9.8
    if max_span > 0.1:
        return 11.0
    return 12.2


def build_shortest_path_preview(
    path_example_df: pd.DataFrame,
    shortest_df: pd.DataFrame,
    stops_lookup_df: pd.DataFrame,
) -> pd.DataFrame:
    if not path_example_df.empty and {"step", "stop_id"}.issubset(path_example_df.columns):
        preview_df = path_example_df.copy()
        preview_df["step"] = pd.to_numeric(preview_df["step"], errors="coerce")
        preview_df["stop_id"] = pd.to_numeric(preview_df["stop_id"], errors="coerce")
        preview_df = preview_df.dropna(subset=["step", "stop_id"])
        preview_df["step"] = preview_df["step"].astype(int)
        preview_df["stop_id"] = preview_df["stop_id"].astype(int)
        preview_df = preview_df.merge(stops_lookup_df, on="stop_id", how="left")
        preview_df["stop_name"] = preview_df["stop_name"].fillna("Stop-" + preview_df["stop_id"].astype(str))
        return (
            preview_df[["step", "stop_name", "stop_id"]]
            .rename(columns={"step": "Step", "stop_name": "Stop", "stop_id": "Stop ID"})
            .sort_values("Step")
            .reset_index(drop=True)
        )

    if shortest_df.empty:
        return pd.DataFrame(columns=["Step", "From", "To", "Cost"])

    preview_df = shortest_df.head(12).copy().reset_index(drop=True)
    preview_df["Step"] = np.arange(1, preview_df.shape[0] + 1)
    preview_df = preview_df.rename(
        columns={
            "source_stop": "From",
            "destination_stop": "To",
            "estimated_cost": "Cost",
        }
    )
    return preview_df[["Step", "From", "To", "Cost"]]


def apply_live_map_page_theme() -> None:
    st.markdown(
        """
        <style>
            [data-testid="stHeader"] {
                background: #1f222d;
            }
            [data-testid="stToolbar"] {
                right: 1rem;
            }
            .stApp {
                background: linear-gradient(180deg, #1f222d 0%, #11131a 100%);
            }
            .stSidebar {
                background: #2a2d37;
            }
            .block-container {
                padding-top: 1rem;
                padding-bottom: 1.5rem;
            }
            section[data-testid="stSidebar"] * {
                color: #f3f4f6;
            }
            .stApp div[data-testid="stMetric"] {
                background: #242833;
                border: 1px solid #343948;
                box-shadow: none;
            }
            .stApp div[data-testid="stMetricLabel"] {
                color: #cbd5e1;
            }
            .stApp div[data-testid="stMetricValue"] {
                color: #f8fafc;
            }
            .stApp [data-testid="stVerticalBlockBorderWrapper"] {
                background: transparent;
                border: none;
                box-shadow: none;
            }
            .stApp h1, .stApp h2, .stApp h3, .stApp p, .stApp label, .stApp div, .stApp span {
                color: #f3f4f6;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_legacy_live_map_page(context: dict[str, object]) -> None:
    apply_live_map_page_theme()

    route_df = context["route_df"]
    route_catalog = context["route_catalog"]
    route_data_note = str(context["route_data_note"])

    st.title("KSRTC 3D Transit Control Dashboard")
    st.caption("Live route simulation with stops, route geometry, moving buses, and analytics")
    if route_data_note:
        st.caption(route_data_note)

    st.sidebar.header("Dashboard Controls")

    if route_catalog.empty:
        st.error("No route points available for the live map.")
        return

    origin_choices = ["All Origins"] + sorted(
        [name for name in route_catalog["origin_city"].dropna().astype(str).str.strip().unique().tolist() if name]
    )
    selected_origin = st.sidebar.selectbox("Origin Filter", origin_choices, index=0)
    route_search = st.sidebar.text_input("Route Search", value="", placeholder="Type origin/destination or route id")

    filtered_catalog = route_catalog.copy()
    if selected_origin != "All Origins":
        filtered_catalog = filtered_catalog[filtered_catalog["origin_city"].astype(str) == selected_origin]

    if route_search.strip():
        search_token = route_search.strip()
        filtered_catalog = filtered_catalog[
            filtered_catalog["route_name"].astype(str).str.contains(search_token, case=False, na=False)
            | filtered_catalog["route_id"].astype(str).str.contains(search_token, case=False, na=False)
            | filtered_catalog["destination_city"].astype(str).str.contains(search_token, case=False, na=False)
        ]

    if filtered_catalog.empty:
        st.sidebar.warning("No routes match the current filter. Showing all routes.")
        filtered_catalog = route_catalog.copy()

    st.sidebar.caption(f"Routes available: {filtered_catalog.shape[0]} / {route_catalog.shape[0]}")
    st.sidebar.caption(f"Trips represented: {int(filtered_catalog['trip_count'].sum())}")

    default_route_id = str(st.session_state.get("legacy_selected_route_id", filtered_catalog.iloc[0]["route_id"]))
    if default_route_id not in filtered_catalog["route_id"].astype(str).tolist():
        default_route_id = str(filtered_catalog.iloc[0]["route_id"])
    filtered_labels = filtered_catalog["label"].tolist()
    selected_label = st.sidebar.selectbox(
        "Route",
        filtered_labels,
        index=filtered_catalog["route_id"].astype(str).tolist().index(default_route_id),
    )
    selected_route_meta = filtered_catalog[filtered_catalog["label"] == selected_label].iloc[0]
    selected_route_id = str(selected_route_meta["route_id"])
    selected_route_name = str(selected_route_meta["route_name"])
    st.session_state["legacy_selected_route_id"] = selected_route_id

    st.sidebar.caption(f"Route stops: {int(selected_route_meta['stop_count'])}")
    st.sidebar.caption(f"Trips in dataset: {int(selected_route_meta['trip_count'])}")

    selected_route_df = (
        route_df[route_df["route_id"].astype(str) == selected_route_id]
        .sort_values("seq")
        .reset_index(drop=True)
    )
    visible_route_ids = filtered_catalog["route_id"].astype(str).tolist()
    visible_route_df = route_df[route_df["route_id"].astype(str).isin(visible_route_ids)].copy()

    if selected_route_df.shape[0] < 2:
        st.error("Selected route does not have enough points for simulation.")
        return

    route_point_count = selected_route_df.shape[0]
    min_stop_count = min(3, route_point_count)
    max_stop_count = min(50, route_point_count)
    stop_count = min(12, max_stop_count) if max_stop_count >= min_stop_count else min_stop_count
    start_index = 0
    loop_route = True
    points_per_segment = 28
    fleet_scope_options = ["Selected Route"]
    if route_catalog.shape[0] > 1:
        fleet_scope_options.insert(0, "All Routes")
    fleet_scope = st.sidebar.radio("Bus View", fleet_scope_options, index=0)

    if fleet_scope == "All Routes":
        visible_route_count = max(1, visible_route_df["route_id"].nunique())
        default_bus_count = visible_route_count
        st.sidebar.caption(f"Fleet distributed across {visible_route_count} visible routes.")
    else:
        default_bus_count = min(24, max(15, selected_route_df.shape[0] + 4))
    bus_count = default_bus_count
    trail_length = 16
    zoom = 12.8
    pitch = 58
    bearing = 28
    auto_rotate_map = False
    rotation_speed = 3
    enable_touch_rotate = True
    show_stop_labels = False
    show_city_context = True
    use_bus_icons = True
    show_traffic_overlay = True
    expanded_map_view = True
    map_height = 920
    map_style, resolved_token, style_warning = resolve_map_style("Road", "")

    active_route = slice_route_points(selected_route_df, start_index=start_index, stop_count=stop_count)
    path_points = interpolate_route_points(active_route, points_per_segment=points_per_segment, loop_route=loop_route)

    if "simulation_frame" not in st.session_state:
        st.session_state["simulation_frame"] = 0
    if "rotation_tick" not in st.session_state:
        st.session_state["rotation_tick"] = 0
    if "manual_bearing_offset" not in st.session_state:
        st.session_state["manual_bearing_offset"] = 0

    controls_left, controls_mid, controls_move_l, controls_move_r = st.columns(
        [1.0, 1.6, 1.0, 1.0],
        gap="small",
    )
    auto_play = controls_left.toggle("Auto Play", value=False)
    refresh_seconds = controls_mid.slider("Refresh (seconds)", min_value=0.1, max_value=1.5, value=0.5, step=0.1)
    if controls_move_l.button("Left Move", use_container_width=True):
        st.session_state["manual_bearing_offset"] -= 15
    if controls_move_r.button("Right Move", use_container_width=True):
        st.session_state["manual_bearing_offset"] += 15

    st.session_state["manual_bearing_offset"] = ((int(st.session_state["manual_bearing_offset"]) + 180) % 360) - 180
    if auto_play:
        st.info("Auto Play is ON. Turn it OFF for smooth manual touch rotation.", icon="ℹ️")
        st.session_state["simulation_frame"] += 1

    frame_index = int(st.session_state["simulation_frame"])
    if auto_rotate_map:
        st.session_state["rotation_tick"] += 1
    rotation_tick = int(st.session_state["rotation_tick"])

    base_bearing = int(bearing + int(st.session_state["manual_bearing_offset"]))
    map_bearing = base_bearing
    if auto_rotate_map:
        map_bearing = int((base_bearing + rotation_tick * rotation_speed) % 360)
    if map_bearing > 180:
        map_bearing -= 360
    if map_bearing < -180:
        map_bearing += 360

    map_chart_key = f"legacy-live-map-{selected_route_id}-{fleet_scope}-{map_bearing}-{frame_index}"
    stop_points_df = active_route.copy()
    stop_points_df["detail"] = "Highlighted route stop"
    city_columns_df = build_city_columns(active_route, density=2) if show_city_context else pd.DataFrame()
    distance_km = estimate_route_distance_km(active_route, loop_route=loop_route)

    if fleet_scope == "All Routes":
        map_points_df, route_layer_df, route_segments_df, buses_df, trails_df = build_network_fleet_state(
            route_df=visible_route_df,
            selected_route_id=selected_route_id,
            frame_index=frame_index,
            points_per_segment=points_per_segment,
            loop_route=loop_route,
            bus_count=bus_count,
            trail_length=trail_length,
        )
        analytics_heading = "Fleet Analytics"
        route_caption = "Highlighted route"
        status_columns = ["bus_id", "route_id", "status", "next_stop"]
        live_panel_columns = ["bus_id", "route_id", "status", "speed_kmh", "delay_min", "next_stop"]
    else:
        map_points_df = active_route[["lat", "lon"]].copy()
        route_layer_df = build_route_layer_frame(
            route_points=active_route,
            path_points=path_points,
            route_id=selected_route_id,
            route_name=selected_route_name,
            highlighted=True,
        )
        buses_df, trails_df = build_bus_states(
            path_points=path_points,
            route_points=active_route,
            frame_index=frame_index,
            bus_count=bus_count,
            trail_length=trail_length,
            route_id=selected_route_id,
            route_name=selected_route_name,
        )
        route_segments_df = build_route_segments(
            active_route,
            frame_index=frame_index,
            loop_route=loop_route,
            route_name=selected_route_name,
            route_id=selected_route_id,
        )
        analytics_heading = "Route Analytics"
        route_caption = "Current route"
        status_columns = ["bus_id", "status", "next_stop"]
        live_panel_columns = ["bus_id", "status", "speed_kmh", "delay_min", "next_stop"]

    if not show_traffic_overlay:
        route_segments_df = pd.DataFrame(columns=["name", "path", "color", "traffic", "detail"])

    avg_speed = buses_df["speed_kmh"].mean() if not buses_df.empty else 0.0
    avg_delay = buses_df["delay_min"].mean() if not buses_df.empty else 0.0
    on_time_share = (buses_df["status"].eq("On Time").mean() * 100) if not buses_df.empty else 0.0
    delayed_bus_count = int(buses_df["status"].eq("Delayed").sum()) if not buses_df.empty else 0

    traffic_counts = route_segments_df["traffic"].value_counts() if not route_segments_df.empty else pd.Series(dtype=int)
    high_traffic = int(traffic_counts.get("High", 0))
    med_traffic = int(traffic_counts.get("Medium", 0))
    low_traffic = int(traffic_counts.get("Low", 0))

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Active Buses", f"{buses_df.shape[0]}")
    if fleet_scope == "All Routes":
        m2.metric("Visible Routes", f"{route_layer_df.shape[0]}")
        m3.metric("Delayed Buses", f"{delayed_bus_count}")
    else:
        m2.metric("Active Stops", f"{active_route.shape[0]}")
        m3.metric("Route Distance", f"{distance_km:.2f} km")
    m4.metric("On-Time Share", f"{on_time_share:.1f}%")

    if expanded_map_view:
        render_live_map(
            map_points_df=map_points_df,
            route_layer_df=route_layer_df,
            route_segments_df=route_segments_df,
            buses_df=buses_df,
            trails_df=trails_df,
            stop_points_df=stop_points_df,
            city_columns_df=city_columns_df,
            map_style=map_style,
            mapbox_token=resolved_token,
            zoom=zoom,
            pitch=pitch,
            bearing=map_bearing,
            show_stop_labels=show_stop_labels,
            use_bus_icons=use_bus_icons,
            enable_touch_rotate=enable_touch_rotate,
            map_height=map_height,
            chart_key=map_chart_key,
        )

        st.subheader(analytics_heading)
        panel_a, panel_b, panel_c = st.columns(3)
        panel_a.metric("Average Speed", f"{avg_speed:.1f} km/h")
        panel_b.metric("Average Delay", f"{avg_delay:.1f} min")
        panel_c.metric("Traffic (H/M/L)", f"{high_traffic}/{med_traffic}/{low_traffic}")

        details_col, status_col = st.columns([1.2, 2.8], gap="large")
        with details_col:
            st.caption(route_caption)
            st.write(f"{selected_route_name} [{selected_route_id}]")

        with status_col:
            if not buses_df.empty:
                st.caption("Bus status")
                st.dataframe(
                    buses_df[status_columns],
                    use_container_width=True,
                    hide_index=True,
                )
    else:
        map_col, panel_col = st.columns([4.6, 1.0], gap="large")

        with map_col:
            render_live_map(
                map_points_df=map_points_df,
                route_layer_df=route_layer_df,
                route_segments_df=route_segments_df,
                buses_df=buses_df,
                trails_df=trails_df,
                stop_points_df=stop_points_df,
                city_columns_df=city_columns_df,
                map_style=map_style,
                mapbox_token=resolved_token,
                zoom=zoom,
                pitch=pitch,
                bearing=map_bearing,
                show_stop_labels=show_stop_labels,
                use_bus_icons=use_bus_icons,
                enable_touch_rotate=enable_touch_rotate,
                map_height=map_height,
                chart_key=map_chart_key,
            )

        with panel_col:
            st.subheader(analytics_heading)
            st.metric("Average Speed", f"{avg_speed:.1f} km/h")
            st.metric("Average Delay", f"{avg_delay:.1f} min")
            st.metric("Traffic (H/M/L)", f"{high_traffic}/{med_traffic}/{low_traffic}")
            st.caption(route_caption)
            st.write(f"{selected_route_name} [{selected_route_id}]")

            if not buses_df.empty:
                st.caption("Bus status")
                st.dataframe(
                    buses_df[status_columns],
                    use_container_width=True,
                    hide_index=True,
                )

    st.caption("Live bus panel")
    st.dataframe(
        buses_df[live_panel_columns],
        use_container_width=True,
        hide_index=True,
    )

    if auto_play or auto_rotate_map:
        time.sleep(refresh_seconds)
        st.rerun()


def render_live_bus_map_page(context: dict[str, object]) -> None:
    route_df = context["route_df"]
    route_catalog = context["route_catalog"]
    route_data_note = str(context["route_data_note"])

    render_page_header(
        "Live Bus Map",
        "A simple live view with one route at a time, clearer stats, and less clutter on the screen.",
    )
    if route_data_note:
        st.caption(route_data_note)

    if route_catalog.empty:
        st.error("No route points are available for the live map.")
        return

    route_options = route_catalog["route_id"].astype(str).tolist()
    route_labels = route_catalog.set_index("route_id")["label"].to_dict()
    current_route_id = str(st.session_state.get("selected_route_id", route_options[0]))
    if current_route_id not in route_options:
        current_route_id = route_options[0]

    speed_profiles = {
        "Slow": {"refresh_seconds": 1.0, "frame_step": 1, "points_per_segment": 28},
        "Normal": {"refresh_seconds": 0.55, "frame_step": 2, "points_per_segment": 24},
        "Fast": {"refresh_seconds": 0.3, "frame_step": 4, "points_per_segment": 18},
    }

    with st.container(border=True):
        control_route, control_play, control_speed = st.columns([4.0, 1.2, 1.8], gap="medium")
        selected_route_id = control_route.selectbox(
            "Select Route",
            options=route_options,
            index=route_options.index(current_route_id),
            format_func=lambda route_id: route_labels.get(route_id, route_id),
        )
        auto_play = control_play.toggle("Play / Pause", value=bool(st.session_state.get("live_map_playing", False)))
        speed_label = control_speed.select_slider(
            "Speed",
            options=list(speed_profiles.keys()),
            value=str(st.session_state.get("live_map_speed", "Normal")),
        )

    st.session_state["selected_route_id"] = selected_route_id
    st.session_state["live_map_playing"] = auto_play
    st.session_state["live_map_speed"] = speed_label

    selected_route_meta = route_catalog[route_catalog["route_id"].astype(str) == selected_route_id].iloc[0]
    selected_route_df = (
        route_df[route_df["route_id"].astype(str) == selected_route_id]
        .sort_values("seq")
        .reset_index(drop=True)
    )
    previous_route_id = st.session_state.get("route_toast_id")
    if previous_route_id and previous_route_id != selected_route_id:
        st.toast(f"Route updated: {selected_route_meta['route_name']}")
    st.session_state["route_toast_id"] = selected_route_id

    if selected_route_df.shape[0] < 2:
        st.error("The selected route does not have enough points to display on the map.")
        return

    if "simulation_frame" not in st.session_state:
        st.session_state["simulation_frame"] = 0

    profile = speed_profiles[speed_label]
    if auto_play:
        st.session_state["simulation_frame"] = int(st.session_state["simulation_frame"]) + int(profile["frame_step"])

    frame_index = int(st.session_state["simulation_frame"])
    points_per_segment = int(profile["points_per_segment"])
    path_points = interpolate_route_points(selected_route_df, points_per_segment=points_per_segment, loop_route=False)

    route_name = str(selected_route_meta["route_name"])
    route_distance_km = estimate_route_distance_km(selected_route_df, loop_route=False)
    bus_count = max(4, min(16, max(6, int(selected_route_meta["trip_count"]) + 2)))

    route_layer_df = build_route_layer_frame(
        route_points=selected_route_df,
        path_points=path_points,
        route_id=selected_route_id,
        route_name=route_name,
        highlighted=True,
    )
    route_segments_df = build_route_segments(
        selected_route_df,
        frame_index=frame_index,
        loop_route=False,
        route_name=route_name,
        route_id=selected_route_id,
    )
    buses_df, _ = build_bus_states(
        path_points=path_points,
        route_points=selected_route_df,
        frame_index=frame_index,
        bus_count=bus_count,
        trail_length=1,
        route_id=selected_route_id,
        route_name=route_name,
    )
    trails_df = pd.DataFrame(columns=["bus_id", "trail_path", "color"])

    total_buses = int(buses_df.shape[0])
    avg_speed = float(buses_df["speed_kmh"].mean()) if not buses_df.empty else 0.0
    delay_share = float((buses_df["status"].ne("On Time").mean() * 100)) if not buses_df.empty else 0.0
    avg_delay = float(buses_df["delay_min"].mean()) if not buses_df.empty else 0.0

    traffic_counts = route_segments_df["traffic"].value_counts() if not route_segments_df.empty else pd.Series(dtype=int)
    high_traffic = int(traffic_counts.get("High", 0))
    medium_traffic = int(traffic_counts.get("Medium", 0))
    low_traffic = int(traffic_counts.get("Low", 0))

    metric_1, metric_2, metric_3 = st.columns(3)
    metric_1.metric("Total Buses", f"{total_buses}")
    metric_2.metric("Avg Speed", f"{avg_speed:.1f} km/h")
    metric_3.metric("Delay %", f"{delay_share:.1f}%")

    stop_points_df = selected_route_df.copy()
    stop_points_df["detail"] = "Route stop"

    with st.container(border=True):
        render_live_map(
            map_points_df=selected_route_df[["lat", "lon"]].copy(),
            route_layer_df=route_layer_df,
            route_segments_df=route_segments_df,
            buses_df=buses_df,
            trails_df=trails_df,
            stop_points_df=stop_points_df,
            city_columns_df=pd.DataFrame(),
            map_style="light",
            mapbox_token=None,
            zoom=estimate_map_zoom(selected_route_df),
            pitch=24,
            bearing=0,
            show_stop_labels=False,
            use_bus_icons=False,
            enable_touch_rotate=False,
            map_height=700,
            chart_key=f"live-map-{selected_route_id}-{frame_index}-{speed_label}",
        )

    st.caption(
        "Showing one route at a time keeps the map easy to read while tooltips reveal each bus ID, speed, delay, and next stop."
    )

    insight_left, insight_mid, insight_right = st.columns([1.2, 1.0, 1.8], gap="large")

    with insight_left:
        with st.container(border=True):
            st.subheader("Selected Route")
            st.write(route_name)
            st.caption(f"{selected_route_meta['first_stop']} to {selected_route_meta['last_stop']}")
            st.write(f"Stops: {int(selected_route_meta['stop_count'])} | Distance: {route_distance_km:.1f} km")
            st.write(f"Trips in data: {int(selected_route_meta['trip_count'])}")

    with insight_mid:
        with st.container(border=True):
            st.subheader("Small Insights")
            st.write(f"Average delay: {avg_delay:.1f} min")
            st.write(f"Traffic mix: {high_traffic} high, {medium_traffic} medium, {low_traffic} low")
            st.write("Bus view: selected route only")
            st.progress(max(5, int(100 - delay_share)), text="On-time health")

    with insight_right:
        with st.container(border=True):
            st.subheader("Live Bus Status")
            if buses_df.empty:
                st.info("No buses are active for the selected route.")
            else:
                status_table = (
                    buses_df[["bus_id", "speed_kmh", "delay_min", "status", "next_stop"]]
                    .rename(
                        columns={
                            "bus_id": "Bus ID",
                            "speed_kmh": "Speed (km/h)",
                            "delay_min": "Delay (min)",
                            "status": "Status",
                            "next_stop": "Next Stop",
                        }
                    )
                    .sort_values(["Delay (min)", "Speed (km/h)"], ascending=[False, False])
                    .head(8)
                )
                st.dataframe(status_table, use_container_width=True, hide_index=True)

    if auto_play:
        time.sleep(float(profile["refresh_seconds"]))
        st.rerun()


def render_traffic_heatmap(gps_df: pd.DataFrame) -> None:
    if gps_df.empty:
        st.info("No GPS data is available for the heatmap.")
        return

    heatmap_df = gps_df.rename(columns={"Latitude": "lat", "Longitude": "lon"}).copy()
    heatmap_df["weight"] = (65 - heatmap_df["Speed_kmh"].clip(lower=0, upper=65)).fillna(20) + 1

    layer = pdk.Layer(
        "HeatmapLayer",
        data=heatmap_df.to_dict(orient="records"),
        get_position="[lon, lat]",
        get_weight="weight",
        radiusPixels=50,
        intensity=1.1,
        threshold=0.03,
        opacity=0.65,
    )
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(
            latitude=float(heatmap_df["lat"].mean()),
            longitude=float(heatmap_df["lon"].mean()),
            zoom=6.2,
            pitch=20,
        ),
        map_style="light",
    )
    st.pydeck_chart(deck, use_container_width=True, height=560, key="traffic-heatmap")


def render_traffic_insights_page(context: dict[str, object]) -> None:
    gps_df = context["gps_df"]

    render_page_header(
        "Traffic Insights",
        "Charts only: where buses slow down, how speed changes across the day, and where traffic is denser.",
    )

    if gps_df.empty:
        st.info("GPS speed data is not available in data/raw/gps.csv.")
        return

    avg_speed = float(gps_df["Speed_kmh"].mean())
    slow_share = float((gps_df["Speed_kmh"] < 20).mean() * 100)
    metric_left, metric_mid = st.columns(2)
    metric_left.metric("Average Speed", f"{avg_speed:.1f} km/h")
    metric_mid.metric("Slow Traffic", f"{slow_share:.1f}%")

    chart_left, chart_right = st.columns(2, gap="large")

    with chart_left:
        with st.container(border=True):
            st.subheader("Speed Distribution")
            bins = [0, 10, 20, 30, 40, 50, 60, 80, 120]
            speed_bands = pd.cut(gps_df["Speed_kmh"], bins=bins, include_lowest=True)
            distribution = speed_bands.value_counts().sort_index()
            distribution_chart = pd.DataFrame(
                {
                    "Speed Range": [str(interval) for interval in distribution.index],
                    "Data Points": distribution.values,
                }
            )
            st.bar_chart(distribution_chart.set_index("Speed Range")["Data Points"])

    with chart_right:
        with st.container(border=True):
            st.subheader("Traffic Trend")
            timestamped_gps_df = gps_df.dropna(subset=["Timestamp"])
            if timestamped_gps_df.empty:
                st.info("No timestamps are available for the hourly traffic trend.")
            else:
                hourly_speed = (
                    timestamped_gps_df.assign(hour=lambda frame: frame["Timestamp"].dt.hour)
                    .groupby("hour", as_index=False)["Speed_kmh"]
                    .mean()
                    .rename(columns={"Speed_kmh": "Average Speed"})
                )
                st.line_chart(hourly_speed.set_index("hour")["Average Speed"])

    with st.container(border=True):
        st.subheader("Traffic Heatmap")
        st.caption("Slower GPS points are weighted more heavily so congestion zones stand out faster.")
        st.progress(max(5, int(100 - slow_share)), text="Traffic health")
        render_traffic_heatmap(gps_df)


def render_route_analysis_page(context: dict[str, object]) -> None:
    stops_lookup_df = context["stops_lookup_df"]
    pagerank_df = context["pagerank_df"]
    shortest_df = context["shortest_df"]
    path_example_df = context["path_example_df"]
    component_summary_df = context["component_summary_df"]

    render_page_header(
        "Route Analysis",
        "Important stops, shortest-path results, and network structure without the live-map noise.",
    )

    if pagerank_df.empty and shortest_df.empty and component_summary_df.empty:
        st.info("Route analysis files are not available in results/csv/.")
        return

    if not pagerank_df.empty:
        top_hubs = pagerank_df.merge(stops_lookup_df, on="stop_id", how="left")
        top_hubs["stop_name"] = top_hubs["stop_name"].fillna("Stop-" + top_hubs["stop_id"].astype(str))
        top_hubs = (
            top_hubs.sort_values("pagerank", ascending=False)
            .head(15)
            .rename(
                columns={
                    "stop_name": "Stop",
                    "stop_id": "Stop ID",
                    "pagerank": "Importance Score",
                }
            )
        )
        top_hubs["Importance Score"] = top_hubs["Importance Score"].round(4)
    else:
        top_hubs = pd.DataFrame(columns=["Stop", "Stop ID", "Importance Score"])

    path_preview_df = build_shortest_path_preview(path_example_df, shortest_df, stops_lookup_df)
    network_groups = int(component_summary_df.shape[0]) if not component_summary_df.empty else 0

    metric_1, metric_2, metric_3 = st.columns(3)
    metric_1.metric("Important Stops", f"{top_hubs.shape[0]}")
    metric_2.metric("Route Steps", f"{path_preview_df.shape[0]}")
    metric_3.metric("Connected Areas", f"{network_groups}")

    table_left, table_right = st.columns(2, gap="large")

    with table_left:
        with st.container(border=True):
            st.subheader("Important Stops")
            if top_hubs.empty:
                st.info("PageRank results are not available.")
            else:
                card_columns = st.columns(2, gap="medium")
                tone_cycle = ["spotlight", "success", "warning", "danger", "spotlight"]
                for idx, (_, row) in enumerate(top_hubs.head(5).iterrows()):
                    with card_columns[idx % 2]:
                        render_metric_card(
                            label=f"Top {idx + 1}",
                            value=str(row["Stop"]),
                            meta=f"Score {row['Importance Score']:.4f} | Stop ID {row['Stop ID']}",
                            tone=tone_cycle[idx],
                        )
                with st.expander("View more important stops"):
                    st.dataframe(top_hubs, use_container_width=True, hide_index=True)

    with table_right:
        with st.container(border=True):
            st.subheader("Route Steps")
            if path_preview_df.empty:
                st.info("Shortest-path results are not available.")
            else:
                st.dataframe(path_preview_df, use_container_width=True, hide_index=True)

    with st.container(border=True):
        st.subheader("Connected Areas")
        if component_summary_df.empty:
            st.info("Connected-group summary is not available.")
        else:
            summary_df = component_summary_df.rename(
                columns={"component_id": "Connected Area", "stop_count": "Stops"}
            )
            if summary_df.shape[0] <= 4:
                summary_columns = st.columns(summary_df.shape[0], gap="medium")
                for idx, (_, row) in enumerate(summary_df.iterrows()):
                    with summary_columns[idx]:
                        render_metric_card(
                            label=f"Area {row['Connected Area']}",
                            value=f"{row['Stops']} stops",
                            meta="Connected network summary",
                            tone="success",
                        )
            else:
                st.dataframe(summary_df, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="KSRTC Dashboard", page_icon=":bus:", layout="wide")
    apply_light_theme()

    context = load_dashboard_context(
        stops_version=file_version(STOPS_FILE),
        shortest_version=file_version(SHORTEST_PATHS_FILE),
        path_example_version=file_version(SHORTEST_PATH_EXAMPLE_FILE),
        pagerank_version=file_version(PAGERANK_FILE),
        predictions_version=file_version(PREDICTIONS_FILE),
        gps_version=file_version(GPS_FILE),
        component_summary_version=file_version(CONNECTED_COMPONENTS_SUMMARY_FILE),
        route_version=file_version(ROUTE_FILE),
        schedule_version=file_version(SCHEDULE_FILE),
        city_cache_version=file_version(CITY_COORDINATES_CACHE_FILE),
        unresolved_cache_version=file_version(CITY_COORDINATES_MISSING_FILE),
    )

    route_df = context["route_df"]
    if route_df.empty:
        st.error(
            "No route points are available. Add geocoded route data or ensure cleaned_ksrtc_data.csv can be resolved into coordinates."
        )
        st.stop()

    st.sidebar.header("Select Page")
    selected_page = st.sidebar.radio(
        "Select Page",
        ["Live Transit Map", "Traffic Analytics", "Route Optimization"],
        index=0,
    )

    if selected_page == "Live Transit Map":
        render_legacy_live_map_page(context)
    elif selected_page == "Traffic Analytics":
        render_traffic_insights_page(context)
    else:
        render_route_analysis_page(context)


if __name__ == "__main__":
    main()








