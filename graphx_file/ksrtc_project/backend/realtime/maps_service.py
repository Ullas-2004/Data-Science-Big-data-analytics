from __future__ import annotations

import http.client
import os
from urllib.parse import quote_plus

MAPS_HOST = "google-map-places.p.rapidapi.com"


def get_map_image(location: str = "Hubli") -> bytes:
    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        raise RuntimeError("RAPIDAPI_KEY is not set")

    conn = http.client.HTTPSConnection(MAPS_HOST, timeout=10)
    headers = {
        "x-rapidapi-host": MAPS_HOST,
        "x-rapidapi-key": api_key,
    }

    endpoint = f"/maps/api/streetview?size=600x400&location={quote_plus(location)}"
    conn.request("GET", endpoint, headers=headers)
    res = conn.getresponse()
    data = res.read()

    if res.status >= 400:
        raise RuntimeError(f"Maps API error {res.status}: {data.decode('utf-8', errors='ignore')}")

    return data
