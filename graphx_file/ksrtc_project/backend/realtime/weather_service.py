from __future__ import annotations

import http.client
import json
import os
from urllib.parse import quote_plus

WEATHER_HOST = "weather-api138.p.rapidapi.com"


def get_weather(city: str = "Hubli") -> dict:
    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        # Keep pipeline runnable in offline/demo environments.
        return {
            "city": city,
            "temperature": 27.0,
            "humidity": 68,
            "condition": "partly cloudy",
            "source": "offline_fallback",
        }

    conn = http.client.HTTPSConnection(WEATHER_HOST, timeout=10)
    headers = {
        "x-rapidapi-host": WEATHER_HOST,
        "x-rapidapi-key": api_key,
    }

    endpoint = f"/weather?city_name={quote_plus(city)}"
    conn.request("GET", endpoint, headers=headers)

    res = conn.getresponse()
    payload = res.read().decode("utf-8")
    if res.status >= 400:
        raise RuntimeError(f"Weather API error {res.status}: {payload}")

    weather = json.loads(payload)
    return {
        "city": city,
        "temperature": weather["main"]["temp"],
        "humidity": weather["main"]["humidity"],
        "condition": weather["weather"][0]["description"],
    }
