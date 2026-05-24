"""Compatibility entrypoint for weather stream naming convention."""
from backend.realtime.store_weather import main


if __name__ == "__main__":
    raise SystemExit(main())
