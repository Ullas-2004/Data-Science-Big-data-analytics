"""Compatibility entrypoint for realtime producer naming convention."""
from backend.realtime.store_gps import main


if __name__ == "__main__":
    raise SystemExit(main())
