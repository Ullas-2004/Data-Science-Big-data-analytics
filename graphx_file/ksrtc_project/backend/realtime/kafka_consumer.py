"""Compatibility entrypoint for realtime consumer naming convention."""
from backend.realtime.store_schedule import main


if __name__ == "__main__":
    raise SystemExit(main())
