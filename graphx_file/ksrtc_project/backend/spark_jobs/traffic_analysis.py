"""Compatibility Spark entrypoint for traffic analysis."""


def main() -> int:
    print("Traffic and demand artifacts are generated via backend/analysis/generate_report_assets.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
