"""Compatibility Spark entrypoint for route optimization."""


def main() -> int:
    print("Route optimization is executed in backend/spark_jobs/graph_route_engine.scala")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
