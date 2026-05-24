"""
Local comparison dashboard server for GraphFrames vs GraphX.

Run:
    python dashboard_server.py

Then open:
    http://127.0.0.1:8000
"""

from __future__ import annotations

import json
import mimetypes
import threading
import traceback
import uuid
from datetime import datetime
import gzip
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlparse

import run_comparison


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data_lake"
FRONTEND_DIR = ROOT_DIR / "webapp"
RUNS_DIR = ROOT_DIR / "Comparison_Report" / "runs"
RUNS_DIR.mkdir(parents=True, exist_ok=True)

JOBS: Dict[str, Dict[str, object]] = {}
JOBS_LOCK = threading.Lock()
PIPELINE_LOCK = threading.Lock()
NETWORK_CACHE: Dict[str, Dict[str, object]] = {}


def now_iso() -> str:
    """Return a compact timestamp string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def candidate_datasets() -> List[Path]:
    """Return all supported dataset files."""
    patterns = ["*.txt", "*.txt.gz", "*.csv", "*.csv.gz", "*.tsv", "*.tsv.gz"]
    files: List[Path] = []
    for pattern in patterns:
        files.extend(DATA_DIR.glob(pattern))
    return sorted(files, key=lambda path: path.name.lower())


def result_path_for_slug(slug: str) -> Path:
    """Return the dataset result payload path."""
    return RUNS_DIR / slug / "comparison_data.json"


def summarize_payload(payload: Dict[str, object]) -> Dict[str, object]:
    """Create a lightweight payload summary for the selector rail."""
    dataset = payload.get("dataset", {})
    hero_cards = payload.get("hero_cards", [])
    runs = payload.get("runs", [])
    return {
        "slug": dataset.get("slug"),
        "name": dataset.get("name"),
        "file_name": dataset.get("file_name"),
        "generated_at": payload.get("generated_at"),
        "hero_cards": hero_cards,
        "runs": runs,
    }


def load_payload(slug: str) -> Optional[Dict[str, object]]:
    """Load a saved comparison payload for a dataset."""
    path = result_path_for_slug(slug)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def list_results() -> List[Dict[str, object]]:
    """List all saved comparison summaries."""
    items: List[Dict[str, object]] = []
    for path in sorted(RUNS_DIR.glob("*/comparison_data.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        items.append(summarize_payload(payload))
    return items


def resolve_dataset(dataset_ref: str) -> Path:
    """Resolve a dataset slug, file name, or absolute path to a real file."""
    if not dataset_ref:
        raise FileNotFoundError("A dataset reference is required.")

    raw_path = Path(dataset_ref)
    if raw_path.is_absolute() and raw_path.exists():
        return raw_path

    for candidate in candidate_datasets():
        slug = run_comparison.get_dataset_slug(str(candidate))
        if candidate.name == dataset_ref or slug == dataset_ref:
            return candidate

    raise FileNotFoundError(f"Dataset not found: {dataset_ref}")


def dataset_records() -> List[Dict[str, object]]:
    """Build metadata for the dataset picker."""
    saved = {item["slug"]: item for item in list_results()}
    items: List[Dict[str, object]] = []
    for path in candidate_datasets():
        slug = run_comparison.get_dataset_slug(str(path))
        result = saved.get(slug)
        items.append(
            {
                "slug": slug,
                "name": run_comparison.get_dataset_name(str(path)),
                "file_name": path.name,
                "path": str(path),
                "size_mb": round(path.stat().st_size / (1024 * 1024), 3),
                "has_result": result is not None,
                "latest_result": result,
            }
        )
    return items


def job_snapshot(job_id: str) -> Optional[Dict[str, object]]:
    """Return a thread-safe snapshot of a job."""
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        return dict(job) if job else None


def update_job(job_id: str, **updates: object) -> None:
    """Mutate a job in a thread-safe way."""
    with JOBS_LOCK:
        if job_id not in JOBS:
            return
        JOBS[job_id].update(updates)
        JOBS[job_id]["updated_at"] = now_iso()


def existing_active_job(dataset_slug: str) -> Optional[Dict[str, object]]:
    """Return a queued/running job for the same dataset if one exists."""
    with JOBS_LOCK:
        for job in JOBS.values():
            if job.get("dataset_slug") == dataset_slug and job.get("status") in {"queued", "running"}:
                return dict(job)
    return None


def run_job(job_id: str, dataset_path: Path) -> None:
    """Execute a dataset comparison in the background."""
    dataset_slug = run_comparison.get_dataset_slug(str(dataset_path))
    dataset_name = run_comparison.get_dataset_name(str(dataset_path))
    output_dir = RUNS_DIR / dataset_slug

    try:
        update_job(
            job_id,
            status="queued",
            stage="waiting",
            progress=4,
            message="Waiting for the graph processing pipeline to become available.",
        )

        with PIPELINE_LOCK:
            artifacts = run_comparison.build_artifact_paths(
                run_comparison.resolve_report_dir(str(output_dir))
            )

            update_job(
                job_id,
                status="running",
                stage="graphframes",
                progress=18,
                message="Running GraphFrames on the selected SNAP dataset.",
            )
            gf_result = run_comparison.run_graphframes(str(dataset_path), artifacts)

            update_job(
                job_id,
                status="running",
                stage="graphx",
                progress=58,
                message="Running GraphX and collecting graph-engine proof logs.",
            )
            gx_result = run_comparison.run_graphx(str(dataset_path), artifacts)

            update_job(
                job_id,
                status="running",
                stage="packaging",
                progress=82,
                message="Archiving per-dataset proof files and building the presentation dashboard.",
            )
            payload = run_comparison.generate_outputs(
                str(dataset_path),
                dataset_name,
                gf_result,
                gx_result,
                artifacts,
            )

        update_job(
            job_id,
            status="completed",
            stage="done",
            progress=100,
            message="Comparison complete. The dashboard has been refreshed with saved proof files.",
            finished_at=now_iso(),
            result_slug=dataset_slug,
            result_path=payload["artifacts"]["json_report"],
        )
    except Exception as exc:  # pragma: no cover - defensive for runtime failures
        update_job(
            job_id,
            status="failed",
            stage="failed",
            progress=100,
            message="The comparison run failed. Open the error details for the traceback.",
            finished_at=now_iso(),
            error=str(exc),
            traceback=traceback.format_exc(),
        )


def create_job(dataset_ref: str) -> Dict[str, object]:
    """Create or reuse an active job for a dataset."""
    dataset_path = resolve_dataset(dataset_ref)
    dataset_slug = run_comparison.get_dataset_slug(str(dataset_path))
    active = existing_active_job(dataset_slug)
    if active:
        return active

    job_id = uuid.uuid4().hex[:10]
    job = {
        "id": job_id,
        "dataset_slug": dataset_slug,
        "dataset_name": run_comparison.get_dataset_name(str(dataset_path)),
        "file_name": dataset_path.name,
        "status": "queued",
        "stage": "created",
        "progress": 1,
        "message": "Queued for execution.",
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "finished_at": None,
        "result_slug": None,
        "result_path": None,
        "error": None,
        "traceback": None,
    }

    with JOBS_LOCK:
        JOBS[job_id] = job

    worker = threading.Thread(target=run_job, args=(job_id, dataset_path), daemon=True)
    worker.start()
    return job


def safe_preview(path_str: str) -> Dict[str, object]:
    """Read a text preview of a proof file under the project directory."""
    path = Path(path_str).resolve()
    root = ROOT_DIR.resolve()
    if root != path and root not in path.parents:
        raise PermissionError("Preview path is outside the project workspace.")
    if not path.exists():
        raise FileNotFoundError(str(path))

    raw = path.read_text(encoding="utf-8", errors="replace")
    lines = raw.splitlines()
    limit = 40
    return {
        "path": str(path),
        "line_count": len(lines),
        "truncated": len(lines) > limit,
        "content": "\n".join(lines[:limit]),
    }


def open_dataset_text(path: Path):
    """Open plain-text or gzip-compressed dataset files as text."""
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def build_network_preview(dataset_slug: str) -> Dict[str, object]:
    """Build a lightweight node-link snapshot from a dataset."""
    if dataset_slug in NETWORK_CACHE:
        return NETWORK_CACHE[dataset_slug]

    dataset_path = resolve_dataset(dataset_slug)
    sample_edges = []
    degrees: Dict[str, int] = {}

    with open_dataset_text(dataset_path) as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            tokens = stripped.replace(",", " ").split()
            if len(tokens) < 2:
                continue
            src, dst = tokens[0], tokens[1]
            sample_edges.append((src, dst))
            degrees[src] = degrees.get(src, 0) + 1
            degrees[dst] = degrees.get(dst, 0) + 1
            if len(sample_edges) >= 900:
                break

    ranked_nodes = sorted(degrees.items(), key=lambda item: (-item[1], item[0]))
    allowed = {node_id for node_id, _ in ranked_nodes[:70]}
    filtered_edges = [(src, dst) for src, dst in sample_edges if src in allowed and dst in allowed][:180]
    node_degree = {
        node_id: 0
        for node_id in allowed
    }
    for src, dst in filtered_edges:
        node_degree[src] = node_degree.get(src, 0) + 1
        node_degree[dst] = node_degree.get(dst, 0) + 1

    nodes = [
        {"id": node_id, "degree": degree}
        for node_id, degree in sorted(node_degree.items(), key=lambda item: (-item[1], item[0]))
    ]
    preview = {
        "dataset_slug": dataset_slug,
        "dataset_name": run_comparison.get_dataset_name(str(dataset_path)),
        "nodes": nodes,
        "edges": [{"source": src, "target": dst} for src, dst in filtered_edges],
    }
    NETWORK_CACHE[dataset_slug] = preview
    return preview


class DashboardHandler(BaseHTTPRequestHandler):
    """Serve the app shell plus a small local JSON API."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return

    def send_json(self, payload: object, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, text: str, status: int = HTTPStatus.OK) -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def serve_file(self, path: Path) -> None:
        if not path.exists() or not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return
        content_type, _ = mimetypes.guess_type(str(path))
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        route = parsed.path

        if route == "/api/datasets":
            self.send_json({"datasets": dataset_records(), "results": list_results()})
            return

        if route == "/api/results":
            self.send_json({"results": list_results()})
            return

        if route.startswith("/api/results/"):
            slug = route.rsplit("/", 1)[-1]
            payload = load_payload(slug)
            if not payload:
                self.send_error(HTTPStatus.NOT_FOUND, "Result not found")
                return
            self.send_json(payload)
            return

        if route.startswith("/api/jobs/"):
            job_id = route.rsplit("/", 1)[-1]
            job = job_snapshot(job_id)
            if not job:
                self.send_error(HTTPStatus.NOT_FOUND, "Job not found")
                return
            self.send_json(job)
            return

        if route == "/api/proof-preview":
            path_arg = parse_qs(parsed.query).get("path", [""])[0]
            try:
                preview = safe_preview(unquote(path_arg))
            except Exception as exc:  # pragma: no cover - runtime validation
                self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(preview)
            return

        if route.startswith("/api/network/"):
            slug = route.rsplit("/", 1)[-1]
            try:
                preview = build_network_preview(slug)
            except Exception as exc:  # pragma: no cover - runtime validation
                self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(preview)
            return

        if route in {"/", "/index.html"}:
            self.serve_file(FRONTEND_DIR / "index.html")
            return

        candidate = (FRONTEND_DIR / route.lstrip("/")).resolve()
        if FRONTEND_DIR.resolve() in candidate.parents or candidate == FRONTEND_DIR.resolve():
            if candidate.exists() and candidate.is_file():
                self.serve_file(candidate)
                return

        self.send_error(HTTPStatus.NOT_FOUND, "Route not found")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/api/run":
            self.send_error(HTTPStatus.NOT_FOUND, "Route not found")
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            dataset_ref = payload.get("dataset")
            job = create_job(str(dataset_ref or ""))
            self.send_json(job, status=HTTPStatus.ACCEPTED)
        except FileNotFoundError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:  # pragma: no cover - runtime validation
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)


def main() -> None:
    """Start the local dashboard server."""
    host = "127.0.0.1"
    port = 8000
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print("=" * 72)
    print("GraphFrames vs GraphX Local Dashboard Server")
    print("=" * 72)
    print(f"URL   : http://{host}:{port}")
    print(f"Data  : {DATA_DIR}")
    print(f"Runs  : {RUNS_DIR}")
    print("Press Ctrl+C to stop.")
    server.serve_forever()


if __name__ == "__main__":
    main()
