"""
Metrics reader for the dashboard.

Each pipeline process writes a JSON sidecar to the shared ``logs/`` volume:

    generator_metrics.json        data_generator (Kafka producer)
    spark_stream_metrics.json     spark_jobs/streaming_job.py
    spark_state_metrics.json      spark_jobs/vehicle_state_job.py
    rust_metrics.json             rust-pipeline stream
    rust_state_metrics.json       rust-pipeline state
    validation_report_<table>.json  rust-pipeline validate (one per table/job)

The dashboard only reads these files; it never writes pipeline metrics.
"""

import json
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

METRICS_DIR = Path(__file__).resolve().parent.parent / "logs"
GENERATOR_JSON_PATH = METRICS_DIR / "generator_metrics.json"
SPARK_STREAM_JSON_PATH = METRICS_DIR / "spark_stream_metrics.json"
SPARK_STATE_JSON_PATH = METRICS_DIR / "spark_state_metrics.json"
# Spark jobs started outside the dashboard write to the bridge's default file
LEGACY_SPARK_JSON_PATH = METRICS_DIR / "spark_metrics.json"
RUST_STREAM_JSON_PATH = METRICS_DIR / "rust_metrics.json"
RUST_STATE_JSON_PATH = METRICS_DIR / "rust_state_metrics.json"

_lock = threading.Lock()


def _read_json(path: Path) -> dict | None:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read metrics file %s: %s", path, exc)
    return None


def _default_engine_metrics() -> dict:
    return {
        "status": "idle",
        "micro_batches_completed": 0,
        "total_rows_processed": 0,
        "last_batch_rows": 0,
        "first_update": None,
        "last_update": None,
        "batch_durations_ms": [],
        "transform_times_ms": [],
        "delta_write_times_ms": [],
        "batch_details": [],
        "executor": {},
    }


def _default_generator_metrics() -> dict:
    return {
        "status": "idle",
        "start_time": None,
        "target_bytes": 0,
        "bytes_sent": 0,
        "records_sent": 0,
        "batches_completed": 0,
        "elapsed_seconds": 0.0,
        "throughput_mb_per_sec": 0.0,
        "avg_batch_time_ms": 0.0,
        "avg_publish_latency_ms": 0.0,
        "batch_timings_ms": [],
        "publish_latencies_ms": [],
        "errors": 0,
        "last_update": None,
    }


def _looks_like_stream_metrics(data: dict | None) -> bool:
    """The streaming job reports transform timings; the state job does not."""
    return bool(data and (data.get("transform_times_ms") or data.get("executor")))


def _read_spark_stream() -> dict:
    live = _read_json(SPARK_STREAM_JSON_PATH)
    if live:
        return live
    legacy = _read_json(LEGACY_SPARK_JSON_PATH)
    return legacy if _looks_like_stream_metrics(legacy) else _default_engine_metrics()


def _read_spark_state() -> dict:
    live = _read_json(SPARK_STATE_JSON_PATH)
    if live:
        return live
    legacy = _read_json(LEGACY_SPARK_JSON_PATH)
    is_state = legacy and legacy.get("micro_batches_completed", 0) > 0 and not _looks_like_stream_metrics(legacy)
    return legacy if is_state else _default_engine_metrics()


def _with_averages(data: dict) -> dict:
    """Normalise a raw engine sidecar (Spark or Rust) and add averages."""
    merged = {**_default_engine_metrics(), **data}

    def avg(values: list) -> float:
        return sum(values) / len(values) if values else 0.0

    merged["avg_batch_duration_ms"] = avg(merged["batch_durations_ms"])
    merged["avg_transform_time_ms"] = avg(merged["transform_times_ms"])
    merged["avg_delta_write_time_ms"] = avg(merged["delta_write_times_ms"])
    return merged


def reset_metrics():
    """Delete all engine and generator sidecars (validation reports are kept)."""
    with _lock:
        for path in [GENERATOR_JSON_PATH, SPARK_STREAM_JSON_PATH, SPARK_STATE_JSON_PATH,
                     LEGACY_SPARK_JSON_PATH, RUST_STREAM_JSON_PATH, RUST_STATE_JSON_PATH]:
            path.unlink(missing_ok=True)


def get_metrics() -> dict:
    """Current snapshot of generator, Spark and Rust metrics."""
    with _lock:
        return {
            "generator": {**_default_generator_metrics(), **(_read_json(GENERATOR_JSON_PATH) or {})},
            "spark_stream": _with_averages(_read_spark_stream()),
            "spark_state": _with_averages(_read_spark_state()),
            "rust_stream": _with_averages(_read_json(RUST_STREAM_JSON_PATH) or {}),
            "rust_state": _with_averages(_read_json(RUST_STATE_JSON_PATH) or {}),
        }


def get_validation_report(table: str) -> dict | None:
    """The last `rust-pipeline validate` report for a table or analytics job,
    with the file's modification time added as ``written_at`` (epoch secs)."""
    path = METRICS_DIR / f"validation_report_{table}.json"
    report = _read_json(path)
    if report is not None:
        report["written_at"] = path.stat().st_mtime
    return report
