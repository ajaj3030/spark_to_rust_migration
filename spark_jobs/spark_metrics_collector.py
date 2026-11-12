"""
Collect detailed Spark execution metrics from the Spark REST API.

Queries the Spark driver (localhost:4040) after each micro-batch to gather
CPU time, GC time, memory, shuffle, I/O, and task-level statistics.
These metrics are critical for evaluating Spark-to-Rust migration feasibility.

All functions are designed to run **inside the Spark container** where the
REST API is available at localhost:4040.
"""

import json
import logging
import urllib.request

logger = logging.getLogger(__name__)

_SPARK_API = "http://localhost:4040/api/v1/applications"
_TIMEOUT = 5  # seconds


def _api_get(path):
    # type: (str) -> Optional[object]
    """GET a Spark REST API endpoint, return parsed JSON or None."""
    try:
        url = _SPARK_API + path
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req, timeout=_TIMEOUT)
        return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def get_app_id():
    # type: () -> Optional[str]
    """Return the first active Spark application ID."""
    apps = _api_get("")
    if apps and len(apps) > 0:
        return apps[0]["id"]
    return None


def collect_executor_metrics(app_id):
    # type: (str) -> Dict
    """Collect executor-level metrics (CPU, GC, memory, tasks)."""
    executors = _api_get("/{}/executors".format(app_id))
    if not executors:
        return {}

    total_tasks = 0
    total_duration_ms = 0
    total_gc_ms = 0
    total_input_bytes = 0
    total_output_bytes = 0
    total_shuffle_read_bytes = 0
    total_shuffle_write_bytes = 0
    peak_memory_bytes = 0
    max_memory_bytes = 0
    active_cores = 0

    for ex in executors:
        if ex.get("id") == "driver":
            continue  # Skip driver, only count worker executors
        total_tasks += ex.get("totalTasks", 0)
        total_duration_ms += ex.get("totalDuration", 0)
        total_gc_ms += ex.get("totalGCTime", 0)
        total_input_bytes += ex.get("totalInputBytes", 0)
        total_output_bytes += ex.get("totalOutputBytes", 0)
        total_shuffle_read_bytes += ex.get("totalShuffleRead", 0)
        total_shuffle_write_bytes += ex.get("totalShuffleWrite", 0)
        active_cores += ex.get("totalCores", 0)

        # Peak memory from memoryMetrics
        mem = ex.get("peakMemoryMetrics", {})
        jvm_heap = mem.get("JVMHeapMemory", 0)
        if jvm_heap > peak_memory_bytes:
            peak_memory_bytes = jvm_heap

        max_memory_bytes += ex.get("maxMemory", 0)

    return {
        "total_tasks": total_tasks,
        "total_duration_ms": total_duration_ms,
        "total_gc_ms": total_gc_ms,
        "gc_pct": round(100.0 * total_gc_ms / total_duration_ms, 1) if total_duration_ms > 0 else 0.0,
        "total_input_mb": round(total_input_bytes / 1048576, 2),
        "total_output_mb": round(total_output_bytes / 1048576, 2),
        "total_shuffle_read_mb": round(total_shuffle_read_bytes / 1048576, 2),
        "total_shuffle_write_mb": round(total_shuffle_write_bytes / 1048576, 2),
        "peak_memory_mb": round(peak_memory_bytes / 1048576, 1),
        "max_memory_mb": round(max_memory_bytes / 1048576, 1),
        "active_cores": active_cores,
    }


def collect_latest_stage_metrics(app_id, last_known_stage_id=-1):
    # type: (str, int) -> Dict
    """Collect metrics for stages completed since last_known_stage_id.

    Returns aggregated metrics for the newest batch of stages.
    """
    stages = _api_get("/{}/stages".format(app_id))
    if not stages:
        return {}

    # Find stages newer than last_known_stage_id and COMPLETE
    new_stages = [
        s for s in stages
        if s.get("stageId", 0) > last_known_stage_id
        and s.get("status") == "COMPLETE"
    ]
    if not new_stages:
        return {}

    total_cpu_ns = 0
    total_gc_ms = 0
    total_executor_run_ms = 0
    total_tasks = 0
    total_input_bytes = 0
    total_input_records = 0
    total_output_bytes = 0
    total_output_records = 0
    total_shuffle_read_bytes = 0
    total_shuffle_read_records = 0
    total_shuffle_write_bytes = 0
    total_shuffle_write_records = 0
    peak_memory = 0
    max_stage_id = last_known_stage_id

    for s in new_stages:
        sid = s.get("stageId", 0)
        if sid > max_stage_id:
            max_stage_id = sid

        total_tasks += s.get("numCompleteTasks", 0)
        total_executor_run_ms += s.get("executorRunTime", 0)
        total_cpu_ns += s.get("executorCpuTime", 0)
        total_gc_ms += s.get("jvmGcTime", 0)
        total_input_bytes += s.get("inputBytes", 0)
        total_input_records += s.get("inputRecords", 0)
        total_output_bytes += s.get("outputBytes", 0)
        total_output_records += s.get("outputRecords", 0)
        total_shuffle_read_bytes += s.get("shuffleReadBytes", 0)
        total_shuffle_read_records += s.get("shuffleReadRecords", 0)
        total_shuffle_write_bytes += s.get("shuffleWriteBytes", 0)
        total_shuffle_write_records += s.get("shuffleWriteRecords", 0)

        pmem = s.get("peakExecutionMemory", 0)
        if pmem > peak_memory:
            peak_memory = pmem

    cpu_ms = total_cpu_ns / 1e6
    cpu_util = round(100.0 * cpu_ms / total_executor_run_ms, 1) if total_executor_run_ms > 0 else 0.0

    return {
        "last_stage_id": max_stage_id,
        "stages_count": len(new_stages),
        "tasks": total_tasks,
        "executor_run_ms": total_executor_run_ms,
        "cpu_ms": round(cpu_ms, 1),
        "cpu_util_pct": cpu_util,
        "gc_ms": total_gc_ms,
        "gc_pct": round(100.0 * total_gc_ms / total_executor_run_ms, 1) if total_executor_run_ms > 0 else 0.0,
        "input_mb": round(total_input_bytes / 1048576, 2),
        "input_records": total_input_records,
        "output_mb": round(total_output_bytes / 1048576, 2),
        "output_records": total_output_records,
        "shuffle_read_mb": round(total_shuffle_read_bytes / 1048576, 2),
        "shuffle_read_records": total_shuffle_read_records,
        "shuffle_write_mb": round(total_shuffle_write_bytes / 1048576, 2),
        "shuffle_write_records": total_shuffle_write_records,
        "peak_execution_memory_mb": round(peak_memory / 1048576, 1),
    }


def collect_batch_metrics(app_id, last_known_stage_id=-1):
    # type: (str, int) -> Dict
    """Collect all metrics for a single batch (executor + stage level).

    Call this after each foreachBatch completes.
    Returns a dict with both executor-level and stage-level metrics.
    """
    result = {}

    # Executor-level (cumulative but useful for totals)
    ex_metrics = collect_executor_metrics(app_id)
    if ex_metrics:
        result["executor"] = ex_metrics

    # Stage-level (per-batch delta)
    stage_metrics = collect_latest_stage_metrics(app_id, last_known_stage_id)
    if stage_metrics:
        result["stage"] = stage_metrics

    return result
