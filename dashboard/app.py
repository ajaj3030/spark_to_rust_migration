"""
Vehicle Telemetry Pipeline dashboard: Streamlit UI for the Spark → Rust
migration.

Pages:
  1. Overview        pipeline status, generator progress, core Delta tables
  2. Fleet           battery health, charging, OTA rollout and diagnostics
                     from the batch analytics tables (Spark or Rust output)
  3. Migration       which jobs are ported to Rust and whether their output
                     matches Spark's (runs the Rust port + parity check)
  4. Data Generator  publish synthetic vehicle telemetry to Kafka
  5. Pipeline Jobs   start/stop the Spark and Rust jobs, reset state
  6. Delta Explorer  schema, sample rows, SQL and history for any table
  7. Performance     Spark vs Rust streaming timings

Run (from the repo root):
    streamlit run dashboard/app.py
"""

import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
import yaml

# Ensure project root is importable
_IN_DOCKER = os.environ.get("DASHBOARD_DOCKER") == "1"
PROJECT_ROOT = Path("/app") if _IN_DOCKER else Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.metrics import get_metrics, get_validation_report, reset_metrics  # noqa: E402

# ── Page Config ───────────────────────────────────────────────

st.set_page_config(
    page_title="Vehicle Telemetry Pipeline",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Helpers ───────────────────────────────────────────────────


def load_config() -> dict:
    cfg_path = PROJECT_ROOT / "config" / "app_config.yaml"
    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)


def resolve_path(path: str) -> Path:
    """Config paths are relative to the repo root."""
    p = Path(path)
    return p if p.is_absolute() else PROJECT_ROOT / p


@st.cache_data(ttl=2, show_spinner=False)
def get_spark_cluster_status() -> dict:
    """Query Spark Master REST API for active applications and worker info."""
    try:
        spark_host = os.environ.get("SPARK_MASTER_HOST", "localhost")
        resp = requests.get(f"http://{spark_host}:8080/json/", timeout=3)
        data = resp.json()
        active_apps = data.get("activeapps", [])
        return {
            "status": "running" if active_apps else "idle",
            "active_apps": len(active_apps),
            "apps": [{"id": a["id"], "name": a["name"]} for a in active_apps],
            "app_names": [a["name"] for a in active_apps],
            "workers": data.get("aliveworkers", 0),
            "reachable": True,
        }
    except (requests.RequestException, ValueError):
        return {"status": "unreachable", "active_apps": 0, "apps": [], "app_names": [], "workers": 0, "reachable": False}


def fmt_bytes(b: float) -> str:
    if b >= 1e9:
        return f"{b / 1e9:.2f} GB"
    if b >= 1e6:
        return f"{b / 1e6:.1f} MB"
    if b >= 1e3:
        return f"{b / 1e3:.0f} KB"
    return f"{b:.0f} B"


def fmt_duration(s: float) -> str:
    if s >= 3600:
        return f"{s / 3600:.1f}h"
    if s >= 60:
        return f"{s / 60:.1f}m"
    return f"{s:.1f}s"


def fmt_ago(epoch: float | None) -> str:
    if not epoch:
        return "—"
    return f"{fmt_duration(max(0.0, time.time() - epoch))} ago"


def is_delta_table(path: str) -> bool:
    return (resolve_path(path) / "_delta_log").exists()


def get_delta_table_info(table_path: str) -> dict | None:
    """Read Delta table metadata (no data scan). None if the table doesn't exist."""
    if not is_delta_table(table_path):
        return None
    try:
        from deltalake import DeltaTable
        import pyarrow as pa
        dt = DeltaTable(str(resolve_path(table_path)))
        add_actions = pa.table(dt.get_add_actions(flatten=True)).to_pydict()
        schema = dt.schema().to_arrow()
        return {
            "version": dt.version(),
            "num_files": len(add_actions.get("path", [])),
            "row_count": int(sum(add_actions.get("num_records", [0]))),
            "columns": len(schema),
            "size_bytes": sum(add_actions.get("size_bytes", [0])),
            "schema": schema,
            "partition_columns": dt.metadata().partition_columns,
        }
    except Exception as e:
        return {"error": str(e)}


@st.cache_data(show_spinner=False, max_entries=32)
def _read_delta(path: str, version: int) -> pd.DataFrame:
    from deltalake import DeltaTable
    return DeltaTable(path).to_pandas()


def load_delta_table(table_path: str) -> pd.DataFrame | None:
    """Whole table as a DataFrame, cached until the table's version changes."""
    info = get_delta_table_info(table_path)
    if not info or "error" in info:
        return None
    return _read_delta(str(resolve_path(table_path)), info["version"])


def query_delta_table(table_path: str, sql: str) -> pd.DataFrame:
    """Query a Delta table with DuckDB SQL; the table is registered as `tbl`."""
    try:
        import duckdb
        from deltalake import DeltaTable
        arrow = DeltaTable(str(resolve_path(table_path))).to_pyarrow_table()
        con = duckdb.connect()
        con.register("tbl", arrow)
        return con.execute(sql).fetchdf()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def show_log_tail(log_path: Path, lines: int = 100):
    if log_path.exists():
        tail = log_path.read_text(errors="replace").strip().split("\n")[-lines:]
        st.code("\n".join(tail), language="log")
    else:
        st.info("No log file yet.")


cfg = load_config()

# ── Registry: analytics jobs, tables, Spark and Rust jobs ─────
#
# Every page reads from these lists, so adding a job or table is one entry.

ANALYTICS_JOBS = [
    {
        "name": "battery_health_daily",
        "class": "BatteryHealthDaily",
        "title": "Battery health (daily)",
        "desc": "Per vehicle per day: charge, cell temperatures, thermal alerts, "
                "end-of-day battery health and its day-over-day change.",
    },
    {
        "name": "charging_sessions",
        "class": "ChargingSessions",
        "title": "Charging sessions",
        "desc": "One row per charge session: duration, energy added, peak and "
                "average power, start/end state of charge, faults.",
    },
    {
        "name": "ota_rollout",
        "class": "OtaRollout",
        "title": "OTA rollout",
        "desc": "Latest OTA update state per vehicle, pivoted by target software "
                "version and model, with success and failure rates.",
    },
    {
        "name": "diagnostic_alerts",
        "class": "DiagnosticAlerts",
        "title": "Diagnostic alerts",
        "desc": "Fault codes (DTCs) by ECU, model and battery pack: events, "
                "vehicles affected, share of fleet, rank within ECU.",
    },
]

_ANALYTICS_BASE = cfg["analytics"]["local_output_base"]


def _build_tables() -> list[dict]:
    tables = [
        {"name": "vehicle_telemetry", "label": "Telemetry", "kind": "telemetry",
         "spark": cfg["spark"]["local_delta_output"], "rust": cfg["rust"]["local_delta_output"]},
        {"name": "vehicle_state", "label": "Vehicle state", "kind": "state",
         "spark": cfg["vehicle_state"]["local_delta_output"], "rust": cfg["rust"]["local_state_delta_output"]},
    ]
    for job in ANALYTICS_JOBS:
        tables.append({
            "name": job["name"], "label": job["title"], "kind": "analytics",
            "spark": f"{_ANALYTICS_BASE}/{job['name']}",
            "rust": f"{_ANALYTICS_BASE}/{job['name']}_rust",
        })
    return tables


TABLES = _build_tables()
TABLES_BY_NAME = {t["name"]: t for t in TABLES}

SPARK_JARS = [
    "spark-sql-kafka-0-10_2.12-3.5.1.jar",
    "spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
    "kafka-clients-3.4.1.jar",
    "commons-pool2-2.11.1.jar",
    "delta-spark_2.12-3.1.0.jar",
    "delta-storage-3.1.0.jar",
]
# One 4-core worker runs both streams side by side
SPARK_CORES_PER_JOB = 2
_SPARK_APP = cfg["spark"]["app_name"]


# Built with sbt package in spark_jobs/analytics (see README)
ANALYTICS_JAR = "spark_jobs/analytics/target/scala-2.12/telemetry-analytics_2.12-1.0.0.jar"
ANALYTICS_JAR_IN_CONTAINER = "/opt/spark-jobs/analytics/target/scala-2.12/telemetry-analytics_2.12-1.0.0.jar"


def _build_spark_jobs() -> dict:
    jobs = {
        "spark_telemetry": {
            "title": "📡 Telemetry stream (Kafka → Delta)",
            "desc": "Reads vehicle telemetry from Kafka, applies the enrichment chain "
                    "(battery and health features, coarse location, VIN hashing) and "
                    "appends to the vehicle_telemetry table. Runs until stopped.",
            "app": ["--py-files", "/opt/spark-jobs/transforms.py", "/opt/spark-jobs/streaming_job.py"],
            "process": "streaming_job.py",
            "app_name": _SPARK_APP,
            "metrics_file": "spark_stream_metrics.json",
            "long_running": True,
        },
        "spark_state": {
            "title": "🚗 Vehicle state (Kafka → Delta MERGE)",
            "desc": "Reads per-vehicle state snapshots and MERGEs them into the "
                    "vehicle_state table (one row per VIN; older sequence numbers never "
                    "overwrite newer state). Runs until stopped.",
            "app": ["--py-files", "/opt/extra-jars/delta-spark_2.12-3.1.0.jar",
                    "/opt/spark-jobs/vehicle_state_job.py"],
            "process": "vehicle_state_job.py",
            "app_name": f"{_SPARK_APP}_VehicleState",
            "metrics_file": "spark_state_metrics.json",
            "long_running": True,
        },
    }
    for job in ANALYTICS_JOBS:
        jobs[f"spark_{job['name']}"] = {
            "title": f"📊 {job['title']}",
            "desc": job["desc"] + " Scala batch job over the telemetry table.",
            "app": ["--class", f"telemetry.analytics.{job['class']}", ANALYTICS_JAR_IN_CONTAINER],
            "process": f"telemetry.analytics.{job['class']}",
            "app_name": f"{_SPARK_APP}_{job['name']}",
            "metrics_file": None,
            "long_running": False,
        }
    return jobs


SPARK_JOBS = _build_spark_jobs()
SPARK_STREAM_KEYS = ["spark_telemetry", "spark_state"]

RUST_JOBS = {
    "rust_telemetry": {
        "title": "📡 Telemetry stream (Kafka → Delta)",
        "desc": "Rust port of the telemetry stream: same transforms, writes the "
                "vehicle_telemetry_rust table. Separate consumer group from Spark.",
        "args": ["stream"],
    },
    "rust_state": {
        "title": "🚗 Vehicle state (Kafka → Delta MERGE)",
        "desc": "Rust port of the vehicle-state MERGE, writes vehicle_state_rust.",
        "args": ["state"],
    },
}

# ── Job runners ───────────────────────────────────────────────

if "procs" not in st.session_state:
    st.session_state.procs = {}

_LOCAL_RUST_BINARY = PROJECT_ROOT / "rust_pipeline" / "target" / "debug" / "rust-pipeline"


def rust_command(args: list[str]) -> list[str]:
    if _IN_DOCKER:
        return ["docker", "exec", "rust-pipeline", "/usr/local/bin/rust-pipeline",
                "--config", "/opt/config/app_config.yaml", *args]
    return [str(_LOCAL_RUST_BINARY), "--config", "config/app_config.yaml", *args]


def _rust_process_pattern(args: list[str]) -> str:
    return f"rust-pipeline --config [^ ]+ {' '.join(args)}$"


def build_rust_pipeline() -> subprocess.CompletedProcess | None:
    """Rebuild the local binary so a freshly merged port is picked up.
    In Docker the rust-pipeline image is rebuilt with docker compose instead."""
    if _IN_DOCKER:
        return None
    cargo = shutil.which("cargo") or str(Path.home() / ".cargo" / "bin" / "cargo")
    return subprocess.run(
        [cargo, "build", "--manifest-path", "rust_pipeline/Cargo.toml"],
        cwd=str(PROJECT_ROOT), capture_output=True, text=True, timeout=900,
        env={**os.environ, "CMAKE_POLICY_VERSION_MINIMUM": "3.5"},
    )


def spark_submit_command(job: dict) -> list[str]:
    env = ["-e", f"SPARK_METRICS_FILENAME={job['metrics_file']}"] if job["metrics_file"] else []
    return [
        "docker", "exec", "-w", "/opt", *env, "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--total-executor-cores", str(SPARK_CORES_PER_JOB),
        "--jars", ",".join(f"/opt/extra-jars/{jar}" for jar in SPARK_JARS),
        *job["app"],
        "--config", "/opt/config/app_config.yaml",
    ]


def start_job(key: str, cmd: list[str]):
    """Run a job in the background, logging to logs/<key>.log."""
    log_path = PROJECT_ROOT / "logs" / f"{key}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w") as log_file:
        st.session_state.procs[key] = subprocess.Popen(
            cmd, cwd=str(PROJECT_ROOT), stdout=log_file, stderr=subprocess.STDOUT,
        )


def _local_proc_status(key: str) -> str | None:
    proc = st.session_state.procs.get(key)
    if proc is None:
        return None
    ret = proc.poll()
    if ret is None:
        return "running"
    return "completed" if ret == 0 else f"failed (exit {ret})"


def spark_job_status(key: str) -> str:
    # The cluster is the source of truth for jobs started outside this session
    if SPARK_JOBS[key]["app_name"] in get_spark_cluster_status()["app_names"]:
        return "running"
    return _local_proc_status(key) or "stopped"


def rust_job_status(key: str) -> str:
    local = _local_proc_status(key)
    if local:
        return local
    pattern = _rust_process_pattern(RUST_JOBS[key]["args"])
    cmd = ["docker", "exec", "rust-pipeline", "pgrep", "-f", pattern] if _IN_DOCKER else ["pgrep", "-f", pattern]
    try:
        found = subprocess.run(cmd, capture_output=True, timeout=5).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        found = False
    return "running" if found else "stopped"


def _terminate_local_proc(key: str):
    proc = st.session_state.procs.pop(key, None)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def stop_spark_job(key: str):
    """Kill the app on the cluster and the spark-submit driver in the container.
    The local process is only a `docker exec` wrapper; killing it alone
    leaves the JVM running."""
    job = SPARK_JOBS[key]
    spark_host = os.environ.get("SPARK_MASTER_HOST", "localhost")
    for app in get_spark_cluster_status()["apps"]:
        if app["name"] == job["app_name"]:
            try:
                requests.post(f"http://{spark_host}:8080/app/kill/?id={app['id']}&terminate=true", timeout=5)
            except requests.RequestException as e:
                st.warning(f"Could not kill Spark app {app['id']}: {e}")
    subprocess.run(["docker", "exec", "spark-master", "pkill", "-f", job["process"]],
                   capture_output=True, timeout=10)
    _terminate_local_proc(key)
    get_spark_cluster_status.clear()


def stop_rust_job(key: str):
    pattern = _rust_process_pattern(RUST_JOBS[key]["args"])
    cmd = ["docker", "exec", "rust-pipeline", "pkill", "-f", pattern] if _IN_DOCKER else ["pkill", "-f", pattern]
    subprocess.run(cmd, capture_output=True, timeout=10)
    _terminate_local_proc(key)


def stop_all_jobs():
    for key in SPARK_JOBS:
        if spark_job_status(key) == "running":
            stop_spark_job(key)
    for key in RUST_JOBS:
        stop_rust_job(key)


_STATUS_ICON = {"running": "🟢", "completed": "✅", "stopped": "⚪", "idle": "⚪"}


def render_job_card(key: str, job: dict, status: str, on_start, on_stop):
    with st.container(border=True):
        st.subheader(job["title"])
        st.caption(job["desc"])
        col1, col2, col3 = st.columns([2, 1, 1])
        col1.write(f"**Status:** {_STATUS_ICON.get(status, '🔴')} {status}")
        with col2:
            if status == "running":
                if st.button("⏹️ Stop", key=f"stop_{key}"):
                    on_stop(key)
                    st.rerun()
            elif st.button("▶️ Start", key=f"start_{key}", type="primary"):
                on_start(key)
                time.sleep(0.5)
                st.rerun()
        with col3:
            if (PROJECT_ROOT / "logs" / f"{key}.log").exists():
                if st.button("📄 Logs", key=f"logs_{key}"):
                    st.session_state[f"show_logs_{key}"] = not st.session_state.get(f"show_logs_{key}", False)
        if st.session_state.get(f"show_logs_{key}", False):
            show_log_tail(PROJECT_ROOT / "logs" / f"{key}.log")


# ── Migration status ──────────────────────────────────────────

_PORT_PENDING = re.compile(r"^\s*#\[ignore", re.MULTILINE)


def is_ported(job_name: str) -> bool:
    """A job is ported once its Spark parity test is no longer #[ignore]d
    (the README's definition of done)."""
    src = PROJECT_ROOT / "rust_pipeline" / "src" / "analytics" / f"{job_name}.rs"
    return src.exists() and not _PORT_PENDING.search(src.read_text())


def _validate_args(table: dict) -> list[str]:
    if table["kind"] == "analytics":
        return ["validate", "--job", table["name"]]
    return ["validate", "--state"] if table["kind"] == "state" else ["validate"]


def _row_count(path: str) -> int | None:
    info = get_delta_table_info(path)
    return info["row_count"] if info and "error" not in info else None


def migration_status(table: dict) -> dict:
    report = get_validation_report(table["name"])
    ported = table["kind"] != "analytics" or is_ported(table["name"])
    return {
        "table": table,
        "ported": ported,
        "spark_rows": _row_count(table["spark"]),
        "rust_rows": _row_count(table["rust"]),
        "report": report,
    }


def _parity_label(status: dict) -> str:
    report = status["report"]
    if not report:
        return "— not validated"
    return "✅ PASS" if report.get("status") == "PASS" else "❌ FAIL"


def run_rust_port_and_validate(table: dict) -> tuple[bool, str]:
    """Build, run the Rust job (analytics only) and validate it against Spark.
    Returns (ok, combined output)."""
    output = []
    steps = []
    if table["kind"] == "analytics":
        steps.append(["analytics", table["name"]])
    steps.append(_validate_args(table))

    build = build_rust_pipeline()
    if build is not None:
        output.append(build.stderr[-3000:])
        if build.returncode != 0:
            return False, "\n".join(output)
    for args in steps:
        result = subprocess.run(rust_command(args), cwd=str(PROJECT_ROOT),
                                capture_output=True, text=True, timeout=900)
        output.append(f"$ rust-pipeline {' '.join(args)}\n{result.stdout}{result.stderr}")
        if result.returncode != 0:
            return False, "\n".join(output)
    return True, "\n".join(output)


# ── Sidebar ───────────────────────────────────────────────────

st.sidebar.title("⚡ Vehicle Telemetry")
page = st.sidebar.radio(
    "Navigate",
    ["📊 Overview", "🚗 Fleet", "🦀 Migration", "🚀 Data Generator",
     "⚙️ Pipeline Jobs", "🔍 Delta Explorer", "📈 Performance"],
)

auto_refresh = st.sidebar.checkbox("Auto-refresh (3s)", value=False)

st.sidebar.markdown("---")
st.sidebar.caption("Config: `config/app_config.yaml`")
st.sidebar.caption(f"Kafka: `{cfg['kafka']['bootstrap_servers']}`")
st.sidebar.caption(f"Topics: `{cfg['kafka']['topic']}`, `{cfg['kafka']['state_topic']}`")
st.sidebar.caption("Synthetic data only. Field names follow the public, unofficial "
                   "python-lucidmotors client.")

# ══════════════════════════════════════════════════════════════
# PAGE: Overview
# ══════════════════════════════════════════════════════════════

if page == "📊 Overview":
    st.title("📊 Pipeline Overview")

    metrics = get_metrics()
    gen = metrics["generator"]

    # ── Status Row ────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        gen_status = gen["status"]
        color = {"running": "🟢", "completed": "✅", "idle": "⚪"}.get(gen_status, "🔴")
        st.metric("Generator", f"{color} {gen_status.upper()}")
    with c2:
        cluster = get_spark_cluster_status()
        spark_status = cluster["status"]
        color = {"running": "🟢", "idle": "⚪", "unreachable": "🔴"}.get(spark_status, "🔴")
        st.metric("Spark Cluster", f"{color} {spark_status.upper()}")
        if cluster["active_apps"] > 0:
            st.caption(f"{cluster['active_apps']} app(s): {', '.join(cluster['app_names'])}")
    with c3:
        rust_status = rust_job_status("rust_telemetry")
        st.metric("Rust Stream", f"{_STATUS_ICON.get(rust_status, '🔴')} {rust_status.upper()}")
    with c4:
        ported = sum(is_ported(j["name"]) for j in ANALYTICS_JOBS)
        st.metric("Analytics Jobs Ported", f"{ported} / {len(ANALYTICS_JOBS)}")
        st.caption("See 🦀 Migration for parity status")

    st.markdown("---")

    # ── Generator Progress ────────────────────────────────
    st.subheader("Data Generation Progress")

    if gen["target_bytes"] > 0:
        progress = min(1.0, gen["bytes_sent"] / gen["target_bytes"])
        st.progress(progress, text=f"{progress * 100:.1f}% — {fmt_bytes(gen['bytes_sent'])} / {fmt_bytes(gen['target_bytes'])}")
    else:
        st.progress(0.0, text="Not started")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Events Sent", f"{gen['records_sent']:,}")
    c2.metric("Throughput", f"{gen['throughput_mb_per_sec']:.1f} MB/s")
    c3.metric("Elapsed", fmt_duration(gen["elapsed_seconds"]))
    c4.metric("Batches", f"{gen['batches_completed']:,}")

    st.markdown("---")

    # ── Generator Latency Charts ──────────────────────────
    st.subheader("Generator Timing")
    col_a, col_b = st.columns(2)

    with col_a:
        if gen["batch_timings_ms"]:
            st.caption("Batch Production Time (ms)")
            st.line_chart(pd.DataFrame({"batch_time_ms": gen["batch_timings_ms"]}))
        else:
            st.info("No batch timing data yet")

    with col_b:
        if gen["publish_latencies_ms"]:
            st.caption("Kafka Publish Latency — flush() time (ms)")
            st.line_chart(pd.DataFrame({"publish_latency_ms": gen["publish_latencies_ms"]}))
        else:
            st.info("No publish latency data yet")

    st.markdown("---")

    # ── Delta Table Summary ───────────────────────────────
    st.subheader("Delta Tables")
    for table in [TABLES_BY_NAME["vehicle_telemetry"], TABLES_BY_NAME["vehicle_state"]]:
        cols = st.columns(2)
        for col, engine in zip(cols, ["spark", "rust"]):
            with col:
                icon = "🦀" if engine == "rust" else "⚡"
                st.caption(f"📁 {table['label']} — {icon} {engine.title()}")
                info = get_delta_table_info(table[engine])
                if info and "error" not in info:
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Rows", f"{info['row_count']:,}")
                    m2.metric("Files", f"{info['num_files']:,}")
                    m3.metric("Version", info["version"])
                    m4.metric("Size", fmt_bytes(info["size_bytes"]))
                elif info:
                    st.warning(f"Error: {info['error']}")
                else:
                    st.info("Table not found — start this job on ⚙️ Pipeline Jobs")


# ══════════════════════════════════════════════════════════════
# PAGE: Fleet
# ══════════════════════════════════════════════════════════════

elif page == "🚗 Fleet":
    st.title("🚗 Fleet Analytics")
    st.markdown(
        "Business views over the batch analytics tables. Vehicles are identified "
        "only by `vin_hash`; raw VINs and GPS never reach these tables."
    )

    engine = st.radio(
        "Source", ["Spark", "Rust"], horizontal=True,
        help="Rust output exists only for jobs that have been ported and run "
             "(see 🦀 Migration).",
    ).lower()

    def load_job_output(job_name: str) -> pd.DataFrame | None:
        table = TABLES_BY_NAME[job_name]
        df = load_delta_table(table[engine])
        if df is None:
            hint = ("port it and run it from 🦀 Migration" if engine == "rust"
                    else "run it from ⚙️ Pipeline Jobs")
            st.info(f"No {engine.title()} output for `{job_name}` yet — {hint}.")
        return df

    def short_hash(series: pd.Series) -> pd.Series:
        return series.str.slice(0, 12) + "…"

    def render_battery_health(df: pd.DataFrame):
        latest = df.sort_values("event_date").groupby("vin_hash").tail(1)
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Vehicles", f"{df['vin_hash'].nunique():,}")
        k2.metric("Days Covered", f"{df['event_date'].nunique()}")
        k3.metric("Avg Battery Health", f"{latest['health_level_end_of_day'].mean():.1f}%")
        k4.metric("Vehicles With Thermal Alerts",
                  f"{df.loc[df['thermal_alert_count'] > 0, 'vin_hash'].nunique():,}")

        c1, c2 = st.columns(2)
        with c1:
            daily = (df.groupby(["event_date", "model_safe"], as_index=False)
                     ["health_level_end_of_day"].mean())
            st.plotly_chart(px.line(daily, x="event_date", y="health_level_end_of_day",
                                    color="model_safe", markers=True,
                                    title="Average end-of-day battery health",
                                    labels={"health_level_end_of_day": "Health %", "event_date": ""}),
                            width="stretch")
        with c2:
            st.plotly_chart(px.histogram(latest, x="health_level_end_of_day", color="model_safe",
                                         nbins=30, title="Latest battery health per vehicle",
                                         labels={"health_level_end_of_day": "Health %"}),
                            width="stretch")

        st.markdown("##### Largest day-over-day health drops")
        drops = df.dropna(subset=["health_change"]).nsmallest(10, "health_change").copy()
        drops["vin_hash"] = short_hash(drops["vin_hash"])
        st.dataframe(drops[["vin_hash", "event_date", "model_safe", "health_level_end_of_day",
                            "health_change", "peak_cell_temp", "thermal_alert_count"]],
                     hide_index=True, width="stretch")

    def render_charging(df: pd.DataFrame):
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Sessions", f"{len(df):,}")
        k2.metric("Energy Added", f"{df['energy_added_kwh'].sum():,.0f} kWh")
        k3.metric("Sessions With Faults", f"{df['had_fault'].mean() * 100:.1f}%")
        k4.metric("Median Duration", f"{df['duration_minutes'].median():.0f} min")

        c1, c2 = st.columns(2)
        with c1:
            by_state = df["end_state"].value_counts().rename_axis("end_state").reset_index(name="sessions")
            st.plotly_chart(px.bar(by_state, x="sessions", y="end_state", orientation="h",
                                   title="Sessions by final charge state",
                                   labels={"end_state": ""}),
                            width="stretch")
        with c2:
            energy = df.groupby(["session_date", "model_safe"], as_index=False)["energy_added_kwh"].sum()
            st.plotly_chart(px.bar(energy, x="session_date", y="energy_added_kwh", color="model_safe",
                                   title="Energy added per day (kWh)",
                                   labels={"energy_added_kwh": "kWh", "session_date": ""}),
                            width="stretch")

    def render_ota_rollout(df: pd.DataFrame):
        state_cols = [c for c in df.columns if c.startswith("UPDATE_")]
        total = df["vehicles"].sum()
        k1, k2, k3 = st.columns(3)
        k1.metric("Vehicles in Rollout", f"{total:,}")
        k2.metric("Success Rate", f"{(df['success_rate_pct'] * df['vehicles']).sum() / max(1, total):.1f}%")
        k3.metric("Failure Rate", f"{(df['failure_rate_pct'] * df['vehicles']).sum() / max(1, total):.1f}%")

        long = df.melt(id_vars=["target_software_version", "model_safe"], value_vars=state_cols,
                       var_name="update_state", value_name="count")
        long["rollout"] = long["target_software_version"] + " · " + long["model_safe"]
        st.plotly_chart(px.bar(long, x="rollout", y="count", color="update_state",
                               title="Latest OTA state per vehicle, by target version and model",
                               labels={"rollout": "", "count": "Vehicles"}),
                        width="stretch")
        st.dataframe(df.sort_values(["target_software_version", "model_safe"]),
                     hide_index=True, width="stretch")

    def render_diagnostics(df: pd.DataFrame):
        ecu = st.selectbox("ECU", ["All"] + sorted(df["ecu"].unique()))
        view = df if ecu == "All" else df[df["ecu"] == ecu]
        k1, k2, k3 = st.columns(3)
        k1.metric("Distinct Fault Codes", f"{view['dtc_code'].nunique()}")
        k2.metric("Fault Events", f"{view['events'].sum():,}")
        k3.metric("Events With Warning Lamp", f"{view['warning_on_events'].sum():,}")

        # A vehicle has one model and pack, so groups don't overlap and sums are exact
        top = (view.groupby(["dtc_code", "ecu"], as_index=False)["vehicles_affected"].sum()
               .nlargest(15, "vehicles_affected"))
        top["code"] = top["dtc_code"] + " (" + top["ecu"] + ")"
        st.plotly_chart(px.bar(top, x="vehicles_affected", y="code", orientation="h",
                               title="Most widespread fault codes",
                               labels={"vehicles_affected": "Vehicles affected", "code": ""}),
                        width="stretch")
        st.dataframe(view.sort_values(["ecu", "rank_in_ecu"]), hide_index=True, width="stretch")

    FLEET_TABS = [
        ("🔋 Battery Health", "battery_health_daily", render_battery_health),
        ("⚡ Charging", "charging_sessions", render_charging),
        ("📦 OTA Rollout", "ota_rollout", render_ota_rollout),
        ("🛠️ Diagnostics", "diagnostic_alerts", render_diagnostics),
    ]
    for tab, (_, job_name, render) in zip(st.tabs([t[0] for t in FLEET_TABS]), FLEET_TABS):
        with tab:
            output = load_job_output(job_name)
            if output is not None:
                render(output)


# ══════════════════════════════════════════════════════════════
# PAGE: Migration
# ══════════════════════════════════════════════════════════════

elif page == "🦀 Migration":
    st.title("🦀 Spark → Rust Migration")
    st.markdown(
        "Each Spark job gets a Rust port. A port is done when, on the same input, "
        "it produces exactly Spark's output: the same rows and values, the same "
        "column names and types, and no raw VIN or GPS columns. CI enforces this "
        "on committed fixtures; this page checks the live tables."
    )

    statuses = [migration_status(t) for t in TABLES]
    analytics = [s for s in statuses if s["table"]["kind"] == "analytics"]
    passing = [s for s in statuses if s["report"] and s["report"].get("status") == "PASS"]
    last_run = max((s["report"]["written_at"] for s in statuses if s["report"]), default=None)

    k1, k2, k3 = st.columns(3)
    k1.metric("Analytics Jobs Ported", f"{sum(s['ported'] for s in analytics)} / {len(analytics)}")
    k2.metric("Live Parity PASS", f"{len(passing)} / {len(statuses)}")
    k3.metric("Last Validation", fmt_ago(last_run))

    def fmt_rows(n: int | None) -> str:
        return f"{n:,}" if n is not None else "—"

    st.dataframe(
        pd.DataFrame([{
            "Job": s["table"]["name"],
            "Type": "streaming" if s["table"]["kind"] != "analytics" else "batch analytics",
            "Rust port": "✅ ported" if s["ported"] else "⏳ not ported",
            "Spark rows": fmt_rows(s["spark_rows"]),
            "Rust rows": fmt_rows(s["rust_rows"]),
            "Parity": _parity_label(s),
            "Validated": fmt_ago(s["report"]["written_at"]) if s["report"] else "—",
        } for s in statuses]),
        hide_index=True, width="stretch",
    )

    st.markdown("---")
    st.subheader("Run & validate")
    if not _IN_DOCKER:
        st.caption("Runs `cargo build` first, so a port merged since the last build is picked up.")

    def render_report(report: dict):
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Spark rows", f"{report.get('spark_row_count', 0):,}")
        r2.metric("Rust rows", f"{report.get('rust_row_count', 0):,}")
        r3.metric("Missing in Rust", f"{report.get('missing_in_rust', 0):,}")
        r4.metric("Missing in Spark", f"{report.get('missing_in_spark', 0):,}")
        if report.get("failures"):
            st.error("**Parity failures**\n\n" + "\n".join(f"- {f}" for f in report["failures"]))
        mismatches = report.get("column_mismatches", {})
        if mismatches:
            with st.expander(f"Per-column comparison ({len(mismatches)} columns)"):
                st.dataframe(pd.DataFrame(
                    [{"Column": c, "Mismatched rows": n, "": "✅" if n == 0 else "❌"}
                     for c, n in mismatches.items()]),
                    hide_index=True, width="stretch")

    for s in statuses:
        table = s["table"]
        with st.container(border=True):
            head, action = st.columns([3, 1])
            with head:
                st.markdown(f"**{table['label']}** · `{table['name']}` — {_parity_label(s)}")
                if table["kind"] == "analytics":
                    job_class = next(j["class"] for j in ANALYTICS_JOBS if j["name"] == table["name"])
                    st.caption(f"Spark: `spark_jobs/analytics/…/{job_class}.scala` → "
                               f"Rust: `rust_pipeline/src/analytics/{table['name']}.rs`")
            with action:
                blocked_reason = None
                if not s["ported"]:
                    blocked_reason = "Not ported yet: the parity test is still #[ignore]d."
                elif s["spark_rows"] is None:
                    blocked_reason = "No Spark output yet: run the Spark job on ⚙️ Pipeline Jobs."
                elif table["kind"] != "analytics" and s["rust_rows"] is None:
                    blocked_reason = "No Rust output yet: start the Rust stream on ⚙️ Pipeline Jobs."
                label = "▶️ Run Rust & validate" if table["kind"] == "analytics" else "✅ Validate"
                clicked = st.button(label, key=f"validate_{table['name']}",
                                    disabled=blocked_reason is not None, help=blocked_reason,
                                    type="primary", width="stretch")
            if blocked_reason:
                st.caption(blocked_reason)
            if clicked:
                with st.spinner(f"Running {table['name']}…"):
                    ok, output = run_rust_port_and_validate(table)
                st.session_state[f"migration_output_{table['name']}"] = output
                if ok:
                    st.success("Parity PASS — Rust output matches Spark.")
                else:
                    st.error("Run or validation failed — see output below.")
                s["report"] = get_validation_report(table["name"])
            if s["report"]:
                render_report(s["report"])
            last_output = st.session_state.get(f"migration_output_{table['name']}")
            if last_output:
                with st.expander("Command output"):
                    st.code(last_output[-6000:], language="log")


# ══════════════════════════════════════════════════════════════
# PAGE: Data Generator
# ══════════════════════════════════════════════════════════════

elif page == "🚀 Data Generator":
    st.title("🚀 Data Generator Control")

    st.markdown(f"""
    Publish synthetic vehicle telemetry for a fleet of
    {cfg['generator']['num_vehicles']:,} vehicles to Kafka: battery, charging,
    diagnostic and OTA events on `{cfg['kafka']['topic']}`, plus per-vehicle state
    snapshots on `{cfg['kafka']['state_topic']}`. Messages are keyed by VIN and
    each vehicle is produced by a single thread, so per-vehicle order holds.
    """)

    # ── Config & Trigger ──────────────────────────────────
    with st.form("generator_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            target_gb = st.number_input(
                "Target Data (GB)", min_value=0.01, max_value=50.0,
                value=0.05, step=0.05, format="%.2f",
                help="0.05 GB is about 60,000 events",
            )
        with c2:
            batch_size = st.number_input(
                "Batch Size", min_value=100, max_value=100000,
                value=cfg["generator"]["batch_size"], step=1000,
            )
        with c3:
            num_threads = st.number_input(
                "Producer Threads", min_value=1, max_value=16,
                value=cfg["generator"]["num_threads"], step=1,
            )

        submitted = st.form_submit_button("🚀 Start Data Generation", type="primary")

    if submitted:
        # Delete stale generator metrics so the UI shows fresh progress
        (PROJECT_ROOT / "logs" / "generator_metrics.json").unlink(missing_ok=True)
        cmd = [
            sys.executable, "-m", "data_generator.run",
            "--target-gb", str(target_gb),
            "--batch-size", str(int(batch_size)),
            "-t", str(int(num_threads)),
        ]
        start_job("generator", cmd)
        st.success(f"Data generation started: {target_gb} GB, {int(batch_size)} batch, {int(num_threads)} threads")
        time.sleep(1)
        st.rerun()

    st.markdown("---")

    # ── Live Progress ─────────────────────────────────────
    st.subheader("Live Progress")
    metrics = get_metrics()
    gen = metrics["generator"]

    _gen_is_running = gen["status"] == "running"

    if _gen_is_running:
        if gen["target_bytes"] > 0:
            progress = min(1.0, gen["bytes_sent"] / gen["target_bytes"])
            st.progress(progress, text=f"{progress * 100:.1f}%")

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Status", gen["status"].upper())
        c2.metric("Data Sent", fmt_bytes(gen["bytes_sent"]))
        c3.metric("Events", f"{gen['records_sent']:,}")
        c4.metric("Throughput", f"{gen['throughput_mb_per_sec']:.1f} MB/s")
        c5.metric("Errors", gen["errors"])

    elif gen["status"] == "completed":
        st.success("Generation completed!")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Data", fmt_bytes(gen["bytes_sent"]))
        c2.metric("Total Events", f"{gen['records_sent']:,}")
        c3.metric("Duration", fmt_duration(gen["elapsed_seconds"]))
        c4.metric("Throughput", f"{gen['throughput_mb_per_sec']:.1f} MB/s")
    else:
        st.info("Generator is idle. Submit the form above to start.")
        if (PROJECT_ROOT / "logs" / "generator.log").exists():
            with st.expander("Last generator log"):
                show_log_tail(PROJECT_ROOT / "logs" / "generator.log", lines=40)

    st.markdown("---")

    # ── Performance Stats ─────────────────────────────────
    st.subheader("Performance Metrics")

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Avg Batch Production Time", f"{gen['avg_batch_time_ms']:.0f} ms")
        if gen["batch_timings_ms"]:
            st.caption("Batch timing histogram (ms)")
            df = pd.DataFrame({"ms": gen["batch_timings_ms"]})
            st.bar_chart(df["ms"].value_counts().sort_index().head(30))
    with c2:
        st.metric("Avg Kafka Publish Latency", f"{gen['avg_publish_latency_ms']:.0f} ms")
        if gen["publish_latencies_ms"]:
            st.caption("Publish latency histogram (ms)")
            df = pd.DataFrame({"ms": gen["publish_latencies_ms"]})
            st.bar_chart(df["ms"].value_counts().sort_index().head(30))

    # ── Reset ─────────────────────────────────────────────
    st.markdown("---")
    if st.button("🗑️ Reset Metrics"):
        reset_metrics()
        st.rerun()

    # ── Auto-refresh while generator is running ───────────
    if _gen_is_running:
        time.sleep(2)
        st.rerun()


# ══════════════════════════════════════════════════════════════
# PAGE: Pipeline Jobs
# ══════════════════════════════════════════════════════════════

elif page == "⚙️ Pipeline Jobs":
    st.title("⚙️ Pipeline Jobs")

    st.markdown("""
    Spark jobs run in the Docker Spark cluster; Rust jobs write separate `_rust`
    tables from their own Kafka consumer groups, so both can run side by side.
    """)

    def start_spark(key: str):
        uses_jar = ANALYTICS_JAR_IN_CONTAINER in SPARK_JOBS[key]["app"]
        if uses_jar and not _IN_DOCKER and not (PROJECT_ROOT / ANALYTICS_JAR).exists():
            st.error("Scala analytics jar not built — run `sbt package` in spark_jobs/analytics/ first (see README).")
            return
        start_job(key, spark_submit_command(SPARK_JOBS[key]))

    def start_rust(key: str):
        if not _IN_DOCKER and not _LOCAL_RUST_BINARY.exists():
            st.error("Rust binary not built — run `cargo build` in rust_pipeline/ first.")
            return
        start_job(key, rust_command(RUST_JOBS[key]["args"]))

    spark_statuses = {key: spark_job_status(key) for key in SPARK_JOBS}
    rust_statuses = {key: rust_job_status(key) for key in RUST_JOBS}

    st.subheader("⚡ Spark — streaming")
    for key in SPARK_STREAM_KEYS:
        render_job_card(key, SPARK_JOBS[key], spark_statuses[key], start_spark, stop_spark_job)

    st.subheader("⚡ Spark — batch analytics")
    st.caption("Read the Spark telemetry table and write one table each under "
               f"`{_ANALYTICS_BASE}`. Their Rust ports are run from 🦀 Migration.")
    for key, job in SPARK_JOBS.items():
        if not job["long_running"]:
            render_job_card(key, job, spark_statuses[key], start_spark, stop_spark_job)

    st.markdown("---")
    st.subheader("🦀 Rust — streaming")
    for key, job in RUST_JOBS.items():
        render_job_card(key, job, rust_statuses[key], start_rust, stop_rust_job)

    st.markdown("---")

    # ── Bulk Controls ─────────────────────────────────────
    st.subheader("Bulk Controls")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("▶️ Start Spark Streaming", type="primary"):
            for key in SPARK_STREAM_KEYS:
                if spark_statuses[key] != "running":
                    start_spark(key)
            st.success("Spark streaming jobs started!")
            time.sleep(0.5)
            st.rerun()
    with col2:
        if st.button("🦀 Start Rust Streaming", type="primary"):
            for key in RUST_JOBS:
                if rust_statuses[key] != "running":
                    start_rust(key)
            st.success("Rust streaming jobs started!")
            time.sleep(0.5)
            st.rerun()
    with col3:
        if st.button("⏹️ Stop All Jobs"):
            stop_all_jobs()
            st.success("All jobs stopped.")
            time.sleep(0.5)
            st.rerun()

    # ── Live Pipeline Metrics ─────────────────────────────
    st.markdown("---")
    st.subheader("📊 Live Pipeline Metrics")

    _jobs_metrics = get_metrics()
    _any_job_running = "running" in list(spark_statuses.values()) + list(rust_statuses.values())

    def render_engine_metrics(label: str, m: dict):
        st.markdown(f"**{label}**")
        status = m.get("status", "idle")
        st.write(f"Status: {_STATUS_ICON.get(status, '🔴')} **{status.upper()}**")
        m1, m2, m3 = st.columns(3)
        m1.metric("Batches", f"{m['micro_batches_completed']:,}")
        m2.metric("Rows", f"{m['total_rows_processed']:,}")
        m3.metric("Avg Batch", f"{m['avg_batch_duration_ms']:.0f} ms")

    _pairs = [
        ("Telemetry stream", "spark_stream", "rust_stream"),
        ("Vehicle state MERGE", "spark_state", "rust_state"),
    ]
    if any(_jobs_metrics[k]["micro_batches_completed"] > 0 for _, s, r in _pairs for k in (s, r)):
        for title, spark_key, rust_key in _pairs:
            st.markdown(f"##### {title}")
            col_s, col_r = st.columns(2)
            with col_s:
                render_engine_metrics("⚡ Spark", _jobs_metrics[spark_key])
            with col_r:
                render_engine_metrics("🦀 Rust", _jobs_metrics[rust_key])
    elif _any_job_running:
        st.info("Pipeline jobs are running — metrics will appear after the first batch completes.")
    else:
        st.info("Start a pipeline job above to see live metrics.")

    # ── Pipeline Reset ────────────────────────────────────
    st.markdown("---")
    st.subheader("🔄 Pipeline Reset")
    st.markdown(
        "Stop all running jobs, wipe checkpoint / Delta data, and "
        "optionally restart ingestion from the beginning of the Kafka topics."
    )

    reset_tab1, reset_tab2, reset_tab3 = st.tabs([
        "🗑️ Wipe Checkpoints & Restart",
        "💣 Delete All Delta Files",
        "☢️ Full Reset (Both)",
    ])

    _ckpt_paths = [
        ("Spark telemetry checkpoint", resolve_path(cfg["spark"]["local_checkpoint"])),
        ("Spark vehicle state checkpoint", resolve_path(cfg["vehicle_state"]["local_checkpoint"])),
        ("Rust telemetry checkpoint", resolve_path(cfg["rust"]["local_checkpoint_db"]).parent),
        ("Rust vehicle state checkpoint", resolve_path(cfg["rust"]["local_state_checkpoint_db"]).parent),
    ]
    _delta_paths = [
        (f"{t['label']} ({engine.title()})", resolve_path(t[engine]))
        for t in TABLES if t["kind"] != "analytics" for engine in ("spark", "rust")
    ] + [("Analytics tables (Spark and Rust)", resolve_path(_ANALYTICS_BASE))]

    def _wipe_paths(paths: list[tuple[str, Path]]) -> list[str]:
        removed = []
        for label, p in paths:
            if p.exists():
                shutil.rmtree(p)
                removed.append(f"{label} (`{p}`)")
        return removed

    def _restart_streaming_jobs():
        for key in SPARK_STREAM_KEYS:
            start_spark(key)

    # ── Tab 1: Wipe Checkpoints & Restart ─────────────────
    with reset_tab1:
        st.info(
            "This will **stop all running jobs**, delete checkpoint directories, "
            "then restart the Spark streaming jobs. They will re-read from the earliest "
            "available Kafka offset."
        )
        existing_ckpts = [(label, p) for label, p in _ckpt_paths if p.exists()]
        if existing_ckpts:
            for label, p in existing_ckpts:
                st.write(f"  - {label}: `{p}`")
        else:
            st.write("No checkpoint directories found on disk.")

        ckpt_confirm = st.text_input(
            "Type **WIPE CHECKPOINTS** to confirm", key="ckpt_confirm"
        )
        if st.button("🗑️ Wipe Checkpoints & Restart", key="btn_wipe_ckpt"):
            if ckpt_confirm.strip() == "WIPE CHECKPOINTS":
                stop_all_jobs()
                removed = _wipe_paths(_ckpt_paths)
                if removed:
                    st.success("Removed: " + ", ".join(removed))
                else:
                    st.info("No checkpoint directories to remove.")
                _restart_streaming_jobs()
                st.success("Streaming jobs restarted — ingestion will resume from earliest offset.")
                time.sleep(1)
                st.rerun()
            else:
                st.error("Type exactly: `WIPE CHECKPOINTS`")

    # ── Tab 2: Delete All Delta Files ─────────────────────
    with reset_tab2:
        st.warning(
            "This will **stop all running jobs** and permanently delete all "
            "Delta Lake table data. The tables will be empty until jobs re-create them."
        )
        existing_delta = [(label, p) for label, p in _delta_paths if p.exists()]
        if existing_delta:
            for label, p in existing_delta:
                st.write(f"  - {label}: `{p}`")
        else:
            st.write("No Delta table directories found on disk.")

        delta_confirm = st.text_input(
            "Type **DELETE ALL DELTA** to confirm", key="delta_confirm"
        )
        if st.button("💣 Delete All Delta Files", key="btn_del_delta"):
            if delta_confirm.strip() == "DELETE ALL DELTA":
                stop_all_jobs()
                removed = _wipe_paths(_delta_paths)
                if removed:
                    st.success("Removed: " + ", ".join(removed))
                else:
                    st.info("No Delta directories to remove.")
                time.sleep(1)
                st.rerun()
            else:
                st.error("Type exactly: `DELETE ALL DELTA`")

    # ── Tab 3: Full Reset (checkpoints + delta + restart) ─
    with reset_tab3:
        st.error(
            "⚠️ **Nuclear option** — stops all jobs, deletes **all** checkpoint "
            "and Delta Lake data, resets metrics, then restarts ingestion from scratch."
        )
        full_confirm = st.text_input(
            "Type **FULL RESET** to confirm", key="full_reset_confirm"
        )
        if st.button("☢️ Full Reset", key="btn_full_reset", type="secondary"):
            if full_confirm.strip() == "FULL RESET":
                stop_all_jobs()
                all_removed = _wipe_paths(_ckpt_paths) + _wipe_paths(_delta_paths)
                reset_metrics()
                if all_removed:
                    st.success("Removed: " + ", ".join(all_removed))
                else:
                    st.info("Nothing to remove — directories were already clean.")
                st.info("Metrics reset.")
                _restart_streaming_jobs()
                st.success("Streaming jobs restarted — full pipeline reset complete.")
                time.sleep(1)
                st.rerun()
            else:
                st.error("Type exactly: `FULL RESET`")

    # ── Auto-refresh while any pipeline job is running ────
    if _any_job_running:
        time.sleep(3)
        st.rerun()


# ══════════════════════════════════════════════════════════════
# PAGE: Delta Explorer
# ══════════════════════════════════════════════════════════════

elif page == "🔍 Delta Explorer":
    st.title("🔍 Delta Table Explorer")

    _choices = {
        f"{t['label']} ({engine.title()})": (t, engine)
        for t in TABLES for engine in ("spark", "rust")
    }
    table_choice = st.selectbox("Select Table", list(_choices.keys()))
    chosen_table, _engine = _choices[table_choice]
    table_path = chosen_table[_engine]

    info = get_delta_table_info(table_path)

    if info is None:
        st.warning(f"Delta table not found at `{table_path}`. Run the pipeline first.")
    elif "error" in info:
        st.error(f"Error reading table: {info['error']}")
    else:
        # ── Table Info ────────────────────────────────────
        st.subheader("Table Metadata")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Version", info["version"])
        c2.metric("Rows", f"{info['row_count']:,}")
        c3.metric("Files", f"{info['num_files']:,}")
        c4.metric("Size", fmt_bytes(info["size_bytes"]))

        if info.get("partition_columns"):
            st.caption(f"**Partitioned by:** {', '.join(info['partition_columns'])}")

        # ── Schema ────────────────────────────────────────
        st.subheader("Schema")
        schema_data = [
            {"#": i, "Column": field.name, "Type": str(field.type), "Nullable": field.nullable}
            for i, field in enumerate(info["schema"])
        ]
        st.dataframe(pd.DataFrame(schema_data), width="stretch", hide_index=True)

        # ── Sample Data ───────────────────────────────────
        st.subheader("Sample Data")
        num_rows = st.slider("Number of rows", 5, 500, 20)
        try:
            from deltalake import DeltaTable
            dt = DeltaTable(str(resolve_path(table_path)))
            st.dataframe(dt.to_pyarrow_dataset().head(num_rows).to_pandas(), width="stretch")
        except Exception as e:
            st.error(f"Error loading sample: {e}")

        # ── SQL Query ─────────────────────────────────────
        st.subheader("SQL Query")
        st.caption("Query the table with SQL (DuckDB). The table is registered as `tbl`.")

        _common_queries = {
            "Row count": "SELECT COUNT(*) AS total_rows FROM tbl",
            "First rows": "SELECT * FROM tbl LIMIT 20",
        }
        _presets_by_kind = {
            "telemetry": {
                "Events by type": "SELECT event_type, COUNT(*) AS events FROM tbl GROUP BY event_type ORDER BY events DESC",
                "Fleet by model": "SELECT model_safe, COUNT(DISTINCT vin_hash) AS vehicles, COUNT(*) AS events FROM tbl GROUP BY model_safe ORDER BY vehicles DESC",
                "Battery health tiers": "SELECT health_tier, COUNT(DISTINCT vin_hash) AS vehicles FROM tbl WHERE health_tier IS NOT NULL GROUP BY health_tier ORDER BY vehicles DESC",
                "Thermal alerts": "SELECT model_safe, battery_pack_type, COUNT(*) AS alerts FROM tbl WHERE thermal_alert GROUP BY model_safe, battery_pack_type ORDER BY alerts DESC",
                "Faults by ECU": "SELECT ecu, dtc_code, COUNT(*) AS events FROM tbl WHERE is_fault GROUP BY ecu, dtc_code ORDER BY events DESC LIMIT 20",
                "Events per hour": "SELECT event_hour, COUNT(*) AS events FROM tbl GROUP BY event_hour ORDER BY event_hour",
                "Duplicate events (exactly-once check)": "SELECT event_id, COUNT(*) AS copies FROM tbl GROUP BY event_id HAVING COUNT(*) > 1 LIMIT 20",
                "Raw identifiers present (privacy check)": "SELECT column_name FROM (DESCRIBE tbl) WHERE column_name IN ('vin', 'latitude', 'longitude', 'kafka_key')",
            },
            "state": {
                "By model": "SELECT model, model_variant, COUNT(*) AS vehicles, ROUND(AVG(battery_health_level), 2) AS avg_health FROM tbl GROUP BY model, model_variant ORDER BY vehicles DESC",
                "Software versions": "SELECT software_version, COUNT(*) AS vehicles FROM tbl GROUP BY software_version ORDER BY software_version",
                "Needs battery service": "SELECT vin, model, battery_health_level, odometer_km FROM tbl WHERE battery_health_level < 70 ORDER BY battery_health_level",
                "Most active vehicles": "SELECT vin, model, event_count, sequence_number, updated_at FROM tbl ORDER BY event_count DESC LIMIT 20",
            },
            "analytics": {},
        }
        default_queries = {**_common_queries, **_presets_by_kind[chosen_table["kind"]], "Custom": ""}

        query_choice = st.selectbox("Preset queries", list(default_queries.keys()))
        sql_input = st.text_area(
            "SQL",
            value=default_queries[query_choice],
            height=80,
            placeholder="SELECT * FROM tbl LIMIT 10",
        )

        if st.button("▶️ Run Query", type="primary"):
            if sql_input.strip():
                with st.spinner("Running query..."):
                    result_df = query_delta_table(table_path, sql_input.strip())
                if not result_df.empty:
                    st.dataframe(result_df, width="stretch")
                    st.caption(f"{len(result_df)} rows returned")
                else:
                    st.info("Query returned no results")
            else:
                st.warning("Enter a SQL query first")

        # ── History ───────────────────────────────────────
        st.subheader("Table History")
        try:
            from deltalake import DeltaTable
            history = DeltaTable(str(resolve_path(table_path))).history()
            if history:
                hist_df = pd.DataFrame(history)
                display_cols = [c for c in ["version", "timestamp", "operation", "operationParameters"] if c in hist_df.columns]
                st.dataframe(hist_df[display_cols].head(20), width="stretch")
            else:
                st.info("No history available")
        except Exception as e:
            st.error(f"Error loading history: {e}")


# ══════════════════════════════════════════════════════════════
# PAGE: Performance
# ══════════════════════════════════════════════════════════════

elif page == "📈 Performance":
    st.title("📈 Performance Analytics")

    metrics = get_metrics()
    gen = metrics["generator"]
    spark = metrics["spark_stream"]
    rust = metrics["rust_stream"]

    # ── Quick Spark vs Rust Summary (always visible at top) ──
    _rs = rust.get("micro_batches_completed", 0)
    _ss = spark.get("micro_batches_completed", 0)
    if _rs > 0 or _ss > 0:
        st.markdown("---")
        st.subheader("🦀 Spark vs Rust — Quick Summary (telemetry stream)")
        _qs1, _qs2, _qs3 = st.columns(3)
        _qs1.metric("Spark Batches", f"{_ss:,}")
        _qs2.metric("Rust Batches", f"{_rs:,}")
        _spark_avg = spark.get("avg_batch_duration_ms", 0)
        _rust_bd = rust.get("batch_details", [])
        _rust_avg = (sum(b.get("wall_ms", 0) for b in _rust_bd) / len(_rust_bd)) if _rust_bd else 0
        if _spark_avg > 0 and _rust_avg > 0:
            _ratio = _spark_avg / _rust_avg
            _qs3.metric("Rust Speedup", f"{_ratio:.1f}x",
                        delta=f"{'Rust' if _ratio > 1 else 'Spark'} faster")
        else:
            _qs3.metric("Rust Speedup", "—")
        st.info("📊 Scroll down to **'Comprehensive Batch Run Statistics'** for the full comparison table with per-category winners.")
        st.markdown("---")

    # ── Generator Performance ─────────────────────────────
    st.subheader("🔧 Data Generator Performance")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Avg Batch Time", f"{gen['avg_batch_time_ms']:.0f} ms")
    c2.metric("Avg Publish Latency", f"{gen['avg_publish_latency_ms']:.0f} ms")
    c3.metric("Throughput", f"{gen['throughput_mb_per_sec']:.1f} MB/s")
    c4.metric("Batches", f"{gen['batches_completed']:,}")

    if gen["batch_timings_ms"]:
        col1, col2 = st.columns(2)
        with col1:
            st.caption("Batch Production Time (ms) — per batch")
            st.line_chart(
                pd.DataFrame({"batch_time_ms": gen["batch_timings_ms"]}),
                width="stretch",
            )
        with col2:
            st.caption("Kafka Publish Latency (ms) — flush() time")
            if gen["publish_latencies_ms"]:
                st.line_chart(
                    pd.DataFrame({"publish_latency_ms": gen["publish_latencies_ms"]}),
                    width="stretch",
                )

        # Distribution summary
        st.caption("Timing Distribution Summary")
        batch_series = pd.Series(gen["batch_timings_ms"])
        pub_series = pd.Series(gen["publish_latencies_ms"]) if gen["publish_latencies_ms"] else pd.Series([0])
        summary = pd.DataFrame({
            "Metric": ["Batch Time (ms)", "Publish Latency (ms)"],
            "Min": [batch_series.min(), pub_series.min()],
            "P50": [batch_series.median(), pub_series.median()],
            "P95": [batch_series.quantile(0.95), pub_series.quantile(0.95)],
            "P99": [batch_series.quantile(0.99), pub_series.quantile(0.99)],
            "Max": [batch_series.max(), pub_series.max()],
            "Avg": [batch_series.mean(), pub_series.mean()],
        }).round(2)
        st.dataframe(summary, width="stretch", hide_index=True)
    else:
        st.info("No generator timing data yet. Start a data generation run.")

    st.markdown("---")

    # ── Spark Performance ─────────────────────────────────
    st.subheader("⚡ Spark Streaming Performance")

    # ── Cost & Runtime Summary ────────────────────────────
    #
    # Estimate compute cost based on cluster resources and
    # wall-clock time, using the per-core-hour rate in the config.
    COST_PER_CORE_HOUR = float(cfg.get("spark", {}).get("cost_per_core_hour", 0.752))
    WORKER_CORES = int(cfg.get("spark", {}).get("worker_cores", 4))

    first_ts = spark.get("first_update")
    last_ts = spark.get("last_update")
    wall_clock_s = (last_ts - first_ts) if (first_ts and last_ts and last_ts > first_ts) else 0.0
    wall_clock_hours = wall_clock_s / 3600.0
    total_compute_hours = wall_clock_hours * WORKER_CORES
    estimated_cost = total_compute_hours * COST_PER_CORE_HOUR

    # Total time actually spent processing (sum of batch durations)
    total_transform_ms = sum(spark["transform_times_ms"])
    total_write_ms = sum(spark["delta_write_times_ms"])
    total_processing_ms = sum(spark["batch_durations_ms"])

    st.markdown("##### 💰 Compute Cost & Runtime")
    cc1, cc2, cc3, cc4, cc5 = st.columns(5)
    cc1.metric("Wall-Clock Time", fmt_duration(wall_clock_s) if wall_clock_s > 0 else "—")
    cc2.metric("CPU Processing", fmt_duration(total_processing_ms / 1000))
    cc3.metric("Cluster Cores", f"{WORKER_CORES}")
    cc4.metric("Compute Hours", f"{total_compute_hours:.2f} hrs")
    cc5.metric("Est. Cost", f"${estimated_cost:.3f}",
               help=f"Based on {WORKER_CORES} cores × {wall_clock_hours:.2f} hrs × ${COST_PER_CORE_HOUR}/core-hr (spark.cost_per_core_hour in config)")

    if wall_clock_s > 0:
        idle_s = max(0, wall_clock_s - (total_processing_ms / 1000))
        util_pct = min(100.0, (total_processing_ms / 1000) / wall_clock_s * 100)
        uc1, uc2, uc3 = st.columns(3)
        uc1.metric("Cluster Utilization", f"{util_pct:.1f}%",
                   help="% of wall-clock time spent actively processing batches")
        uc2.metric("Idle Time", fmt_duration(idle_s))
        uc3.metric("Cost / 1M Events",
                   f"${(estimated_cost / max(1, spark['total_rows_processed']) * 1_000_000):.4f}"
                   if spark['total_rows_processed'] > 0 else "—")

    st.markdown("")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Micro-batches", f"{spark['micro_batches_completed']:,}")
    c2.metric("Avg Batch Duration", f"{spark['avg_batch_duration_ms']:.0f} ms")
    c3.metric("Avg Transform Time", f"{spark['avg_transform_time_ms']:.0f} ms")
    c4.metric("Avg Delta Write Time", f"{spark['avg_delta_write_time_ms']:.0f} ms")

    if spark["batch_durations_ms"]:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.caption("Micro-batch Duration (ms)")
            st.line_chart(
                pd.DataFrame({"duration_ms": spark["batch_durations_ms"]}),
                width="stretch",
            )
        with col2:
            st.caption("Transform Time (ms)")
            if spark["transform_times_ms"]:
                st.line_chart(
                    pd.DataFrame({"transform_ms": spark["transform_times_ms"]}),
                    width="stretch",
                )
        with col3:
            st.caption("Delta Write Time (ms)")
            if spark["delta_write_times_ms"]:
                st.line_chart(
                    pd.DataFrame({"write_ms": spark["delta_write_times_ms"]}),
                    width="stretch",
                )

        # Spark distribution summary
        st.caption("Spark Timing Distribution")
        batch_s = pd.Series(spark["batch_durations_ms"])
        transform_s = pd.Series(spark["transform_times_ms"]) if spark["transform_times_ms"] else pd.Series([0])
        write_s = pd.Series(spark["delta_write_times_ms"]) if spark["delta_write_times_ms"] else pd.Series([0])
        spark_summary = pd.DataFrame({
            "Metric": ["Batch Duration (ms)", "Transform Time (ms)", "Delta Write (ms)"],
            "Min": [batch_s.min(), transform_s.min(), write_s.min()],
            "P50": [batch_s.median(), transform_s.median(), write_s.median()],
            "P95": [batch_s.quantile(0.95), transform_s.quantile(0.95), write_s.quantile(0.95)],
            "P99": [batch_s.quantile(0.99), transform_s.quantile(0.99), write_s.quantile(0.99)],
            "Max": [batch_s.max(), transform_s.max(), write_s.max()],
            "Avg": [batch_s.mean(), transform_s.mean(), write_s.mean()],
        }).round(2)
        st.dataframe(spark_summary, width="stretch", hide_index=True)

        # ── Transform vs Write Breakdown ──────────────────
        st.markdown("")
        st.markdown("##### ⏱️ Time Breakdown: Transform vs Delta Write")

        n = min(len(spark["transform_times_ms"]), len(spark["delta_write_times_ms"]))
        if n > 0:
            breakdown_df = pd.DataFrame({
                "Transform (ms)": spark["transform_times_ms"][:n],
                "Delta Write (ms)": spark["delta_write_times_ms"][:n],
            })
            st.area_chart(breakdown_df, width="stretch")

            tc1, tc2, tc3 = st.columns(3)
            tc1.metric("Total Transform Time", fmt_duration(total_transform_ms / 1000))
            tc2.metric("Total Delta Write Time", fmt_duration(total_write_ms / 1000))
            pct_transform = total_transform_ms / max(1, total_processing_ms) * 100
            pct_write = total_write_ms / max(1, total_processing_ms) * 100
            tc3.metric("Transform / Write Ratio", f"{pct_transform:.0f}% / {pct_write:.0f}%")

        st.metric("Total Events Processed", f"{spark['total_rows_processed']:,}")
        st.metric("Last Batch Events", f"{spark['last_batch_rows']:,}")

        # ── Detailed Execution Metrics (for migration evaluation) ──
        st.markdown("---")
        st.markdown("##### 🔬 Execution Metrics (Migration Baseline)")

        batch_details = spark.get("batch_details", [])
        executor_data = spark.get("executor", {})

        if executor_data:
            st.markdown("###### Executor Summary (cumulative)")
            ex1, ex2, ex3, ex4 = st.columns(4)
            ex1.metric("Total Tasks", f"{executor_data.get('total_tasks', 0):,}")
            ex2.metric("Active Cores", f"{executor_data.get('active_cores', 0)}")
            ex3.metric("Peak JVM Heap", f"{executor_data.get('peak_memory_mb', 0):.0f} MB")
            ex4.metric("Max Memory", f"{executor_data.get('max_memory_mb', 0):.0f} MB")

            ex5, ex6, ex7, ex8 = st.columns(4)
            total_dur = executor_data.get("total_duration_ms", 0)
            ex5.metric("Total Task Time", fmt_duration(total_dur / 1000) if total_dur else "—")
            ex6.metric("Total GC Time", f"{executor_data.get('total_gc_ms', 0):,.0f} ms")
            ex7.metric("GC Overhead", f"{executor_data.get('gc_pct', 0):.1f}%")
            total_shuf = executor_data.get("total_shuffle_read_mb", 0) + executor_data.get("total_shuffle_write_mb", 0)
            ex8.metric("Total Shuffle", f"{total_shuf:.1f} MB")

            io1, io2, io3, io4 = st.columns(4)
            io1.metric("Total Input", f"{executor_data.get('total_input_mb', 0):.1f} MB")
            io2.metric("Total Output", f"{executor_data.get('total_output_mb', 0):.1f} MB")
            io3.metric("Shuffle Read", f"{executor_data.get('total_shuffle_read_mb', 0):.1f} MB")
            io4.metric("Shuffle Write", f"{executor_data.get('total_shuffle_write_mb', 0):.1f} MB")

        if batch_details:
            st.markdown("###### Per-Batch Breakdown")

            detail_df = pd.DataFrame(batch_details)
            rename_map = {
                "batch_id": "Batch",
                "rows": "Rows",
                "wall_ms": "Wall (ms)",
                "cpu_ms": "CPU (ms)",
                "gc_ms": "GC (ms)",
                "cpu_util_pct": "CPU %",
                "gc_pct": "GC %",
                "input_mb": "In (MB)",
                "output_mb": "Out (MB)",
                "shuffle_read_mb": "Shuf R (MB)",
                "shuffle_write_mb": "Shuf W (MB)",
                "peak_memory_mb": "Peak Mem (MB)",
                "tasks": "Tasks",
            }
            available = [c for c in rename_map if c in detail_df.columns]
            if available:
                st.dataframe(detail_df[available].rename(columns=rename_map), width="stretch", hide_index=True)

            # Charts for CPU, GC, Memory across batches
            ch1, ch2, ch3 = st.columns(3)
            with ch1:
                st.caption("CPU Time vs GC Time (ms)")
                if "cpu_ms" in detail_df.columns and "gc_ms" in detail_df.columns:
                    st.bar_chart(detail_df[["cpu_ms", "gc_ms"]].rename(
                        columns={"cpu_ms": "CPU", "gc_ms": "GC"}), width="stretch")
            with ch2:
                st.caption("CPU Utilization (%)")
                if "cpu_util_pct" in detail_df.columns:
                    st.line_chart(detail_df["cpu_util_pct"], width="stretch")
            with ch3:
                st.caption("Peak Memory (MB)")
                if "peak_memory_mb" in detail_df.columns:
                    st.area_chart(detail_df["peak_memory_mb"], width="stretch")

            # Shuffle charts
            if {"shuffle_read_mb", "shuffle_write_mb", "input_mb", "output_mb"} <= set(detail_df.columns):
                sc1, sc2 = st.columns(2)
                with sc1:
                    st.caption("Shuffle I/O (MB)")
                    st.bar_chart(detail_df[["shuffle_read_mb", "shuffle_write_mb"]].rename(
                        columns={"shuffle_read_mb": "Read", "shuffle_write_mb": "Write"}),
                        width="stretch")
                with sc2:
                    st.caption("I/O (MB)")
                    st.bar_chart(detail_df[["input_mb", "output_mb"]].rename(
                        columns={"input_mb": "Input", "output_mb": "Output"}),
                        width="stretch")

        elif not executor_data:
            st.info("Detailed metrics will appear when the streaming job runs. "
                    "The Spark REST API is queried after each micro-batch.")
    else:
        st.info("No Spark metrics yet. Run the streaming job to collect data.")

    # ── Delta Table Statistics (always visible) ─────────────
    st.markdown("---")
    st.subheader("📁 Delta Table Statistics")

    table_stats = {}
    for t in TABLES:
        for engine in ("spark", "rust"):
            info = get_delta_table_info(t[engine])
            if info and "error" not in info:
                table_stats[(t["name"], engine)] = info

    if table_stats:
        stream_tables = [t for t in TABLES if t["kind"] != "analytics"]
        cols = st.columns(len(stream_tables) * 2)
        for i, (t, engine) in enumerate((t, e) for t in stream_tables for e in ("spark", "rust")):
            info = table_stats.get((t["name"], engine))
            with cols[i]:
                icon = "🦀" if engine == "rust" else "⚡"
                st.markdown(f"**{icon} {t['label']} ({engine.title()})**")
                if not info:
                    st.caption("Not created yet")
                    continue
                st.metric("Rows", f"{info['row_count']:,}")
                st.metric("Files", f"{info['num_files']:,}")
                st.metric("Size", fmt_bytes(info["size_bytes"]))
                st.metric("Version", info["version"])
                if info.get("partition_columns"):
                    st.caption(f"Partitions: {', '.join(info['partition_columns'])}")

        # ── Schema Comparison ─────────────────────────────
        spark_tel = table_stats.get(("vehicle_telemetry", "spark"))
        rust_tel = table_stats.get(("vehicle_telemetry", "rust"))
        if spark_tel and rust_tel:
            st.markdown("##### 📋 Schema Comparison: Telemetry")
            spark_cols = {f.name: str(f.type) for f in spark_tel["schema"]}
            rust_cols = {f.name: str(f.type) for f in rust_tel["schema"]}
            schema_rows = []
            for col in sorted(set(spark_cols) | set(rust_cols)):
                s_type = spark_cols.get(col, "—")
                r_type = rust_cols.get(col, "—")
                match = "✅" if s_type == r_type else ("⚠️" if s_type != "—" and r_type != "—" else "❌")
                schema_rows.append({"Column": col, "Spark Type": s_type, "Rust Type": r_type, "Match": match})
            st.dataframe(pd.DataFrame(schema_rows), width="stretch", hide_index=True)

        # ── Row Count Parity ─────────────────────────────
        parity_rows = []
        for t in TABLES:
            si = table_stats.get((t["name"], "spark"))
            ri = table_stats.get((t["name"], "rust"))
            if si or ri:
                spark_rc = si["row_count"] if si else 0
                rust_rc = ri["row_count"] if ri else 0
                diff = abs(spark_rc - rust_rc)
                pct = (min(spark_rc, rust_rc) / max(spark_rc, rust_rc) * 100) if max(spark_rc, rust_rc) > 0 else 0
                status = "✅ Match" if diff == 0 else (f"⚠️ Δ{diff:,}" if pct > 95 else f"❌ Δ{diff:,}")
                parity_rows.append({
                    "Table": t["name"],
                    "Spark Rows": f"{spark_rc:,}" if si else "—",
                    "Rust Rows": f"{rust_rc:,}" if ri else "—",
                    "Parity": f"{pct:.1f}%",
                    "Status": status,
                })
        if parity_rows:
            st.markdown("##### 🔄 Row Count Parity")
            st.caption("Row counts only — value-level parity is on the 🦀 Migration page.")
            st.dataframe(pd.DataFrame(parity_rows), width="stretch", hide_index=True)
    else:
        st.info("No Delta tables found. Run the pipeline to generate data.")

    # ── Spark vs Rust Runtime Comparison ──────────────────
    st.markdown("---")
    st.subheader("🦀 Spark vs Rust — Batch Run Statistics (telemetry stream)")

    def _engine_stats(engine_data: dict) -> dict:
        """Build a comprehensive stats dict from engine metrics."""
        bd = engine_data.get("batch_details", [])
        executor = engine_data.get("executor", {})

        total_wall_ms = sum(d.get("wall_ms", 0) for d in bd) if bd else sum(engine_data["batch_durations_ms"])
        total_transform_ms = sum(d.get("transform_ms", d.get("cpu_ms", 0)) for d in bd) if bd else sum(engine_data["transform_times_ms"])
        total_write_ms = sum(d.get("write_ms", 0) for d in bd) if bd else sum(engine_data["delta_write_times_ms"])
        total_rows = engine_data.get("total_rows_processed", 0)
        n_batches = engine_data.get("micro_batches_completed", 0)

        # Kafka read time = wall time - transform - write (approximate)
        total_kafka_ms = max(0, total_wall_ms - total_transform_ms - total_write_ms)

        avg_cpu_util = 0.0
        avg_peak_mem = 0.0
        total_gc_ms = 0.0
        total_shuffle_read = 0.0
        total_shuffle_write = 0.0
        active_cores = executor.get("active_cores", 0)
        total_tasks = executor.get("total_tasks", 0)

        if bd:
            cpu_utils = [d.get("cpu_util_pct", 0) for d in bd if d.get("cpu_util_pct") is not None]
            avg_cpu_util = sum(cpu_utils) / len(cpu_utils) if cpu_utils else 0.0
            mems = [d.get("peak_memory_mb", 0) for d in bd if d.get("peak_memory_mb") is not None]
            avg_peak_mem = sum(mems) / len(mems) if mems else 0.0
            total_gc_ms = sum(d.get("gc_ms", 0) for d in bd)
            total_shuffle_read = sum(d.get("shuffle_read_mb", 0) for d in bd)
            total_shuffle_write = sum(d.get("shuffle_write_mb", 0) for d in bd)
            if not active_cores:
                tasks_list = [d.get("tasks", 0) for d in bd if d.get("tasks")]
                active_cores = max(tasks_list) if tasks_list else 0
            if not total_tasks:
                total_tasks = sum(d.get("tasks", 0) for d in bd)

        throughput = (total_rows / (total_wall_ms / 1000)) if total_wall_ms > 0 else 0

        return {
            "n_batches": n_batches,
            "total_rows": total_rows,
            "total_wall_ms": total_wall_ms,
            "total_transform_ms": total_transform_ms,
            "total_write_ms": total_write_ms,
            "total_kafka_ms": total_kafka_ms,
            "avg_batch_ms": (total_wall_ms / n_batches) if n_batches else 0,
            "avg_transform_ms": (total_transform_ms / n_batches) if n_batches else 0,
            "avg_write_ms": (total_write_ms / n_batches) if n_batches else 0,
            "avg_kafka_ms": (total_kafka_ms / n_batches) if n_batches else 0,
            "avg_cpu_util": avg_cpu_util,
            "avg_peak_mem_mb": avg_peak_mem,
            "total_gc_ms": total_gc_ms,
            "gc_pct": (total_gc_ms / total_wall_ms * 100) if total_wall_ms > 0 else 0,
            "total_shuffle_read_mb": total_shuffle_read,
            "total_shuffle_write_mb": total_shuffle_write,
            "active_cores": active_cores,
            "total_tasks": total_tasks,
            "throughput_rows_sec": throughput,
        }

    spark_s = _engine_stats(spark)
    rust_s = _engine_stats(rust)

    has_spark = spark_s["n_batches"] > 0
    has_rust = rust_s["n_batches"] > 0

    if has_spark or has_rust:

        # ── Speedup Banner ────────────────────────────────
        if has_spark and has_rust and spark_s["avg_batch_ms"] > 0 and rust_s["avg_batch_ms"] > 0:
            speedup = spark_s["avg_batch_ms"] / rust_s["avg_batch_ms"]
            delta_label = (f"{(speedup - 1) * 100:.0f}% faster"
                           if speedup > 1
                           else f"{(1 - speedup) * 100:.0f}% slower")
            st.metric("🏎️ Rust Batch Speedup", f"{speedup:.2f}x", delta=delta_label)

        # ── Comprehensive Statistics Table ────────────────
        st.markdown("##### 📊 Comprehensive Batch Run Statistics")

        def _fmt_ms(v):
            if v >= 60000:
                return f"{v / 60000:.1f} min"
            if v >= 1000:
                return f"{v / 1000:.1f} s"
            return f"{v:.1f} ms"

        def _fmt_val(v, fmt="ms"):
            if fmt == "ms":
                return _fmt_ms(v) if v else "—"
            if fmt == "int":
                return f"{int(v):,}" if v else "—"
            if fmt == "pct":
                return f"{v:.1f}%" if v else "—"
            if fmt == "mb":
                return f"{v:.1f} MB" if v else "—"
            if fmt == "rows_sec":
                return f"{v:,.0f}" if v else "—"
            return str(v) if v else "—"

        def _winner(s_val, r_val, metric_type="lower_better"):
            """Determine winner. lower_better for times, higher_better for throughput."""
            if not s_val or not r_val:
                return "—"
            if metric_type == "lower_better":
                return "🦀 Rust" if r_val < s_val else ("⚡ Spark" if s_val < r_val else "Tie")
            return "🦀 Rust" if r_val > s_val else ("⚡ Spark" if s_val > r_val else "Tie")

        def _row(category, metric, key, fmt="ms", better="lower_better"):
            return {"Category": category, "Metric": metric,
                    "Spark (⚡)": _fmt_val(spark_s[key], fmt),
                    "Rust (🦀)": _fmt_val(rust_s[key], fmt),
                    "Winner": _winner(spark_s[key], rust_s[key], better) if better else "—"}

        stats_rows = [
            _row("📋 Batch Overview", "Total Batches Completed", "n_batches", "int", None),
            _row("", "Total Events Processed", "total_rows", "int", None),
            _row("", "Throughput (events/sec)", "throughput_rows_sec", "rows_sec", "higher_better"),
            _row("⏱️ End-to-End Timing", "Total Batch Wall-Clock Time", "total_wall_ms"),
            _row("", "Avg Batch Duration", "avg_batch_ms"),
            _row("📨 Kafka Read", "Total Kafka Read Time", "total_kafka_ms"),
            _row("", "Avg Kafka Read / Batch", "avg_kafka_ms"),
            _row("🔄 Transform (CPU)", "Total Transform Time", "total_transform_ms"),
            _row("", "Avg Transform / Batch", "avg_transform_ms"),
            _row("💾 Delta Lake Write", "Total Delta Write Time", "total_write_ms"),
            _row("", "Avg Delta Write / Batch", "avg_write_ms"),
            _row("🧠 Memory Footprint", "Avg Peak Memory / Batch", "avg_peak_mem_mb", "mb"),
            _row("⚙️ CPU Utilization", "Avg CPU Utilization", "avg_cpu_util", "pct", "higher_better"),
            _row("", "Active Cores / Threads", "active_cores", "int", None),
            _row("🧵 Thread Pool", "Total Tasks Executed", "total_tasks", "int", None),
            {"Category": "🗑️ GC / Pause Time", "Metric": "Total GC Time",
             "Spark (⚡)": _fmt_val(spark_s["total_gc_ms"]),
             "Rust (🦀)": "0 ms (no GC)",
             "Winner": "🦀 Rust" if spark_s["total_gc_ms"] > 0 else "Tie"},
            {"Category": "🔀 Shuffle (Spark)", "Metric": "Total Shuffle Read + Write",
             "Spark (⚡)": _fmt_val(spark_s["total_shuffle_read_mb"] + spark_s["total_shuffle_write_mb"], "mb"),
             "Rust (🦀)": "N/A (single process)",
             "Winner": "—"},
        ]

        st.dataframe(
            pd.DataFrame(stats_rows),
            width="stretch",
            hide_index=True,
            column_config={
                "Category": st.column_config.TextColumn(width="medium"),
                "Metric": st.column_config.TextColumn(width="large"),
                "Spark (⚡)": st.column_config.TextColumn(width="medium"),
                "Rust (🦀)": st.column_config.TextColumn(width="medium"),
                "Winner": st.column_config.TextColumn(width="small"),
            },
        )

        # ── Batch Duration Overlay Chart ──────────────────
        rust_bd = rust["batch_durations_ms"]
        spark_bd = spark["batch_durations_ms"]
        if rust_bd or spark_bd:
            st.markdown("##### Batch Duration Comparison (ms)")
            max_len = max(len(spark_bd), len(rust_bd))
            chart_data = {}
            if spark_bd:
                chart_data["Spark"] = spark_bd + [None] * (max_len - len(spark_bd))
            if rust_bd:
                chart_data["Rust"] = rust_bd + [None] * (max_len - len(rust_bd))
            st.line_chart(pd.DataFrame(chart_data), width="stretch")

        # ── Time breakdown stacked chart ──────────────────
        if has_spark and has_rust:
            st.markdown("##### ⏱️ Time Breakdown: Where Time Is Spent")
            breakdown_data = {
                "Stage": ["Kafka Read", "Transform (CPU)", "Delta Write", "GC Overhead"],
                "Spark (ms)": [spark_s["total_kafka_ms"], spark_s["total_transform_ms"],
                               spark_s["total_write_ms"], spark_s["total_gc_ms"]],
                "Rust (ms)": [rust_s["total_kafka_ms"], rust_s["total_transform_ms"],
                              rust_s["total_write_ms"], 0],
            }
            st.bar_chart(pd.DataFrame(breakdown_data).set_index("Stage"), width="stretch")

        # ── Per-Batch Detail Tables ───────────────────────
        with st.expander("📋 Spark Per-Batch Details"):
            if spark["batch_details"]:
                detail_df = pd.DataFrame(spark["batch_details"])
                display_cols = [
                    "batch_id", "rows", "wall_ms", "transform_ms", "write_ms",
                    "cpu_ms", "gc_ms", "cpu_util_pct", "gc_pct",
                    "shuffle_read_mb", "shuffle_write_mb", "peak_memory_mb", "tasks",
                ]
                available = [c for c in display_cols if c in detail_df.columns]
                st.dataframe(detail_df[available], width="stretch", hide_index=True)
            else:
                st.info("No Spark batch details available.")

        with st.expander("📋 Rust Per-Batch Details"):
            if rust["batch_details"]:
                rdf = pd.DataFrame(rust["batch_details"])
                r_display = [c for c in ["batch_id", "rows", "wall_ms", "transform_ms", "write_ms",
                                         "cpu_ms", "peak_memory_mb", "tasks"] if c in rdf.columns]
                st.dataframe(rdf[r_display], width="stretch", hide_index=True)
            else:
                st.info("No Rust batch details available.")
    else:
        st.info("No runtime metrics yet. Start **Rust Streaming** or **Spark Streaming** "
                "from the ⚙️ Pipeline Jobs page to collect performance data.")

    st.markdown("---")

    # ── Stage breakdown ───────────────────────────────────
    st.subheader("🔗 Average Time per Stage")
    st.caption("Generator batch averages and Spark micro-batch averages; not a per-event latency.")
    waterfall_data = {
        "Stage": ["Generate + Serialize", "Kafka Publish (flush)", "Spark Transform", "Delta Write"],
        "Avg (ms)": [
            max(0, gen["avg_batch_time_ms"] - gen["avg_publish_latency_ms"]),
            gen["avg_publish_latency_ms"],
            spark["avg_transform_time_ms"],
            spark["avg_delta_write_time_ms"],
        ],
    }
    wf_df = pd.DataFrame(waterfall_data)
    st.bar_chart(wf_df.set_index("Stage"), width="stretch")
    st.dataframe(wf_df, width="stretch", hide_index=True)

    # ── Raw Metrics JSON ──────────────────────────────────
    st.markdown("---")
    with st.expander("📋 Raw Metrics JSON"):
        st.json(metrics)

# ── Auto-refresh ──────────────────────────────────────────────
# At the end so the page renders before the rerun

if auto_refresh:
    time.sleep(3)
    st.rerun()
