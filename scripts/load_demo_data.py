"""
Load demo data for the dashboard without running Kafka or Spark.

Builds every table the dashboard reads from the committed parity fixtures
(rust_pipeline/tests/fixtures), at the paths in config/app_config.yaml and
partitioned like the real pipeline:

    vehicle_telemetry, vehicle_telemetry_rust   ← fixtures/input/telemetry
    vehicle_state,     vehicle_state_rust       ← fixtures/input/vehicle_state
    analytics/<job>                             ← fixtures/expected/<job> (Spark's output)

The streaming tables are identical on both sides (those jobs are already
ported). Rust analytics output is left for the ports to produce: on the
dashboard's Migration page, "Run Rust & validate" runs a ported job on this
data and compares it with Spark's, the same check CI runs.

Replaces those tables and clears earlier validation reports.

    .venv/bin/python scripts/load_demo_data.py
"""

import shutil
from pathlib import Path

import pyarrow.parquet as pq
import yaml
from deltalake import write_deltalake

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "rust_pipeline" / "tests" / "fixtures"

# Partitioning used by spark_jobs/* and rust_pipeline (Job::partition_cols)
TELEMETRY_PARTITIONS = ["event_year", "event_month", "event_type"]
STATE_PARTITIONS = ["model"]
ANALYTICS_PARTITIONS = {
    "battery_health_daily": ["event_date"],
    "charging_sessions": ["session_date"],
    "ota_rollout": [],
    "diagnostic_alerts": [],
}


def write_table(source: Path, target: Path, partition_by: list[str]) -> int:
    table = pq.read_table(source)
    if target.exists():
        shutil.rmtree(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(target), table, partition_by=partition_by or None, mode="overwrite")
    print(f"  {target.relative_to(ROOT)}: {table.num_rows:,} rows")
    return table.num_rows


def main() -> None:
    cfg = yaml.safe_load((ROOT / "config" / "app_config.yaml").read_text())
    path = lambda p: ROOT / p
    analytics_base = path(cfg["analytics"]["local_output_base"])

    print("Streaming tables (Spark and Rust):")
    for target in (cfg["spark"]["local_delta_output"], cfg["rust"]["local_delta_output"]):
        write_table(FIXTURES / "input" / "telemetry", path(target), TELEMETRY_PARTITIONS)
    for target in (cfg["vehicle_state"]["local_delta_output"], cfg["rust"]["local_state_delta_output"]):
        write_table(FIXTURES / "input" / "vehicle_state", path(target), STATE_PARTITIONS)

    print("Spark analytics output:")
    for job, partitions in ANALYTICS_PARTITIONS.items():
        write_table(FIXTURES / "expected" / job, analytics_base / job, partitions)
        stale_rust_output = analytics_base / f"{job}_rust"
        if stale_rust_output.exists():
            shutil.rmtree(stale_rust_output)

    for report in (ROOT / "logs").glob("validation_report_*.json"):
        report.unlink()
    print("Cleared Rust analytics output and validation reports. Start the dashboard:")
    print("  .venv/bin/streamlit run dashboard/app.py")


if __name__ == "__main__":
    main()
