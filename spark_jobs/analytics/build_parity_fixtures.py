"""
Build the parity fixtures the Rust analytics ports are tested against.

Takes a small, fixed slice of the Spark-written telemetry and vehicle_state
tables (the first N vehicles by vin_hash), saves it as the test input, then
runs every analytics job's Spark `summarize` on that input and saves the
result as the expected output:

    rust_pipeline/tests/fixtures/input/telemetry/
    rust_pipeline/tests/fixtures/input/vehicle_state/
    rust_pipeline/tests/fixtures/expected/<job>/

Run inside the Spark container after the streaming jobs have written data:

    docker exec -w /opt spark-master /opt/spark/bin/spark-submit --jars $JARS \\
        /opt/spark-jobs/analytics/build_parity_fixtures.py --local \\
        --config /opt/config/app_config.yaml
"""

import argparse
import glob
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import battery_health_daily
import charging_sessions
import diagnostic_alerts
import ota_rollout
from common import create_spark_session, load_config, telemetry_path, vehicle_state_path

logger = logging.getLogger("build_parity_fixtures")

JOBS = {
    "battery_health_daily": lambda t, s: battery_health_daily.summarize(t),
    "charging_sessions": lambda t, s: charging_sessions.summarize(t),
    "ota_rollout": lambda t, s: ota_rollout.summarize(t),
    "diagnostic_alerts": lambda t, s: diagnostic_alerts.summarize(t, s),
}


def write_fixture(df: DataFrame, path: str) -> None:
    """One Parquet file per fixture, without Hadoop's _SUCCESS/.crc clutter."""
    df.coalesce(1).write.mode("overwrite").parquet(path)
    for junk in glob.glob(f"{path}/_SUCCESS") + glob.glob(f"{path}/.*.crc"):
        os.remove(junk)


def build(spark: SparkSession, cfg: dict, local: bool, out_dir: str, vehicles: int) -> None:
    telemetry = spark.read.format("delta").load(telemetry_path(cfg, local))
    state = spark.read.format("delta").load(vehicle_state_path(cfg, local))

    sample = telemetry.select("vin_hash").distinct().orderBy("vin_hash").limit(vehicles)
    write_fixture(telemetry.join(sample, "vin_hash"), f"{out_dir}/input/telemetry")
    sampled_state = state.withColumn("vin_hash", F.sha2("vin", 256)).join(sample, "vin_hash").drop("vin_hash")
    write_fixture(sampled_state, f"{out_dir}/input/vehicle_state")

    # Read the fixtures back so expected outputs come from exactly what Rust will read
    t_in = spark.read.parquet(f"{out_dir}/input/telemetry")
    s_in = spark.read.parquet(f"{out_dir}/input/vehicle_state")
    logger.info(f"Input: {t_in.count():,} telemetry rows, {s_in.count():,} vehicles")
    for name, summarize in JOBS.items():
        result = summarize(t_in, s_in)
        write_fixture(result, f"{out_dir}/expected/{name}")
        logger.info(f"Expected {name}: {result.count():,} rows")


def main():
    parser = argparse.ArgumentParser(description="Build Rust parity fixtures from Spark output")
    parser.add_argument("--config", default="config/app_config.yaml")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--out", default="./rust_pipeline/tests/fixtures")
    parser.add_argument("--vehicles", type=int, default=150)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    cfg = load_config(args.config)
    spark = create_spark_session(cfg, "ParityFixtures", args.local)
    spark.sparkContext.setLogLevel("WARN")
    # Plain microsecond timestamps instead of legacy INT96, for portable Parquet
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    build(spark, cfg, args.local, args.out, args.vehicles)
    spark.stop()


if __name__ == "__main__":
    main()
