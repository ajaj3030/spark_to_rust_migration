"""
Shared plumbing for the batch analytics jobs.

Each job in this package is independent: it reads the enriched telemetry
table (and, for diagnostic_alerts, the vehicle_state table), computes one
summary, and overwrites one Delta table under analytics.output_base. The
job logic lives in a pure `summarize(...)` function so it can be ported and
parity-checked on its own.
"""

import argparse
import logging
from typing import Callable, List, Optional

import yaml
from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def create_spark_session(cfg: dict, job_name: str, local: bool) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(f"{cfg['spark']['app_name']}_{job_name}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.master("local[*]" if local else cfg["spark"]["master"]).getOrCreate()


def telemetry_path(cfg: dict, local: bool) -> str:
    return cfg["spark"]["local_delta_output"] if local else cfg["spark"]["delta_output_path"]


def vehicle_state_path(cfg: dict, local: bool) -> str:
    state = cfg["vehicle_state"]
    return state["local_delta_output"] if local else state["delta_output_path"]


def output_path(cfg: dict, job_name: str, local: bool) -> str:
    base = cfg["analytics"]["local_output_base"] if local else cfg["analytics"]["output_base"]
    return f"{base.rstrip('/')}/{job_name}"


def write_table(df: DataFrame, path: str, partition_cols: Optional[List[str]] = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)


def run_job(job_name: str,
            build: Callable[[SparkSession, dict, bool], DataFrame],
            partition_cols: Optional[List[str]] = None) -> None:
    """CLI entry point shared by every analytics job."""
    parser = argparse.ArgumentParser(description=f"Batch analytics: {job_name}")
    parser.add_argument("--config", default="config/app_config.yaml")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()

    logger = logging.getLogger(job_name)
    cfg = load_config(args.config)
    spark = create_spark_session(cfg, job_name, args.local)
    spark.sparkContext.setLogLevel("WARN")

    result = build(spark, cfg, args.local)
    path = output_path(cfg, job_name, args.local)
    write_table(result, path, partition_cols)
    logger.info(f"Wrote {spark.read.format('delta').load(path).count():,} rows → {path}")
    spark.stop()
