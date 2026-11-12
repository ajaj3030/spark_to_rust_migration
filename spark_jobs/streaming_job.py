"""
Spark Structured Streaming job: Kafka → Delta Lake.

Reads vehicle telemetry JSON from Kafka (keyed by VIN), applies the
enrichment chain in transforms.py (battery metrics, health/fault flags,
GPS coarsening, VIN pseudonymisation) and appends to a Delta table.

This is the job that has already been ported to Rust (`rust-pipeline
stream`); `rust-pipeline validate` checks the two outputs match.

──────────────────────────────────────────────────────────────────────
SPARK CONCEPTS EXERCISED IN THIS PIPELINE
──────────────────────────────────────────────────────────────────────
  1. Structured Streaming     — readStream / writeStream with Kafka
  2. from_json + schema       — parse raw bytes into typed columns
  3. withColumn               — add/replace columns declaratively
  4. when / otherwise         — SQL CASE-WHEN as column expressions
  5. UDFs (Python)            — classify_health
  6. Built-in functions       — year, month, dayofweek, sha2, round,
                                coalesce, split, isin
  7. Null handling            — coalesce, na.fill, isNull checks
  8. .transform()             — composable DataFrame → DataFrame chains
  9. Delta Lake sink          — append with merge-schema + partitioning
 10. Checkpointing            — exactly-once delivery guarantee
──────────────────────────────────────────────────────────────────────

Run inside Docker:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \\
        /opt/spark-jobs/streaming_job.py

Run locally:
    python spark_jobs/streaming_job.py --local
"""

import argparse
import logging
import sys
import time as _time

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Import the reusable transforms module
from transforms import apply_all_transforms

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("streaming_job")

# ── Metrics integration (graceful if unavailable) ─────────────
try:
    from metrics_bridge import update_spark_metrics
    _HAS_METRICS = True
    logger.info("Dashboard metrics integration enabled (JSON bridge)")
except Exception as exc:
    _HAS_METRICS = False
    logger.warning("Dashboard metrics unavailable: %s", exc)

try:
    from spark_metrics_collector import get_app_id, collect_batch_metrics
    _HAS_COLLECTOR = True
    logger.info("Spark REST API metrics collector enabled")
except Exception as exc:
    _HAS_COLLECTOR = False
    logger.warning("Spark metrics collector unavailable: %s", exc)

# Track the last known stage ID for per-batch delta calculations
_last_stage_id = -1
_spark_app_id = None

# ── Telemetry JSON schema ─────────────────────────────────────

TELEMETRY_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("vin", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("sequence_number", LongType(), False),
    StructField("model", StringType(), True),
    StructField("model_variant", StringType(), True),
    StructField("software_version", StringType(), True),
    StructField("power_state", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("odometer_km", DoubleType(), True),
    StructField("charge_percent", DoubleType(), True),
    StructField("remaining_range", DoubleType(), True),
    StructField("kwhr", DoubleType(), True),
    StructField("capacity_kwhr", DoubleType(), True),
    StructField("battery_health_level", DoubleType(), True),
    StructField("max_cell_temp", DoubleType(), True),
    StructField("min_cell_temp", DoubleType(), True),
    StructField("preconditioning_status", StringType(), True),
    StructField("battery_pack_type", StringType(), True),
    StructField("charge_state", StringType(), True),
    StructField("charge_session_id", StringType(), True),
    StructField("charging_power_kw", DoubleType(), True),
    StructField("energy_added_kwh", DoubleType(), True),
    StructField("ecu", StringType(), True),
    StructField("dtc_code", StringType(), True),
    StructField("warning_state", StringType(), True),
    StructField("update_state", StringType(), True),
    StructField("target_software_version", StringType(), True),
    StructField("update_progress_percent", IntegerType(), True),
])


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def create_spark_session(cfg: dict, local: bool) -> SparkSession:
    """Build a SparkSession with Delta Lake support."""
    builder = (
        SparkSession.builder
        .appName(cfg["spark"]["app_name"])
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        .config("spark.sql.streaming.schemaInference", "false")
        # event_time is UTC; derive calendar parts in UTC regardless of host TZ
        .config("spark.sql.session.timeZone", "UTC")
    )

    if local:
        builder = builder.master("local[*]")
    else:
        builder = builder.master(cfg["spark"]["master"])

    return builder.getOrCreate()


def run_streaming_job(cfg: dict, local: bool):
    """Main streaming pipeline: Kafka source → transform → Delta sink.

    The transformation chain uses .transform() to compose reusable
    DataFrame→DataFrame functions from the transforms module:

        raw → parse → null_handling → time_dims → battery_features
            → health_features → location (coarsen GPS) → anonymize VIN
            → event_features → drop kafka_key (VIN) → write to Delta
    """
    spark = create_spark_session(cfg, local)
    spark.sparkContext.setLogLevel("WARN")

    kafka_cfg = cfg["kafka"]
    spark_cfg = cfg["spark"]

    bootstrap = (
        kafka_cfg["bootstrap_servers"] if local
        else kafka_cfg["internal_bootstrap_servers"]
    )
    checkpoint = (
        spark_cfg["local_checkpoint"] if local
        else spark_cfg["checkpoint_location"]
    )
    delta_path = (
        spark_cfg["local_delta_output"] if local
        else spark_cfg["delta_output_path"]
    )

    logger.info(f"Kafka bootstrap: {bootstrap}")
    logger.info(f"Delta output:    {delta_path}")
    logger.info(f"Checkpoint:      {checkpoint}")

    # ── 1. Read from Kafka (Structured Streaming source) ──────
    #
    # Spark concept: readStream creates an unbounded DataFrame.
    # Each micro-batch pulls up to maxOffsetsPerTrigger records.
    # Columns: key (binary), value (binary), topic, partition,
    #          offset, timestamp, timestampType
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", kafka_cfg["topic"])
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", spark_cfg["max_offsets_per_trigger"])
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. Parse JSON payload (from_json + schema) ────────────
    #
    # Spark concept: from_json() deserializes a JSON string column
    # into a nested StructType.  We then flatten with "data.*"
    # to promote nested fields to top-level columns.
    parsed_df = (
        raw_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), TELEMETRY_SCHEMA).alias("data"),
            col("topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "kafka_key", "data.*",
            "topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        )
    )

    # ── 3 + 4. foreachBatch with instrumentation ────────────────
    #
    # We use foreachBatch so we can measure the time spent on:
    #   (a) apply_all_transforms — the enrichment / UDF phase
    #   (b) the Delta write itself
    # and report both to the metrics module for the dashboard.

    def _process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            logger.info(f"Batch {batch_id}: empty, skipping")
            return

        # ── Transform phase ───────────────────────────────────
        t0 = _time.perf_counter()
        # kafka_key is the VIN — drop it so the table holds no raw identifiers
        enriched = batch_df.transform(apply_all_transforms).drop("kafka_key")
        # Force materialisation so transform time is real
        row_count = enriched.count()
        t_transform = (_time.perf_counter() - t0) * 1000  # ms

        # ── Delta write phase ─────────────────────────────────
        t1 = _time.perf_counter()
        (
            enriched.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("event_year", "event_month", "event_type")
            .save(delta_path)
        )
        t_write = (_time.perf_counter() - t1) * 1000  # ms
        t_batch = t_transform + t_write

        logger.info(
            f"Batch {batch_id}: {row_count} rows | "
            f"transform {t_transform:.0f} ms | "
            f"delta_write {t_write:.0f} ms | "
            f"total {t_batch:.0f} ms"
        )

        if _HAS_METRICS:
            try:
                batch_detail = None
                executor_snapshot = None

                # Collect detailed metrics from Spark REST API
                if _HAS_COLLECTOR:
                    global _spark_app_id, _last_stage_id
                    if _spark_app_id is None:
                        _spark_app_id = get_app_id()
                    if _spark_app_id:
                        api_data = collect_batch_metrics(_spark_app_id, _last_stage_id)
                        if "stage" in api_data:
                            stage = api_data["stage"]
                            batch_detail = {
                                "batch_id": batch_id,
                                "rows": row_count,
                                "wall_ms": round(t_batch, 1),
                                "transform_ms": round(t_transform, 1),
                                "write_ms": round(t_write, 1),
                                "cpu_ms": stage.get("cpu_ms", 0),
                                "gc_ms": stage.get("gc_ms", 0),
                                "cpu_util_pct": stage.get("cpu_util_pct", 0),
                                "gc_pct": stage.get("gc_pct", 0),
                                "input_mb": stage.get("input_mb", 0),
                                "output_mb": stage.get("output_mb", 0),
                                "shuffle_read_mb": stage.get("shuffle_read_mb", 0),
                                "shuffle_write_mb": stage.get("shuffle_write_mb", 0),
                                "shuffle_read_records": stage.get("shuffle_read_records", 0),
                                "shuffle_write_records": stage.get("shuffle_write_records", 0),
                                "peak_memory_mb": stage.get("peak_execution_memory_mb", 0),
                                "tasks": stage.get("tasks", 0),
                            }
                            _last_stage_id = stage.get("last_stage_id", _last_stage_id)
                        if "executor" in api_data:
                            executor_snapshot = api_data["executor"]

                update_spark_metrics(
                    status="running",
                    batch_duration_ms=t_batch,
                    transform_time_ms=t_transform,
                    delta_write_time_ms=t_write,
                    rows_processed=row_count,
                    batch_detail=batch_detail,
                    executor_metrics=executor_snapshot,
                )
            except Exception as exc:
                logger.warning(f"Metrics update failed: {exc}")

    query = (
        parsed_df.writeStream
        .foreachBatch(_process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=spark_cfg["trigger_interval"])
        .start()
    )

    logger.info("Streaming query started — waiting for termination...")
    query.awaitTermination()


def main():
    parser = argparse.ArgumentParser(description="Kafka → Delta Lake streaming job")
    parser.add_argument(
        "--config",
        default="config/app_config.yaml",
        help="Path to YAML config file",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run in local mode (no Spark cluster required)",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_streaming_job(cfg, args.local)


if __name__ == "__main__":
    main()
