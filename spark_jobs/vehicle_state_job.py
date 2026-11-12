"""
Vehicle State Streaming Job: Kafka → Delta Lake MERGE.

Reads latest-state snapshots (keyed by VIN) from Kafka and upserts them into
a one-row-per-vehicle Delta table — the table service, warranty and OTA
systems read to answer "what is this car's current state?".

Ordering and idempotency
────────────────────────
Each snapshot carries the vehicle's sequence_number. The MERGE only applies
a snapshot when its sequence_number is greater than the stored one, so:
  • out-of-order arrivals never overwrite newer state, and
  • replaying a batch after a failure is a no-op.
Combined with the streaming checkpoint this gives effectively-once updates.
Known limit: if one micro-batch mixes already-applied and new snapshots for
the same VIN, event_count counts the whole batch. The checkpoint prevents
that case in normal operation.

Run inside Docker:
    spark-submit --jars <kafka + delta jars> /opt/spark-jobs/vehicle_state_job.py \\
        --config /opt/config/app_config.yaml

Run locally:
    python spark_jobs/vehicle_state_job.py --local
"""

import argparse
import logging
import time as _time

import yaml
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("vehicle_state")

try:
    from metrics_bridge import update_spark_metrics
    _HAS_METRICS = True
except ImportError as exc:
    _HAS_METRICS = False
    logger.warning("Dashboard metrics unavailable: %s", exc)

VEHICLE_STATE_SCHEMA = StructType([
    StructField("vin", StringType(), False),
    StructField("model", StringType(), True),
    StructField("model_variant", StringType(), True),
    StructField("battery_pack_type", StringType(), True),
    StructField("software_version", StringType(), True),
    StructField("power_state", StringType(), True),
    StructField("odometer_km", DoubleType(), True),
    StructField("charge_percent", DoubleType(), True),
    StructField("battery_health_level", DoubleType(), True),
    StructField("last_event_type", StringType(), True),
    StructField("sequence_number", LongType(), False),
    StructField("event_count", IntegerType(), True),
    StructField("updated_at", StringType(), True),
    StructField("first_seen_at", StringType(), True),
])

# Fields replaced wholesale by the newest snapshot.
LATEST_STATE_FIELDS = [
    "model", "model_variant", "battery_pack_type", "software_version",
    "power_state", "odometer_km", "charge_percent", "battery_health_level",
    "last_event_type", "sequence_number", "updated_at",
]


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def create_spark_session(cfg: dict, local: bool) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(cfg["spark"]["app_name"] + "_VehicleState")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        .config("spark.sql.session.timeZone", "UTC")
    )
    builder = builder.master("local[*]" if local else cfg["spark"]["master"])
    return builder.getOrCreate()


def collapse_batch_per_vehicle(batch_df: DataFrame) -> DataFrame:
    """One row per VIN: the newest snapshot, with this batch's event count."""
    newest_first = Window.partitionBy("vin").orderBy(F.col("sequence_number").desc())
    latest = (
        batch_df
        .withColumn("rn", F.row_number().over(newest_first))
        .filter(F.col("rn") == 1)
        .drop("rn", "event_count", "first_seen_at")
    )
    batch_totals = batch_df.groupBy("vin").agg(
        F.sum("event_count").cast("int").alias("event_count"),
        F.min("first_seen_at").alias("first_seen_at"),
    )
    return latest.join(batch_totals, on="vin", how="inner")


def merge_into_vehicle_state(source: DataFrame, delta_path: str,
                             spark: SparkSession) -> None:
    """MERGE one collapsed batch into the vehicle_state table."""
    if not DeltaTable.isDeltaTable(spark, delta_path):
        logger.info(f"Creating vehicle_state table at {delta_path}")
        source.write.format("delta").mode("overwrite").partitionBy("model").save(delta_path)
        return

    updates = {name: f"source.{name}" for name in LATEST_STATE_FIELDS}
    updates["event_count"] = (
        F.coalesce(F.col("target.event_count"), F.lit(0)) + F.col("source.event_count")
    )
    updates["first_seen_at"] = F.least("target.first_seen_at", "source.first_seen_at")

    (
        DeltaTable.forPath(spark, delta_path).alias("target")
        .merge(source.alias("source"), "target.vin = source.vin")
        .whenMatchedUpdate(
            condition="source.sequence_number > target.sequence_number",
            set=updates,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_vehicle_state(cfg: dict, local: bool):
    """Kafka → foreachBatch → Delta MERGE."""
    spark = create_spark_session(cfg, local)
    spark.sparkContext.setLogLevel("WARN")

    kafka_cfg = cfg["kafka"]
    state_cfg = cfg["vehicle_state"]
    bootstrap = kafka_cfg["bootstrap_servers"] if local else kafka_cfg["internal_bootstrap_servers"]
    checkpoint = state_cfg["local_checkpoint"] if local else state_cfg["checkpoint_location"]
    delta_path = state_cfg["local_delta_output"] if local else state_cfg["delta_output_path"]

    logger.info(f"Kafka bootstrap: {bootstrap} | topic: {kafka_cfg['state_topic']}")
    logger.info(f"Delta output:    {delta_path} | checkpoint: {checkpoint}")

    snapshots = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", kafka_cfg["state_topic"])
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200_000)
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), VEHICLE_STATE_SCHEMA).alias("data"))
        .select("data.*")
        .filter(F.col("vin").isNotNull())
    )

    def _process_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: empty, skipping")
            return
        t0 = _time.perf_counter()
        row_count = batch_df.count()
        source = collapse_batch_per_vehicle(batch_df)
        vehicles = source.count()
        merge_into_vehicle_state(source, delta_path, spark)
        t_batch = (_time.perf_counter() - t0) * 1000
        logger.info(
            f"Batch {batch_id}: {row_count} snapshots → {vehicles} vehicles | "
            f"merge {t_batch:.0f} ms"
        )
        if _HAS_METRICS:
            try:
                update_spark_metrics(
                    status="running",
                    batch_duration_ms=t_batch,
                    delta_write_time_ms=t_batch,
                    rows_processed=row_count,
                )
            except Exception as exc:
                logger.warning(f"Metrics update failed: {exc}")

    query = (
        snapshots.writeStream
        .foreachBatch(_process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=cfg["spark"]["trigger_interval"])
        .start()
    )
    logger.info("Vehicle state stream started — waiting for termination...")
    query.awaitTermination()


def main():
    parser = argparse.ArgumentParser(description="Kafka → Delta vehicle_state MERGE job")
    parser.add_argument("--config", default="config/app_config.yaml")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()
    run_vehicle_state(load_config(args.config), args.local)


if __name__ == "__main__":
    main()
