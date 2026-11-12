"""
Diagnostic alerts — fleet impact of each trouble code, for service triage.

Inputs: enriched telemetry (DIAGNOSTIC events) and the vehicle_state table
Output: analytics/diagnostic_alerts, key (dtc_code, ecu, model_safe,
        battery_pack_type)

Behaviour a port must preserve:
  • privacy: vehicle_state is keyed by raw VIN, telemetry by vin_hash. The
    join hashes the VIN on the state side (SHA-256, same as transforms.py);
    the raw VIN must never appear in the output
  • vehicles_affected is an exact distinct count, not approximate
  • fleet_pct_affected divides by the whole fleet (vehicle_state rows), 3 dp
  • rank_in_ecu is a dense rank by vehicles_affected within each ECU, so
    ties share a rank and no rank is skipped
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import run_job, telemetry_path, vehicle_state_path

JOB_NAME = "diagnostic_alerts"


def summarize(telemetry: DataFrame, vehicle_state: DataFrame) -> DataFrame:
    fleet = vehicle_state.select(
        F.sha2(F.col("vin"), 256).alias("vin_hash"),
        "battery_pack_type",
    )
    fleet_size = F.lit(vehicle_state.count())

    alerts = (
        telemetry
        .filter(F.col("event_type") == "DIAGNOSTIC")
        # Diagnostic events carry no pack type; take it from the vehicle record
        .drop("battery_pack_type")
        .join(F.broadcast(fleet), on="vin_hash", how="left")
        .groupBy("dtc_code", "ecu", "model_safe", "battery_pack_type")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("vin_hash").alias("vehicles_affected"),
            F.sum(F.when(F.col("warning_state") == "WARNING_ON", 1).otherwise(0))
            .alias("warning_on_events"),
            F.min(F.to_timestamp("event_time")).alias("first_seen"),
            F.max(F.to_timestamp("event_time")).alias("last_seen"),
        )
    )
    by_impact = Window.partitionBy("ecu").orderBy(F.col("vehicles_affected").desc())
    return (
        alerts
        .withColumn("fleet_pct_affected",
                    F.round(F.col("vehicles_affected") / fleet_size * 100, 3))
        .withColumn("rank_in_ecu", F.dense_rank().over(by_impact))
    )


def build(spark: SparkSession, cfg: dict, local: bool) -> DataFrame:
    telemetry = spark.read.format("delta").load(telemetry_path(cfg, local))
    vehicle_state = spark.read.format("delta").load(vehicle_state_path(cfg, local))
    return summarize(telemetry, vehicle_state)


if __name__ == "__main__":
    run_job(JOB_NAME, build)
