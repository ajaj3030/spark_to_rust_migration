"""
Charging sessions — one row per charge_session_id.

Input:  enriched telemetry, CHARGING events with a session id
Output: analytics/charging_sessions, key charge_session_id

Behaviour a port must preserve:
  • soc_start / soc_end / end_state come from the lowest / highest
    sequence_number in the session, not min/max of the values
  • session_start / session_end are compared as timestamps; duration is
    in minutes, rounded to 2 dp
  • energy_added_kwh is a floating-point SUM: summation order changes the
    last bits, so it is rounded to 3 dp before being written
  • avg_power_kw is null for zero-length sessions
  • had_fault is true if any event in the session was a fault
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import run_job, telemetry_path

JOB_NAME = "charging_sessions"


def summarize(telemetry: DataFrame) -> DataFrame:
    sessions = (
        telemetry
        .filter((F.col("event_type") == "CHARGING") & F.col("charge_session_id").isNotNull())
        .groupBy("charge_session_id", "vin_hash", "model_safe")
        .agg(
            F.min(F.to_timestamp("event_time")).alias("session_start"),
            F.max(F.to_timestamp("event_time")).alias("session_end"),
            F.count("*").alias("events"),
            F.round(F.sum("energy_added_kwh"), 3).alias("energy_added_kwh"),
            F.max("charging_power_kw").alias("peak_power_kw"),
            F.min_by("charge_percent", "sequence_number").alias("soc_start"),
            F.max_by("charge_percent", "sequence_number").alias("soc_end"),
            F.max_by("charge_state", "sequence_number").alias("end_state"),
            F.max(F.col("is_fault").cast("int")).cast("boolean").alias("had_fault"),
        )
    )
    duration_minutes = F.round(
        (F.col("session_end").cast("double") - F.col("session_start").cast("double")) / 60, 2
    )
    return (
        sessions
        .withColumn("duration_minutes", duration_minutes)
        .withColumn(
            "avg_power_kw",
            F.when(
                F.col("duration_minutes") > 0,
                F.round(F.col("energy_added_kwh") / (F.col("duration_minutes") / 60), 2),
            ),
        )
        .withColumn("session_date", F.to_date("session_start"))
    )


def build(spark: SparkSession, cfg: dict, local: bool) -> DataFrame:
    return summarize(spark.read.format("delta").load(telemetry_path(cfg, local)))


if __name__ == "__main__":
    run_job(JOB_NAME, build, partition_cols=["session_date"])
