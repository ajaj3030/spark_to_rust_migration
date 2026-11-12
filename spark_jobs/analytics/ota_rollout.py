"""
OTA rollout — fleet status per target software version and model.

Input:  enriched telemetry, OTA events only
Output: analytics/ota_rollout, key (target_software_version, model_safe)

Behaviour a port must preserve:
  • each vehicle counts once per target version, in its latest state by
    sequence_number (a vehicle that failed then succeeded is a success)
  • the pivot columns are the fixed UPDATE_STATES list, so the schema is
    stable even when a state has no vehicles (count 0, not missing)
  • rates are percentages of vehicles attempting that version, 2 dp
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import run_job, telemetry_path

JOB_NAME = "ota_rollout"

UPDATE_STATES = [
    "UPDATE_STATE_IN_PROGRESS",
    "UPDATE_STATE_WAITING_ON_BCM",
    "UPDATE_STATE_SUCCESS",
    "UPDATE_SUCCESS_WITH_WARNINGS",
    "UPDATE_STATE_FAILED",
    "UPDATE_FAILED_DRIVE_ALLOWED",
    "UPDATE_FAILED_NO_ACTION",
]
SUCCEEDED = ["UPDATE_STATE_SUCCESS", "UPDATE_SUCCESS_WITH_WARNINGS"]
FAILED = ["UPDATE_STATE_FAILED", "UPDATE_FAILED_DRIVE_ALLOWED", "UPDATE_FAILED_NO_ACTION"]


def _pct(numerator: F.Column, denominator: F.Column) -> F.Column:
    return F.round(numerator / denominator * 100, 2)


def summarize(telemetry: DataFrame) -> DataFrame:
    newest_first = (
        Window.partitionBy("vin_hash", "target_software_version")
        .orderBy(F.col("sequence_number").desc())
    )
    latest_per_vehicle = (
        telemetry
        .filter(F.col("event_type") == "OTA")
        .withColumn("rn", F.row_number().over(newest_first))
        .filter(F.col("rn") == 1)
    )
    by_state = (
        latest_per_vehicle
        .groupBy("target_software_version", "model_safe")
        .pivot("update_state", UPDATE_STATES)
        .count()
        .na.fill(0, subset=UPDATE_STATES)
    )
    total = sum((F.col(s) for s in UPDATE_STATES), F.lit(0))
    succeeded = sum((F.col(s) for s in SUCCEEDED), F.lit(0))
    failed = sum((F.col(s) for s in FAILED), F.lit(0))
    return (
        by_state
        .withColumn("vehicles", total)
        .withColumn("success_rate_pct", _pct(succeeded, F.col("vehicles")))
        .withColumn("failure_rate_pct", _pct(failed, F.col("vehicles")))
    )


def build(spark: SparkSession, cfg: dict, local: bool) -> DataFrame:
    return summarize(spark.read.format("delta").load(telemetry_path(cfg, local)))


if __name__ == "__main__":
    run_job(JOB_NAME, build)
