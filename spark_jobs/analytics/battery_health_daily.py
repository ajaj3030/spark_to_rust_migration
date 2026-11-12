"""
Battery health — one row per vehicle per day, for warranty and service.

Input:  enriched telemetry, BATTERY events only
Output: analytics/battery_health_daily, key (vin_hash, event_date)

Behaviour a port must preserve:
  • health_level_end_of_day is the reading with the highest sequence_number
    that day, not the last one to arrive
  • peak_cell_temp_3d is a RANGE window over calendar days (today and the
    two before), so gaps in the data shrink the window instead of reaching
    further back
  • health_change is null on a vehicle's first day
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import run_job, telemetry_path

JOB_NAME = "battery_health_daily"
SECONDS_PER_DAY = 86_400


def summarize(telemetry: DataFrame) -> DataFrame:
    daily = (
        telemetry
        .filter(F.col("event_type") == "BATTERY")
        .groupBy("vin_hash", "event_date", "model_safe")
        .agg(
            F.count("*").alias("readings"),
            F.round(F.avg("charge_percent"), 2).alias("avg_charge_percent"),
            F.min("charge_percent").alias("min_charge_percent"),
            F.max("max_cell_temp").alias("peak_cell_temp"),
            F.round(F.avg("cell_temp_spread"), 3).alias("avg_cell_temp_spread"),
            F.sum(F.col("thermal_alert").cast("int")).alias("thermal_alert_count"),
            F.max_by("battery_health_level", "sequence_number").alias("health_level_end_of_day"),
        )
    )

    day_seconds = F.col("event_date").cast("timestamp").cast("long")
    last_three_days = (
        Window.partitionBy("vin_hash").orderBy(day_seconds)
        .rangeBetween(-2 * SECONDS_PER_DAY, 0)
    )
    by_day = Window.partitionBy("vin_hash").orderBy("event_date")

    return (
        daily
        .withColumn("peak_cell_temp_3d", F.max("peak_cell_temp").over(last_three_days))
        .withColumn(
            "health_change",
            F.round(
                F.col("health_level_end_of_day")
                - F.lag("health_level_end_of_day").over(by_day),
                2,
            ),
        )
    )


def build(spark: SparkSession, cfg: dict, local: bool) -> DataFrame:
    return summarize(spark.read.format("delta").load(telemetry_path(cfg, local)))


if __name__ == "__main__":
    run_job(JOB_NAME, build, partition_cols=["event_date"])
