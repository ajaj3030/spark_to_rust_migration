"""
Reusable Spark transformation library for vehicle telemetry.

The streaming job applies these in order to every telemetry micro-batch.
The Rust port (rust_pipeline/src/transforms/) mirrors each function one to
one; `rust-pipeline validate` checks the two outputs match.

Privacy contract: the enriched table must never contain the raw VIN or
precise GPS. add_location_features coarsens coordinates to 2 dp (~1 km) and
add_anonymized_columns replaces the VIN with a SHA-256 hash; both drop the
raw columns.
"""

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

THERMAL_MAX_CELL_TEMP_C = 55.0
THERMAL_CELL_SPREAD_C = 15.0
SERVICE_HEALTH_THRESHOLD = 70.0

FAULT_CHARGE_STATES = [
    "CHARGE_STATE_CHARGING_END_ERROR",
    "CHARGE_STATE_EVSE_MALFUNCTION",
    "CHARGE_STATE_TEMP_PROTECTION",
    "CHARGE_STATE_AUTHENTICATION_PNC_FAILURE",
    "CHARGE_STATE_CABLE_CONNECTION_FAULT",
    "CHARGE_STATE_INTERNAL_HARDWARE_FAULT",
]

FAILED_UPDATE_STATES = [
    "UPDATE_STATE_FAILED",
    "UPDATE_FAILED_DRIVE_ALLOWED",
    "UPDATE_FAILED_NO_ACTION",
]

PII_COLUMNS = ["vin", "latitude", "longitude"]


# ─────────────────────────────────────────────────────────────
# UDFs
# ─────────────────────────────────────────────────────────────

@F.udf(returnType=StringType())
def classify_health(battery_health_level: Optional[float]) -> str:
    """Bucket battery state-of-health into service tiers."""
    if battery_health_level is None:
        return "UNKNOWN"
    if battery_health_level >= 90:
        return "EXCELLENT"
    if battery_health_level >= 80:
        return "GOOD"
    if battery_health_level >= 70:
        return "FAIR"
    return "SERVICE_REQUIRED"


def _is_true(condition) -> F.Column:
    """Null-safe boolean: a null comparison counts as False."""
    return F.coalesce(condition, F.lit(False))


# ─────────────────────────────────────────────────────────────
# Transforms — DataFrame → DataFrame
# ─────────────────────────────────────────────────────────────

def add_null_handling(df: DataFrame) -> DataFrame:
    """T1: defaults for vehicle-context fields every event should carry."""
    return (
        df
        .withColumn("model_safe", F.coalesce(F.col("model"), F.lit("MODEL_UNKNOWN")))
        .withColumn("software_version_safe",
                    F.coalesce(F.col("software_version"), F.lit("0.0.0")))
        .withColumn("power_state_safe",
                    F.coalesce(F.col("power_state"), F.lit("POWER_STATE_UNKNOWN")))
        .na.fill({"odometer_km": 0.0})
    )


def add_time_dimensions(df: DataFrame) -> DataFrame:
    """T2: calendar parts of the vehicle-recorded event_time (UTC)."""
    ts = F.col("event_time")
    return (
        df
        .withColumn("event_date", F.to_date(ts))
        .withColumn("event_year", F.year(ts))
        .withColumn("event_month", F.month(ts))
        .withColumn("event_day", F.dayofmonth(ts))
        .withColumn("event_hour", F.hour(ts))
        .withColumn("event_dayofweek", F.dayofweek(ts))
        .withColumn("is_weekend", F.dayofweek(ts).isin(1, 7))
        .withColumn("event_quarter", F.quarter(ts))
        .withColumn("date_str", F.date_format(ts, "yyyy-MM-dd"))
    )


def add_battery_features(df: DataFrame) -> DataFrame:
    """T3: derived battery metrics. Null when the source fields are absent."""
    soc = F.col("charge_percent")
    return (
        df
        .withColumn(
            "soc_bucket",
            F.when(soc.isNull(), "UNKNOWN")
            .when(soc < 10, "CRITICAL")
            .when(soc < 20, "LOW")
            .when(soc < 80, "NORMAL")
            .otherwise("HIGH"),
        )
        .withColumn(
            "energy_pct",
            F.when(F.col("capacity_kwhr") > 0,
                   F.round(F.col("kwhr") / F.col("capacity_kwhr") * 100, 2)),
        )
        .withColumn("cell_temp_spread",
                    F.round(F.col("max_cell_temp") - F.col("min_cell_temp"), 2))
        .withColumn("degradation_pct", F.round(100 - F.col("battery_health_level"), 2))
        .withColumn(
            "km_per_percent",
            F.when(soc > 0, F.round(F.col("remaining_range") / soc, 3)),
        )
    )


def add_health_features(df: DataFrame) -> DataFrame:
    """T4: service and safety flags consumed by warranty and service systems."""
    thermal = (
        _is_true(F.col("max_cell_temp") >= THERMAL_MAX_CELL_TEMP_C)
        | _is_true(F.col("cell_temp_spread") >= THERMAL_CELL_SPREAD_C)
    )
    fault = (
        _is_true(F.col("warning_state") == "WARNING_ON")
        | _is_true(F.col("charge_state").isin(FAULT_CHARGE_STATES))
        | _is_true(F.col("update_state").isin(FAILED_UPDATE_STATES))
    )
    return (
        df
        .withColumn("health_tier", classify_health(F.col("battery_health_level")))
        .withColumn("thermal_alert", thermal)
        .withColumn("is_fault", fault)
        .withColumn(
            "needs_service",
            _is_true(F.col("battery_health_level") < SERVICE_HEALTH_THRESHOLD)
            | F.col("thermal_alert")
            | F.col("is_fault"),
        )
        .withColumn(
            "severity_level",
            F.when(F.col("thermal_alert"), 3)
            .when(F.col("is_fault"), 2)
            .when(F.col("needs_service"), 1)
            .otherwise(0),
        )
    )


def add_location_features(df: DataFrame) -> DataFrame:
    """T5 (privacy): coarsen GPS to 2 dp (~1 km) and drop precise coordinates."""
    return (
        df
        .withColumn("lat_coarse", F.round(F.col("latitude"), 2))
        .withColumn("lon_coarse", F.round(F.col("longitude"), 2))
        .drop("latitude", "longitude")
    )


def add_anonymized_columns(df: DataFrame) -> DataFrame:
    """T6 (privacy): replace the VIN with a stable pseudonym; keep only the WMI."""
    return (
        df
        .withColumn("vin_hash", F.sha2(F.col("vin"), 256))
        .withColumn("vin_wmi", F.substring(F.col("vin"), 1, 3))
        .drop("vin")
    )


def add_event_features(df: DataFrame) -> DataFrame:
    """T7: row identity and software version parts."""
    version_parts = F.split(F.col("software_version_safe"), r"\.")
    return (
        df
        .withColumn("row_id", F.sha2(F.col("event_id"), 256))
        .withColumn("sw_major", version_parts.getItem(0).cast("int"))
        .withColumn("sw_minor", version_parts.getItem(1).cast("int"))
    )


def apply_all_transforms(df: DataFrame) -> DataFrame:
    """Chain every enrichment transform. T4 depends on T3 (cell_temp_spread)."""
    return (
        df
        .transform(add_null_handling)
        .transform(add_time_dimensions)
        .transform(add_battery_features)
        .transform(add_health_features)
        .transform(add_location_features)
        .transform(add_anonymized_columns)
        .transform(add_event_features)
    )
