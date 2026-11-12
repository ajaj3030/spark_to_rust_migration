//! Parity check: compare a Spark-written Delta table with its Rust-written
//! counterpart. This is the acceptance gate for every migrated job.
//!
//! PASS requires: no rows missing on either side, every compared column
//! matching on every joined row, and (for telemetry) no raw PII columns.

use anyhow::{bail, Result};
use deltalake::arrow::array::Int64Array;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::*;
use serde_json::json;
use std::sync::Arc;

/// Raw identifiers that must never reach the enriched telemetry table.
const FORBIDDEN_PII_COLUMNS: &[&str] = &["vin", "latitude", "longitude", "kafka_key"];

/// Doubles are produced by the same Spark-compatible rounding on both sides,
/// so they must match to within float noise.
const FLOAT_TOLERANCE: f64 = 1e-9;

struct TableProfile {
    name: &'static str,
    join_col: &'static str,
    float_cols: &'static [&'static str],
    exact_cols: &'static [&'static str],
    check_pii: bool,
}

const TELEMETRY: TableProfile = TableProfile {
    name: "vehicle_telemetry",
    join_col: "event_id",
    float_cols: &[
        "odometer_km", "energy_pct", "cell_temp_spread", "degradation_pct",
        "km_per_percent", "lat_coarse", "lon_coarse",
    ],
    exact_cols: &[
        "event_type", "sequence_number", "model_safe", "software_version_safe",
        "power_state_safe", "event_date", "event_year", "event_month", "event_day",
        "event_hour", "event_dayofweek", "is_weekend", "event_quarter", "date_str",
        "soc_bucket", "health_tier", "thermal_alert", "is_fault", "needs_service",
        "severity_level", "vin_hash", "vin_wmi", "row_id", "sw_major", "sw_minor",
    ],
    check_pii: true,
};

const VEHICLE_STATE: TableProfile = TableProfile {
    name: "vehicle_state",
    join_col: "vin",
    float_cols: &["odometer_km", "charge_percent", "battery_health_level"],
    exact_cols: &[
        "sequence_number", "event_count", "software_version", "power_state",
        "last_event_type", "updated_at", "first_seen_at",
    ],
    check_pii: false,
};

pub async fn run_validation(spark_path: &str, rust_path: &str) -> Result<()> {
    let ctx = SessionContext::new();
    tracing::info!("Opening Spark table: {}", spark_path);
    ctx.register_table("spark_data", Arc::new(deltalake::open_table(spark_path).await?))?;
    tracing::info!("Opening Rust table: {}", rust_path);
    ctx.register_table("rust_data", Arc::new(deltalake::open_table(rust_path).await?))?;

    let spark_columns = column_names(&ctx, "spark_data").await?;
    let profile = if spark_columns.iter().any(|c| c == "event_id") { &TELEMETRY } else { &VEHICLE_STATE };
    tracing::info!("Profile: {} (join on {})", profile.name, profile.join_col);

    let spark_rows = sql_count(&ctx, "SELECT COUNT(*) FROM spark_data").await?;
    let rust_rows = sql_count(&ctx, "SELECT COUNT(*) FROM rust_data").await?;
    let jc = profile.join_col;
    let missing_in_rust = sql_count(&ctx, &format!(
        "SELECT COUNT(*) FROM spark_data s LEFT ANTI JOIN rust_data r ON s.{jc} = r.{jc}"
    )).await?;
    let missing_in_spark = sql_count(&ctx, &format!(
        "SELECT COUNT(*) FROM rust_data r LEFT ANTI JOIN spark_data s ON s.{jc} = r.{jc}"
    )).await?;
    tracing::info!("Row counts — Spark: {}  Rust: {}", spark_rows, rust_rows);
    tracing::info!("Missing in Rust: {}  Missing in Spark: {}", missing_in_rust, missing_in_spark);

    let mut column_results = serde_json::Map::new();
    let mut mismatched_columns = 0;
    for col in profile.float_cols {
        let n = count_mismatches(&ctx, jc, &format!(
            "(s.{col} IS NULL) <> (r.{col} IS NULL) OR ABS(s.{col} - r.{col}) > {FLOAT_TOLERANCE}"
        )).await?;
        mismatched_columns += report_column(col, n, &mut column_results);
    }
    for col in profile.exact_cols {
        let n = count_mismatches(&ctx, jc, &format!("s.{col} IS DISTINCT FROM r.{col}")).await?;
        mismatched_columns += report_column(col, n, &mut column_results);
    }

    let mut pii_leaks = Vec::new();
    if profile.check_pii {
        let rust_columns = column_names(&ctx, "rust_data").await?;
        for (side, cols) in [("spark", &spark_columns), ("rust", &rust_columns)] {
            for forbidden in FORBIDDEN_PII_COLUMNS {
                if cols.iter().any(|c| c == forbidden) {
                    pii_leaks.push(format!("{side}.{forbidden}"));
                }
            }
        }
        if pii_leaks.is_empty() {
            tracing::info!("  privacy – OK (no raw VIN / GPS / kafka_key columns)");
        } else {
            tracing::error!("  privacy – raw PII columns present: {:?}", pii_leaks);
        }
    }

    let passed = missing_in_rust == 0 && missing_in_spark == 0
        && mismatched_columns == 0 && pii_leaks.is_empty();
    let status = if passed { "PASS" } else { "FAIL" };
    let report = json!({
        "table": profile.name,
        "spark_table": spark_path,
        "rust_table": rust_path,
        "spark_row_count": spark_rows,
        "rust_row_count": rust_rows,
        "missing_in_rust": missing_in_rust,
        "missing_in_spark": missing_in_spark,
        "column_mismatches": column_results,
        "pii_columns_present": pii_leaks,
        "status": status,
    });
    write_report(profile.name, &report)?;
    tracing::info!("Status: {}", status);

    if !passed {
        bail!("parity validation failed for {}", profile.name);
    }
    Ok(())
}

fn report_column(col: &str, mismatches: i64, results: &mut serde_json::Map<String, serde_json::Value>) -> usize {
    results.insert(col.to_string(), json!(mismatches));
    if mismatches > 0 {
        tracing::warn!("  {} – {} mismatches", col, mismatches);
        1
    } else {
        tracing::info!("  {} – OK", col);
        0
    }
}

async fn count_mismatches(ctx: &SessionContext, join_col: &str, predicate: &str) -> Result<i64> {
    sql_count(ctx, &format!(
        "SELECT COUNT(*) FROM spark_data s JOIN rust_data r ON s.{join_col} = r.{join_col} \
         WHERE {predicate}"
    )).await
}

async fn column_names(ctx: &SessionContext, table: &str) -> Result<Vec<String>> {
    let df = ctx.table(table).await?;
    Ok(df.schema().fields().iter().map(|f| f.name().clone()).collect())
}

fn write_report(table: &str, report: &serde_json::Value) -> Result<()> {
    let dir = if std::path::Path::new("/opt/logs").exists() { "/opt/logs" } else { "./logs" };
    std::fs::create_dir_all(dir)?;
    let path = format!("{dir}/validation_report_{table}.json");
    std::fs::write(&path, serde_json::to_string_pretty(report)?)?;
    tracing::info!("Validation report written to {}", path);
    Ok(())
}

async fn sql_count(ctx: &SessionContext, sql: &str) -> Result<i64> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(first_count(&batches))
}

fn first_count(batches: &[RecordBatch]) -> i64 {
    batches
        .first()
        .filter(|b| b.num_rows() > 0)
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>().map(|a| a.value(0)))
        .unwrap_or(0)
}
