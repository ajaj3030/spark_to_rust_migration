//! Parity check: compare a Spark-produced table with its Rust counterpart.
//! This is the acceptance gate for every migrated job, used both by the
//! `validate` CLI (live Delta tables) and by the parity tests (fixtures).
//!
//! PASS requires: no rows missing on either side, every compared column
//! equal on every row (doubles within FLOAT_TOLERANCE), no forbidden
//! columns, and — when `check_types` is set — the same columns with the
//! same types as Spark, so downstream readers see an identical schema.

use anyhow::{bail, Result};
use deltalake::arrow::array::Int64Array;
use deltalake::arrow::datatypes::{DataType, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::*;
use serde_json::json;
use std::sync::Arc;

/// Raw identifiers that must never reach an analytics-facing table.
pub const FORBIDDEN_PII_COLUMNS: &[&str] = &["vin", "latitude", "longitude", "kafka_key"];

/// Both engines must round identically (see transforms/rounding.rs), so
/// doubles may only differ by float noise.
const FLOAT_TOLERANCE: f64 = 1e-9;

/// What to compare and how. Tables are registered as `spark_data` (the
/// reference) and `rust_data` (the port) before calling `compare`.
pub struct Comparison {
    pub table: &'static str,
    pub key_cols: &'static [&'static str],
    pub forbidden_columns: &'static [&'static str],
    pub ignore_columns: &'static [&'static str],
    pub check_types: bool,
}

pub const TELEMETRY: Comparison = Comparison {
    table: "vehicle_telemetry",
    key_cols: &["event_id"],
    forbidden_columns: FORBIDDEN_PII_COLUMNS,
    // Spark stores the Kafka timestamp as TIMESTAMP, Rust as epoch millis
    ignore_columns: &["kafka_timestamp"],
    check_types: false,
};

pub const VEHICLE_STATE: Comparison = Comparison {
    table: "vehicle_state",
    key_cols: &["vin"],
    forbidden_columns: &[],
    ignore_columns: &[],
    check_types: false,
};

pub struct ParityReport {
    pub passed: bool,
    pub failures: Vec<String>,
    pub json: serde_json::Value,
}

/// Open two Delta tables, compare them, write a JSON report, and fail
/// (non-zero exit) if they differ.
pub async fn run_validation(spark_path: &str, rust_path: &str, cmp: &Comparison) -> Result<()> {
    let ctx = SessionContext::new();
    tracing::info!("Spark table: {}", spark_path);
    ctx.register_table(
        "spark_data",
        Arc::new(deltalake::open_table(spark_path).await?),
    )?;
    tracing::info!("Rust table:  {}", rust_path);
    ctx.register_table(
        "rust_data",
        Arc::new(deltalake::open_table(rust_path).await?),
    )?;

    let report = compare(&ctx, cmp).await?;
    write_report(cmp.table, &report.json)?;
    if !report.passed {
        bail!(
            "parity validation failed for {}: {}",
            cmp.table,
            report.failures.join("; ")
        );
    }
    Ok(())
}

pub async fn compare(ctx: &SessionContext, cmp: &Comparison) -> Result<ParityReport> {
    let spark_schema = table_schema(ctx, "spark_data").await?;
    let rust_schema = table_schema(ctx, "rust_data").await?;
    let mut failures = Vec::new();
    tracing::info!("Comparing {} on ({})", cmp.table, cmp.key_cols.join(", "));

    check_forbidden_columns(cmp, &spark_schema, &rust_schema, &mut failures);
    if cmp.check_types {
        check_schema_contract(&spark_schema, &rust_schema, &mut failures);
    }

    let spark_rows = sql_count(ctx, "SELECT COUNT(*) FROM spark_data").await?;
    let rust_rows = sql_count(ctx, "SELECT COUNT(*) FROM rust_data").await?;
    let on = join_condition(cmp.key_cols);
    let missing_in_rust = sql_count(
        ctx,
        &format!("SELECT COUNT(*) FROM spark_data s LEFT ANTI JOIN rust_data r ON {on}"),
    )
    .await?;
    let missing_in_spark = sql_count(
        ctx,
        &format!("SELECT COUNT(*) FROM rust_data r LEFT ANTI JOIN spark_data s ON {on}"),
    )
    .await?;
    tracing::info!("Rows — Spark: {spark_rows}  Rust: {rust_rows}  missing in Rust: {missing_in_rust}  missing in Spark: {missing_in_spark}");
    if missing_in_rust > 0 || missing_in_spark > 0 {
        failures.push(format!(
            "rows missing: {missing_in_rust} in Rust, {missing_in_spark} in Spark"
        ));
    }

    let mut column_mismatches = serde_json::Map::new();
    for field in spark_schema.fields() {
        let col = field.name();
        let skip = cmp.key_cols.contains(&col.as_str())
            || cmp.ignore_columns.contains(&col.as_str())
            || rust_schema.field_with_name(col).is_err();
        if skip {
            continue;
        }
        let mismatches = count_column_mismatches(ctx, &on, col, field.data_type()).await?;
        column_mismatches.insert(col.clone(), json!(mismatches));
        if mismatches > 0 {
            tracing::warn!("  {col} – {mismatches} mismatched rows");
            failures.push(format!("{col}: {mismatches} mismatched rows"));
        } else {
            tracing::info!("  {col} – OK");
        }
    }

    let passed = failures.is_empty();
    tracing::info!("Status: {}", if passed { "PASS" } else { "FAIL" });
    let json = json!({
        "table": cmp.table,
        "key_columns": cmp.key_cols,
        "spark_row_count": spark_rows,
        "rust_row_count": rust_rows,
        "missing_in_rust": missing_in_rust,
        "missing_in_spark": missing_in_spark,
        "column_mismatches": column_mismatches,
        "failures": failures,
        "status": if passed { "PASS" } else { "FAIL" },
    });
    Ok(ParityReport {
        passed,
        failures,
        json,
    })
}

fn check_forbidden_columns(
    cmp: &Comparison,
    spark: &Schema,
    rust: &Schema,
    failures: &mut Vec<String>,
) {
    if cmp.forbidden_columns.is_empty() {
        return;
    }
    let mut leaked = Vec::new();
    for (side, schema) in [("spark", spark), ("rust", rust)] {
        for forbidden in cmp.forbidden_columns {
            if schema.field_with_name(forbidden).is_ok() {
                leaked.push(format!("{side}.{forbidden}"));
            }
        }
    }
    if leaked.is_empty() {
        tracing::info!(
            "  privacy – OK (none of {:?} present)",
            cmp.forbidden_columns
        );
    } else {
        tracing::error!("  privacy – forbidden columns present: {:?}", leaked);
        failures.push(format!("forbidden columns present: {}", leaked.join(", ")));
    }
}

/// Downstream consumers read these tables by name and type, so a port must
/// produce exactly Spark's columns and types (nullability may differ).
fn check_schema_contract(spark: &Schema, rust: &Schema, failures: &mut Vec<String>) {
    for field in spark.fields() {
        match rust.field_with_name(field.name()) {
            Err(_) => failures.push(format!("column missing in Rust: {}", field.name())),
            Ok(r) if !same_type(r.data_type(), field.data_type()) => failures.push(format!(
                "type mismatch for {}: Spark {:?}, Rust {:?}",
                field.name(),
                field.data_type(),
                r.data_type()
            )),
            Ok(_) => {}
        }
    }
    for field in rust.fields() {
        if spark.field_with_name(field.name()).is_err() {
            failures.push(format!("extra column in Rust: {}", field.name()));
        }
    }
}

/// Timestamps compare by unit only: Spark marks them UTC, which DataFusion
/// may surface as "UTC" or "+00:00".
fn same_type(a: &DataType, b: &DataType) -> bool {
    match (a, b) {
        (DataType::Timestamp(unit_a, _), DataType::Timestamp(unit_b, _)) => unit_a == unit_b,
        _ => a == b,
    }
}

async fn count_column_mismatches(
    ctx: &SessionContext,
    on: &str,
    col: &str,
    dt: &DataType,
) -> Result<i64> {
    let predicate = if matches!(dt, DataType::Float32 | DataType::Float64) {
        format!("(s.\"{col}\" IS NULL) <> (r.\"{col}\" IS NULL) OR ABS(s.\"{col}\" - r.\"{col}\") > {FLOAT_TOLERANCE}")
    } else {
        format!("s.\"{col}\" IS DISTINCT FROM r.\"{col}\"")
    };
    sql_count(
        ctx,
        &format!("SELECT COUNT(*) FROM spark_data s JOIN rust_data r ON {on} WHERE {predicate}"),
    )
    .await
}

/// Null-safe equi-join on the key columns. `IS NOT DISTINCT FROM` would be
/// planned as a nested-loop join (quadratic), so null keys are mapped to a
/// sentinel instead and compared with `=`, which stays a hash join.
fn join_condition(key_cols: &[&str]) -> String {
    let null_safe =
        |side: &str, k: &str| format!("COALESCE(CAST({side}.\"{k}\" AS VARCHAR), '\u{0}NULL')");
    key_cols
        .iter()
        .map(|k| format!("{} = {}", null_safe("s", k), null_safe("r", k)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

async fn table_schema(ctx: &SessionContext, table: &str) -> Result<Schema> {
    Ok(ctx.table(table).await?.schema().as_arrow().clone())
}

fn write_report(table: &str, report: &serde_json::Value) -> Result<()> {
    let dir = if std::path::Path::new("/opt/logs").exists() {
        "/opt/logs"
    } else {
        "./logs"
    };
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
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(0))
        })
        .unwrap_or(0)
}
