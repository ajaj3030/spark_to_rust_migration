//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala

use super::spark_functions::{spark_divide, spark_round};
use super::Job;
use anyhow::Result;
use deltalake::datafusion::functions_aggregate::expr_fn::count;
use deltalake::datafusion::functions_window::expr_fn::row_number;
use deltalake::datafusion::logical_expr::ExprFunctionExt;
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "ota_rollout",
    key_cols: &["target_software_version", "model_safe"],
};

/// Pivot values, in Spark's column order. Output column names are these
/// exact (uppercase) strings; the dashboard selects them by the `UPDATE_` prefix.
pub const UPDATE_STATES: [&str; 7] = [
    "UPDATE_STATE_IN_PROGRESS",
    "UPDATE_STATE_WAITING_ON_BCM",
    "UPDATE_STATE_SUCCESS",
    "UPDATE_SUCCESS_WITH_WARNINGS",
    "UPDATE_STATE_FAILED",
    "UPDATE_FAILED_DRIVE_ALLOWED",
    "UPDATE_FAILED_NO_ACTION",
];
const SUCCEEDED: [&str; 2] = ["UPDATE_STATE_SUCCESS", "UPDATE_SUCCESS_WITH_WARNINGS"];
const FAILED: [&str; 3] = [
    "UPDATE_STATE_FAILED",
    "UPDATE_FAILED_DRIVE_ALLOWED",
    "UPDATE_FAILED_NO_ACTION",
];

/// Sum of pivot columns (BIGINT). `ident` keeps the uppercase names as-is.
fn total(states: &[&str]) -> Expr {
    states
        .iter()
        .fold(lit(0i64), |sum, state| sum + ident(*state))
}

/// Spark: `round(numerator / denominator * 100, 2)` — NULL when denominator is 0.
fn pct(numerator: Expr, denominator: Expr) -> Expr {
    spark_round(spark_divide(numerator, denominator) * lit(100.0), 2)
}

pub async fn summarize(ctx: &SessionContext) -> Result<DataFrame> {
    // Spark's `desc` sorts NULLs last; DataFusion's DESC defaults to NULLS FIRST.
    let newest_first = row_number()
        .partition_by(vec![col("vin_hash"), col("target_software_version")])
        .order_by(vec![col("sequence_number").sort(false, false)])
        .build()?;

    let latest_per_vehicle = ctx
        .table("telemetry")
        .await?
        .filter(col("event_type").eq(lit("OTA")))?
        .window(vec![newest_first.alias("rn")])?
        .filter(col("rn").eq(lit(1u64)))?;

    // Spark's pivot groups every row (states outside the list included) and
    // counts only the listed values; na.fill(0) turns missing counts into 0.
    let pivot_counts = UPDATE_STATES
        .iter()
        .map(|state| {
            Ok(count(when(col("update_state").eq(lit(*state)), lit(1)).end()?).alias(*state))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(latest_per_vehicle
        .aggregate(
            vec![col("target_software_version"), col("model_safe")],
            pivot_counts,
        )?
        .with_column("vehicles", total(&UPDATE_STATES))?
        .with_column("success_rate_pct", pct(total(&SUCCEEDED), col("vehicles")))?
        .with_column("failure_rate_pct", pct(total(&FAILED), col("vehicles")))?)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }
}
