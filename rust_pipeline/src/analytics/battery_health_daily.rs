//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala

use super::spark_functions::{spark_max_by, spark_round};
use super::Job;
use anyhow::Result;
use deltalake::arrow::datatypes::DataType;
use deltalake::datafusion::common::ScalarValue;
use deltalake::datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use deltalake::datafusion::functions_aggregate::min_max::max_udaf;
use deltalake::datafusion::functions_window::lead_lag::lag_udwf;
use deltalake::datafusion::logical_expr::expr::{Sort, WindowFunction, WindowFunctionDefinition};
use deltalake::datafusion::logical_expr::ExprFunctionExt;
use deltalake::datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "battery_health_daily",
    key_cols: &["vin_hash", "event_date"],
};

/// Spark's `max(peak_cell_temp)` as a window aggregate over a RANGE frame.
fn max_over(
    arg: Expr,
    partition_by: Vec<Expr>,
    order_by: Sort,
    window_frame: WindowFrame,
) -> Result<Expr> {
    Expr::WindowFunction(WindowFunction::new(
        WindowFunctionDefinition::AggregateUDF(max_udaf()),
        vec![arg],
    ))
    .partition_by(partition_by)
    .order_by(vec![order_by])
    .window_frame(window_frame)
    .build()
    .map_err(Into::into)
}

/// Spark's `lag(value, 1)`: NULL at the start of each partition.
fn lag_by_one(arg: Expr, partition_by: Vec<Expr>, order_by: Sort) -> Result<Expr> {
    Expr::WindowFunction(WindowFunction::new(
        WindowFunctionDefinition::WindowUDF(lag_udwf()),
        vec![arg, lit(1i64), lit(ScalarValue::Null)],
    ))
    .partition_by(partition_by)
    .order_by(vec![order_by])
    .build()
    .map_err(Into::into)
}

pub async fn summarize(ctx: &SessionContext) -> Result<DataFrame> {
    let daily = ctx
        .table("telemetry")
        .await?
        .filter(col("event_type").eq(lit("BATTERY")))?
        .aggregate(
            vec![col("vin_hash"), col("event_date"), col("model_safe")],
            vec![
                count(lit(1i64)).alias("readings"),
                // Rounding is applied after aggregation (Spark: round(avg(x), n));
                // a ScalarUDF cannot wrap an aggregate inside `aggregate`.
                avg(col("charge_percent")).alias("avg_charge_percent_raw"),
                min(col("charge_percent")).alias("min_charge_percent"),
                max(col("max_cell_temp")).alias("peak_cell_temp"),
                avg(col("cell_temp_spread")).alias("avg_cell_temp_spread_raw"),
                sum(cast(col("thermal_alert"), DataType::Int64)).alias("thermal_alert_count"),
                spark_max_by(col("battery_health_level"), col("sequence_number"))
                    .alias("health_level_end_of_day"),
            ],
        )?;

    // Spark: .orderBy(event_date as timestamp-long seconds).rangeBetween(-2 * 86400, 0)
    // — a calendar-day range, not a row count. `event_date` cast to Int64 is
    // days since epoch, so the equivalent bound is 2 days.
    // Spark's asc is NULLS FIRST; spell it out for DataFusion.
    let day_number = cast(col("event_date"), DataType::Int64);
    let last_three_days = WindowFrame::new_bounds(
        WindowFrameUnits::Range,
        WindowFrameBound::Preceding(ScalarValue::Int64(Some(2))),
        WindowFrameBound::CurrentRow,
    );
    let peak_cell_temp_3d = max_over(
        col("peak_cell_temp"),
        vec![col("vin_hash")],
        day_number.sort(true, true),
        last_three_days,
    )?;

    let prev_health = lag_by_one(
        col("health_level_end_of_day"),
        vec![col("vin_hash")],
        col("event_date").sort(true, true),
    )?;

    Ok(daily
        .with_column(
            "avg_charge_percent",
            spark_round(col("avg_charge_percent_raw"), 2),
        )?
        .with_column(
            "avg_cell_temp_spread",
            spark_round(col("avg_cell_temp_spread_raw"), 3),
        )?
        .window(vec![
            peak_cell_temp_3d.alias("peak_cell_temp_3d"),
            prev_health.alias("prev_health"),
        ])?
        .with_column(
            "health_change",
            spark_round(col("health_level_end_of_day") - col("prev_health"), 2),
        )?
        .select(vec![
            col("vin_hash"),
            col("event_date"),
            col("model_safe"),
            col("readings"),
            col("avg_charge_percent"),
            col("min_charge_percent"),
            col("peak_cell_temp"),
            col("avg_cell_temp_spread"),
            col("thermal_alert_count"),
            col("health_level_end_of_day"),
            col("peak_cell_temp_3d"),
            col("health_change"),
        ])?)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }
}
