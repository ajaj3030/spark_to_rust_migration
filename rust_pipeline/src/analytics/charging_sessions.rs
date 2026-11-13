//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala

use super::spark_functions::{
    spark_divide, spark_max_by, spark_min_by, spark_round, spark_timestamp,
};
use super::Job;
use anyhow::Result;
use deltalake::arrow::datatypes::{DataType, TimeUnit};
use deltalake::datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
use deltalake::datafusion::logical_expr::expr_fn::try_cast;
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "charging_sessions",
    key_cols: &["charge_session_id"],
};

pub async fn summarize(ctx: &SessionContext) -> Result<DataFrame> {
    // Spark `to_timestamp` parses event_time strings and returns NULL on
    // malformed input; `try_cast` does the same where `to_timestamp` would
    // error, then cast to Spark's timestamp[us, UTC] for the output type.
    let event_time = spark_timestamp(try_cast(
        col("event_time"),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ));

    // One row per charge_session_id; Spark groupBy keeps NULL keys, so the
    // is_not_null filter (not the group) is what drops them.
    let sessions = ctx
        .table("telemetry")
        .await?
        .filter(
            col("event_type")
                .eq(lit("CHARGING"))
                .and(col("charge_session_id").is_not_null()),
        )?
        .aggregate(
            vec![col("charge_session_id"), col("vin_hash"), col("model_safe")],
            vec![
                min(event_time.clone()).alias("session_start"),
                max(event_time.clone()).alias("session_end"),
                // Spark count("*") counts every row, including NULL columns.
                count(lit(1i64)).alias("events"),
                // spark_round can't wrap an aggregate inside aggregate(); the
                // sum is rounded in the projection below (same Spark value).
                sum(col("energy_added_kwh")).alias("energy_added_kwh"),
                max(col("charging_power_kw")).alias("peak_power_kw"),
                // min_by/max_by have no DataFusion equivalent; the helpers
                // ignore NULL orderings and keep the first row on ties.
                spark_min_by(col("charge_percent"), col("sequence_number")).alias("soc_start"),
                spark_max_by(col("charge_percent"), col("sequence_number")).alias("soc_end"),
                spark_max_by(col("charge_state"), col("sequence_number")).alias("end_state"),
                // Spark: max(is_fault cast int) cast boolean; the outer cast
                // is applied in the projection below (aggregate expressions
                // can't be wrapped in a cast).
                max(cast(col("is_fault"), DataType::Int32)).alias("had_fault"),
            ],
        )?;

    // Spark casts each timestamp to DOUBLE as epoch seconds (micros / 1e6)
    // before subtracting; DataFusion's cast yields the raw micros, so divide
    // each side by 1e6 first to match Spark's arithmetic bit-for-bit.
    let duration_minutes = spark_round(
        spark_divide(
            spark_divide(cast(col("session_end"), DataType::Float64), lit(1e6))
                - spark_divide(cast(col("session_start"), DataType::Float64), lit(1e6)),
            lit(60.0),
        ),
        2,
    );

    Ok(sessions
        .with_column("energy_added_kwh", spark_round(col("energy_added_kwh"), 3))?
        .with_column("had_fault", cast(col("had_fault"), DataType::Boolean))?
        .with_column("duration_minutes", duration_minutes)?
        .with_column(
            // Spark guards on the *rounded* duration and uses it in the
            // divisor; when() without otherwise yields NULL (zero-length
            // sessions get NULL avg_power_kw).
            "avg_power_kw",
            when(
                col("duration_minutes").gt(lit(0.0)),
                spark_round(
                    spark_divide(
                        col("energy_added_kwh"),
                        spark_divide(col("duration_minutes"), lit(60.0)),
                    ),
                    2,
                ),
            )
            .end()?,
        )?
        // session_date is the UTC calendar date of the session *start*
        // (sessions can cross midnight).
        .with_column("session_date", cast(col("session_start"), DataType::Date32))?)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }
}
