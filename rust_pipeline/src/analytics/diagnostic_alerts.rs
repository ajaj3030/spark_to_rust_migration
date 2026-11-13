//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala

use super::spark_functions::{
    spark_divide, spark_int, spark_round, spark_sha2, spark_timestamp, spark_to_timestamp,
};
use super::Job;
use anyhow::Result;

use deltalake::datafusion::functions_aggregate::expr_fn::{count, count_distinct, max, min, sum};
use deltalake::datafusion::functions_window::expr_fn::dense_rank;
use deltalake::datafusion::logical_expr::ExprFunctionExt;
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "diagnostic_alerts",
    key_cols: &["dtc_code", "ecu", "model_safe", "battery_pack_type"],
};

pub async fn summarize(ctx: &SessionContext) -> Result<DataFrame> {
    // vehicle_state is keyed by raw VIN; Spark's sha2(vin, 256) gives a
    // lowercase hex string -> spark_sha2. The key is renamed so the join
    // doesn't leave a qualified and an unqualified "vin_hash" that
    // DataFusion rejects as ambiguous.
    let fleet = ctx.table("vehicle_state").await?.select(vec![
        spark_sha2(col("vin")).alias("fleet_vin_hash"),
        col("battery_pack_type"),
    ])?;

    // Spark: lit(vehicleState.count()) — the whole table, broadcast as a
    // constant column, BIGINT. Used only as a DOUBLE divisor below.
    let fleet_size = ctx.table("vehicle_state").await?.count().await? as f64;

    // Diagnostic events carry no pack type; drop telemetry's column and take
    // it from the vehicle record via the left join (NULL when unmatched).
    let telemetry = ctx.table("telemetry").await?;
    let diagnostic_columns = telemetry
        .schema()
        .fields()
        .iter()
        .filter(|f| f.name() != "battery_pack_type")
        .map(|f| col(f.name()))
        .collect::<Vec<_>>();

    let alerts = telemetry
        .filter(col("event_type").eq(lit("DIAGNOSTIC")))?
        .select(diagnostic_columns)?
        .join(
            fleet,
            JoinType::Left,
            &["vin_hash"],
            &["fleet_vin_hash"],
            None,
        )?
        .aggregate(
            vec![
                col("dtc_code"),
                col("ecu"),
                col("model_safe"),
                col("battery_pack_type"),
            ],
            vec![
                count(lit(1)).alias("events"),
                count_distinct(col("vin_hash")).alias("vehicles_affected"),
                sum(when(col("warning_state").eq(lit("WARNING_ON")), lit(1)).otherwise(lit(0))?)
                    .alias("warning_on_events"),
                // spark_to_timestamp: a malformed event_time is NULL (Spark
                // non-ANSI), where DataFusion's to_timestamp would abort.
                min(spark_timestamp(spark_to_timestamp(col("event_time")))).alias("first_seen"),
                max(spark_timestamp(spark_to_timestamp(col("event_time")))).alias("last_seen"),
            ],
        )?;

    // Spark's desc sorts NULLs last; DataFusion's DESC defaults to NULLS FIRST.
    // dense_rank is UInt64 in DataFusion but INT in Spark -> spark_int.
    let rank = dense_rank()
        .partition_by(vec![col("ecu")])
        .order_by(vec![col("vehicles_affected").sort(false, false)])
        .build()?;

    // Spark: round(vehicles_affected / fleetSize * 100, 3) — NULL if 0 vehicles.
    alerts
        .with_column(
            "fleet_pct_affected",
            spark_round(
                spark_divide(col("vehicles_affected"), lit(fleet_size)) * lit(100.0),
                3,
            ),
        )?
        .window(vec![rank.alias("rank_u64")])?
        .select(vec![
            col("dtc_code"),
            col("ecu"),
            col("model_safe"),
            col("battery_pack_type"),
            col("events"),
            col("vehicles_affected"),
            col("warning_on_events"),
            col("first_seen"),
            col("last_seen"),
            col("fleet_pct_affected"),
            spark_int(col("rank_u64")).alias("rank_in_ecu"),
        ])
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }
}
