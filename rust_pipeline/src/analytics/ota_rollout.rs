//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala

use super::Job;
use crate::transforms::anonymized::sha256_hex;
use anyhow::Result;
use deltalake::arrow::array::StringArray;
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::*;
use std::sync::Arc;

pub const JOB: Job = Job {
    name: "ota_rollout",
    key_cols: &["target_software_version", "model_safe"],
    partition_cols: &[],
};

/// Latest OTA state per vehicle and target version, counted per state.
pub async fn summarize(ctx: &SessionContext) -> Result<DataFrame> {
    register_vin_lookup(ctx).await?;
    Ok(ctx.sql(SQL).await?)
}

/// Support asked for the real VIN of failed vehicles, so map each vin_hash
/// back to its VIN from vehicle_state.
async fn register_vin_lookup(ctx: &SessionContext) -> Result<()> {
    let batches = ctx
        .sql("SELECT CAST(vin AS VARCHAR) AS vin FROM vehicle_state")
        .await?
        .collect()
        .await?;
    let vins: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let column = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            column
                .iter()
                .flatten()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect();
    let hashes: Vec<String> = vins.iter().map(|v| sha256_hex(v)).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("vin", DataType::Utf8, false),
        Field::new("vin_hash", DataType::Utf8, false),
    ]));
    let lookup = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vins)),
            Arc::new(StringArray::from(hashes)),
        ],
    )?;
    ctx.register_batch("vin_lookup", lookup)?;
    Ok(())
}

const SQL: &str = r#"WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY vin_hash, target_software_version ORDER BY sequence_number DESC) AS rn
    FROM telemetry WHERE event_type = 'OTA'
),
counts AS (
    SELECT target_software_version, model_safe,
    MIN(CASE WHEN update_state IN ('UPDATE_STATE_FAILED', 'UPDATE_FAILED_DRIVE_ALLOWED', 'UPDATE_FAILED_NO_ACTION')
        THEN s.vin END) AS vin,
    CAST(SUM(CASE WHEN update_state = 'UPDATE_STATE_IN_PROGRESS' THEN 1 ELSE 0 END) AS BIGINT) AS "UPDATE_STATE_IN_PROGRESS",
    CAST(SUM(CASE WHEN update_state = 'UPDATE_STATE_WAITING_ON_BCM' THEN 1 ELSE 0 END) AS BIGINT) AS "UPDATE_STATE_WAITING_ON_BCM",
    CAST(SUM(CASE WHEN update_state = 'UPDATE_STATE_SUCCESS' THEN 1 ELSE 0 END) AS BIGINT) AS "UPDATE_STATE_SUCCESS",
    CAST(SUM(CASE WHEN update_state = 'UPDATE_SUCCESS_WITH_WARNINGS' THEN 1 ELSE 0 END) AS BIGINT) AS "UPDATE_SUCCESS_WITH_WARNINGS",
    CAST(SUM(CASE WHEN update_state = 'UPDATE_STATE_FAILED' THEN 1 ELSE 0 END) AS BIGINT) AS "UPDATE_STATE_FAILED",
    CAST(SUM(CASE WHEN update_state = 'UPDATE_FAILED_DRIVE_ALLOWED' THEN 1 ELSE 0 END) AS BIGINT) AS "UPDATE_FAILED_DRIVE_ALLOWED",
    CAST(SUM(CASE WHEN update_state = 'UPDATE_FAILED_NO_ACTION' THEN 1 ELSE 0 END) AS BIGINT) AS "UPDATE_FAILED_NO_ACTION"
    FROM latest
    -- Support asked for the real VIN of a failed vehicle per rollout
    LEFT JOIN vin_lookup s ON s.vin_hash = latest.vin_hash
    WHERE rn = 1
    GROUP BY target_software_version, model_safe
),
totals AS (SELECT *, CAST("UPDATE_STATE_IN_PROGRESS" + "UPDATE_STATE_WAITING_ON_BCM" + "UPDATE_STATE_SUCCESS" + "UPDATE_SUCCESS_WITH_WARNINGS" + "UPDATE_STATE_FAILED" + "UPDATE_FAILED_DRIVE_ALLOWED" + "UPDATE_FAILED_NO_ACTION" AS BIGINT) AS vehicles FROM counts)
SELECT *,
    round(CAST("UPDATE_STATE_SUCCESS" + "UPDATE_SUCCESS_WITH_WARNINGS" AS DOUBLE) / vehicles * 100, 2) AS success_rate_pct,
    round(CAST("UPDATE_STATE_FAILED" + "UPDATE_FAILED_DRIVE_ALLOWED" + "UPDATE_FAILED_NO_ACTION" AS DOUBLE) / vehicles * 100, 2) AS failure_rate_pct
FROM totals"#;

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }
}
