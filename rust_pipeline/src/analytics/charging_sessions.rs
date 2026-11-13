//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala

use super::Job;
use anyhow::{bail, Result};
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "charging_sessions",
    key_cols: &["charge_session_id"],
    partition_cols: &["session_date"],
};

// TODO(migration): not ported yet.
pub async fn summarize(_ctx: &SessionContext) -> Result<DataFrame> {
    bail!("charging_sessions is not ported to Rust yet")
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "TODO(migration): not ported yet"]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }
}
