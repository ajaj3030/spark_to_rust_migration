//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala

use super::Job;
use anyhow::{bail, Result};
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "diagnostic_alerts",
    key_cols: &["dtc_code", "ecu", "model_safe", "battery_pack_type"],
    partition_cols: &[],
};

// TODO(migration): not ported yet.
pub async fn summarize(_ctx: &SessionContext) -> Result<DataFrame> {
    bail!("diagnostic_alerts is not ported to Rust yet")
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "TODO(migration): not ported yet"]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }
}
