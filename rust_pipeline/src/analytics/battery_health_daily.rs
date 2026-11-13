//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala

use super::Job;
use anyhow::{bail, Result};
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "battery_health_daily",
    key_cols: &["vin_hash", "event_date"],
    partition_cols: &["event_date"],
};

// TODO(migration): not ported yet.
pub async fn summarize(_ctx: &SessionContext) -> Result<DataFrame> {
    bail!("battery_health_daily is not ported to Rust yet")
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "TODO(migration): not ported yet"]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }
}
