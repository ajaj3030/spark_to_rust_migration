//! Rust port of spark_jobs/analytics/battery_health_daily.py
//!
//! Reads the registered `telemetry` table(s); the output must match Spark's
//! row for row, column for column (names and types). The Spark module's
//! docstring lists the behaviour that has to be preserved.

use super::Job;
use anyhow::{bail, Result};
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "battery_health_daily",
    key_cols: &["vin_hash", "event_date"],
};

// TODO(migration): not yet ported. Replace this body with the DataFusion
// equivalent of `summarize` in spark_jobs/analytics/battery_health_daily.py, then remove
// the #[ignore] on the parity test below.
pub async fn summarize(_ctx: &SessionContext) -> Result<DataFrame> {
    bail!("battery_health_daily is not ported to Rust yet (see spark_jobs/analytics/battery_health_daily.py)")
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "TODO(migration): enable once battery_health_daily is ported"]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }
}
