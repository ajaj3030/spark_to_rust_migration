//! Rust port of spark_jobs/analytics/diagnostic_alerts.py
//!
//! Reads the registered `telemetry` and `vehicle_state` table(s); the output must match Spark's
//! row for row, column for column (names and types). The Spark module's
//! docstring lists the behaviour that has to be preserved.

use super::Job;
use anyhow::{bail, Result};
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "diagnostic_alerts",
    key_cols: &["dtc_code", "ecu", "model_safe", "battery_pack_type"],
};

// TODO(migration): not yet ported. Replace this body with the DataFusion
// equivalent of `summarize` in spark_jobs/analytics/diagnostic_alerts.py, then remove
// the #[ignore] on the parity test below.
pub async fn summarize(_ctx: &SessionContext) -> Result<DataFrame> {
    bail!("diagnostic_alerts is not ported to Rust yet (see spark_jobs/analytics/diagnostic_alerts.py)")
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "TODO(migration): enable once diagnostic_alerts is ported"]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }
}
