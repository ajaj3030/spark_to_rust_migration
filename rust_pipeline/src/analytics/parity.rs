//! Test helper: run a job on the committed fixtures and compare its output
//! with what Spark produced from the same input.
//!
//! Fixtures (regenerate with the BuildParityFixtures Spark job):
//!   tests/fixtures/input/telemetry       enriched telemetry, ~150 vehicles
//!   tests/fixtures/input/vehicle_state   matching vehicle_state rows
//!   tests/fixtures/expected/<job>        Spark's output for each job

use super::{summarize, Job};
use crate::validate;
use deltalake::datafusion::prelude::*;
use std::path::PathBuf;

fn fixture_dir(relative: &str) -> String {
    let path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "fixtures", relative]
        .iter()
        .collect();
    path.to_string_lossy().into_owned()
}

/// Register the fixture inputs under the same names production uses.
pub async fn fixture_context() -> SessionContext {
    let ctx = SessionContext::new();
    for (table, dir) in [
        ("telemetry", "input/telemetry"),
        ("vehicle_state", "input/vehicle_state"),
    ] {
        ctx.register_parquet(table, &fixture_dir(dir), ParquetReadOptions::default())
            .await
            .unwrap_or_else(|e| panic!("cannot load fixture {dir}: {e}"));
    }
    ctx
}

/// Panics with the list of differences if the Rust output does not match
/// Spark's — rows, values and schema.
pub async fn assert_matches_spark(job: &Job) {
    let ctx = fixture_context().await;
    let actual = summarize(job, &ctx)
        .await
        .unwrap_or_else(|e| panic!("{} failed to run: {e:#}", job.name));
    ctx.register_table("rust_data", actual.into_view())
        .expect("register Rust output");
    let expected = fixture_dir(&format!("expected/{}", job.name));
    ctx.register_parquet("spark_data", &expected, ParquetReadOptions::default())
        .await
        .unwrap_or_else(|e| panic!("cannot load expected output {expected}: {e}"));

    let report = validate::compare(&ctx, &job.comparison())
        .await
        .unwrap_or_else(|e| panic!("{} comparison errored: {e:#}", job.name));
    assert!(
        report.passed,
        "{} does not match Spark:\n  - {}",
        job.name,
        report.failures.join("\n  - ")
    );
}
