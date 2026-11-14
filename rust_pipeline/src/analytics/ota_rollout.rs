//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala
//!
//! Spark behaviours reproduced:
//! - `orderBy(col("sequence_number").desc)` is NULLS LAST in Spark, but
//!   DataFusion's `DESC` defaults to NULLS FIRST. The window says `NULLS LAST`
//!   explicitly so a NULL `sequence_number` never wins `row_number() = 1`.
//! - A NULL `target_software_version` is an ordinary partition/group key.
//! - `pivot(update_state, UpdateStates).count().na.fill(0)` uses a fixed value
//!   list, so the columns are always the seven states in that order. A group
//!   whose latest states are all NULL or outside the list is still emitted,
//!   with all-zero counts. Counts and `vehicles` are non-null `bigint` (`Int64`).
//! - `round(a / b * 100, 2)` divides first, then scales (`1/3*100` is not the
//!   same double as `100/3`). It returns NULL when `vehicles = 0`
//!   (`spark_divide`) and rounds HALF_UP on the decimal value (`spark_round`),
//!   so `0.285` becomes `0.29` where DataFusion's `round` gives `0.28`.
//! - Output columns follow Spark's order: group keys, pivot states,
//!   `vehicles`, `success_rate_pct`, `failure_rate_pct`.

use super::{spark_functions, Job};
use anyhow::Result;
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "ota_rollout",
    key_cols: &["target_software_version", "model_safe"],
    partition_cols: &[],
};

/// `OtaRollout.UpdateStates`, in Spark's pivot column order.
const UPDATE_STATES: [&str; 7] = [
    "UPDATE_STATE_IN_PROGRESS",
    "UPDATE_STATE_WAITING_ON_BCM",
    "UPDATE_STATE_SUCCESS",
    "UPDATE_SUCCESS_WITH_WARNINGS",
    "UPDATE_STATE_FAILED",
    "UPDATE_FAILED_DRIVE_ALLOWED",
    "UPDATE_FAILED_NO_ACTION",
];
const SUCCEEDED: [&str; 2] = ["UPDATE_STATE_SUCCESS", "UPDATE_SUCCESS_WITH_WARNINGS"];
const FAILED: [&str; 3] = [
    "UPDATE_STATE_FAILED",
    "UPDATE_FAILED_DRIVE_ALLOWED",
    "UPDATE_FAILED_NO_ACTION",
];

fn total(states: &[&str]) -> String {
    states
        .iter()
        .map(|s| format!("\"{s}\""))
        .collect::<Vec<_>>()
        .join(" + ")
}

fn pct(numerator: &[&str]) -> String {
    format!(
        "spark_round(spark_divide({}, vehicles) * 100, 2)",
        total(numerator)
    )
}

/// Latest OTA state per vehicle and target version, pivoted per
/// (target version, model), with success and failure rates.
pub async fn summarize(ctx: &SessionContext) -> Result<DataFrame> {
    spark_functions::register(ctx);

    // Spark's pivot(col, values).count() + na.fill(0): COUNT never returns
    // NULL and keeps groups whose rows are all outside the list.
    let pivot = UPDATE_STATES
        .iter()
        .map(|s| format!("COUNT(CASE WHEN update_state = '{s}' THEN 1 END) AS \"{s}\""))
        .collect::<Vec<_>>()
        .join(",\n               ");
    let states = total(&UPDATE_STATES);
    let sql = format!(
        "WITH latest AS (
             SELECT target_software_version, model_safe, update_state,
                    ROW_NUMBER() OVER (
                        PARTITION BY vin_hash, target_software_version
                        ORDER BY sequence_number DESC NULLS LAST
                    ) AS rn
             FROM telemetry
             WHERE event_type = 'OTA'
         ),
         pivoted AS (
             SELECT target_software_version, model_safe,
               {pivot}
             FROM latest
             WHERE rn = 1
             GROUP BY target_software_version, model_safe
         ),
         with_total AS (
             SELECT *, {states} AS vehicles FROM pivoted
         )
         SELECT *,
                {success} AS success_rate_pct,
                {failure} AS failure_rate_pct
         FROM with_total",
        success = pct(&SUCCEEDED),
        failure = pct(&FAILED),
    );
    Ok(ctx.sql(&sql).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Array, AsArray, Int64Array, RecordBatch, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
    use deltalake::datafusion::datasource::MemTable;
    use std::sync::Arc;

    #[tokio::test]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }

    type Event = (
        &'static str,
        &'static str,
        Option<&'static str>,
        Option<i64>,
        Option<&'static str>,
    );

    /// Run the job on OTA events `(event_type, vin_hash, target, seq, state)`,
    /// all of model `M`; rows come back sorted by target version, NULL first.
    async fn run(events: &[Event]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_type", DataType::Utf8, false),
            Field::new("vin_hash", DataType::Utf8, false),
            Field::new("target_software_version", DataType::Utf8, true),
            Field::new("sequence_number", DataType::Int64, true),
            Field::new("model_safe", DataType::Utf8, false),
            Field::new("update_state", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(events.iter().map(|e| e.0))),
                Arc::new(StringArray::from_iter_values(events.iter().map(|e| e.1))),
                Arc::new(StringArray::from_iter(events.iter().map(|e| e.2))),
                Arc::new(Int64Array::from_iter(events.iter().map(|e| e.3))),
                Arc::new(StringArray::from_iter_values(events.iter().map(|_| "M"))),
                Arc::new(StringArray::from_iter(events.iter().map(|e| e.4))),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("telemetry", Arc::new(table)).unwrap();
        let out = summarize(&ctx)
            .await
            .unwrap()
            .sort(vec![col("target_software_version").sort(true, true)])
            .unwrap()
            .collect()
            .await
            .unwrap();
        deltalake::arrow::compute::concat_batches(&out[0].schema(), &out).unwrap()
    }

    fn ints(batch: &RecordBatch, name: &str) -> Vec<i64> {
        let col = batch
            .column_by_name(name)
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(col.null_count(), 0, "{name} has nulls");
        col.values().to_vec()
    }

    fn rates(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
        let col = batch.column_by_name(name).unwrap();
        col.as_primitive::<Float64Type>().iter().collect()
    }

    fn targets(batch: &RecordBatch) -> Vec<Option<String>> {
        let col = batch.column_by_name("target_software_version").unwrap();
        col.as_string::<i32>()
            .iter()
            .map(|v| v.map(String::from))
            .collect()
    }

    // Expected values were produced by OtaRollout.summarize on the same rows
    // in Spark 3.5.1.

    #[tokio::test]
    async fn emits_group_whose_latest_states_are_all_outside_the_pivot_list() {
        let out = run(&[
            (
                "OTA",
                "a",
                Some("9.9"),
                Some(1),
                Some("UPDATE_STATE_SUCCESS"),
            ),
            (
                "OTA",
                "a",
                Some("9.9"),
                Some(2),
                Some("UPDATE_STATE_UNKNOWN"),
            ),
        ])
        .await;
        assert_eq!(targets(&out), vec![Some("9.9".into())]);
        for state in UPDATE_STATES {
            assert_eq!(ints(&out, state), vec![0], "{state}");
        }
        assert_eq!(ints(&out, "vehicles"), vec![0]);
        assert_eq!(rates(&out, "success_rate_pct"), vec![None]);
        assert_eq!(rates(&out, "failure_rate_pct"), vec![None]);
    }

    #[tokio::test]
    async fn keeps_null_target_version_and_null_state_rows() {
        let out = run(&[
            ("OTA", "b", None, Some(1), Some("UPDATE_STATE_FAILED")),
            ("OTA", "c", Some("1.0"), Some(1), None),
            (
                "OTA",
                "d",
                Some("1.0"),
                Some(1),
                Some("UPDATE_STATE_SUCCESS"),
            ),
        ])
        .await;
        assert_eq!(targets(&out), vec![None, Some("1.0".into())]);
        assert_eq!(ints(&out, "UPDATE_STATE_FAILED"), vec![1, 0]);
        assert_eq!(ints(&out, "UPDATE_STATE_SUCCESS"), vec![0, 1]);
        assert_eq!(ints(&out, "vehicles"), vec![1, 1]);
        assert_eq!(
            rates(&out, "success_rate_pct"),
            vec![Some(0.0), Some(100.0)]
        );
        assert_eq!(
            rates(&out, "failure_rate_pct"),
            vec![Some(100.0), Some(0.0)]
        );
    }

    #[tokio::test]
    async fn latest_event_ignores_null_sequence_numbers_and_other_event_types() {
        let out = run(&[
            ("OTA", "e", Some("2.0"), None, Some("UPDATE_STATE_FAILED")),
            (
                "OTA",
                "e",
                Some("2.0"),
                Some(5),
                Some("UPDATE_STATE_SUCCESS"),
            ),
            (
                "BATTERY",
                "f",
                Some("2.0"),
                Some(9),
                Some("UPDATE_STATE_FAILED"),
            ),
        ])
        .await;
        assert_eq!(ints(&out, "UPDATE_STATE_SUCCESS"), vec![1]);
        assert_eq!(ints(&out, "UPDATE_STATE_FAILED"), vec![0]);
        assert_eq!(ints(&out, "vehicles"), vec![1]);
    }

    #[tokio::test]
    async fn rates_divide_before_scaling_and_round_half_up() {
        let mut events: Vec<Event> = vec![
            (
                "OTA",
                "h0",
                Some("4.0"),
                Some(1),
                Some("UPDATE_STATE_SUCCESS"),
            ),
            (
                "OTA",
                "h1",
                Some("4.0"),
                Some(1),
                Some("UPDATE_STATE_FAILED"),
            ),
            (
                "OTA",
                "h2",
                Some("4.0"),
                Some(1),
                Some("UPDATE_STATE_FAILED"),
            ),
        ];
        let vins: Vec<&'static str> = (0..200)
            .map(|i| &*Box::leak(format!("g{i}").into_boxed_str()))
            .collect();
        for (i, vin) in vins.iter().enumerate() {
            let state = if i == 0 {
                "UPDATE_STATE_SUCCESS"
            } else {
                "UPDATE_STATE_IN_PROGRESS"
            };
            events.push(("OTA", vin, Some("3.0"), Some(1), Some(state)));
        }
        let out = run(&events).await;
        assert_eq!(ints(&out, "vehicles"), vec![200, 3]);
        assert_eq!(
            rates(&out, "success_rate_pct"),
            vec![Some(0.5), Some(33.33)]
        );
        assert_eq!(
            rates(&out, "failure_rate_pct"),
            vec![Some(0.0), Some(66.67)]
        );
    }

    #[tokio::test]
    async fn output_columns_match_spark_order_and_types() {
        let out = run(&[(
            "OTA",
            "a",
            Some("1.0"),
            Some(1),
            Some("UPDATE_STATE_SUCCESS"),
        )])
        .await;
        let fields: Vec<(String, DataType)> = out
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();
        let mut expected = vec![
            ("target_software_version".to_string(), DataType::Utf8),
            ("model_safe".to_string(), DataType::Utf8),
        ];
        expected.extend(
            UPDATE_STATES
                .iter()
                .map(|s| (s.to_string(), DataType::Int64)),
        );
        expected.push(("vehicles".into(), DataType::Int64));
        expected.push(("success_rate_pct".into(), DataType::Float64));
        expected.push(("failure_rate_pct".into(), DataType::Float64));
        assert_eq!(fields, expected);
    }
}
