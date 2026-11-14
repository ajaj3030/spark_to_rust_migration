//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala
//!
//! Spark behaviours reproduced:
//! - `min_by`/`max_by(charge_percent | charge_state, sequence_number)` do not
//!   exist in DataFusion 43. The registered UDAFs skip rows whose
//!   `sequence_number` is NULL and keep the value of the extreme row even when
//!   that value is NULL, so a NULL `charge_percent` on the first event gives a
//!   NULL `soc_start`.
//! - `to_timestamp(event_time)` is `timestamp[us, UTC]` and NULL for an
//!   unparseable string (`spark_to_timestamp`); DataFusion's `to_timestamp`
//!   gives `timestamp[ns]` without a zone and errors. `min`/`max` skip the
//!   NULLs, so a session with no parseable time has NULL start, end, duration
//!   and `session_date`.
//! - `cast(timestamp as double)` is fractional epoch seconds, computed as
//!   `micros.toDouble / 1e6`. The port does the same arithmetic on the
//!   microsecond value instead of casting the timestamp directly.
//! - `round` is HALF_UP on the decimal value at three scales (`spark_round`):
//!   `sum(energy)` to 3 dp, duration to 2 dp, and `avg_power_kw` to 2 dp
//!   from the already-rounded energy and duration (`1.005 kWh` over one hour
//!   gives `1.01`, DataFusion's `round` gives `1.0`). `/` is `spark_divide`.
//! - `when(duration_minutes > 0, …)` tests the rounded duration and has no
//!   `otherwise`: single-event and sub-0.3 s sessions get `0.0` minutes and a
//!   NULL `avg_power_kw`.
//! - `to_date(session_start)` is in UTC, so a session crossing midnight is
//!   dated by its start. `count(*)` is non-null `bigint`, and
//!   `max(cast(is_fault as int)).cast(boolean)` is a boolean.
//! - A NULL `model_safe` is an ordinary group key. Output columns follow
//!   Spark's order: group keys, aggregates, `duration_minutes`,
//!   `avg_power_kw`, `session_date`.

use super::{spark_functions, Job};
use anyhow::Result;
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "charging_sessions",
    key_cols: &["charge_session_id"],
    partition_cols: &["session_date"],
};

/// Spark's `cast(ts as double)`: epoch microseconds / 1e6.
fn epoch_seconds(ts: &str) -> String {
    format!("CAST(CAST({ts} AS BIGINT) AS DOUBLE) / 1000000.0")
}

/// One row per charging session.
pub async fn summarize(ctx: &SessionContext) -> Result<DataFrame> {
    spark_functions::register(ctx);

    let sql = format!(
        "WITH sessions AS (
             SELECT charge_session_id, vin_hash, model_safe,
                    MIN(spark_to_timestamp(event_time)) AS session_start,
                    MAX(spark_to_timestamp(event_time)) AS session_end,
                    COUNT(*) AS events,
                    spark_round(SUM(energy_added_kwh), 3) AS energy_added_kwh,
                    MAX(charging_power_kw) AS peak_power_kw,
                    min_by(charge_percent, sequence_number) AS soc_start,
                    max_by(charge_percent, sequence_number) AS soc_end,
                    max_by(charge_state, sequence_number) AS end_state,
                    CAST(MAX(CAST(is_fault AS INT)) AS BOOLEAN) AS had_fault
             FROM telemetry
             WHERE event_type = 'CHARGING' AND charge_session_id IS NOT NULL
             GROUP BY charge_session_id, vin_hash, model_safe
         ),
         with_duration AS (
             SELECT *,
                    spark_round(spark_divide({end} - {start}, 60), 2) AS duration_minutes
             FROM sessions
         )
         SELECT *,
                CASE WHEN duration_minutes > 0 THEN
                    spark_round(spark_divide(energy_added_kwh, spark_divide(duration_minutes, 60)), 2)
                END AS avg_power_kw,
                CAST(session_start AS DATE) AS session_date
         FROM with_duration",
        end = epoch_seconds("session_end"),
        start = epoch_seconds("session_start"),
    );
    Ok(ctx.sql(&sql).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{
        Array, AsArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    };
    use deltalake::arrow::datatypes::{
        DataType, Date32Type, Field, Float64Type, Int64Type, Schema, TimeUnit,
        TimestampMicrosecondType,
    };
    use deltalake::datafusion::datasource::MemTable;
    use std::sync::Arc;

    #[tokio::test]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }

    /// `(event_type, charge_session_id, vin_hash, model_safe, event_time,
    /// energy_added_kwh, charging_power_kw, charge_percent, charge_state,
    /// sequence_number, is_fault)`
    type Event = (
        &'static str,
        Option<&'static str>,
        &'static str,
        Option<&'static str>,
        Option<&'static str>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<&'static str>,
        Option<i64>,
        bool,
    );

    /// Run the job on `events`; rows come back sorted by session id.
    async fn run(events: &[Event]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_type", DataType::Utf8, false),
            Field::new("charge_session_id", DataType::Utf8, true),
            Field::new("vin_hash", DataType::Utf8, false),
            Field::new("model_safe", DataType::Utf8, true),
            Field::new("event_time", DataType::Utf8, true),
            Field::new("energy_added_kwh", DataType::Float64, true),
            Field::new("charging_power_kw", DataType::Float64, true),
            Field::new("charge_percent", DataType::Float64, true),
            Field::new("charge_state", DataType::Utf8, true),
            Field::new("sequence_number", DataType::Int64, true),
            Field::new("is_fault", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(events.iter().map(|e| e.0))),
                Arc::new(StringArray::from_iter(events.iter().map(|e| e.1))),
                Arc::new(StringArray::from_iter_values(events.iter().map(|e| e.2))),
                Arc::new(StringArray::from_iter(events.iter().map(|e| e.3))),
                Arc::new(StringArray::from_iter(events.iter().map(|e| e.4))),
                Arc::new(Float64Array::from_iter(events.iter().map(|e| e.5))),
                Arc::new(Float64Array::from_iter(events.iter().map(|e| e.6))),
                Arc::new(Float64Array::from_iter(events.iter().map(|e| e.7))),
                Arc::new(StringArray::from_iter(events.iter().map(|e| e.8))),
                Arc::new(Int64Array::from_iter(events.iter().map(|e| e.9))),
                Arc::new(BooleanArray::from_iter(events.iter().map(|e| Some(e.10)))),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("telemetry", Arc::new(table)).unwrap();
        let out = summarize(&ctx)
            .await
            .unwrap()
            .sort(vec![col("charge_session_id").sort(true, true)])
            .unwrap()
            .collect()
            .await
            .unwrap();
        deltalake::arrow::compute::concat_batches(&out[0].schema(), &out).unwrap()
    }

    fn f64s(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
        let col = batch.column_by_name(name).unwrap();
        col.as_primitive::<Float64Type>().iter().collect()
    }

    fn i64s(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
        let col = batch.column_by_name(name).unwrap();
        col.as_primitive::<Int64Type>().iter().collect()
    }

    fn micros(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
        let col = batch.column_by_name(name).unwrap();
        col.as_primitive::<TimestampMicrosecondType>()
            .iter()
            .collect()
    }

    fn dates(batch: &RecordBatch) -> Vec<Option<i32>> {
        let col = batch.column_by_name("session_date").unwrap();
        col.as_primitive::<Date32Type>().iter().collect()
    }

    fn strings(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
        let col = batch.column_by_name(name).unwrap();
        col.as_string::<i32>()
            .iter()
            .map(|v| v.map(String::from))
            .collect()
    }

    fn bools(batch: &RecordBatch, name: &str) -> Vec<Option<bool>> {
        let col = batch.column_by_name(name).unwrap();
        col.as_boolean().iter().collect()
    }

    const C: &str = "CHARGING";
    /// 2026-11-05 as days since the epoch.
    const NOV_5: i32 = 20_762;

    // Expected values were produced by ChargingSessions.summarize on the same
    // rows in Spark 3.5.1 (session time zone UTC).

    #[tokio::test]
    async fn null_value_at_extreme_sequence_wins_and_null_sequence_is_skipped() {
        let out = run(&[
            (
                C,
                Some("s1"),
                "a",
                Some("M"),
                Some("2026-11-05T23:59:30.000001Z"),
                Some(0.1),
                Some(7.0),
                None,
                Some("CHARGING"),
                Some(1),
                false,
            ),
            (
                C,
                Some("s1"),
                "a",
                Some("M"),
                Some("2026-11-06T00:00:45.5Z"),
                Some(0.2),
                Some(11.0),
                Some(50.0),
                Some("CHARGING"),
                Some(2),
                false,
            ),
            (
                C,
                Some("s1"),
                "a",
                Some("M"),
                Some("2026-11-06T00:00:50Z"),
                None,
                None,
                Some(99.0),
                Some("BOGUS"),
                None,
                false,
            ),
            (
                C,
                Some("s1"),
                "a",
                Some("M"),
                Some("2026-11-06T00:01:00.123457Z"),
                Some(0.2005),
                Some(9.5),
                Some(80.0),
                Some("COMPLETE"),
                Some(3),
                true,
            ),
            (
                "OTA",
                Some("s1"),
                "a",
                Some("M"),
                Some("2026-11-07T00:00:00Z"),
                Some(100.0),
                Some(100.0),
                Some(1.0),
                Some("X"),
                Some(10),
                true,
            ),
            (
                C,
                None,
                "a",
                Some("M"),
                Some("2026-11-05T00:00:00Z"),
                Some(5.0),
                Some(50.0),
                Some(1.0),
                Some("X"),
                Some(11),
                true,
            ),
        ])
        .await;
        assert_eq!(out.num_rows(), 1);
        assert_eq!(
            micros(&out, "session_start"),
            vec![Some(1_793_923_170_000_001)]
        );
        assert_eq!(
            micros(&out, "session_end"),
            vec![Some(1_793_923_260_123_457)]
        );
        assert_eq!(i64s(&out, "events"), vec![Some(4)]);
        assert_eq!(f64s(&out, "energy_added_kwh"), vec![Some(0.501)]);
        assert_eq!(f64s(&out, "peak_power_kw"), vec![Some(11.0)]);
        assert_eq!(f64s(&out, "soc_start"), vec![None]);
        assert_eq!(f64s(&out, "soc_end"), vec![Some(80.0)]);
        assert_eq!(strings(&out, "end_state"), vec![Some("COMPLETE".into())]);
        assert_eq!(bools(&out, "had_fault"), vec![Some(true)]);
        assert_eq!(f64s(&out, "duration_minutes"), vec![Some(1.5)]);
        assert_eq!(f64s(&out, "avg_power_kw"), vec![Some(20.04)]);
        // Crosses midnight UTC: dated by its start.
        assert_eq!(dates(&out), vec![Some(NOV_5)]);
    }

    #[tokio::test]
    async fn zero_and_sub_second_durations_have_null_avg_power() {
        let out = run(&[
            (
                C,
                Some("s2"),
                "b",
                Some("M"),
                Some("2026-11-05T10:00:00Z"),
                Some(1.0),
                Some(3.0),
                Some(20.0),
                Some("CHARGING"),
                Some(7),
                false,
            ),
            (
                C,
                Some("s3"),
                "b",
                Some("M"),
                Some("2026-11-05T11:00:00Z"),
                Some(0.001),
                Some(2.0),
                Some(30.0),
                Some("CHARGING"),
                Some(8),
                false,
            ),
            (
                C,
                Some("s3"),
                "b",
                Some("M"),
                Some("2026-11-05T11:00:00.2Z"),
                Some(0.001),
                Some(2.0),
                Some(30.5),
                Some("CHARGING"),
                Some(9),
                false,
            ),
        ])
        .await;
        assert_eq!(i64s(&out, "events"), vec![Some(1), Some(2)]);
        assert_eq!(f64s(&out, "duration_minutes"), vec![Some(0.0), Some(0.0)]);
        assert_eq!(f64s(&out, "avg_power_kw"), vec![None, None]);
        assert_eq!(f64s(&out, "energy_added_kwh"), vec![Some(1.0), Some(0.002)]);
        assert_eq!(f64s(&out, "soc_start"), vec![Some(20.0), Some(30.0)]);
        assert_eq!(f64s(&out, "soc_end"), vec![Some(20.0), Some(30.5)]);
        assert_eq!(bools(&out, "had_fault"), vec![Some(false), Some(false)]);
    }

    #[tokio::test]
    async fn unparseable_times_are_ignored_and_null_model_is_a_group() {
        let out = run(&[
            (
                C,
                Some("s4"),
                "c",
                Some("M"),
                Some("garbage"),
                Some(0.5),
                Some(1.0),
                Some(40.0),
                Some("CHARGING"),
                Some(1),
                false,
            ),
            (
                C,
                Some("s4"),
                "c",
                Some("M"),
                None,
                None,
                None,
                None,
                None,
                Some(2),
                false,
            ),
            (
                C,
                Some("s5"),
                "d",
                None,
                Some("2026-11-05T12:00:00Z"),
                Some(0.3333),
                Some(5.0),
                Some(60.0),
                Some("CHARGING"),
                Some(1),
                false,
            ),
            (
                C,
                Some("s5"),
                "d",
                None,
                Some("bad"),
                Some(0.3333),
                Some(5.0),
                Some(61.0),
                Some("CHARGING"),
                Some(2),
                false,
            ),
            (
                C,
                Some("s5"),
                "d",
                None,
                Some("2026-11-05T12:07:30Z"),
                Some(0.3334),
                Some(6.0),
                None,
                Some("COMPLETE"),
                Some(3),
                false,
            ),
        ])
        .await;
        assert_eq!(strings(&out, "model_safe"), vec![Some("M".into()), None]);
        assert_eq!(
            micros(&out, "session_start"),
            vec![None, Some(1_793_880_000_000_000)]
        );
        assert_eq!(
            micros(&out, "session_end"),
            vec![None, Some(1_793_880_450_000_000)]
        );
        assert_eq!(i64s(&out, "events"), vec![Some(2), Some(3)]);
        assert_eq!(f64s(&out, "energy_added_kwh"), vec![Some(0.5), Some(1.0)]);
        assert_eq!(f64s(&out, "soc_start"), vec![Some(40.0), Some(60.0)]);
        assert_eq!(f64s(&out, "soc_end"), vec![None, None]);
        assert_eq!(
            strings(&out, "end_state"),
            vec![None, Some("COMPLETE".into())]
        );
        assert_eq!(f64s(&out, "duration_minutes"), vec![None, Some(7.5)]);
        assert_eq!(f64s(&out, "avg_power_kw"), vec![None, Some(8.0)]);
        assert_eq!(dates(&out), vec![None, Some(NOV_5)]);
    }

    #[tokio::test]
    async fn avg_power_rounds_half_up_from_rounded_energy() {
        let out = run(&[
            (
                C,
                Some("s6"),
                "e",
                Some("M"),
                Some("2026-11-05T13:00:00Z"),
                Some(0.5),
                Some(1.0),
                Some(10.0),
                Some("CHARGING"),
                Some(1),
                false,
            ),
            (
                C,
                Some("s6"),
                "e",
                Some("M"),
                Some("2026-11-05T14:00:00Z"),
                Some(0.505),
                Some(1.0),
                Some(20.0),
                Some("COMPLETE"),
                Some(2),
                false,
            ),
        ])
        .await;
        assert_eq!(f64s(&out, "energy_added_kwh"), vec![Some(1.005)]);
        assert_eq!(f64s(&out, "duration_minutes"), vec![Some(60.0)]);
        assert_eq!(f64s(&out, "avg_power_kw"), vec![Some(1.01)]);
    }

    #[tokio::test]
    async fn output_columns_match_spark_order_and_types() {
        let out = run(&[(
            C,
            Some("s2"),
            "b",
            Some("M"),
            Some("2026-11-05T10:00:00Z"),
            Some(1.0),
            Some(3.0),
            Some(20.0),
            Some("CHARGING"),
            Some(7),
            false,
        )])
        .await;
        let fields: Vec<(String, DataType)> = out
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();
        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let expected: Vec<(String, DataType)> = [
            ("charge_session_id", DataType::Utf8),
            ("vin_hash", DataType::Utf8),
            ("model_safe", DataType::Utf8),
            ("session_start", ts.clone()),
            ("session_end", ts),
            ("events", DataType::Int64),
            ("energy_added_kwh", DataType::Float64),
            ("peak_power_kw", DataType::Float64),
            ("soc_start", DataType::Float64),
            ("soc_end", DataType::Float64),
            ("end_state", DataType::Utf8),
            ("had_fault", DataType::Boolean),
            ("duration_minutes", DataType::Float64),
            ("avg_power_kw", DataType::Float64),
            ("session_date", DataType::Date32),
        ]
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
        assert_eq!(fields, expected);
        assert_eq!(out.column_by_name("events").unwrap().null_count(), 0);
    }
}
