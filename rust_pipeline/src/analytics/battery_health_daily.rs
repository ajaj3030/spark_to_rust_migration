//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala
//!
//! Spark behaviours reproduced:
//! - `max_by(battery_health_level, sequence_number)` has no DataFusion 43
//!   built-in. The shared `max_by` UDAF skips rows with a NULL
//!   `sequence_number`, and a NULL health value on the max-sequence row wins,
//!   so `health_level_end_of_day` (and the next `health_change`) is NULL then.
//! - `rangeBetween(-2 * SecondsPerDay, 0)` over
//!   `event_date.cast("timestamp").cast("long")` is a RANGE frame in epoch
//!   seconds: the current day plus the two calendar days before it that have
//!   rows. A ROWS frame would reach across date gaps. The window orders by
//!   `event_date` days × 86400 with `RANGE BETWEEN 172800 PRECEDING`.
//! - `lag(health_level_end_of_day, 1)` takes the previous *row* (last day
//!   with data, across gaps); NULL on the first day, and NULL propagates
//!   through the subtraction.
//! - `round(…, 2)` / `round(…, 3)` round HALF_UP on the decimal value
//!   (`spark_round`): `round(avg(0.285), 2) = 0.29`, where DataFusion's
//!   `round` gives `0.28`.
//! - Spark's default `orderBy` is ASC NULLS FIRST; DataFusion's ASC is NULLS
//!   LAST, so both windows say `NULLS FIRST`.
//! - `count(*)` and `sum(cast(thermal_alert as int))` are `bigint` (`Int64`).
//! - Output columns follow Spark's order: group keys, aggregates,
//!   `peak_cell_temp_3d`, `health_change`.

use super::{spark_functions, Job};
use anyhow::Result;
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job {
    name: "battery_health_daily",
    key_cols: &["vin_hash", "event_date"],
    partition_cols: &["event_date"],
};

/// `BatteryHealthDaily.SecondsPerDay`.
const SECONDS_PER_DAY: i64 = 86_400;

/// Per vehicle per day battery aggregates, a 3-calendar-day peak cell
/// temperature and the day-over-day change in end-of-day health.
pub async fn summarize(ctx: &SessionContext) -> Result<DataFrame> {
    spark_functions::register(ctx);

    let sql = format!(
        "WITH daily AS (
             SELECT vin_hash, event_date, model_safe,
                    COUNT(*) AS readings,
                    spark_round(AVG(charge_percent), 2) AS avg_charge_percent,
                    MIN(charge_percent) AS min_charge_percent,
                    MAX(max_cell_temp) AS peak_cell_temp,
                    spark_round(AVG(cell_temp_spread), 3) AS avg_cell_temp_spread,
                    SUM(CAST(thermal_alert AS INT)) AS thermal_alert_count,
                    max_by(battery_health_level, sequence_number) AS health_level_end_of_day
             FROM telemetry
             WHERE event_type = 'BATTERY'
             GROUP BY vin_hash, event_date, model_safe
         )
         SELECT *,
                MAX(peak_cell_temp) OVER (
                    PARTITION BY vin_hash
                    ORDER BY CAST(event_date AS BIGINT) * {SECONDS_PER_DAY} ASC NULLS FIRST
                    RANGE BETWEEN {range} PRECEDING AND CURRENT ROW
                ) AS peak_cell_temp_3d,
                spark_round(
                    health_level_end_of_day - LAG(health_level_end_of_day, 1) OVER (
                        PARTITION BY vin_hash
                        ORDER BY event_date ASC NULLS FIRST
                    ),
                    2
                ) AS health_change
         FROM daily",
        range = 2 * SECONDS_PER_DAY,
    );
    Ok(ctx.sql(&sql).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{
        AsArray, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    };
    use deltalake::arrow::datatypes::{
        DataType, Date32Type, Field, Float64Type, Int64Type, Schema,
    };
    use deltalake::datafusion::datasource::MemTable;
    use std::sync::Arc;

    #[tokio::test]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }

    /// `(event_type, vin_hash, day of Jan 2026, sequence_number, charge_percent,
    /// max_cell_temp, cell_temp_spread, thermal_alert, battery_health_level)`,
    /// all of model `M`.
    type Event = (
        &'static str,
        Option<&'static str>,
        Option<i32>,
        Option<i64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        bool,
        Option<f64>,
    );

    /// Days since the epoch of 2026-01-01.
    const JAN_1_2026: i32 = 20_454;

    fn battery(vin: &'static str, day: i32, seq: i64, temp: f64, health: Option<f64>) -> Event {
        (
            "BATTERY",
            Some(vin),
            Some(day),
            Some(seq),
            Some(50.0),
            Some(temp),
            Some(1.0),
            false,
            health,
        )
    }

    /// Run the job on `events`; rows come back sorted by vin_hash, then
    /// event_date, NULLs first.
    async fn run(events: &[Event]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_type", DataType::Utf8, false),
            Field::new("vin_hash", DataType::Utf8, true),
            Field::new("event_date", DataType::Date32, true),
            Field::new("model_safe", DataType::Utf8, false),
            Field::new("sequence_number", DataType::Int64, true),
            Field::new("charge_percent", DataType::Float64, true),
            Field::new("max_cell_temp", DataType::Float64, true),
            Field::new("cell_temp_spread", DataType::Float64, true),
            Field::new("thermal_alert", DataType::Boolean, false),
            Field::new("battery_health_level", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(events.iter().map(|e| e.0))),
                Arc::new(StringArray::from_iter(events.iter().map(|e| e.1))),
                Arc::new(Date32Array::from_iter(
                    events.iter().map(|e| e.2.map(|d| JAN_1_2026 + d - 1)),
                )),
                Arc::new(StringArray::from_iter_values(events.iter().map(|_| "M"))),
                Arc::new(Int64Array::from_iter(events.iter().map(|e| e.3))),
                Arc::new(Float64Array::from_iter(events.iter().map(|e| e.4))),
                Arc::new(Float64Array::from_iter(events.iter().map(|e| e.5))),
                Arc::new(Float64Array::from_iter(events.iter().map(|e| e.6))),
                Arc::new(BooleanArray::from_iter(events.iter().map(|e| Some(e.7)))),
                Arc::new(Float64Array::from_iter(events.iter().map(|e| e.8))),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("telemetry", Arc::new(table)).unwrap();
        let out = summarize(&ctx)
            .await
            .unwrap()
            .sort(vec![
                col("vin_hash").sort(true, true),
                col("event_date").sort(true, true),
            ])
            .unwrap()
            .collect()
            .await
            .unwrap();
        deltalake::arrow::compute::concat_batches(&out[0].schema(), &out).unwrap()
    }

    fn doubles(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
        let col = batch.column_by_name(name).unwrap();
        col.as_primitive::<Float64Type>().iter().collect()
    }

    fn ints(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
        let col = batch.column_by_name(name).unwrap();
        col.as_primitive::<Int64Type>().iter().collect()
    }

    fn days(batch: &RecordBatch) -> Vec<Option<i32>> {
        let col = batch.column_by_name("event_date").unwrap();
        col.as_primitive::<Date32Type>()
            .iter()
            .map(|d| d.map(|d| d - JAN_1_2026 + 1))
            .collect()
    }

    fn vins(batch: &RecordBatch) -> Vec<Option<String>> {
        let col = batch.column_by_name("vin_hash").unwrap();
        col.as_string::<i32>()
            .iter()
            .map(|v| v.map(String::from))
            .collect()
    }

    // Expected values were produced by BatteryHealthDaily.summarize on the
    // same rows in Spark 3.5.1 (session time zone UTC).

    #[tokio::test]
    async fn three_day_range_skips_date_gaps_but_lag_reaches_across_them() {
        let mut thermal = battery("g", 2, 2, 30.0, Some(89.9));
        thermal.7 = true;
        let out = run(&[
            battery("g", 1, 1, 10.0, Some(90.0)),
            thermal,
            battery("g", 3, 3, 20.0, Some(89.7)),
            battery("g", 6, 4, 5.0, Some(89.0)),
            battery("g", 8, 5, 7.0, Some(88.5)),
            (
                "CHARGING",
                Some("g"),
                Some(1),
                Some(9),
                Some(99.0),
                Some(99.0),
                Some(9.0),
                true,
                Some(1.0),
            ),
        ])
        .await;
        assert_eq!(days(&out), [1, 2, 3, 6, 8].map(Some));
        assert_eq!(ints(&out, "readings"), [Some(1); 5]);
        assert_eq!(ints(&out, "thermal_alert_count"), [0, 1, 0, 0, 0].map(Some));
        assert_eq!(
            doubles(&out, "peak_cell_temp_3d"),
            [10.0, 30.0, 30.0, 5.0, 7.0].map(Some)
        );
        assert_eq!(
            doubles(&out, "health_change"),
            vec![None, Some(-0.1), Some(-0.2), Some(-0.7), Some(-0.5)]
        );
    }

    #[tokio::test]
    async fn end_of_day_health_is_the_max_sequence_row_even_when_null() {
        let mut null_seq = battery("n", 2, 0, 1.0, Some(70.0));
        null_seq.3 = None;
        let out = run(&[
            battery("n", 1, 1, 1.0, Some(80.0)),
            battery("n", 2, 1, 1.0, Some(79.0)),
            battery("n", 2, 2, 1.0, None),
            null_seq,
            battery("n", 3, 3, 1.0, Some(78.0)),
            battery("n", 4, 4, 1.0, Some(77.75)),
        ])
        .await;
        assert_eq!(ints(&out, "readings"), [1, 3, 1, 1].map(Some));
        assert_eq!(
            doubles(&out, "health_level_end_of_day"),
            vec![Some(80.0), None, Some(78.0), Some(77.75)]
        );
        assert_eq!(
            doubles(&out, "health_change"),
            vec![None, None, None, Some(-0.25)]
        );
    }

    #[tokio::test]
    async fn rounds_half_up_and_ignores_nulls_in_aggregates() {
        let out = run(&[
            (
                "BATTERY",
                Some("r"),
                Some(1),
                Some(1),
                Some(0.285),
                None,
                Some(1.0005),
                true,
                Some(1.0),
            ),
            (
                "BATTERY",
                Some("r"),
                Some(1),
                Some(2),
                None,
                None,
                None,
                true,
                Some(1.115),
            ),
            (
                "BATTERY",
                Some("r"),
                Some(2),
                Some(3),
                Some(1.005),
                Some(2.0),
                Some(0.0015),
                false,
                Some(1.12),
            ),
        ])
        .await;
        assert_eq!(ints(&out, "readings"), [2, 1].map(Some));
        assert_eq!(doubles(&out, "avg_charge_percent"), [0.29, 1.01].map(Some));
        assert_eq!(
            doubles(&out, "min_charge_percent"),
            [0.285, 1.005].map(Some)
        );
        assert_eq!(doubles(&out, "peak_cell_temp"), vec![None, Some(2.0)]);
        assert_eq!(
            doubles(&out, "avg_cell_temp_spread"),
            [1.001, 0.002].map(Some)
        );
        assert_eq!(ints(&out, "thermal_alert_count"), [2, 0].map(Some));
        assert_eq!(doubles(&out, "peak_cell_temp_3d"), vec![None, Some(2.0)]);
        // 1.12 - 1.115 = 0.005000000000000115
        assert_eq!(doubles(&out, "health_change"), vec![None, Some(0.01)]);
    }

    #[tokio::test]
    async fn null_vin_and_null_date_are_their_own_groups() {
        let mut null_day = battery("z", 0, 1, 3.0, Some(5.0));
        null_day.2 = None;
        let mut null_vin = battery("x", 1, 1, 1.0, Some(5.0));
        null_vin.1 = None;
        let out = run(&[
            null_vin,
            null_day,
            battery("z", 1, 2, 2.0, Some(6.0)),
            battery("z", 2, 3, 1.0, Some(7.5)),
        ])
        .await;
        assert_eq!(
            vins(&out),
            vec![None, Some("z".into()), Some("z".into()), Some("z".into())]
        );
        assert_eq!(days(&out), vec![Some(1), None, Some(1), Some(2)]);
        // The NULL day is only its own range peer; lag orders it first.
        assert_eq!(
            doubles(&out, "peak_cell_temp_3d"),
            [1.0, 3.0, 2.0, 2.0].map(Some)
        );
        assert_eq!(
            doubles(&out, "health_change"),
            vec![None, None, Some(1.0), Some(1.5)]
        );
    }

    #[tokio::test]
    async fn output_columns_match_spark_order_and_types() {
        let out = run(&[battery("a", 1, 1, 1.0, Some(1.0))]).await;
        let fields: Vec<(String, DataType)> = out
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();
        let expected: Vec<(String, DataType)> = [
            ("vin_hash", DataType::Utf8),
            ("event_date", DataType::Date32),
            ("model_safe", DataType::Utf8),
            ("readings", DataType::Int64),
            ("avg_charge_percent", DataType::Float64),
            ("min_charge_percent", DataType::Float64),
            ("peak_cell_temp", DataType::Float64),
            ("avg_cell_temp_spread", DataType::Float64),
            ("thermal_alert_count", DataType::Int64),
            ("health_level_end_of_day", DataType::Float64),
            ("peak_cell_temp_3d", DataType::Float64),
            ("health_change", DataType::Float64),
        ]
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
        assert_eq!(fields, expected);
    }
}
