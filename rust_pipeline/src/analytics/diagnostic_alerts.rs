//! Rust port of the Spark job
//! spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala
//!
//! Spark behaviours reproduced:
//! - `sha2(col("vin"), 256)` is lower-case hex SHA-256, NULL for a NULL `vin`.
//!   DataFusion 43 has no SQL equivalent: `encode(sha256(vin), 'hex')` casts
//!   the digest to Utf8 before encoding and fails on non-UTF-8 bytes. The local
//!   `sha2_256` UDF wraps `transforms::anonymized::sha256_hex`, the same hash
//!   the stream uses for `vin_hash`. The raw `vin` never leaves the `fleet` CTE.
//! - `drop("battery_pack_type")` + `join(.., Seq("vin_hash"), "left")`: the pack
//!   type always comes from `vehicle_state`. A vehicle with no state row (or a
//!   NULL `vin_hash`) keeps its events with a NULL `battery_pack_type`, which
//!   is its own group.
//! - `lit(vehicleState.count())` is an eager action: the fleet size is counted
//!   once over every state row, vehicles without diagnostics included, and used
//!   as a literal. With an empty `vehicle_state` it is 0, so
//!   `fleet_pct_affected` is NULL (`spark_divide`), not `inf`/`NaN`.
//! - `round(vehicles_affected / fleetSize * 100, 3)` divides first, then
//!   scales, and rounds HALF_UP on the decimal value (`spark_round`).
//! - `to_timestamp(event_time)` is `timestamp[us, UTC]` and NULL when
//!   unparseable, so `min`/`max` skip it (`spark_to_timestamp`; DataFusion's
//!   `to_timestamp` gives `timestamp[ns]` and errors).
//! - `count("*")`, `countDistinct` and `sum(when(..).otherwise(0))` are
//!   `bigint` (`Int64`). `countDistinct` skips a NULL `vin_hash`, `count("*")`
//!   does not, and a NULL `warning_state` counts as 0.
//! - `dense_rank().over(partitionBy("ecu").orderBy(vehicles_affected.desc))` is
//!   `int` (`Int32`) in Spark but `UInt64` in DataFusion, and `desc` is NULLS
//!   LAST. A NULL `ecu` is its own partition; ties share a rank and the next
//!   rank is consecutive.

use super::{spark_functions, Job};
use crate::transforms::anonymized::sha256_hex;
use anyhow::Result;
use deltalake::arrow::array::{AsArray, StringArray};
use deltalake::arrow::compute::cast as arrow_cast;
use deltalake::arrow::datatypes::DataType;
use deltalake::datafusion::common::{plan_err, Result as DFResult};
use deltalake::datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use deltalake::datafusion::prelude::*;
use std::any::Any;
use std::sync::Arc;

pub const JOB: Job = Job {
    name: "diagnostic_alerts",
    key_cols: &["dtc_code", "ecu", "model_safe", "battery_pack_type"],
    partition_cols: &[],
};

/// Events, vehicles affected, share of the known fleet and rank within the
/// ECU for each (DTC, ECU, model, battery pack).
pub async fn summarize(ctx: &SessionContext) -> Result<DataFrame> {
    spark_functions::register(ctx);
    ctx.register_udf(ScalarUDF::new_from_impl(Sha2_256::new()));

    // Spark evaluates `vehicleState.count()` eagerly and embeds the result.
    let fleet_size = ctx.table("vehicle_state").await?.count().await?;
    let sql = format!(
        "WITH fleet AS (
             SELECT sha2_256(vin) AS vin_hash, battery_pack_type
             FROM vehicle_state
         ),
         alerts AS (
             SELECT t.dtc_code, t.ecu, t.model_safe, f.battery_pack_type,
                    COUNT(*) AS events,
                    COUNT(DISTINCT t.vin_hash) AS vehicles_affected,
                    SUM(CASE WHEN t.warning_state = 'WARNING_ON' THEN 1 ELSE 0 END)
                        AS warning_on_events,
                    MIN(spark_to_timestamp(t.event_time)) AS first_seen,
                    MAX(spark_to_timestamp(t.event_time)) AS last_seen
             FROM telemetry t
             LEFT JOIN fleet f ON t.vin_hash = f.vin_hash
             WHERE t.event_type = 'DIAGNOSTIC'
             GROUP BY t.dtc_code, t.ecu, t.model_safe, f.battery_pack_type
         )
         SELECT *,
                spark_round(spark_divide(vehicles_affected, CAST({fleet_size} AS BIGINT)) * 100, 3)
                    AS fleet_pct_affected,
                CAST(DENSE_RANK() OVER (
                    PARTITION BY ecu ORDER BY vehicles_affected DESC NULLS LAST
                ) AS INT) AS rank_in_ecu
         FROM alerts"
    );
    Ok(ctx.sql(&sql).await?)
}

/// Spark's `sha2(string, 256)`: lower-case hex SHA-256 of the UTF-8 bytes,
/// NULL for NULL.
#[derive(Debug)]
struct Sha2_256 {
    signature: Signature,
}

impl Sha2_256 {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Sha2_256 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha2_256"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        match arg_types {
            [DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null] => {
                Ok(vec![DataType::Utf8])
            }
            _ => plan_err!("sha2_256 takes one string argument, got {arg_types:?}"),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> DFResult<ColumnarValue> {
        let strings = arrow_cast(&args[0].clone().into_array(number_rows)?, &DataType::Utf8)?;
        let hashes: StringArray = strings
            .as_string::<i32>()
            .iter()
            .map(|s| s.map(sha256_hex))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(hashes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Array, ArrayRef, RecordBatch};
    use deltalake::arrow::datatypes::{
        Field, Float64Type, Int32Type, Int64Type, Schema, TimestampMicrosecondType,
    };
    use deltalake::arrow::util::pretty::pretty_format_batches;
    use deltalake::datafusion::datasource::MemTable;

    #[tokio::test]
    async fn matches_spark_output() {
        crate::analytics::parity::assert_matches_spark(&super::JOB).await;
    }

    /// `(event_type, vin, dtc_code, ecu, warning_state, event_time,
    /// battery_pack_type)`; `vin` is hashed into `vin_hash`, model is `M`.
    type Event = (
        &'static str,
        Option<&'static str>,
        Option<&'static str>,
        Option<&'static str>,
        Option<&'static str>,
        &'static str,
        Option<&'static str>,
    );

    const EVENTS: [Event; 9] = [
        (
            "DIAGNOSTIC",
            Some("V1"),
            Some("P1"),
            Some("BMS"),
            Some("WARNING_ON"),
            "2026-11-01T10:00:00.123456Z",
            Some("TEL_PACK"),
        ),
        (
            "DIAGNOSTIC",
            Some("V3"),
            Some("P1"),
            Some("BMS"),
            Some("WARNING_OFF"),
            "2026-11-01 09:00:00",
            Some("TEL_PACK"),
        ),
        (
            "DIAGNOSTIC",
            Some("V1"),
            Some("P1"),
            Some("BMS"),
            Some("WARNING_ON"),
            "2026-10-31T23:59:59Z",
            None,
        ),
        (
            "DIAGNOSTIC",
            Some("V2"),
            Some("P1"),
            Some("BMS"),
            None,
            "not a time",
            None,
        ),
        (
            "DIAGNOSTIC",
            Some("V2"),
            Some("P2"),
            Some("BMS"),
            Some("WARNING_ON"),
            "2026-11-02T00:00:00Z",
            None,
        ),
        (
            "DIAGNOSTIC",
            Some("V9"),
            Some("P1"),
            Some("BMS"),
            Some("WARNING_ON"),
            "2026-11-03T00:00:00Z",
            None,
        ),
        (
            "DIAGNOSTIC",
            None,
            Some("P1"),
            Some("BMS"),
            Some("WARNING_ON"),
            "2026-11-04T00:00:00Z",
            None,
        ),
        (
            "DIAGNOSTIC",
            Some("V1"),
            None,
            None,
            Some("WARNING_ON"),
            "2026-11-05T00:00:00+02:00",
            None,
        ),
        (
            "BATTERY",
            Some("V1"),
            Some("P1"),
            Some("BMS"),
            Some("WARNING_ON"),
            "2026-11-06T00:00:00Z",
            None,
        ),
    ];

    /// V9 has telemetry but no state row; V4–V6 have no diagnostics.
    const FLEET: [(&str, Option<&str>); 6] = [
        ("V1", Some("PACK_A")),
        ("V2", Some("PACK_B")),
        ("V3", Some("PACK_A")),
        ("V4", Some("PACK_A")),
        ("V5", Some("PACK_B")),
        ("V6", None),
    ];

    fn strings<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
        Arc::new(StringArray::from_iter(values))
    }

    fn table(columns: Vec<(&str, ArrayRef)>) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch =
            RecordBatch::try_new(schema.clone(), columns.into_iter().map(|c| c.1).collect())
                .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    /// Run the job; rows come back sorted like Spark's
    /// `orderBy(ecu.asc, dtc_code, battery_pack_type.asc)` (NULLS FIRST).
    async fn run(events: &[Event], fleet: &[(&str, Option<&str>)]) -> RecordBatch {
        let ctx = SessionContext::new();
        let hashes: Vec<Option<String>> = events.iter().map(|e| e.1.map(sha256_hex)).collect();
        ctx.register_table(
            "telemetry",
            table(vec![
                ("event_type", strings(events.iter().map(|e| Some(e.0)))),
                ("vin_hash", strings(hashes.iter().map(|h| h.as_deref()))),
                ("dtc_code", strings(events.iter().map(|e| e.2))),
                ("ecu", strings(events.iter().map(|e| e.3))),
                ("model_safe", strings(events.iter().map(|_| Some("M")))),
                ("warning_state", strings(events.iter().map(|e| e.4))),
                ("event_time", strings(events.iter().map(|e| Some(e.5)))),
                ("battery_pack_type", strings(events.iter().map(|e| e.6))),
            ]),
        )
        .unwrap();
        ctx.register_table(
            "vehicle_state",
            table(vec![
                ("vin", strings(fleet.iter().map(|v| Some(v.0)))),
                ("battery_pack_type", strings(fleet.iter().map(|v| v.1))),
            ]),
        )
        .unwrap();
        let out = summarize(&ctx)
            .await
            .unwrap()
            .sort(vec![
                col("ecu").sort(true, true),
                col("dtc_code").sort(true, true),
                col("battery_pack_type").sort(true, true),
            ])
            .unwrap()
            .collect()
            .await
            .unwrap();
        deltalake::arrow::compute::concat_batches(&out[0].schema(), &out).unwrap()
    }

    fn str_col(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
        batch
            .column_by_name(name)
            .unwrap()
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    fn i64_col(batch: &RecordBatch, name: &str) -> Vec<i64> {
        let a = batch
            .column_by_name(name)
            .unwrap()
            .as_primitive::<Int64Type>();
        assert_eq!(a.null_count(), 0, "{name} has NULLs");
        a.values().to_vec()
    }

    fn pct_col(batch: &RecordBatch) -> Vec<Option<f64>> {
        batch
            .column_by_name("fleet_pct_affected")
            .unwrap()
            .as_primitive::<Float64Type>()
            .iter()
            .collect()
    }

    fn ts_col(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
        batch
            .column_by_name(name)
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .collect()
    }

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    // Expected values below come from PySpark 3.5.1 running the Scala logic
    // on the same rows, with spark.sql.session.timeZone=UTC.

    #[tokio::test]
    async fn sha2_matches_spark() {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::new_from_impl(Sha2_256::new()));
        let out = ctx
            .sql(
                "SELECT sha2_256('abc') AS a, sha2_256('') AS b,
                        sha2_256(CAST(NULL AS VARCHAR)) AS c,
                        sha2_256(arrow_cast('WVWZZZ1JZXW000001', 'Utf8View')) AS d",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let row = &out[0];
        let get = |name: &str| str_col(row, name).remove(0);
        assert_eq!(
            get("a"),
            s("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
        assert_eq!(
            get("b"),
            s("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        );
        assert_eq!(get("c"), None);
        assert_eq!(
            get("d"),
            s("38657c765e00f5c9f487540df0aa7b36e08a0ec6db94de4658cb47eacca32ff3")
        );
    }

    #[tokio::test]
    async fn groups_by_pack_type_from_vehicle_state() {
        let out = run(&EVENTS, &FLEET).await;
        assert_eq!(
            str_col(&out, "dtc_code"),
            vec![None, s("P1"), s("P1"), s("P1"), s("P2")]
        );
        assert_eq!(
            str_col(&out, "ecu"),
            vec![None, s("BMS"), s("BMS"), s("BMS"), s("BMS")]
        );
        // Telemetry's TEL_PACK is dropped; V9 and the NULL vin_hash have no
        // state row, so they share the NULL-pack group.
        assert_eq!(
            str_col(&out, "battery_pack_type"),
            vec![s("PACK_A"), None, s("PACK_A"), s("PACK_B"), s("PACK_B")]
        );
        assert_eq!(i64_col(&out, "events"), vec![1, 2, 3, 1, 1]);
        assert_eq!(i64_col(&out, "vehicles_affected"), vec![1, 1, 2, 1, 1]);
        assert_eq!(i64_col(&out, "warning_on_events"), vec![1, 2, 2, 0, 1]);
    }

    #[tokio::test]
    async fn timestamps_skip_unparseable_event_time() {
        let out = run(&EVENTS, &FLEET).await;
        assert_eq!(
            ts_col(&out, "first_seen"),
            vec![
                Some(1_793_829_600_000_000),
                Some(1_793_664_000_000_000),
                Some(1_793_491_199_000_000),
                None,
                Some(1_793_577_600_000_000),
            ]
        );
        assert_eq!(
            ts_col(&out, "last_seen"),
            vec![
                Some(1_793_829_600_000_000),
                Some(1_793_750_400_000_000),
                Some(1_793_527_200_123_456),
                None,
                Some(1_793_577_600_000_000),
            ]
        );
    }

    #[tokio::test]
    async fn fleet_pct_counts_every_state_row_and_rounds_half_up() {
        let out = run(&EVENTS, &FLEET).await;
        assert_eq!(
            pct_col(&out),
            vec![
                Some(16.667),
                Some(16.667),
                Some(33.333),
                Some(16.667),
                Some(16.667)
            ]
        );
    }

    #[tokio::test]
    async fn empty_vehicle_state_gives_null_pct_and_pack() {
        let out = run(&EVENTS, &[]).await;
        assert_eq!(str_col(&out, "dtc_code"), vec![None, s("P1"), s("P2")]);
        assert_eq!(str_col(&out, "battery_pack_type"), vec![None, None, None]);
        assert_eq!(i64_col(&out, "events"), vec![1, 6, 1]);
        assert_eq!(i64_col(&out, "vehicles_affected"), vec![1, 4, 1]);
        assert_eq!(i64_col(&out, "warning_on_events"), vec![1, 4, 1]);
        assert_eq!(pct_col(&out), vec![None, None, None]);
        let rank = out.column_by_name("rank_in_ecu").unwrap();
        assert_eq!(
            rank.as_primitive::<Int32Type>().values().to_vec(),
            vec![1, 1, 2]
        );
    }

    #[tokio::test]
    async fn dense_rank_within_ecu_with_ties_and_null_ecu() {
        let out = run(&EVENTS, &FLEET).await;
        let rank = out.column_by_name("rank_in_ecu").unwrap();
        assert_eq!(rank.null_count(), 0);
        assert_eq!(
            rank.as_primitive::<Int32Type>().values().to_vec(),
            vec![1, 2, 1, 2, 2],
            "{}",
            pretty_format_batches(std::slice::from_ref(&out)).unwrap()
        );
    }

    #[tokio::test]
    async fn output_columns_match_spark_schema() {
        let out = run(&EVENTS, &FLEET).await;
        let fields: Vec<_> = out
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();
        let utf8 = DataType::Utf8;
        let ts = spark_functions::spark_timestamp_type();
        let expected = vec![
            ("dtc_code", utf8.clone()),
            ("ecu", utf8.clone()),
            ("model_safe", utf8.clone()),
            ("battery_pack_type", utf8),
            ("events", DataType::Int64),
            ("vehicles_affected", DataType::Int64),
            ("warning_on_events", DataType::Int64),
            ("first_seen", ts.clone()),
            ("last_seen", ts),
            ("fleet_pct_affected", DataType::Float64),
            ("rank_in_ecu", DataType::Int32),
        ];
        let expected: Vec<_> = expected
            .into_iter()
            .map(|(n, t)| (n.to_string(), t))
            .collect();
        assert_eq!(fields, expected);
    }
}
