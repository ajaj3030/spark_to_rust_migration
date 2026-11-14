//! Spark SQL semantics that DataFusion 43 (via deltalake 0.22) does not
//! share, for the analytics ports.
//!
//! Every helper is available both as a SQL function, after calling
//! [`register`] on the context, and as an [`Expr`] builder for the
//! DataFrame API. The parity test builds its own context, so a port that
//! uses the SQL names calls `register(ctx)` at the start of `summarize`.
//!
//! | Spark                                   | DataFusion default                          | Helper                                    |
//! |-----------------------------------------|---------------------------------------------|-------------------------------------------|
//! | `round(double, n)`: HALF_UP on the decimal value | `round` on the binary value (`round(1.005, 2) = 1.0`) | `spark_round(x, n)`             |
//! | `a / b` (non-ANSI): DOUBLE, NULL if `b = 0` | integer division for ints; `inf`/`NaN` for `x / 0.0` | `spark_divide(a, b)`              |
//! | `max_by(v, o)` / `min_by(v, o)`         | not available                               | `max_by(v, o)` / `min_by(v, o)`           |
//! | `to_timestamp(string)`: `timestamp[us, UTC]`, NULL if unparseable | `timestamp[ns]`, error if unparseable | `spark_to_timestamp(s)` |
//! | `row_number`/`rank`/`dense_rank` are INT | UInt64                                     | `CAST(... AS INT)` / [`spark_int`]        |
//! | `ORDER BY x DESC` is NULLS LAST; `ASC` is NULLS FIRST | `DESC` is NULLS FIRST; `ASC` is NULLS LAST | write `NULLS LAST`/`NULLS FIRST` / [`spark_desc`], [`spark_asc`] |

use crate::transforms::rounding;
use deltalake::arrow::array::{
    make_comparator, Array, ArrayRef, AsArray, Float64Array, TimestampMicrosecondArray,
};
use deltalake::arrow::compute::{cast as arrow_cast, sort_to_indices, SortOptions};
use deltalake::arrow::datatypes::{DataType, Field, TimeUnit};
use deltalake::datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use deltalake::datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use deltalake::datafusion::logical_expr::utils::format_state_name;
use deltalake::datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarUDF, ScalarUDFImpl,
    Signature, SortExpr, Volatility,
};
use deltalake::datafusion::prelude::*;
use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

/// Largest `scale` accepted by `spark_round`; keeps the rounded digits of
/// any double with a fractional part inside a u128.
const MAX_ROUND_SCALE: i64 = 20;

/// Spark's TimestampType as stored in Parquet/Delta.
pub fn spark_timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// Make every helper below callable from SQL on `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(SparkRound::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(SparkDivide::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(SparkToTimestamp::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(ExtremeBy::max()));
    ctx.register_udaf(AggregateUDF::new_from_impl(ExtremeBy::min()));
}

/// Spark's `round(x, scale)` on a DOUBLE, `scale` in `0..=20`.
#[allow(dead_code)] // DataFrame-API form; not every port uses it yet
pub fn spark_round(value: Expr, scale: i64) -> Expr {
    ScalarUDF::new_from_impl(SparkRound::new()).call(vec![value, lit(scale)])
}

/// Spark's `/` without ANSI mode: both sides as DOUBLE, NULL when the
/// divisor is 0.
#[allow(dead_code)] // DataFrame-API form; not every port uses it yet
pub fn spark_divide(numerator: Expr, denominator: Expr) -> Expr {
    ScalarUDF::new_from_impl(SparkDivide::new()).call(vec![numerator, denominator])
}

/// Spark's `to_timestamp(string)` with session time zone UTC.
#[allow(dead_code)] // DataFrame-API form; not every port uses it yet
pub fn spark_to_timestamp(value: Expr) -> Expr {
    ScalarUDF::new_from_impl(SparkToTimestamp::new()).call(vec![value])
}

/// Spark's `max_by(value, ordering)`: `value` from the row with the largest
/// non-null `ordering`, even if that `value` is NULL.
#[allow(dead_code)] // DataFrame-API form; not every port uses it yet
pub fn max_by(value: Expr, ordering: Expr) -> Expr {
    AggregateUDF::new_from_impl(ExtremeBy::max()).call(vec![value, ordering])
}

/// Spark's `min_by(value, ordering)`; see [`max_by`].
#[allow(dead_code)] // DataFrame-API form; not every port uses it yet
pub fn min_by(value: Expr, ordering: Expr) -> Expr {
    AggregateUDF::new_from_impl(ExtremeBy::min()).call(vec![value, ordering])
}

/// Cast to Spark's IntegerType; use on `row_number`, `rank`, `dense_rank`.
#[allow(dead_code)] // DataFrame-API form; not every port uses it yet
pub fn spark_int(value: Expr) -> Expr {
    cast(value, DataType::Int32)
}

/// Spark's `col.desc` (NULLS LAST).
#[allow(dead_code)] // DataFrame-API form; not every port uses it yet
pub fn spark_desc(value: Expr) -> SortExpr {
    value.sort(false, false)
}

/// Spark's `col.asc` / default ordering (NULLS FIRST).
#[allow(dead_code)] // DataFrame-API form; not every port uses it yet
pub fn spark_asc(value: Expr) -> SortExpr {
    value.sort(true, true)
}

fn null_preserving_numeric(name: &str, arg_types: &[DataType], n: usize) -> Result<()> {
    if arg_types.len() != n {
        return plan_err!("{name} takes {n} arguments, got {}", arg_types.len());
    }
    match arg_types
        .iter()
        .find(|t| !(t.is_numeric() || **t == DataType::Null))
    {
        Some(t) => plan_err!("{name} needs numeric arguments, got {t}"),
        None => Ok(()),
    }
}

#[derive(Debug)]
struct SparkRound {
    signature: Signature,
}

impl SparkRound {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkRound {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_round"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        null_preserving_numeric(self.name(), arg_types, 2)?;
        Ok(vec![DataType::Float64, DataType::Int64])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let scale = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(s)))
                if (0..=MAX_ROUND_SCALE).contains(s) =>
            {
                *s as u32
            }
            _ => {
                return exec_err!(
                    "spark_round: scale must be an integer literal in 0..={MAX_ROUND_SCALE}"
                )
            }
        };
        let values = args[0].clone().into_array(number_rows)?;
        let rounded: Float64Array = values
            .as_primitive::<deltalake::arrow::datatypes::Float64Type>()
            .iter()
            .map(|v| v.map(|x| round_half_up(x, scale)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(rounded)))
    }
}

/// Spark rounds through `BigDecimal`, which has no negative zero.
fn round_half_up(x: f64, scale: u32) -> f64 {
    let r = rounding::spark_round(x, scale);
    if r == 0.0 {
        0.0
    } else {
        r
    }
}

#[derive(Debug)]
struct SparkDivide {
    signature: Signature,
}

impl SparkDivide {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkDivide {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_divide"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        null_preserving_numeric(self.name(), arg_types, 2)?;
        Ok(vec![DataType::Float64, DataType::Float64])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let n = args[0].clone().into_array(number_rows)?;
        let d = args[1].clone().into_array(number_rows)?;
        let quotients: Float64Array = n
            .as_primitive::<deltalake::arrow::datatypes::Float64Type>()
            .iter()
            .zip(
                d.as_primitive::<deltalake::arrow::datatypes::Float64Type>()
                    .iter(),
            )
            .map(|(n, d)| match (n, d) {
                (Some(n), Some(d)) if d != 0.0 => Some(n / d),
                _ => None,
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(quotients)))
    }
}

#[derive(Debug)]
struct SparkToTimestamp {
    signature: Signature,
}

impl SparkToTimestamp {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkToTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_to_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null] => {
                Ok(vec![DataType::Utf8])
            }
            _ => plan_err!("spark_to_timestamp takes one string argument, got {arg_types:?}"),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(spark_timestamp_type())
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let strings = arrow_cast(&args[0].clone().into_array(number_rows)?, &DataType::Utf8)?;
        let micros: TimestampMicrosecondArray = strings
            .as_string::<i32>()
            .iter()
            .map(|s| s.and_then(parse_spark_timestamp))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(micros.with_timezone("UTC"))))
    }
}

/// Microseconds since the epoch for a string, as Spark's string-to-timestamp
/// cast parses it with session time zone UTC; `None` where Spark gives NULL.
///
/// Accepts `[y]yyyy[-[m]m[-[d]d]]`, then optionally `T` or space and
/// `[h]h[:[m]m[:[s]s[.fraction]]]`, then an optional zone: `Z`, `UTC`/`GMT`/`UT`,
/// `±h[h][[:]mm]`, or one of those prefixes followed by an offset. Fractions
/// beyond microseconds are truncated. Region zone ids (`Europe/Berlin`) are
/// not supported and give `None`.
pub fn parse_spark_timestamp(s: &str) -> Option<i64> {
    use chrono::{NaiveDate, NaiveTime};
    let s = s.trim();
    let date_end = s.find(['T', ' ']).unwrap_or(s.len());
    let mut fields = s[..date_end].split('-');
    let year = digits(fields.next()?, 4, 6)?;
    let month = fields.next().map_or(Some(1), |m| digits(m, 1, 2))?;
    let day = fields.next().map_or(Some(1), |d| digits(d, 1, 2))?;
    if fields.next().is_some() {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(year as i32, month, day)?;
    if date_end == s.len() {
        return Some(date.and_time(NaiveTime::MIN).and_utc().timestamp_micros());
    }

    let rest = &s[date_end + 1..];
    let time_end = rest
        .find(|c: char| !(c.is_ascii_digit() || c == ':' || c == '.'))
        .unwrap_or(rest.len());
    let (clock, fraction) = match rest[..time_end].split_once('.') {
        Some((clock, fraction)) => (clock, fraction),
        None => (&rest[..time_end], ""),
    };
    let mut hms = clock.split(':');
    let hour = digits(hms.next()?, 1, 2)?;
    let minute = hms.next().map_or(Some(0), |m| digits(m, 1, 2))?;
    let second = hms.next().map_or(Some(0), |s| digits(s, 1, 2))?;
    if hms.next().is_some() || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let micros: u32 = format!("{:0<6}", &fraction[..fraction.len().min(6)])
        .parse()
        .ok()?;
    let time = NaiveTime::from_hms_micro_opt(hour, minute, second, micros)?;
    let offset_seconds = parse_zone(rest[time_end..].trim_start())?;
    Some(date.and_time(time).and_utc().timestamp_micros() - offset_seconds * 1_000_000)
}

fn digits(s: &str, min: usize, max: usize) -> Option<u32> {
    if (min..=max).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit()) {
        s.parse().ok()
    } else {
        None
    }
}

/// Offset east of UTC, in seconds.
fn parse_zone(zone: &str) -> Option<i64> {
    let offset = ["UTC", "GMT", "UT", "Z"]
        .iter()
        .find_map(|prefix| zone.strip_prefix(prefix))
        .unwrap_or(zone);
    if offset.is_empty() {
        return Some(0);
    }
    if offset.len() < zone.len() && zone.starts_with('Z') {
        return None;
    }
    let sign = match offset.as_bytes()[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let body = &offset[1..];
    let (hours, minutes) = match body.split_once(':') {
        Some((h, m)) => (digits(h, 1, 2)?, digits(m, 2, 2)?),
        None if body.len() == 4 => (digits(&body[..2], 2, 2)?, digits(&body[2..], 2, 2)?),
        None => (digits(body, 1, 2)?, 0),
    };
    if hours > 18 || minutes > 59 {
        return None;
    }
    Some(sign * (hours as i64 * 3600 + minutes as i64 * 60))
}

/// `max_by` / `min_by`.
#[derive(Debug)]
struct ExtremeBy {
    name: &'static str,
    /// Ordering of the kept row's key relative to any other row's.
    keep: Ordering,
    signature: Signature,
}

impl ExtremeBy {
    fn max() -> Self {
        Self::new("max_by", Ordering::Greater)
    }

    fn min() -> Self {
        Self::new("min_by", Ordering::Less)
    }

    fn new(name: &'static str, keep: Ordering) -> Self {
        Self {
            name,
            keep,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ExtremeBy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let value_type = args.exprs[0].data_type(args.schema)?;
        let ordering_type = args.exprs[1].data_type(args.schema)?;
        Ok(Box::new(ExtremeByAccumulator {
            keep: self.keep,
            value: ScalarValue::try_from(&value_type)?,
            ordering: None,
            ordering_type,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "value"),
                args.input_types[0].clone(),
                true,
            ),
            Field::new(
                format_state_name(args.name, "ordering"),
                args.input_types[1].clone(),
                true,
            ),
        ])
    }
}

#[derive(Debug)]
struct ExtremeByAccumulator {
    keep: Ordering,
    value: ScalarValue,
    /// The kept row's ordering key, as a one-element array so it compares
    /// with arrow's total order (NaN largest, like Spark).
    ordering: Option<ArrayRef>,
    ordering_type: DataType,
}

impl ExtremeByAccumulator {
    fn absorb(&mut self, values: &ArrayRef, orderings: &ArrayRef) -> Result<()> {
        let options = SortOptions {
            descending: self.keep == Ordering::Greater,
            nulls_first: false,
        };
        let best = sort_to_indices(orderings, Some(options), Some(1))?;
        if best.is_empty() || orderings.is_null(best.value(0) as usize) {
            return Ok(());
        }
        let row = best.value(0) as usize;
        let wins = match &self.ordering {
            None => true,
            Some(current) => {
                let cmp = make_comparator(orderings.as_ref(), current.as_ref(), options)?;
                // `options` sorts the kept extreme first
                cmp(row, 0) == Ordering::Less
            }
        };
        if wins {
            self.ordering = Some(orderings.slice(row, 1));
            self.value = ScalarValue::try_from_array(values, row)?;
        }
        Ok(())
    }
}

impl Accumulator for ExtremeByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.absorb(&values[0], &values[1])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.absorb(&states[0], &states[1])
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let ordering = match &self.ordering {
            Some(o) => ScalarValue::try_from_array(o, 0)?,
            None => ScalarValue::try_from(&self.ordering_type)?,
        };
        Ok(vec![self.value.clone(), ordering])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.value.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.value)
            + self.value.size()
            + self
                .ordering
                .as_ref()
                .map_or(0, |o| o.get_array_memory_size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{
        Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array,
    };
    use deltalake::arrow::datatypes::Schema;
    use deltalake::datafusion::datasource::MemTable;

    async fn context(batch: RecordBatch) -> SessionContext {
        let ctx = SessionContext::new();
        register(&ctx);
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    async fn query(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        deltalake::arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    fn f64s(batch: &RecordBatch, col: usize) -> Vec<Option<f64>> {
        batch
            .column(col)
            .as_primitive::<deltalake::arrow::datatypes::Float64Type>()
            .iter()
            .collect()
    }

    fn strings(batch: &RecordBatch, col: usize) -> Vec<Option<&str>> {
        batch.column(col).as_string::<i32>().iter().collect()
    }

    // Expected values below were checked against Spark 3.5.1 with
    // spark.sql.session.timeZone=UTC, as the analytics jobs run.

    #[tokio::test]
    async fn round_is_half_up_on_the_decimal_value() {
        let x = Float64Array::from(vec![
            Some(1.005),
            Some(2.675),
            Some(-0.125),
            Some(0.285),
            Some(123456.785),
            Some(-0.001),
            Some(f64::NAN),
            None,
        ]);
        let schema = Schema::new(vec![Field::new("x", DataType::Float64, true)]);
        let ctx = context(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(x)]).unwrap()).await;
        let out = query(&ctx, "SELECT spark_round(x, 2) FROM t").await;
        let got = f64s(&out, 0);
        assert_eq!(
            &got[..6],
            &[
                Some(1.01),
                Some(2.68),
                Some(-0.13),
                Some(0.29),
                Some(123456.79),
                Some(0.0)
            ]
        );
        assert!(got[5].unwrap().is_sign_positive());
        assert!(got[6].unwrap().is_nan());
        assert_eq!(got[7], None);
        assert_eq!(out.schema().field(0).data_type(), &DataType::Float64);
    }

    #[tokio::test]
    async fn round_accepts_integer_input_and_rejects_bad_scale() {
        let x = Int64Array::from(vec![7]);
        let schema = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
        let ctx = context(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(x)]).unwrap()).await;
        let out = query(&ctx, "SELECT spark_round(x, 1) FROM t").await;
        assert_eq!(f64s(&out, 0), vec![Some(7.0)]);
        let err = ctx
            .sql("SELECT spark_round(x, -1) FROM t")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn divide_is_double_and_null_on_zero() {
        let a = Int64Array::from(vec![Some(1), Some(1), Some(0), Some(-1), None]);
        let b = Int64Array::from(vec![Some(3), Some(0), Some(0), Some(0), Some(1)]);
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();
        let ctx = context(batch).await;
        let out = query(
            &ctx,
            "SELECT spark_divide(a, b) * 100, spark_divide(a, 0.0) FROM t",
        )
        .await;
        assert_eq!(
            f64s(&out, 0),
            vec![Some(33.33333333333333), None, None, None, None]
        );
        assert_eq!(f64s(&out, 1), vec![None; 5]);
    }

    #[tokio::test]
    async fn max_by_and_min_by_ignore_null_orderings_and_keep_null_values() {
        // Spark: k -> max_by NULL (value at o=2 is NULL), min_by 'x';
        //        j -> only a NULL ordering -> NULL for both.
        let g = StringArray::from(vec!["k", "k", "k", "j", "n", "n", "n"]);
        let o = Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
            None,
            Some(f64::NAN),
            Some(5.0),
            Some(-1.0),
        ]);
        let v = StringArray::from(vec![
            Some("x"),
            None,
            Some("z"),
            Some("q"),
            Some("nan"),
            Some("five"),
            Some("neg"),
        ]);
        let schema = Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("o", DataType::Float64, true),
            Field::new("v", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(g), Arc::new(o), Arc::new(v)],
        )
        .unwrap();
        let ctx = context(batch).await;
        let out = query(
            &ctx,
            "SELECT g, max_by(v, o), min_by(v, o) FROM t GROUP BY g ORDER BY g",
        )
        .await;
        assert_eq!(strings(&out, 0), vec![Some("j"), Some("k"), Some("n")]);
        assert_eq!(strings(&out, 1), vec![None, None, Some("nan")]);
        assert_eq!(strings(&out, 2), vec![None, Some("x"), Some("neg")]);
        assert_eq!(out.schema().field(1).data_type(), &DataType::Utf8);
    }

    #[tokio::test]
    async fn max_by_merges_across_partitions() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        register(&ctx);
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("o", DataType::Int64, false),
            Field::new("v", DataType::Float64, true),
        ]));
        let partitions: Vec<Vec<RecordBatch>> = (0..4i64)
            .map(|p| {
                let rows: Vec<i64> = (0..250).map(|i| p * 250 + i).collect();
                vec![RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(
                            rows.iter().map(|r| r % 3).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(rows.clone())),
                        Arc::new(Float64Array::from(
                            rows.iter().map(|r| *r as f64).collect::<Vec<_>>(),
                        )),
                    ],
                )
                .unwrap()]
            })
            .collect();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, partitions).unwrap()),
        )
        .unwrap();
        let out = query(
            &ctx,
            "SELECT g, max_by(v, o), min_by(v, o) FROM t GROUP BY g ORDER BY g",
        )
        .await;
        assert_eq!(f64s(&out, 1), vec![Some(999.0), Some(997.0), Some(998.0)]);
        assert_eq!(f64s(&out, 2), vec![Some(0.0), Some(1.0), Some(2.0)]);
    }

    #[test]
    fn parses_timestamps_like_spark() {
        let utc = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_micros()
        };
        let cases = [
            ("2026-11-05T23:23:51.318Z", Some("2026-11-05T23:23:51.318Z")),
            ("2026-11-05 23:23:51", Some("2026-11-05T23:23:51Z")),
            (
                "2026-11-05T23:23:51.318+02:00",
                Some("2026-11-05T21:23:51.318Z"),
            ),
            (
                "2026-11-05T23:23:51.318+0200",
                Some("2026-11-05T21:23:51.318Z"),
            ),
            ("2026-11-05T23:23:51+02", Some("2026-11-05T21:23:51Z")),
            ("2026-11-05T23:23:51+1", Some("2026-11-05T22:23:51Z")),
            ("2026-11-05T23:23:51-02:30", Some("2026-11-06T01:53:51Z")),
            ("2026-11-05T23:23:51 +01:00", Some("2026-11-05T22:23:51Z")),
            ("2026-11-05 23:23:51 UTC", Some("2026-11-05T23:23:51Z")),
            ("2026-11-05 23:23:51 GMT", Some("2026-11-05T23:23:51Z")),
            (
                "2026-11-05T23:23:51.123456789Z",
                Some("2026-11-05T23:23:51.123456Z"),
            ),
            ("2026-11-05T23:23:51.5", Some("2026-11-05T23:23:51.5Z")),
            ("2026-11-05T23:23:51.Z", Some("2026-11-05T23:23:51Z")),
            (" 2026-11-05T23:23:51Z ", Some("2026-11-05T23:23:51Z")),
            ("2026-11-05T23:23", Some("2026-11-05T23:23:00Z")),
            ("2026-11-05T23", Some("2026-11-05T23:00:00Z")),
            ("2026-11-05T1:2:3", Some("2026-11-05T01:02:03Z")),
            ("2026-11-05", Some("2026-11-05T00:00:00Z")),
            ("2026-11-5", Some("2026-11-05T00:00:00Z")),
            ("2026-11", Some("2026-11-01T00:00:00Z")),
            ("2026", Some("2026-01-01T00:00:00Z")),
            ("02026-11-05", Some("2026-11-05T00:00:00Z")),
            ("2026-11-05T23:23:51z", None),
            ("2026-02-30T00:00:00Z", None),
            ("2026-13-01", None),
            ("2026-11-05T24:00:00", None),
            ("2026-11-05T23:60:00", None),
            ("2026-11-05T", None),
            ("26-11-05", None),
            ("garbage", None),
            ("", None),
            ("  ", None),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_spark_timestamp(input),
                expected.map(utc),
                "to_timestamp({input:?})"
            );
        }
    }

    #[tokio::test]
    async fn to_timestamp_is_microseconds_utc() {
        let s = StringArray::from(vec![
            Some("2026-11-05T23:23:51.318Z"),
            Some("garbage"),
            None,
        ]);
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        let ctx = context(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(s)]).unwrap()).await;
        let out = query(&ctx, "SELECT spark_to_timestamp(s) AS ts FROM t").await;
        assert_eq!(out.schema().field(0).data_type(), &spark_timestamp_type());
        let ts: Vec<Option<i64>> = out
            .column(0)
            .as_primitive::<deltalake::arrow::datatypes::TimestampMicrosecondType>()
            .iter()
            .collect();
        assert_eq!(ts, vec![Some(1_793_921_031_318_000), None, None]);
    }

    #[tokio::test]
    async fn ranking_and_null_ordering_need_spark_overrides() {
        // Spark: dense_rank over (ORDER BY v DESC) on [3, NULL, 3, 1] gives
        // 3->1, 1->2, NULL->3 as INT; row_number over (ORDER BY v) puts NULL first.
        let v = Int32Array::from(vec![Some(3), None, Some(3), Some(1)]);
        let schema = Schema::new(vec![Field::new("v", DataType::Int32, true)]);
        let ctx = context(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(v)]).unwrap()).await;

        let default = query(
            &ctx,
            "SELECT v, dense_rank() OVER (ORDER BY v DESC) FROM t ORDER BY v",
        )
        .await;
        assert_eq!(default.schema().field(1).data_type(), &DataType::UInt64);
        let ranks = default
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(ranks.value(3), 1, "DataFusion DESC puts NULL first");

        let df = ctx.table("t").await.unwrap();
        let dense_rank = Expr::WindowFunction(
            deltalake::datafusion::logical_expr::expr::WindowFunction::new(
                deltalake::datafusion::functions_window::rank::dense_rank_udwf(),
                vec![],
            ),
        )
        .order_by(vec![spark_desc(col("v"))])
        .build()
        .unwrap();
        let spark = df
            .window(vec![dense_rank.alias("rank_u64")])
            .unwrap()
            .select(vec![col("v"), spark_int(col("rank_u64")).alias("dr")])
            .unwrap()
            .sort(vec![spark_asc(col("v"))])
            .unwrap();
        let out = spark.collect().await.unwrap();
        let out = deltalake::arrow::compute::concat_batches(&out[0].schema(), &out).unwrap();
        assert_eq!(out.schema().field(1).data_type(), &DataType::Int32);
        let v: Vec<Option<i32>> = out
            .column(0)
            .as_primitive::<deltalake::arrow::datatypes::Int32Type>()
            .iter()
            .collect();
        let dr: Vec<Option<i32>> = out
            .column(1)
            .as_primitive::<deltalake::arrow::datatypes::Int32Type>()
            .iter()
            .collect();
        assert_eq!(v, vec![None, Some(1), Some(3), Some(3)]);
        assert_eq!(dr, vec![Some(3), Some(2), Some(1), Some(1)]);
    }
}
