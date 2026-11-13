//! Spark SQL semantics for the analytics ports, where DataFusion differs.
//!
//! Shared by every job in this module. Each helper exists both as an
//! [`Expr`] builder (DataFrame API) and, via [`register`], as a SQL function:
//!
//! | Spark                         | DataFusion default             | Helper            |
//! |-------------------------------|--------------------------------|-------------------|
//! | `round(double, n)` HALF_UP on the decimal value | binary `(x*10^n).round()/10^n` | [`spark_round`]   |
//! | `a / b` is always DOUBLE, NULL when `b = 0`     | int / int is integer division; x / 0.0 is inf/NaN | [`spark_divide`]  |
//! | `row_number`/`rank`/`dense_rank` are INT        | UInt64                         | [`spark_int`]     |
//! | timestamps are `timestamp[us, UTC]`             | `to_timestamp` gives `timestamp[ns]` | [`spark_timestamp`] |

use crate::transforms::rounding;
use deltalake::arrow::array::Float64Array;
use deltalake::arrow::datatypes::{DataType, Float64Type, TimeUnit};
use deltalake::datafusion::common::cast::{as_float64_array, as_string_array};
use deltalake::datafusion::common::{exec_err, Result, ScalarValue};
use deltalake::datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use deltalake::datafusion::prelude::*;
use std::any::Any;
use std::sync::Arc;

/// Largest scale `spark_round` accepts (keeps the rounded digits in a u128).
const MAX_ROUND_SCALE: i64 = 20;

/// Make `spark_round(x, n)` and `spark_divide(a, b)` callable from SQL on `ctx`.
/// A port that uses them in SQL calls this at the start of its `summarize`.
// Not every helper has a caller until the remaining jobs are ported.
#[allow(dead_code)]
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(spark_round_udf());
    ctx.register_udf(spark_divide_udf());
    ctx.register_udf(spark_sha2_udf());
}

/// Spark's `round(x, scale)` on a DOUBLE: HALF_UP on the value's shortest
/// decimal form, so `round(1.005, 2) = 1.01` (DataFusion's `round` gives 1.0).
/// `scale` must be in `0..=20`.
pub fn spark_round(value: Expr, scale: i64) -> Expr {
    spark_round_udf().call(vec![value, lit(scale)])
}

/// Spark's `/` (non-ANSI): both sides as DOUBLE, NULL when the divisor is 0.
/// DataFusion would do integer division for two integer columns, and return
/// inf/NaN (not NULL) for a zero float divisor.
pub fn spark_divide(numerator: Expr, denominator: Expr) -> Expr {
    spark_divide_udf().call(vec![numerator, denominator])
}

/// Cast to Spark's IntegerType. Use on `row_number`, `rank` and `dense_rank`,
/// which return INT in Spark but UInt64 in DataFusion.
#[allow(dead_code)]
pub fn spark_int(value: Expr) -> Expr {
    cast(value, DataType::Int32)
}

/// Spark's `to_timestamp(string)`: parses the same datetime formats, but a
/// malformed value gives NULL (non-ANSI mode). DataFusion's `to_timestamp`
/// aborts the whole query instead, so this uses `try_cast` to
/// `timestamp[ns]` — pair with [`spark_timestamp`] for `timestamp[us, UTC]`.
#[allow(dead_code)]
pub fn spark_to_timestamp(value: Expr) -> Expr {
    try_cast(value, DataType::Timestamp(TimeUnit::Nanosecond, None))
}

/// Cast to Spark's TimestampType as stored in Parquet/Delta: `timestamp[us, UTC]`.
#[allow(dead_code)]
pub fn spark_timestamp(value: Expr) -> Expr {
    cast(
        value,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    )
}

/// Spark's `sha2(x, 256)`: lowercase hex string of the SHA-256 digest.
/// DataFusion's `sha256` returns Binary, and `encode(digest, 'hex')` first
/// coerces the digest to Utf8 (rejecting/mangling non-UTF8 bytes), so the
/// hex encoding has to happen inside one UDF.
#[allow(dead_code)]
pub fn spark_sha2(value: Expr) -> Expr {
    spark_sha2_udf().call(vec![value])
}

pub fn spark_sha2_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SparkSha2 {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug)]
struct SparkSha2 {
    signature: Signature,
}

impl ScalarUDFImpl for SparkSha2 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_sha2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null] => {
                Ok(vec![DataType::Utf8])
            }
            [other] => exec_err!("spark_sha2: expected a string column, got {other}"),
            _ => exec_err!("spark_sha2 takes 1 argument, got {}", arg_types.len()),
        }
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        use deltalake::arrow::array::StringArray;
        use sha2::Digest;
        let hex = |s: &str| format!("{:x}", sha2::Sha256::digest(s.as_bytes()));
        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(s)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(s.as_ref().map(|s| hex(s))),
            )),
            ColumnarValue::Array(values) => {
                let strings = as_string_array(values)?;
                let hashed: StringArray = strings.iter().map(|opt| opt.map(hex)).collect();
                Ok(ColumnarValue::Array(Arc::new(hashed)))
            }
            other => exec_err!(
                "spark_sha2: expected a string column, got {}",
                other.data_type()
            ),
        }
    }
}

pub fn spark_round_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SparkRound {
        signature: Signature::exact(
            vec![DataType::Float64, DataType::Int64],
            Volatility::Immutable,
        ),
    })
}

pub fn spark_divide_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SparkDivide {
        signature: Signature::exact(
            vec![DataType::Float64, DataType::Float64],
            Volatility::Immutable,
        ),
    })
}

#[derive(Debug)]
struct SparkRound {
    signature: Signature,
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        let scale = match args.get(1) {
            Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(s))))
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
        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Float64(x)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(x.map(|x| rounding::spark_round(x, scale))),
            )),
            ColumnarValue::Array(values) => {
                let rounded = as_float64_array(values)?
                    .unary::<_, Float64Type>(|x| rounding::spark_round(x, scale));
                Ok(ColumnarValue::Array(Arc::new(rounded)))
            }
            other => exec_err!("spark_round: expected a DOUBLE, got {}", other.data_type()),
        }
    }
}

#[derive(Debug)]
struct SparkDivide {
    signature: Signature,
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let (numerators, denominators) = match arrays.as_slice() {
            [n, d] => (as_float64_array(n)?, as_float64_array(d)?),
            _ => return exec_err!("spark_divide takes 2 arguments, got {}", arrays.len()),
        };
        let quotients: Float64Array = numerators
            .iter()
            .zip(denominators.iter())
            .map(|pair| match pair {
                (Some(n), Some(d)) if d != 0.0 => Some(n / d),
                _ => None,
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(quotients)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Array, Float64Array, Int32Array};
    use deltalake::arrow::record_batch::RecordBatch;

    async fn query(sql: &str) -> RecordBatch {
        let ctx = SessionContext::new();
        register(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        deltalake::arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
    }

    fn doubles(batch: &RecordBatch, i: usize) -> Vec<Option<f64>> {
        let col = batch.column(i);
        assert_eq!(col.data_type(), &DataType::Float64);
        col.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    #[tokio::test]
    async fn round_is_half_up_on_the_decimal_value() {
        let b = query(
            "SELECT spark_round(1.005, 2), round(1.005, 2), spark_round(CAST(NULL AS DOUBLE), 2)",
        )
        .await;
        assert_eq!(doubles(&b, 0), [Some(1.01)]);
        assert_eq!(doubles(&b, 1), [Some(1.0)], "DataFusion's round differs");
        assert_eq!(doubles(&b, 2), [None]);
    }

    #[tokio::test]
    async fn round_works_on_columns() {
        let b =
            query("SELECT spark_round(x, 2) FROM (VALUES (2.675), (-1.005), (NULL)) t(x)").await;
        assert_eq!(doubles(&b, 0), [Some(2.68), Some(-1.01), None]);
    }

    #[tokio::test]
    async fn divide_is_double_and_null_on_zero() {
        let b = query(
            "SELECT spark_divide(a, b) FROM (VALUES (CAST(1 AS BIGINT), CAST(4 AS BIGINT)), (3, 0), (NULL, 2), (0, 0)) t(a, b)",
        )
        .await;
        assert_eq!(doubles(&b, 0), [Some(0.25), None, None, None]);
        let b = query("SELECT spark_divide(1.0, 0.0), spark_divide(-0.0, 1.0)").await;
        assert_eq!(doubles(&b, 0), [None]);
        assert_eq!(doubles(&b, 1), [Some(-0.0)]);
    }

    #[tokio::test]
    async fn sha2_gives_lowercase_hex() {
        let b =
            query("SELECT spark_sha2(''), spark_sha2('VIN123'), spark_sha2(CAST(NULL AS VARCHAR))")
                .await;
        let col = |i: usize| {
            b.column(i)
                .as_any()
                .downcast_ref::<deltalake::arrow::array::StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(
            col(0),
            [Some(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )]
        );
        assert_eq!(
            col(1),
            [Some(
                "540b8f09afc89b842c2a048e8f1d624d3b21c5265399c808709348f09957ecc3"
            )]
        );
        assert_eq!(col(2), [None]);
    }

    #[tokio::test]
    async fn to_timestamp_is_null_not_error_on_bad_input() {
        // Spark non-ANSI: unparseable strings are NULL; DataFusion's own
        // to_timestamp aborts with a parse error instead.
        let ctx = SessionContext::new();
        let df = ctx
            .sql("SELECT '2026-01-02T03:04:05Z' AS ok, 'not-a-time' AS bad, CAST(NULL AS VARCHAR) AS none")
            .await
            .unwrap()
            .select(vec![
                spark_to_timestamp(col("ok")).alias("ok"),
                spark_to_timestamp(col("bad")).alias("bad"),
                spark_to_timestamp(col("none")).alias("none"),
            ])
            .unwrap();
        let b = df.collect().await.unwrap().remove(0);
        assert_eq!(
            b.column(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        let ts = |i: usize| {
            b.column(i)
                .as_any()
                .downcast_ref::<deltalake::arrow::array::TimestampNanosecondArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(ts(0), [Some(1767323045000000000)]);
        assert_eq!(ts(1), [None]);
        assert_eq!(ts(2), [None]);
    }

    #[tokio::test]
    async fn expr_builders_match_spark_types() {
        let ctx = SessionContext::new();
        let df = ctx
            .sql("SELECT CAST(7 AS BIGINT) AS n, CAST(8 AS BIGINT) AS d, CAST(1 AS BIGINT UNSIGNED) AS rn, to_timestamp('2026-01-02T03:04:05Z') AS ts")
            .await
            .unwrap()
            .select(vec![
                spark_round(spark_divide(col("n"), col("d")) * lit(100.0), 1).alias("pct"),
                spark_int(col("rn")).alias("rn"),
                spark_timestamp(col("ts")).alias("ts"),
            ])
            .unwrap();
        let b = df.collect().await.unwrap().remove(0);
        assert_eq!(doubles(&b, 0), [Some(87.5)]);
        let rn = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(rn.value(0), 1);
        assert_eq!(
            b.column(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }
}
