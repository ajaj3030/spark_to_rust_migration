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
//! | `max_by(value, ord)` in a groupBy               | no equivalent aggregate          | [`spark_max_by`]  |

use crate::transforms::rounding;
use deltalake::arrow::array::ArrayRef;
use deltalake::arrow::array::Float64Array;
use deltalake::arrow::datatypes::Field;
use deltalake::arrow::datatypes::{DataType, Float64Type, TimeUnit};
use deltalake::datafusion::common::cast::as_float64_array;
use deltalake::datafusion::common::{exec_err, Result, ScalarValue};
use deltalake::datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use deltalake::datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use deltalake::datafusion::prelude::*;
use std::any::Any;
use std::sync::Arc;

/// Largest scale `spark_round` accepts (keeps the rounded digits in a u128).
const MAX_ROUND_SCALE: i64 = 20;

/// Make `spark_round(x, n)`, `spark_divide(a, b)` and `spark_max_by(v, o)`
/// callable from SQL on `ctx`. A port that uses them in SQL calls this at the
/// start of its `summarize`.
// Not every helper has a caller until the remaining jobs are ported.
#[allow(dead_code)]
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(spark_round_udf());
    ctx.register_udf(spark_divide_udf());
    ctx.register_udaf(spark_max_by_udaf());
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

/// Cast to Spark's TimestampType as stored in Parquet/Delta: `timestamp[us, UTC]`.
#[allow(dead_code)]
pub fn spark_timestamp(value: Expr) -> Expr {
    cast(
        value,
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    )
}

/// Spark's `max_by(value, ord)` in a `groupBy.agg`: the `value` of the row
/// with the largest `ord`. Rows with NULL `ord` are ignored; on `ord` ties the
/// first row encountered wins (Spark's tie-break is arbitrary; ordering columns
/// in these jobs are unique). DataFusion has no such aggregate.
/// `value` may be NULL — it is kept as-is.
pub fn spark_max_by(value: Expr, ord: Expr) -> Expr {
    spark_max_by_udaf().call(vec![value, ord])
}

pub fn spark_max_by_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(SparkMaxBy {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

#[derive(Debug)]
struct SparkMaxBy {
    signature: Signature,
}

impl AggregateUDFImpl for SparkMaxBy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_max_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MaxByAccumulator {
            value_type: acc_args.return_type.clone(),
            ord_type: acc_args.exprs[1].data_type(acc_args.schema)?,
            best_value: None,
            best_ord: None,
        }))
    }

    /// Intermediate state: [ord of the current winner, its value].
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok([
            Field::new(
                format!("{}[ord]", args.name),
                args.input_types[1].clone(),
                true,
            ),
            Field::new(
                format!("{}[value]", args.name),
                args.return_type.clone(),
                true,
            ),
        ]
        .into_iter()
        .chain(args.ordering_fields.to_vec())
        .collect())
    }
}

/// Keeps the value of the row with the largest ord so far (first seen on ties).
#[derive(Debug)]
struct MaxByAccumulator {
    value_type: DataType,
    ord_type: DataType,
    best_value: Option<ScalarValue>,
    best_ord: Option<ScalarValue>,
}

impl MaxByAccumulator {
    /// `ord` must come first: `state()` and `merge_batch` pair up.
    fn fold(&mut self, ord: ScalarValue, value: ScalarValue) {
        // Spark ignores rows whose ordering expression is NULL.
        if ord.is_null() {
            return;
        }
        let wins = match &self.best_ord {
            None => true,
            Some(best) => ord > *best,
        };
        if wins {
            self.best_ord = Some(ord);
            self.best_value = Some(value);
        }
    }
}

impl Accumulator for MaxByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for i in 0..values[0].len() {
            self.fold(
                ScalarValue::try_from_array(&values[1], i)?,
                ScalarValue::try_from_array(&values[0], i)?,
            );
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(match &self.best_value {
            Some(value) => value.clone(),
            None => ScalarValue::try_from(&self.value_type)?,
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.best_value.as_ref().map_or(0, ScalarValue::size)
            + self.best_ord.as_ref().map_or(0, ScalarValue::size)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            match &self.best_ord {
                Some(ord) => ord.clone(),
                // A typed NULL: the state field is the input ord type, so an
                // untyped `ScalarValue::Null` would be rejected during
                // partial aggregation.
                None => ScalarValue::try_from(&self.ord_type)?,
            },
            match &self.best_value {
                Some(value) => value.clone(),
                None => ScalarValue::try_from(&self.value_type)?,
            },
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        for i in 0..states[0].len() {
            self.fold(
                ScalarValue::try_from_array(&states[0], i)?,
                ScalarValue::try_from_array(&states[1], i)?,
            );
        }
        Ok(())
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
    async fn max_by_picks_value_at_largest_ord() {
        let b = query(
            "SELECT spark_max_by(v, o) FROM (VALUES (1.0, 3), (2.0, 9), (3.0, 5), (4.0, CAST(NULL AS BIGINT))) t(v, o)",
        )
        .await;
        assert_eq!(doubles(&b, 0), [Some(2.0)], "NULL ord must be ignored");
        let b =
            query("SELECT spark_max_by(v, o) FROM (VALUES (7.0, CAST(NULL AS BIGINT))) t(v, o)")
                .await;
        assert_eq!(doubles(&b, 0), [None], "all-NULL ord gives NULL");
        // Grouped aggregation goes through partial state: the all-NULL-ord
        // group's state() must emit a typed NULL matching state_fields.
        let b = query(
            "SELECT g, spark_max_by(v, o) FROM (VALUES ('a', 7.0, CAST(NULL AS BIGINT)), ('b', 1.0, 4), ('b', 2.0, 6)) t(g, v, o) GROUP BY g ORDER BY g",
        )
        .await;
        assert_eq!(doubles(&b, 1), [None, Some(2.0)]);
        let b = query("SELECT spark_max_by(v, o) FROM (VALUES (9.0, 1), (NULL, 2)) t(v, o)").await;
        assert_eq!(doubles(&b, 0), [None], "NULL value is kept as-is");
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
