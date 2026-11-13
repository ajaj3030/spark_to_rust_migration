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
use deltalake::arrow::array::{Array, ArrayRef, Float64Array};
use deltalake::arrow::datatypes::{DataType, Field, Float64Type, TimeUnit};
use deltalake::datafusion::common::cast::as_float64_array;
use deltalake::datafusion::common::{exec_err, Result, ScalarValue};
use deltalake::datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use deltalake::datafusion::logical_expr::utils::format_state_name;
use deltalake::datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use deltalake::datafusion::prelude::*;
use std::any::Any;
use std::cmp::Ordering;
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
    ctx.register_udaf(spark_max_by_udaf());
    ctx.register_udaf(spark_min_by_udaf());
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

pub fn spark_round_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SparkRound {
        signature: Signature::exact(
            vec![DataType::Float64, DataType::Int64],
            Volatility::Immutable,
        ),
    })
}

/// Spark's `max_by(value, ordering)` / `min_by(value, ordering)` (DataFusion
/// has neither): the `value` of the row with the greatest / smallest
/// `ordering`. Rows with a NULL ordering are ignored, so the result is NULL
/// when every ordering in the group is NULL. On ordering ties the first row
/// seen wins, matching Spark's replace-on-strictly-greater update.
pub fn spark_max_by(value: Expr, ordering: Expr) -> Expr {
    spark_max_by_udaf().call(vec![value, ordering])
}

/// See [`spark_max_by`].
#[allow(dead_code)]
pub fn spark_min_by(value: Expr, ordering: Expr) -> Expr {
    spark_min_by_udaf().call(vec![value, ordering])
}

pub fn spark_max_by_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(SparkMinMaxBy {
        name: "spark_max_by",
        is_max: true,
        signature: Signature::any(2, Volatility::Immutable),
    })
}

pub fn spark_min_by_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(SparkMinMaxBy {
        name: "spark_min_by",
        is_max: false,
        signature: Signature::any(2, Volatility::Immutable),
    })
}

#[derive(Debug)]
struct SparkMinMaxBy {
    name: &'static str,
    is_max: bool,
    signature: Signature,
}

impl AggregateUDFImpl for SparkMinMaxBy {
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
        arg_types.first().cloned().ok_or_else(|| {
            deltalake::datafusion::error::DataFusionError::Plan(format!(
                "{} takes 2 arguments",
                self.name
            ))
        })
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let ordering_type = acc_args
            .exprs
            .get(1)
            .map(|e| e.data_type(acc_args.schema))
            .transpose()?;
        Ok(Box::new(MinMaxByAccumulator {
            is_max: self.is_max,
            value: ScalarValue::try_from(acc_args.return_type)?,
            ordering: ScalarValue::try_from(ordering_type.as_ref().unwrap_or(&DataType::Null))?,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "value"),
                args.return_type.clone(),
                true,
            ),
            Field::new(
                format_state_name(args.name, "ordering"),
                args.input_types.get(1).cloned().unwrap_or(DataType::Null),
                true,
            ),
        ])
    }
}

#[derive(Debug)]
struct MinMaxByAccumulator {
    is_max: bool,
    value: ScalarValue,
    ordering: ScalarValue,
}

impl MinMaxByAccumulator {
    /// One row of state: keep `value` when `ordering` beats the stored one.
    /// A NULL ordering means "no row seen yet" (the stored value is also NULL
    /// then) or is Spark's ignored-null-ordering case.
    fn consider(&mut self, value: ScalarValue, ordering: ScalarValue) -> Result<()> {
        if ordering.is_null() {
            return Ok(());
        }
        let beats = self.ordering.is_null()
            || match self.ordering.partial_cmp(&ordering) {
                Some(Ordering::Less) => self.is_max,
                Some(Ordering::Greater) => !self.is_max,
                // Equal or incomparable: keep the first row seen, like Spark.
                _ => false,
            };
        if beats {
            self.value = value;
            self.ordering = ordering;
        }
        Ok(())
    }
}

impl Accumulator for MinMaxByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values, orderings] = values else {
            return exec_err!("max_by/min_by take 2 arguments, got {}", values.len());
        };
        for i in 0..values.len() {
            self.consider(
                ScalarValue::try_from_array(values.as_ref(), i)?,
                ScalarValue::try_from_array(orderings.as_ref(), i)?,
            )?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.value.clone(), self.ordering.clone()])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.value.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.value.size() + self.ordering.size()
    }
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
    async fn max_by_min_by_pick_value_at_extreme_ordering() {
        let b = query(
            "SELECT spark_max_by(v, o), spark_min_by(v, o) FROM (VALUES (10.0, 1), (30.0, 3), (20.0, 2), (99.0, NULL), (NULL, 4)) t(v, o)",
        )
        .await;
        // Greatest non-NULL ordering is 4, whose value is NULL; smallest is 1.
        assert_eq!(doubles(&b, 0), [None]);
        assert_eq!(doubles(&b, 1), [Some(10.0)]);
    }

    #[tokio::test]
    async fn max_by_keeps_first_row_on_ordering_ties() {
        let b = query(
            "SELECT spark_max_by(s, o) FROM (VALUES ('first', 5), ('second', 5), ('other', 1)) t(s, o)",
        )
        .await;
        let col = b.column(0);
        assert_eq!(col.data_type(), &DataType::Utf8);
        let s = col
            .as_any()
            .downcast_ref::<deltalake::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "first");
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
