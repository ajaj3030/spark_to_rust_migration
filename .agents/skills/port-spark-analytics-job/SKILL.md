---
name: port-spark-analytics-job
description: Port one Scala Spark analytics job (spark_jobs/analytics/) to Rust/DataFusion in rust_pipeline/src/analytics/<job>.rs so its output matches Spark exactly, enable its parity test, get CI green and open a PR. Use when asked to port battery_health_daily, charging_sessions, diagnostic_alerts or any other job in that folder.
---

# Port one Spark analytics job to Rust

A job is ported when `rust_pipeline/src/analytics/<job>.rs` reproduces
`spark_jobs/analytics/src/main/scala/telemetry/analytics/<Job>.scala`
exactly and its parity test is no longer `#[ignore]`d. "Exactly" means the
same rows, values, column names and Arrow types as Spark's output in
`rust_pipeline/tests/fixtures/expected/<job>/`. `ota_rollout.rs` is the
worked example to copy from.

## Rules

1. **Edit only `rust_pipeline/src/analytics/<job>.rs`.** The registry, the
   dispatcher and the parity harness in `mod.rs` already list every job, so
   ports running in parallel never touch the same file.
2. **Never change** the parity test body (the only allowed edit is deleting
   its `#[ignore = ...]` line), `analytics/parity.rs`, `validate.rs`, anything
   under `rust_pipeline/tests/fixtures/`, or any Scala under `spark_jobs/`.
3. **Shared helpers live in `analytics/spark_functions.rs`.** Use them; do
   not reimplement rounding, division or casts inside the job file. If the
   job needs a Spark-compatible helper that is not there yet (e.g. `max_by`),
   add it there and unit-test it. Never modify, rename or delete an existing
   helper or its semantics; if a change is needed, stop and ask.
4. **If Spark's output looks wrong** (a bug in the Scala job, a fixture value
   you believe is incorrect, behaviour you would not want to copy), stop and
   tell the user with the evidence. Do not work around it, "fix" it in Rust,
   or open the PR.
5. Never relax the comparison (no `ignore_columns`, no tolerance changes, no
   `check_types: false`) to make the test pass.

## 1. Read the Scala job

Read `<Job>.scala` and `AnalyticsJob.scala`. Write down, before coding:

- **Inputs**: `telemetry` only, or also `vehicle_state`. Both are registered
  on the `SessionContext` under those names (`parity::fixture_context`, and
  `analytics::run` in production). Read them with `ctx.table("telemetry")`.
- **Every step in order**: filters, `withColumn`s, windows (partition, order,
  frame), `groupBy`/`agg`, `pivot`, joins (type and keys), `na.fill`,
  `dropDuplicates`, final `select`.
- **Output columns** in order, and each one's Spark type. Confirm against
  the fixture schema:
  ```bash
  python3 -c "import pyarrow.parquet as pq; print(pq.read_table('rust_pipeline/tests/fixtures/expected/<job>').schema)"
  ```
- **Key columns**: already in `JOB.key_cols` in the placeholder file. Leave
  them alone.
- **Session config that changes semantics**: `spark.sql.session.timeZone` is
  `UTC`; ANSI mode is off (Spark 3.5 default).
- **`partitionCols`**: Spark partitions some outputs on disk. The Rust writer
  does not, and the parity check ignores file layout. Not part of the port.

Also check who reads the output: `dashboard/app.py` (Fleet tab and Migration
page) selects some columns by name or prefix.

## 2. Shared helpers (`analytics/spark_functions.rs`)

| Spark | DataFusion default | Use |
|---|---|---|
| `round(x, n)` on a double: HALF_UP on the decimal value (`round(1.005, 2) = 1.01`) | binary rounding (`1.0`) | `spark_round(expr, n)` |
| `a / b`: always DOUBLE, NULL when `b = 0` | int / int is integer division; int / 0 errors; x / 0.0 is inf/NaN | `spark_divide(a, b)` |
| `row_number`, `rank`, `dense_rank` are INT | UInt64 | `spark_int(expr)` |
| timestamps are `timestamp[us, UTC]` | `to_timestamp` gives `timestamp[ns]`, no zone | `spark_timestamp(expr)` |

All four are `Expr` builders for the DataFrame API. `spark_round` and
`spark_divide` are also SQL UDFs: if you write SQL, call
`spark_functions::register(ctx)` at the start of `summarize`, because the
parity harness builds its own context. Use `crate::transforms::rounding`
only through these wrappers.

## 3. Spark behaviour to watch for

**Rounding**
- Every Spark `round` becomes `spark_round`. Never use DataFusion's `round`.
- Keep Spark's exact operation order: `round(n / d * 100, 2)` is not the
  same double as `round(n * 100 / d, 2)`.
- Rounding happens on the final aggregate; floating-point sums depend on
  order, so near-.5 values can round differently. If the parity test fails on
  a few rounded values, compare the unrounded values before blaming rounding.
- If a later column is computed from an already-rounded column in Scala, use
  the rounded one in Rust too.

**Division**
- Every Spark `/` becomes `spark_divide`, even when both sides look like
  doubles: a zero divisor must give NULL, not inf/NaN.

**Types** (the parity check compares Arrow types)
- `count`, `countDistinct`, `sum` of integers: BIGINT (`Int64`).
- `avg`, `sum` of doubles, any `/`, `round` of a double: DOUBLE.
- `lit(0) + bigint` is BIGINT; use `lit(0i64)` in Rust.
- Ranking functions: `spark_int(...)` when they reach the output. When only
  used in a filter, compare with `lit(1u64)` and don't output them.
- Timestamps: `spark_timestamp(...)`. Dates (`to_date`, `cast(... as date)`):
  `Date32`, derived from the UTC timestamp.
- Booleans stay `Boolean`; strings stay `Utf8` (check the fixture: not
  `Utf8View`/`LargeUtf8`).

**Column names**
- DataFusion SQL lowercases unquoted identifiers, and `col("...")` parses
  its argument as a (possibly qualified) name. For names with uppercase
  letters use `.alias("NAME")` to create them and `ident("NAME")` to refer to
  them, as `ota_rollout.rs` does.
- Output must have exactly Spark's columns: no helper columns (`rn`, etc.)
  left over, no missing ones. Order does not matter to the check, but keep
  Spark's order anyway.

**NULLs**
- Spark `.desc` is `DESC NULLS LAST`, `.asc` is `ASC NULLS FIRST`.
  DataFusion's `Expr::sort(asc, nulls_first)` must spell this out:
  `sort(false, false)` for desc, `sort(true, true)` for asc.
- `===` with NULL is NULL, so filters drop NULL rows; `=!=` also drops them.
- NULL keys form their own group in `groupBy` and their own partition in
  windows. Both engines agree; don't filter them out.
- `count(col)` skips NULLs, `count(*)`/`count()` doesn't.
- `na.fill(v, cols)` only touches the listed columns, and only matching types.
- Left joins produce NULLs for unmatched rows even when the fixture never has
  any; handle them the way Spark does.

**Pivot** (DataFusion has none)
- `groupBy(keys).pivot(c, values).count()` becomes one
  `count(when(col(c).eq(lit(v)), lit(1)).end()?).alias(v)` per value over
  all rows. Rows whose value is not in the list (or NULL) still create the
  group; they just add 0 to every count. With `na.fill(0)` the counts are
  BIGINT. Matching is case-sensitive.

**Windows**
- `rowsBetween` counts rows; `rangeBetween` counts values of the order key
  (e.g. calendar days). The fixtures have no gaps, so a ROWS frame can pass
  the test while being wrong; use the frame Spark uses.
- Spark's default frame with an `orderBy` is `RANGE BETWEEN UNBOUNDED
  PRECEDING AND CURRENT ROW` (ties included), without `orderBy` it is the whole
  partition.
- `row_number` ties are broken arbitrarily by Spark; you can't match that.
  Check whether ties are possible (`sequence_number` is unique per vehicle)
  and note it in the PR.
- `lag`/`lead` return NULL at the partition edges.

**Functions DataFusion lacks or treats differently**
- `max_by`/`min_by`: none in DataFusion. Replace with a `row_number` window
  or `first_value`/`last_value` with an explicit order; if a reusable helper
  is needed, see rule 3. NULL order values are ignored by Spark.
- Spark `sha2(x, 256)` returns a lowercase hex string; DataFusion's
  `sha256`/`digest` return Binary, so wrap them in `encode(..., 'hex')`.
  Check the input is the same string (no trimming or case change).
- `countDistinct` ignores NULLs.

## 4. Implement

Follow the shape of `ota_rollout.rs`:

```rust
use super::spark_functions::{spark_divide, spark_round};
use super::Job;
use anyhow::Result;
use deltalake::datafusion::prelude::*;

pub const JOB: Job = Job { name: "<job>", key_cols: &[/* unchanged */] };

pub async fn summarize(ctx: &SessionContext) -> Result<DataFrame> {
    let telemetry = ctx.table("telemetry").await?;
    // one DataFrame step per Scala step, same order, with a short comment
    // wherever Spark's default differs from DataFusion's
    todo!()
}
```

Prefer the DataFrame API; it avoids identifier lowercasing. Keep job-only
constants (state lists, thresholds) in the job file.

## 5. Verify

From `rust_pipeline/` (locally the build needs
`sudo apt-get install -y build-essential cmake pkg-config libssl-dev libcurl4-openssl-dev`):

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
CMAKE_POLICY_VERSION_MINIMUM=3.5 cargo test analytics::<job>
CMAKE_POLICY_VERSION_MINIMUM=3.5 cargo test
```

- The parity test must say `ok`, not `ignored`. Its failure message lists
  every missing row, mismatched value and schema difference.
- The fixtures are a small slice (~150 vehicles) with no NULLs, no ties and
  no gaps between days. Passing them does not prove the edge cases in
  section 3. Where possible, run the same steps in PySpark 3.5 on a few
  hand-made edge-case rows and compare with the Rust output; don't commit
  that data.
- Push and wait for every CI job in `.github/workflows/ci.yml` to pass: Rust
  (fmt, clippy, tests incl. parity), Python lint, Scala compile, Kubernetes
  validate. The image build runs on `main` only.
- If a live environment exists, also run
  `rust-pipeline --config config/app_config.yaml analytics <job>` then
  `validate --job <job>`.

## 6. Open the PR

Title: `Port <job> to Rust`. The description must include:
- What `summarize` does, in Spark terms.
- Confirmation that only `<job>.rs` changed, and the test change is just the
  removed `#[ignore]`.
- **Spark behaviour that was subtle to reproduce**: a numbered list, one item
  per behaviour, saying what Spark does, what DataFusion would do by default,
  and how the port matches Spark.
- Verification: CI jobs, the parity test, and any extra edge-case comparison.
- Anything that can't be matched exactly (e.g. arbitrary tie-breaking), and
  why it doesn't matter for this data.

## Known pitfalls of the remaining jobs

- **battery_health_daily**: the 3-day peak is a `rangeBetween` over
  calendar days, not 3 rows; end-of-day health uses `max_by`; rounded
  averages are sensitive to summation order.
- **charging_sessions**: start/end charge and end state use `max_by`/`min_by`;
  timestamps must be `timestamp[us, UTC]`; average power uses the *rounded*
  duration; zero-length sessions give NULL average power; `session_date` comes
  from the session start (sessions cross midnight).
- **diagnostic_alerts**: joins `vehicle_state` on a SHA-256 of the VIN; fleet
  size counts the whole `vehicle_state` table; the left join can give a NULL
  pack type; `rank_in_ecu` is a dense rank cast to INT.
