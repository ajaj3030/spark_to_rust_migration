---
name: port-spark-analytics-job
description: Port one Scala Spark analytics job (spark_jobs/analytics/.../<Job>.scala) to Rust/DataFusion in rust_pipeline/src/analytics/<job>.rs so its output matches Spark exactly (rows, values, column names, types), enable its parity test, and open a PR against demo-prepared. Use for battery_health_daily, charging_sessions, diagnostic_alerts or any later analytics job.
---

# Port one Spark analytics job to Rust

Goal: `rust_pipeline/src/analytics/<job>.rs` returns exactly what the Spark job returns on the committed fixtures: the same rows, values, column names and column order, and the same Arrow types. `ota_rollout.rs` is the reference port. Read it first.

## Rules

- **Branch from and target `demo-prepared`, never `main`.** Apply this to every branch and every PR:
  `git fetch origin demo-prepared && git checkout -b devin/$(date +%s)-port-<job> origin/demo-prepared`
- **Edit only `rust_pipeline/src/analytics/<job>.rs`.** The registry (`JOBS`), the dispatcher (`summarize`), the `Job` constant, the output write path and partitioning already exist in `analytics/mod.rs`. A parallel port of another job must never conflict with yours.
- **Never change:**
  - the parity test body (`matches_spark_output`)
  - `analytics/parity.rs`
  - `validate.rs`
  - anything under `rust_pipeline/tests/fixtures/`
  - the Spark job, or anything under `spark_jobs/`

  "Enabling the parity test" means deleting the `#[ignore = "TODO(migration): not ported yet"]` line, and nothing else.
- **Stop and ask the user if a needed helper is missing** from `analytics/spark_functions.rs`, for example a Spark function with different semantics that no helper covers. Don't add it to `spark_functions.rs` or inline a workaround in the job file without approval. Shared code is a separate, deliberate change.
- **Stop and ask the user if Spark's output looks wrong.** Examples: the expected fixture disagrees with the Scala code, or the Spark behaviour looks like a bug you would have to copy. Report the evidence. Never "fix" Spark, the fixture or the test to make parity pass.
- **No PII in the output.** `vin`, `latitude`, `longitude` and `kafka_key` are in `FORBIDDEN_PII_COLUMNS` (`validate.rs`). Ignore any branch or PR that adds them (for example PR #6's raw `vin` + `vehicle_state` join on `ota_rollout`). Match the Spark job as it is on `demo-prepared`.
- **User preference: start each port with a `//!` doc comment listing the Spark behaviours it reproduces.** Put it under the existing two-line "Rust port of the Spark job …" header. Give one bullet per behaviour that a naive DataFusion translation would get wrong. Example from `ota_rollout.rs`:
  ```rust
  //! Spark behaviours reproduced:
  //! - `orderBy(col("sequence_number").desc)` is NULLS LAST in Spark, but
  //!   DataFusion's `DESC` defaults to NULLS FIRST. The window says `NULLS LAST`
  //!   explicitly so a NULL `sequence_number` never wins `row_number() = 1`.
  //! - `round(a / b * 100, 2)` ... NULL when `vehicles = 0` (`spark_divide`),
  //!   HALF_UP on the decimal value (`spark_round`) ...
  ```

## 1. Preconditions and boundary check (before writing code)

1. Check that `rust_pipeline/src/analytics/spark_functions.rs` exists on `origin/demo-prepared`. The `ota_rollout` port added it. If it is missing, stop and ask the user.
2. Read:
   - `docs/legacy/dependencies.md`: its slice table and the line that no analytics job reads another's output.
   - `docs/legacy/jobs/<job>.md`: the output contract, the null and edge cases, and the **"Spark behaviour hardest to reproduce in Rust"** section.
3. Tell the user the boundary before coding:
   - The only changed file is `rust_pipeline/src/analytics/<job>.rs`.
   - Nothing outside the slice reads `<job>_rust`. The only consumers are the dashboard and `validate`/parity, which read it by table name.
   - No code outside the file depends on what changes.

   Note that the dashboard (`dashboard/app.py`, `_PORT_PENDING`) counts a job as ported once its file has no `#[ignore` line. That is expected. If the boundary check fails, stop and report.

## 2. Read the Scala job

- Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/<Job>.scala`, function `summarize`.
- `AnalyticsJob.scala` sets `spark.sql.session.timeZone=UTC` and does not enable ANSI mode. So dates and timestamps are UTC, `/` gives NULL on 0, and bad casts give NULL rather than an error.
- `summarize` receives the `telemetry` and, if needed, `vehicle_state` tables. Production and parity register them under exactly those names.
- Inspect the expected output's schema and rows. For example: `~/sparkvenv/bin/python -c "import pyarrow.parquet as pq; t=pq.read_table('rust_pipeline/tests/fixtures/expected/<job>'); print(t.schema); print(t.to_pandas())"`. The schema there is the contract: names, order and types.
- Translate each operator in order. Write the SQL as `ctx.sql(...)` after `spark_functions::register(ctx)`, or use the DataFrame API with the `Expr` builders.

| Scala | DataFusion |
|---|---|
| `filter(c === v)` | `WHERE` |
| `withColumn(name, e)` | a `SELECT` expression, in the same position as in Spark's column order |
| `Window.partitionBy(..).orderBy(c.desc)` | `OVER (PARTITION BY .. ORDER BY c DESC NULLS LAST)`, with the same frame (`ROWS` vs `RANGE`) |
| `row_number`/`rank`/`dense_rank` | `CAST(… AS INT)` if it reaches the output |
| `groupBy(..).agg(..)` | `GROUP BY` (NULL keys form their own group, as in Spark) |
| `groupBy(..).pivot(c, values).count()` | one `COUNT(CASE WHEN c = '<v>' THEN 1 END) AS "<v>"` per value, in list order. This keeps groups with no matching value, like Spark. |
| `na.fill(0L, cols)` / `coalesce` | `COALESCE` |
| `max_by`/`min_by` | `max_by`/`min_by` (registered UDAFs) |
| `round(x, n)` | `spark_round(x, n)` |
| `a / b` | `spark_divide(a, b)` |
| `to_timestamp(s)` | `spark_to_timestamp(s)` |
| `sha2(x, 256)` | lower-case hex SHA-256, as Spark's |
| `join(.., "left")` | `LEFT JOIN` (broadcast hints are only an optimisation) |

## 3. Shared helpers (`analytics/spark_functions.rs`)

Call `spark_functions::register(ctx);` at the start of `summarize` to use the helpers in SQL. The parity test builds its own context, so `summarize` must register them itself.

| Spark | DataFusion 43 default (wrong) | Use |
|---|---|---|
| `round(double, n)` HALF_UP on the decimal value | rounds the binary value: `round(0.285, 2) = 0.28`, Spark gives `0.29` | `spark_round(x, n)` / `spark_round(expr, n)` |
| `a / b`: always DOUBLE, NULL if `b = 0` | integer division for ints; `inf`/`NaN` for `/ 0.0` | `spark_divide(a, b)` |
| `max_by(v, o)` / `min_by(v, o)`: skip NULL `o`; a NULL `v` at the extreme row wins | not available | `max_by(v, o)` / `min_by(v, o)` |
| `to_timestamp(string)` → `timestamp[us, UTC]`; NULL if unparseable | `timestamp[ns]` without a zone; error if unparseable | `spark_to_timestamp(s)`; type `spark_timestamp_type()` |
| `row_number`/`rank`/`dense_rank` → INT (`Int32`) | `UInt64` | `CAST(… AS INT)` / `spark_int(e)` |
| `desc` = NULLS LAST, `asc` = NULLS FIRST | the opposite of both | write `NULLS LAST`/`NULLS FIRST` / `spark_desc(e)`, `spark_asc(e)` |

A Spark function that is not in this table and behaves differently in DataFusion needs a new helper. Stop and ask (see Rules).

## 4. Spark behaviours to watch for

- **Rounding:** never use DataFusion's `round`. Keep the expression shape of the Scala code. `round(a / b * 100, 2)` divides first and then scales, and `1/3*100` is not the same double as `100/3`. Different scales in one job each need their own `spark_round(…, n)`.
- **Division:** `spark_divide` for every `/`, including int / int. Doubles are compared within `1e-9` (`validate.rs`), so a different summation or operation order in `avg`/`sum` can still fail parity.
- **Types:** `check_types` compares Arrow types exactly.
  - `count`, `sum(int)` and pivot counts are `Int64` (Spark `bigint`).
  - Ranking functions are `Int32`.
  - Timestamps are `timestamp[us, UTC]`.
  - `cast(timestamp as double)` gives fractional epoch seconds. Keep microsecond precision.
  - `to_date` is evaluated in UTC.
  - `max(int).cast(boolean)` is a non-null boolean.
- **Column names and order:** match the Spark output schema exactly, including case and order. DataFusion lowercases unquoted identifiers, so double-quote any upper- or mixed-case name (`AS "UPDATE_STATE_SUCCESS"`). Output partition columns are listed in `JOB.partition_cols` and must be present.
- **Nulls:**
  - Use explicit `NULLS LAST`/`NULLS FIRST` in every `ORDER BY`.
  - A NULL group key is its own group.
  - `count(col)` skips NULLs, `count(*)` does not.
  - Arithmetic with NULL is NULL.
  - `max_by`/`min_by` ignore a NULL ordering.
  - A pivot keeps all-non-matching groups as zero rows after `na.fill`.
  - A left join keeps unmatched rows with NULLs.
- **Windows:** reproduce the frame exactly. A `RANGE BETWEEN 172800 PRECEDING AND CURRENT ROW` over seconds must order by an integer epoch-seconds expression; a `ROWS` frame is wrong when there are date gaps. `lag` across gaps propagates NULLs.
- **Fixture blind spots:** the fixtures cover few edge cases. Read the job spec's null and edge-case section and test those cases yourself (step 5).

## 5. Implement and test

1. Add the `//!` behaviour doc comment, then `summarize`. Keep `JOB` unchanged.
2. Delete only the `#[ignore …]` line above `matches_spark_output`.
3. In the job's own `#[cfg(test)] mod tests`, add unit tests for each subtle behaviour, using small `MemTable`/`RecordBatch` inputs registered as `telemetry` (see the `run(events)` helper in `ota_rollout.rs`). Cover NULL keys, NULL ordering values, ties, empty or zero denominators, values outside fixed lists, rounding half-cases, and the output column order and types. Take expected values from real Spark, not from your Rust output. A local PySpark works for this:
   ```bash
   python3 -m venv ~/sparkvenv && ~/sparkvenv/bin/pip install pyspark==3.5.1 pyarrow  # needs Java 17
   ```
   Build the session with `.config("spark.sql.session.timeZone", "UTC")`. Optionally, re-run the Scala logic in PySpark on `tests/fixtures/input/telemetry` and diff it against `tests/fixtures/expected/<job>` with `exceptAll` in both directions. Zero rows either way confirms the fixture is current Spark output.
4. Run the CI checks from `rust_pipeline/`:
   ```bash
   cargo fmt --check
   CMAKE_POLICY_VERSION_MINIMUM=3.5 cargo clippy --all-targets -- -D warnings
   CMAKE_POLICY_VERSION_MINIMUM=3.5 cargo test
   ```
   On a fresh VM the build may first need `sudo apt-get install -y pkg-config libssl-dev libcurl4-openssl-dev cmake`. The first build takes several minutes.

   A parity failure panics with `<job> does not match Spark:` followed by one line per difference: missing or extra keys, a column whose values differ, or a type mismatch. Fix the port, never the test or fixtures. If the evidence says Spark is wrong, stop and ask.

## 6. PR and CI

- Open the PR against **`demo-prepared`**. The description must include:
  1. **Boundary check:** the single changed file, and the confirmation from `dependencies.md` that no other job reads this output and nothing else depends on the changed code.
  2. **The Spark behaviour that was subtle to reproduce:** the same list as the doc comment, with concrete examples.
  3. **Parity result:** `matches_spark_output` passes, with the row count and the column count and types, plus any independent Spark cross-check you did.
- Wait for every CI check to be green. CI runs Rust fmt/clippy/test, Python lint, Scala compile and Kubernetes manifest validation. Fix any failure your change caused. Do not merge unless asked.
