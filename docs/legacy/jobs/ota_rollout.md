# `ota_rollout` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/OtaRollout.scala` (abbreviated `OtaRollout.scala`).
Rust target: `rust_pipeline/src/analytics/ota_rollout.rs` (currently a stub, `ota_rollout.rs:16`).

## Purpose

For each OTA rollout (target software version × model), how many vehicles are in each update state *right now*, and the
success and failure rates — "latest OTA update state per vehicle, pivoted by target software version and model, with
success and failure rates" (`dashboard/app.py:178-207`, `OtaRollout.scala:26-44`).

## Inputs

| Table | Columns used | Source |
|-------|--------------|--------|
| `vehicle_telemetry` (Spark) / `telemetry` registered from `vehicle_telemetry_rust` (Rust) | `event_type`, `vin_hash`, `target_software_version`, `sequence_number`, `model_safe`, `update_state` | `OtaRollout.scala:26-44`, `OtaRollout.scala:46-47`, `rust_pipeline/src/analytics/mod.rs:82-87` |

No `vehicle_state`, no other analytics table (`OtaRollout.scala:46-47` reads telemetry only).

## Output schema

Unpartitioned Delta table `analytics/ota_rollout` (`OtaRollout.scala` does not override `partitionCols`; `rust_pipeline/src/analytics/ota_rollout.rs:11`).
Types from `rust_pipeline/tests/fixtures/expected/ota_rollout` (6 rows), column order as Spark emits it:

| # | Column | Type | Nullable in practice |
|---|--------|------|----------------------|
| 1 | `target_software_version` | string | yes (null group key is kept) |
| 2 | `model_safe` | string | no (T1 coalesce, `spark_jobs/transforms.py:73`) |
| 3 | `UPDATE_STATE_IN_PROGRESS` | bigint | no (`na.fill(0)`) |
| 4 | `UPDATE_STATE_WAITING_ON_BCM` | bigint | no |
| 5 | `UPDATE_STATE_SUCCESS` | bigint | no |
| 6 | `UPDATE_SUCCESS_WITH_WARNINGS` | bigint | no |
| 7 | `UPDATE_STATE_FAILED` | bigint | no |
| 8 | `UPDATE_FAILED_DRIVE_ALLOWED` | bigint | no |
| 9 | `UPDATE_FAILED_NO_ACTION` | bigint | no |
| 10 | `vehicles` | bigint | no |
| 11 | `success_rate_pct` | double | yes, when `vehicles = 0` |
| 12 | `failure_rate_pct` | double | yes, when `vehicles = 0` |

Parity key: `target_software_version`, `model_safe` (`rust_pipeline/src/analytics/ota_rollout.rs:10`).

## Calculation

1. Filter telemetry to `event_type = 'OTA'` (`OtaRollout.scala:32`).
2. Window `PARTITION BY vin_hash, target_software_version ORDER BY sequence_number DESC`; keep `row_number() = 1`, i.e. the
   newest OTA event per vehicle per target version (`OtaRollout.scala:27-34`).
3. Group by `target_software_version`, `model_safe` (`OtaRollout.scala:37`).
4. Pivot `update_state` over the **fixed** list of seven values in `UpdateStates` and `count()` rows per value
   (`OtaRollout.scala:11-19`, `38-39`). Columns appear in list order.
5. Replace null pivot counts with `0` (`OtaRollout.scala:40`).
6. `vehicles` = sum of the seven state columns (`OtaRollout.scala:23`, `41`).
7. `success_rate_pct = round((UPDATE_STATE_SUCCESS + UPDATE_SUCCESS_WITH_WARNINGS) / vehicles * 100, 2)` (`Succeeded`, `OtaRollout.scala:20`, `24`, `42`).
8. `failure_rate_pct = round((UPDATE_STATE_FAILED + UPDATE_FAILED_DRIVE_ALLOWED + UPDATE_FAILED_NO_ACTION) / vehicles * 100, 2)` (`Failed`, `OtaRollout.scala:21`, `24`, `43`).
   `IN_PROGRESS` and `WAITING_ON_BCM` count toward `vehicles` but neither rate, so the two rates need not sum to 100.

## Nulls and edge cases

- **Vehicle with several target versions:** counted once per target version — the window partitions by both keys (`OtaRollout.scala:27-29`).
- **Same vehicle in two models:** not possible in practice; `model_safe` derives from the vehicle's model (`spark_jobs/transforms.py:73`), and the generator gives a vehicle one model (`data_generator/generator.py`).
- **`update_state` null or outside the seven values:** the row still survives the window and still forms/joins its group, but contributes to no pivot column. A group whose latest rows are *all* outside the list is still emitted with every state column 0, `vehicles = 0`.
- **`vehicles = 0`:** `x / 0` in non-ANSI Spark SQL is `NULL`, so both rates are null (`OtaRollout.scala:24`; ANSI mode is not enabled in `AnalyticsJob.scala:56-66`).
- **Null `target_software_version`:** forms its own window partition and its own output group (Spark groups nulls together). The parity join treats null keys as equal (`rust_pipeline/src/validate.rs:233-240`).
- **Ties on `sequence_number` within a partition:** `row_number` picks one row arbitrarily. The generator makes `sequence_number` strictly increasing per vehicle (`data_generator/generator.py:150-154`), and the fixture has no duplicate `(vin_hash, sequence_number)` pairs, so this does not arise in fixtures.
- **Fixture coverage:** all seven states appear in `rust_pipeline/tests/fixtures/input/telemetry`, but the expected output has only 6 rows and none with `vehicles = 0` or a null target version — those cases are untested by the parity fixture.

## Spark behaviour hardest to reproduce in Rust

1. **`pivot(col, values).count()` + `na.fill`** — DataFusion has no `PIVOT`; it must be emulated with one
   `SUM(CASE WHEN update_state = '<state>' THEN 1 ELSE 0 END)` per state, preserving Spark's column order and emitting
   groups whose rows fall outside the list (`OtaRollout.scala:38-40`).
2. **Integer width** — pivot counts and `vehicles` are `bigint` (Int64) in the fixture; DataFusion `SUM` of an `Int32`/`Int64`
   literal must be cast to `BIGINT`, and `row_number()` returns `UInt64` in DataFusion, which must not leak into the output.
   `check_types` makes a type mismatch a parity failure (`rust_pipeline/src/analytics/mod.rs:36-45`).
3. **`DESC` null ordering** — Spark `.desc` is `NULLS LAST`; DataFusion `DESC` defaults to `NULLS FIRST`, so the window needs an
   explicit `NULLS LAST` to pick the same row if `sequence_number` is ever null (`OtaRollout.scala:29`).
4. **`round(x, 2)`** — Spark rounds `HALF_UP` on the decimal value; use `spark_round` (`rust_pipeline/src/transforms/rounding.rs:7`)
   or an equivalent, not `f64::round` on the scaled binary value.
5. **Division by zero → `NULL`** rather than `inf`/`NaN`/error (`OtaRollout.scala:24`).

Everything else (filter, window, group by, sum) is plain SQL.

## Pending change to watch

PR #6 (`ota_rollout: include a failed vehicle's VIN for support lookups`, base `demo-privacy-base`) adds a raw `vin` column to
the Rust `ota_rollout` output by joining `vehicle_state`. That would add a second input and a column that
`rust_pipeline/src/validate.rs:19` (`FORBIDDEN_PII_COLUMNS`) rejects, and it diverges from the Spark contract above. This
spec documents the Spark job as it is on `demo-prepared`.
