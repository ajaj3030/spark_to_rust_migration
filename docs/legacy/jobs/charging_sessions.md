# `charging_sessions` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/ChargingSessions.scala` (abbreviated `ChargingSessions.scala`).
Rust target: `rust_pipeline/src/analytics/charging_sessions.rs` (currently a stub, `charging_sessions.rs:16`).

## Purpose

One row per charging session: duration, energy added, peak and average power, start/end state of charge, final charge
state and whether a fault occurred (`dashboard/app.py:178-207`, `ChargingSessions.scala:11-38`).

## Inputs

| Table | Columns used | Source |
|-------|--------------|--------|
| `vehicle_telemetry` | `event_type`, `charge_session_id`, `vin_hash`, `model_safe`, `event_time`, `energy_added_kwh`, `charging_power_kw`, `charge_percent`, `charge_state`, `sequence_number`, `is_fault` | `ChargingSessions.scala:11-38` |

`is_fault` is itself derived in T4 from `charge_state` and DTC/warning fields (`spark_jobs/transforms.py:133-137`).
Sessions are created by the generator: a `charge_session_id` UUID is assigned when charging starts and cleared when the
session ends (`data_generator/generator.py:87-105`).

## Output schema

Delta table `analytics/charging_sessions`, **partitioned by `session_date`** (`ChargingSessions.scala:9`,
`rust_pipeline/src/analytics/charging_sessions.rs:11`). Types from `rust_pipeline/tests/fixtures/expected/charging_sessions`:

| Column | Type | Notes |
|--------|------|-------|
| `charge_session_id` | string | parity key (`charging_sessions.rs:10`) |
| `vin_hash` | string | |
| `model_safe` | string | |
| `session_start` | timestamp (µs, UTC) | |
| `session_end` | timestamp (µs, UTC) | |
| `events` | bigint | |
| `energy_added_kwh` | double | rounded to 3 dp |
| `peak_power_kw` | double | |
| `soc_start` | double | nullable (15 nulls in fixture) |
| `soc_end` | double | nullable |
| `end_state` | string | nullable |
| `had_fault` | boolean | |
| `duration_minutes` | double | rounded to 2 dp |
| `avg_power_kw` | double | null when `duration_minutes <= 0` |
| `session_date` | date | partition column |

## Calculation

1. Filter `event_type = 'CHARGING' AND charge_session_id IS NOT NULL` (`ChargingSessions.scala:13`).
2. Group by `charge_session_id`, `vin_hash`, `model_safe` (`ChargingSessions.scala:14`) and aggregate (`ChargingSessions.scala:15-25`):
   - `session_start = min(to_timestamp(event_time))`, `session_end = max(to_timestamp(event_time))`
   - `events = count(*)`
   - `energy_added_kwh = round(sum(energy_added_kwh), 3)`
   - `peak_power_kw = max(charging_power_kw)`
   - `soc_start = min_by(charge_percent, sequence_number)`, `soc_end = max_by(charge_percent, sequence_number)`
   - `end_state = max_by(charge_state, sequence_number)`
   - `had_fault = cast(max(cast(is_fault as int)) as boolean)`
3. `duration_minutes = round((cast(session_end as double) - cast(session_start as double)) / 60, 2)` — seconds since epoch
   with fractional microseconds (`ChargingSessions.scala:27-28`, `31`).
4. `avg_power_kw = round(energy_added_kwh / (duration_minutes / 60), 2)` **only when** `duration_minutes > 0`; otherwise null
   (`when` without `otherwise`, `ChargingSessions.scala:32-36`). It uses the already-rounded energy and duration.
5. `session_date = to_date(session_start)` in the UTC session time zone (`ChargingSessions.scala:37`, `AnalyticsJob.scala:56-66`).

## Nulls and edge cases

- **Events with no session id** are dropped (`ChargingSessions.scala:13`).
- **Single-event sessions** (194 in the fixture): `session_start = session_end`, `duration_minutes = 0`, `avg_power_kw` null.
- **Sub-second sessions:** a positive duration under 0.3 s rounds to `0.00` and also yields null `avg_power_kw`, because the guard tests the rounded value (`ChargingSessions.scala:31-35`).
- **Null `charge_percent`** at the first/last sequence number: `min_by`/`max_by` return the value of that row even when null — Spark 3.5 `MaxMinBy` keeps the value of the extreme ordering row; the fixture's 15 null `charge_percent` charging rows produce 15 null `soc_start` values.
- **`min`/`max`/`sum`** ignore nulls; all-null inputs give null.
- **Unparseable `event_time`:** `to_timestamp` returns null in non-ANSI Spark; such rows are ignored by `min`/`max`.
- **Session spanning midnight UTC** is dated by its start (`ChargingSessions.scala:37`).
- **Ties on `sequence_number`** within a session: `min_by`/`max_by` pick an arbitrary row; sequence numbers are unique per vehicle (`data_generator/generator.py:150-154`).

## Spark behaviour hardest to reproduce in Rust

1. **`min_by` / `max_by`** — have no built-in equivalent in DataFusion 43 (`rust_pipeline/Cargo.lock`, pulled in by `deltalake = 0.22` in `rust_pipeline/Cargo.toml:13`);
   must be emulated with `FIRST_VALUE(x ORDER BY seq)`/`LAST_VALUE`/window + filter, while keeping "null value at the extreme row
   wins" semantics.
2. **`to_timestamp(string)` units** — Spark yields microsecond `TimestampType`; DataFusion's `to_timestamp` yields nanoseconds
   without a zone. The output must be `timestamp[us, UTC]` and `check_types` compares units (`rust_pipeline/src/validate.rs`).
3. **`cast(timestamp as double)`** — Spark gives fractional epoch seconds; the Rust port must keep microsecond precision before
   dividing by 60, or `duration_minutes` and `avg_power_kw` drift past the `1e-9` tolerance (`rust_pipeline/src/validate.rs:23`).
4. **`round(…)` HALF_UP** for three different scales (`rust_pipeline/src/transforms/rounding.rs:7`).
5. **`to_date` in UTC** and Delta partitioning by a date column (`ChargingSessions.scala:9`, `37`; `rust_pipeline/src/analytics/mod.rs:97-111`).
6. **`max(int).cast(boolean)`** — straightforward, but must produce a non-null boolean when `is_fault` is non-null (it always is after T4's `coalesce`, `spark_jobs/transforms.py:60`).
