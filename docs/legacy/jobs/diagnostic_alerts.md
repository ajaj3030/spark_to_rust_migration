# `diagnostic_alerts` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/DiagnosticAlerts.scala` (abbreviated `DiagnosticAlerts.scala`).
Rust target: `rust_pipeline/src/analytics/diagnostic_alerts.rs` (currently a stub, `diagnostic_alerts.rs:16`).

## Purpose

Fault codes (DTCs) by ECU, model and battery pack: events, vehicles affected, share of fleet, rank within ECU — for service
triage (`dashboard/app.py:178-207`, `DiagnosticAlerts.scala:11-34`).

## Inputs

| Table | Columns used | Source |
|-------|--------------|--------|
| `vehicle_telemetry` | `event_type`, `vin_hash`, `dtc_code`, `ecu`, `model_safe`, `warning_state`, `event_time` (and `battery_pack_type`, which is dropped) | `DiagnosticAlerts.scala:16-28` |
| `vehicle_state` | `vin` (raw), `battery_pack_type`; row count | `DiagnosticAlerts.scala:13-14`, `36-40` |

This is the only analytics job with two inputs and the only one that reads the raw VIN (`vehicle_state` keeps `vin`;
`spark_jobs/vehicle_state_job.py:58`). Rust ports read `vehicle_state_rust` (`rust_pipeline/src/analytics/mod.rs:86-87`).

## Output schema

Unpartitioned Delta table `analytics/diagnostic_alerts` (`rust_pipeline/src/analytics/diagnostic_alerts.rs:11`). Types from
`rust_pipeline/tests/fixtures/expected/diagnostic_alerts`:

| Column | Type | Notes |
|--------|------|-------|
| `dtc_code` | string | parity key (with next three) |
| `ecu` | string | |
| `model_safe` | string | |
| `battery_pack_type` | string | nullable (17 null rows in fixture) |
| `events` | bigint | |
| `vehicles_affected` | bigint | |
| `warning_on_events` | bigint | |
| `first_seen` | timestamp (µs, UTC) | |
| `last_seen` | timestamp (µs, UTC) | |
| `fleet_pct_affected` | double | 3 dp |
| `rank_in_ecu` | **int** (Int32) | |

Parity key: `dtc_code`, `ecu`, `model_safe`, `battery_pack_type` (`diagnostic_alerts.rs:10`).

## Calculation

1. `fleet = vehicle_state.select(sha2(vin, 256) AS vin_hash, battery_pack_type)` (`DiagnosticAlerts.scala:13`) — re-derives the
   same hash T6 uses (`spark_jobs/transforms.py:173`).
2. `fleetSize = vehicle_state.count()` — an eager Spark action evaluated when the plan is built, then used as a literal
   (`DiagnosticAlerts.scala:14`).
3. Telemetry filtered to `event_type = 'DIAGNOSTIC'`, `battery_pack_type` dropped, then **left** join with `broadcast(fleet)` on
   `vin_hash` (`DiagnosticAlerts.scala:17-20`). Pack type therefore always comes from vehicle state.
4. Group by `dtc_code`, `ecu`, `model_safe`, `battery_pack_type` (`DiagnosticAlerts.scala:21`) and aggregate (`DiagnosticAlerts.scala:22-28`):
   `events = count(*)`, `vehicles_affected = count(DISTINCT vin_hash)`,
   `warning_on_events = sum(CASE WHEN warning_state = 'WARNING_ON' THEN 1 ELSE 0 END)`,
   `first_seen = min(to_timestamp(event_time))`, `last_seen = max(to_timestamp(event_time))`.
5. `fleet_pct_affected = round(vehicles_affected / fleetSize * 100, 3)` (`DiagnosticAlerts.scala:32`).
6. `rank_in_ecu = dense_rank() OVER (PARTITION BY ecu ORDER BY vehicles_affected DESC)` (`DiagnosticAlerts.scala:30`, `33`).

## Nulls and edge cases

- **Vehicle in telemetry but not in state** (3 in the fixture): left join keeps the events with `battery_pack_type` null; these
  form their own null-pack groups (17 rows in the fixture).
- **Fleet denominator** is every state row, including vehicles with no diagnostic events (147 state rows in the fixture), and
  excludes telemetry-only vehicles — so the percentage is "of known fleet", not "of vehicles seen".
- **Duplicate VINs in state** would multiply events; prevented by the MERGE keeping one row per VIN (`spark_jobs/vehicle_state_job.py:120-143`).
- **Empty `vehicle_state`:** `fleetSize = 0` → division yields null (non-ANSI) for every row.
- **Null `dtc_code`/`ecu`** form their own groups/rank partitions; `warning_state` null counts as 0 (the `otherwise(0)` branch).
- **Ties in `vehicles_affected`** share a rank, and the next rank is consecutive (dense).
- **Unparseable `event_time`** → null timestamp, ignored by `min`/`max`.

## Spark behaviour hardest to reproduce in Rust

1. **Second input with the raw VIN** — the port must hash `vehicle_state.vin` exactly as Spark's `sha2(…, 256)` (lower-case hex)
   and must keep `vin` out of the output (`rust_pipeline/src/validate.rs:19`).
2. **`dense_rank` type** — Spark returns `Int32`; DataFusion returns `UInt64`. Needs an explicit cast; `check_types` fails otherwise (`rust_pipeline/src/analytics/mod.rs:36-45`).
3. **`DESC` null ordering** in the rank window — Spark `NULLS LAST`, DataFusion `NULLS FIRST` by default.
4. **Eager `count()` as literal** — must be computed from the same `vehicle_state` snapshot as the join.
5. **`to_timestamp` units** (µs UTC vs DataFusion ns) and **`round` HALF_UP** (`rust_pipeline/src/transforms/rounding.rs:7`).
6. **Broadcast left join** is an optimisation only; the semantics are an ordinary left join, but null-keyed group handling must match.
7. **End-to-end dependency on the Rust state stream** (`rust_pipeline/src/vehicle_state.rs`) for live parity, not just the telemetry stream.
