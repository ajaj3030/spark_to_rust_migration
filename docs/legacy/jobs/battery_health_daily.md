# `battery_health_daily` — functional spec

Source: `spark_jobs/analytics/src/main/scala/telemetry/analytics/BatteryHealthDaily.scala` (abbreviated `BatteryHealthDaily.scala`).
Rust target: `rust_pipeline/src/analytics/battery_health_daily.rs` (currently a stub, `battery_health_daily.rs:16`).

## Purpose

Per vehicle per day: charge level, cell temperatures, thermal alerts, end-of-day battery health and its day-over-day change
(`dashboard/app.py:178-207`, `BatteryHealthDaily.scala:14-41`).

## Inputs

| Table | Columns used | Source |
|-------|--------------|--------|
| `vehicle_telemetry` | `event_type`, `vin_hash`, `event_date`, `model_safe`, `charge_percent`, `max_cell_temp`, `cell_temp_spread`, `thermal_alert`, `battery_health_level`, `sequence_number` | `BatteryHealthDaily.scala:14-41` |

`event_date` (UTC), `cell_temp_spread` and `thermal_alert` are enrichment outputs of T2–T4 (`spark_jobs/transforms.py:87`, `117-118`, `129-132`).

## Output schema

Delta table `analytics/battery_health_daily`, **partitioned by `event_date`** (`BatteryHealthDaily.scala:10`,
`rust_pipeline/src/analytics/battery_health_daily.rs:11`). Types from `rust_pipeline/tests/fixtures/expected/battery_health_daily`:

| Column | Type | Notes |
|--------|------|-------|
| `vin_hash` | string | parity key with `event_date` (`battery_health_daily.rs:10`) |
| `event_date` | date | partition column |
| `model_safe` | string | |
| `readings` | bigint | |
| `avg_charge_percent` | double | 2 dp |
| `min_charge_percent` | double | |
| `peak_cell_temp` | double | |
| `avg_cell_temp_spread` | double | 3 dp |
| `thermal_alert_count` | bigint | |
| `health_level_end_of_day` | double | nullable (15 nulls in fixture) |
| `peak_cell_temp_3d` | double | |
| `health_change` | double | 2 dp; nullable (161 nulls in fixture) |

## Calculation

1. Filter `event_type = 'BATTERY'` (`BatteryHealthDaily.scala:16`).
2. Group by `vin_hash`, `event_date`, `model_safe` (`BatteryHealthDaily.scala:17`) and aggregate (`BatteryHealthDaily.scala:18-26`):
   - `readings = count(*)`
   - `avg_charge_percent = round(avg(charge_percent), 2)`; `min_charge_percent = min(charge_percent)`
   - `peak_cell_temp = max(max_cell_temp)`
   - `avg_cell_temp_spread = round(avg(cell_temp_spread), 3)`
   - `thermal_alert_count = sum(cast(thermal_alert as int))`
   - `health_level_end_of_day = max_by(battery_health_level, sequence_number)` — health on the day's last reading
3. `peak_cell_temp_3d = max(peak_cell_temp)` over `PARTITION BY vin_hash ORDER BY cast(cast(event_date as timestamp) as long)
   RANGE BETWEEN 2 days PRECEDING AND CURRENT ROW` (`SecondsPerDay`, `BatteryHealthDaily.scala:12`, `28-32`, `36`) — the
   current day and the two *calendar* days before it, only those that have rows.
4. `health_change = round(health_level_end_of_day - lag(health_level_end_of_day, 1), 2)` over `PARTITION BY vin_hash ORDER BY
   event_date` (`BatteryHealthDaily.scala:33`, `37-40`) — the previous *row*, i.e. the previous day that has data.

## Nulls and edge cases

- **Date gaps** (5 vehicles in the fixture): the 3-day `RANGE` window skips missing days (a day after a 3-day gap sees only
  itself), whereas `lag` reaches back across the gap to the last day with data. The two windows deliberately differ.
- **First day per vehicle:** `lag` is null → `health_change` null.
- **Null health on the last reading:** `max_by` returns the value from the max-`sequence_number` row even when it is null
  (Spark 3.5 `MaxMinBy`), so `health_level_end_of_day` is null and so is that day's and the next day's `health_change`.
  Fixture: 15 null end-of-day values, 161 null `health_change`.
- **`avg`/`min`/`max`** ignore nulls; all-null inputs give null. `thermal_alert` is never null after T4 (`spark_jobs/transforms.py:60`, `129-132`), so `thermal_alert_count` is non-null.
- **`event_date`** is derived in UTC by the stream (`spark_jobs/transforms.py:82-87`); the window's day-seconds also use UTC
  (`AnalyticsJob.scala:56-66`), so midnight boundaries are consistent.
- **Model change within a day** would produce two rows for the same parity key; not possible with generator data (one model per vehicle).

## Spark behaviour hardest to reproduce in Rust

1. **`max_by(value, ordering)`** — has no built-in equivalent in DataFusion 43 (`rust_pipeline/Cargo.lock`, via `deltalake` in `rust_pipeline/Cargo.toml:13`); must be
   emulated (e.g. `row_number` per group, or `LAST_VALUE … ORDER BY sequence_number`) while keeping "null at the max row wins".
2. **`RANGE` window over seconds** — DataFusion must order by an integer epoch-seconds expression (or a date with an interval
   frame) to reproduce `RANGE BETWEEN 172800 PRECEDING AND CURRENT ROW`; a `ROWS` frame gives wrong answers on the 5 gap vehicles.
3. **`lag` across gaps** combined with null propagation in subtraction and rounding.
4. **`round(avg(…))` HALF_UP** at 2 and 3 dp (`rust_pipeline/src/transforms/rounding.rs:7`); `avg` of doubles must match
   Spark's summation order closely enough for the `1e-9` tolerance (`rust_pipeline/src/validate.rs:23`).
5. **Integer widths** — `count(*)` and `sum(int)` are `bigint` in Spark.
6. **Partitioning by a `date` column** (`rust_pipeline/src/analytics/mod.rs:97-111`).
