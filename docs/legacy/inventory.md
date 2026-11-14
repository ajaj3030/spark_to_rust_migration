# Legacy pipeline inventory

Every job that produces or consumes pipeline data, in any language, as of the `demo-prepared` branch.
Each claim cites the file (and line where useful) it was read from. Scala sources live in
`spark_jobs/analytics/src/main/scala/telemetry/analytics/`, abbreviated below as `…/analytics/`.

Related: [dependencies.md](dependencies.md) (data/code dependency map and migration slices) and the
per-job functional specs in [jobs/](jobs/).

## Summary

| # | Job | Kind | Language | Reads | Writes | Partition columns | Deployed by |
|---|-----|------|----------|-------|--------|-------------------|-------------|
| 1 | `data_generator` | Producer | Python | — | Kafka `vehicle_telemetry`, `vehicle_state_updates` | Kafka key = VIN | Host process (`python -m data_generator.generator`) |
| 2 | `streaming_job.py` | Streaming | Python / PySpark | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry` | `event_year`, `event_month`, `event_type` | `spark-submit` in the Docker Spark cluster |
| 3 | `vehicle_state_job.py` | Streaming | Python / PySpark | Kafka `vehicle_state_updates` | Delta `vehicle_state` (MERGE) | `model` | `spark-submit` in the Docker Spark cluster |
| 4 | `rust-pipeline stream` | Streaming | Rust | Kafka `vehicle_telemetry` | Delta `vehicle_telemetry_rust` | `event_year`, `event_month`, `event_type` | Kubernetes Deployment `telemetry-stream`; Docker `rust-pipeline` container |
| 5 | `rust-pipeline state` | Streaming | Rust | Kafka `vehicle_state_updates` | Delta `vehicle_state_rust` (MERGE) | `model` | Kubernetes Deployment `vehicle-state-stream`; Docker `rust-pipeline` container |
| 6 | `BatteryHealthDaily` | Batch analytics | Scala Spark | Delta `vehicle_telemetry` | Delta `analytics/battery_health_daily` | `event_date` | `spark-submit --class telemetry.analytics.BatteryHealthDaily` |
| 7 | `ChargingSessions` | Batch analytics | Scala Spark | Delta `vehicle_telemetry` | Delta `analytics/charging_sessions` | `session_date` | `spark-submit --class telemetry.analytics.ChargingSessions` |
| 8 | `OtaRollout` | Batch analytics | Scala Spark | Delta `vehicle_telemetry` | Delta `analytics/ota_rollout` | none | `spark-submit --class telemetry.analytics.OtaRollout` |
| 9 | `DiagnosticAlerts` | Batch analytics | Scala Spark | Delta `vehicle_telemetry`, `vehicle_state` | Delta `analytics/diagnostic_alerts` | none | `spark-submit --class telemetry.analytics.DiagnosticAlerts` |
| 10 | `BuildParityFixtures` | Batch utility | Scala Spark | Delta `vehicle_telemetry`, `vehicle_state` | Parquet under `rust_pipeline/tests/fixtures/` | none | `sbt`/`spark-submit` by hand |
| 11 | `rust-pipeline analytics <job>` | Batch analytics (ports) | Rust | Delta `vehicle_telemetry_rust`, `vehicle_state_rust` | Delta `analytics/<job>_rust` | same as Spark job | CLI in the `rust-pipeline` container; **all four are unported stubs** |
| 12 | `rust-pipeline validate` | Batch check | Rust | Spark and Rust tables | JSON parity report | — | CLI; CI runs the fixture form |
| 13 | Dashboard | Consumer / launcher | Python (Streamlit) | All Delta tables, metrics JSON | — (launches jobs) | — | Docker `dashboard` service |
| 14 | `metrics_bridge.py`, `spark_metrics_collector.py` | Sidecar metrics | Python | Spark REST API | JSON files under `logs/` | — | Imported by the Spark streaming jobs |

Sources: `README.md` (architecture diagram and job table), `docker-compose.yml`, `deploy/k8s/kustomization.yaml`,
`dashboard/app.py:178-207` (analytics registry), `dashboard/app.py:245-300` (Spark and Rust job commands),
`rust_pipeline/src/main.rs:30-60` (Rust CLI subcommands).

## Shared infrastructure

### Kafka topics

| Topic | Key | Partitions | Retention / cleanup | Producer | Consumers | Source |
|-------|-----|-----------|---------------------|----------|-----------|--------|
| `vehicle_telemetry` | VIN | 8 | 7 days, delete | `data_generator` | `streaming_job.py`, `rust-pipeline stream` (separate consumer group) | `config/app_config.yaml`, `docker-compose.yml:65-72`, `data_generator/generator.py:272`, `README.md` |
| `vehicle_state_updates` | VIN | 8 | compacted (`cleanup.policy=compact`) | `data_generator` | `vehicle_state_job.py`, `rust-pipeline state` | `config/app_config.yaml`, `docker-compose.yml:74-85`, `data_generator/generator.py:273` |

### Table locations

Paths come from `config/app_config.yaml`; container paths are under `/opt/delta_output`, local runs (`--local`) use `./delta_output`.

| Table | Cluster path | Local path | Source |
|-------|--------------|-----------|--------|
| `vehicle_telemetry` | `/opt/delta_output/vehicle_telemetry` | `./delta_output/vehicle_telemetry` | `config/app_config.yaml` (`spark.*`), `…/analytics/AnalyticsJob.scala:49` |
| `vehicle_state` | `/opt/delta_output/vehicle_state` | `./delta_output/vehicle_state` | `config/app_config.yaml` (`vehicle_state.*`), `…/analytics/AnalyticsJob.scala:50` |
| `analytics/<job>` | `/opt/delta_output/analytics/<job>` | `./delta_output/analytics/<job>` | `config/app_config.yaml` (`analytics.*`), `…/analytics/AnalyticsJob.scala:51-52` |
| `vehicle_telemetry_rust` | `/opt/delta_output/vehicle_telemetry_rust` | `./delta_output/vehicle_telemetry_rust` | `rust_pipeline/src/config.rs:89-94`, `config/app_config.yaml` (`rust.*`) |
| `vehicle_state_rust` | `/opt/delta_output/vehicle_state_rust` | `./delta_output/vehicle_state_rust` | `rust_pipeline/src/config.rs:101-106` |
| `analytics/<job>_rust` | `<analytics base>/<job>_rust` | `<analytics base>/<job>_rust` | `rust_pipeline/src/config.rs:230-241` |

All Spark sessions run with `spark.sql.session.timeZone=UTC` (`…/analytics/AnalyticsJob.scala:56-66`, `spark_jobs/streaming_job.py`, `spark_jobs/vehicle_state_job.py`).

---

## 1. `data_generator` (Python producer)

- **What it does:** simulates a fleet of EVs and emits telemetry events (BATTERY, CHARGING, DIAGNOSTIC, OTA, …) plus a latest-state snapshot per event. Each vehicle carries a monotonically increasing `sequence_number` seeded from the wall clock (`data_generator/generator.py:150-154`). Charging events open a `charge_session_id` (UUID) and clear it when the session ends (`data_generator/generator.py:87-105`); OTA events carry `update_state` and `target_software_version` (`data_generator/generator.py:116-129`); snapshots are built in `generate_state_snapshot` (`data_generator/generator.py:186-203`).
- **Inputs:** none (synthetic).
- **Outputs:** Kafka `vehicle_telemetry` and `vehicle_state_updates`, both keyed by VIN (`data_generator/generator.py:272-274`). JSON payload fields are those in `TELEMETRY_SCHEMA` (`spark_jobs/streaming_job.py:83`) and `VEHICLE_STATE_SCHEMA` (`spark_jobs/vehicle_state_job.py:58`).
- **Deployment:** runs on the host against the Docker Kafka broker (`README.md` quick start); not containerised (`docker-compose.yml` has no generator service).

## 2. `streaming_job.py` — telemetry enrichment stream (PySpark)

- **What it does:** Structured Streaming job that parses Kafka JSON with `TELEMETRY_SCHEMA`, keeps Kafka metadata, runs the T1–T7 enrichment chain from `spark_jobs/transforms.py:190-201` (`apply_all_transforms`) and appends to Delta (`spark_jobs/streaming_job.py:182-245`). The VIN (`kafka_key`) is dropped before writing (`spark_jobs/streaming_job.py:232-233`).
  - T1 null handling: `model_safe`, `software_version_safe`, `power_state_safe` coalesce to `MODEL_UNKNOWN` / `0.0.0` / `POWER_STATE_UNKNOWN`; null `odometer_km` → `0.0` (`spark_jobs/transforms.py:69-79`).
  - T2 time dimensions from `event_time` in UTC: `event_date`, `event_year`, `event_month`, `event_day`, `event_hour`, `event_dayofweek`, `is_weekend`, `event_quarter`, `date_str` (`spark_jobs/transforms.py:82-96`).
  - T3 battery: `soc_bucket`, `energy_pct`, `cell_temp_spread`, `degradation_pct`, `km_per_percent` (`spark_jobs/transforms.py:99-124`).
  - T4 health: `health_tier` (Python UDF `classify_health`, `spark_jobs/transforms.py:46`), `thermal_alert`, `is_fault`, `needs_service`, `severity_level`; boolean flags are null-safe via `_is_true` = `coalesce(cond, false)` (`spark_jobs/transforms.py:60`, `127-156`).
  - T5 location: `lat_coarse`/`lon_coarse` (2 dp), raw `latitude`/`longitude` dropped (`spark_jobs/transforms.py:159-166`).
  - T6 anonymisation: `vin_hash = sha2(vin, 256)`, `vin_wmi`, raw `vin` dropped (`spark_jobs/transforms.py:169-176`).
  - T7 event features: `row_id`, `sw_major`, `sw_minor` (`spark_jobs/transforms.py:179-187`).
- **Inputs:** Kafka `vehicle_telemetry`, `startingOffsets=earliest`, `failOnDataLoss=false` (`spark_jobs/streaming_job.py:186-195`).
- **Output:** Delta `vehicle_telemetry`, mode `append`, `mergeSchema=true`, **partitioned by `event_year`, `event_month`, `event_type`** (`spark_jobs/streaming_job.py:241-245`); checkpoint at `spark.checkpoint_location` (`spark_jobs/streaming_job.py:310`, `config/app_config.yaml`).
- **Output columns** (types as written to the fixture snapshot `rust_pipeline/tests/fixtures/input/telemetry`, which `BuildParityFixtures` copies from this table — `…/analytics/BuildParityFixtures.scala:34-44`):

  | Group | Columns (type) |
  |-------|----------------|
  | Identity / event | `vin_hash` string, `event_id` string, `event_type` string, `event_time` string (ISO-8601), `sequence_number` bigint |
  | Raw vehicle | `model`, `model_variant`, `software_version`, `power_state` string; `odometer_km`, `charge_percent`, `remaining_range`, `kwhr`, `capacity_kwhr`, `battery_health_level`, `max_cell_temp`, `min_cell_temp` double; `preconditioning_status`, `battery_pack_type` string |
  | Charging | `charge_state`, `charge_session_id` string; `charging_power_kw`, `energy_added_kwh` double |
  | Diagnostic | `ecu`, `dtc_code`, `warning_state` string |
  | OTA | `update_state`, `target_software_version` string; `update_progress_percent` int |
  | Kafka metadata | `topic` string, `kafka_partition` int, `kafka_offset` bigint, `kafka_timestamp` timestamp |
  | T1 | `model_safe`, `software_version_safe`, `power_state_safe` string |
  | T2 | `event_date` date; `event_year`, `event_month`, `event_day`, `event_hour`, `event_dayofweek`, `event_quarter` int; `is_weekend` boolean; `date_str` string |
  | T3 | `soc_bucket` string; `energy_pct`, `cell_temp_spread`, `degradation_pct`, `km_per_percent` double |
  | T4 | `health_tier` string; `thermal_alert`, `is_fault`, `needs_service` boolean; `severity_level` int |
  | T5–T7 | `lat_coarse`, `lon_coarse` double; `vin_wmi`, `row_id` string; `sw_major`, `sw_minor` int |

- **Deployment:** `spark-submit` inside the `spark-master` container of `docker-compose.yml` (`dashboard/app.py:250-285`, `README.md` quick start). Long-running, one query per cluster.

## 3. `vehicle_state_job.py` — vehicle-state MERGE stream (PySpark)

- **What it does:** reads state snapshots, drops null VINs (`spark_jobs/vehicle_state_job.py:171`), collapses each micro-batch to the newest snapshot per VIN by `sequence_number`, summing `event_count` and taking the minimum `first_seen_at` (`spark_jobs/vehicle_state_job.py:104-117`), then Delta-MERGEs on `vin`: update only when `source.sequence_number > target.sequence_number`, add `event_count`, keep `least(first_seen_at)`; insert unseen VINs (`spark_jobs/vehicle_state_job.py:120-143`). Result: one row per VIN, out-of-order snapshots never regress state.
- **Inputs:** Kafka `vehicle_state_updates` (`spark_jobs/vehicle_state_job.py:164`).
- **Output:** Delta `vehicle_state`, **partitioned by `model`** (`spark_jobs/vehicle_state_job.py:125`). Columns (fixture `rust_pipeline/tests/fixtures/input/vehicle_state`): `vin` string, `model` string, `model_variant` string, `battery_pack_type` string, `software_version` string, `power_state` string, `odometer_km` double, `charge_percent` double, `battery_health_level` double, `last_event_type` string, `sequence_number` bigint, `updated_at` string, `event_count` int, `first_seen_at` string. **This table holds the raw VIN.**
- **Deployment:** `spark-submit` in the Docker Spark cluster (`dashboard/app.py:250-285`); checkpointed (`spark_jobs/vehicle_state_job.py:202`).

## 4. `rust-pipeline stream` — Rust telemetry stream

- **What it does:** port of job 2 (`rust_pipeline/src/main.rs:33-34`); consumes the same topic with its own consumer group (`dashboard/app.py:295`, `rust_pipeline/src/consumer.rs`), applies the Rust ports of T1–T7 (`rust_pipeline/src/transforms/*.rs`) against the Arrow schema in `rust_pipeline/src/schema.rs`, stores offsets in a SQLite checkpoint (`rust_pipeline/src/checkpoint.rs`).
- **Output:** Delta `vehicle_telemetry_rust`, partitioned by `event_year`, `event_month`, `event_type` (`rust_pipeline/src/consumer.rs`). Same columns as job 2 except `kafka_timestamp`, which the parity check ignores (`rust_pipeline/src/validate.rs:40`).
- **Deployment:** `deploy/k8s/telemetry-stream.yaml` — 1 replica, `strategy: Recreate` (the process assigns every partition to itself and owns local checkpoint/data volumes), PVCs from `deploy/k8s/storage.yaml`, image from `rust_pipeline/Dockerfile`, applied with `kubectl apply -k deploy/k8s` (`deploy/k8s/kustomization.yaml`). Locally the `rust-pipeline` service idles on `sleep infinity` and jobs are `docker exec`'d (`docker-compose.yml:162-178`, `dashboard/app.py:290-300`).

## 5. `rust-pipeline state` — Rust vehicle-state stream

- **What it does:** port of job 3 (`rust_pipeline/src/main.rs:35-36`, `rust_pipeline/src/vehicle_state.rs`): same collapse + conditional MERGE.
- **Output:** Delta `vehicle_state_rust`, partitioned by `model` (`rust_pipeline/src/vehicle_state.rs:124`).
- **Deployment:** `deploy/k8s/vehicle-state-stream.yaml` (1 replica, `Recreate`), same image and kustomization as job 4.

## 6–9. Scala Spark analytics jobs

Common framework, `…/analytics/AnalyticsJob.scala`:
- CLI `--config`, `--local` (`AnalyticsJob.scala:11-21`); table paths from `config/app_config.yaml` (`AnalyticsJob.scala:44-52`).
- SparkSession with Delta extensions, `spark.sql.shuffle.partitions` from config, UTC session time zone (`AnalyticsJob.scala:56-66`).
- `main` reads inputs via the job's `build`, then writes Delta with `mode("overwrite")`, `overwriteSchema=true`, and `partitionBy(partitionCols)` when non-empty (`AnalyticsJob.scala:71-95`). Each run fully replaces the output table; there is no incremental state.
- Build: one sbt project (Scala 2.12.18, Spark SQL 3.5.1, Java 11; `spark_jobs/analytics/build.sbt`) packaged as `telemetry-analytics_2.12-1.0.0.jar` (`dashboard/app.py:245-246`).
- Deployment: `spark-submit --class telemetry.analytics.<Class> <jar>` in the Docker Spark cluster (`dashboard/app.py:279`, `README.md`). There is **no scheduler and no Kubernetes manifest** for these jobs — `deploy/k8s/kustomization.yaml` lists only the two Rust streams. CI only compiles/packages them (`.github/workflows/ci.yml:50-65`).

| Job | Purpose | Inputs | Output columns (type) | Partition | Spec |
|-----|---------|--------|-----------------------|-----------|------|
| `battery_health_daily` (`…/analytics/BatteryHealthDaily.scala`) | Daily battery summary per vehicle | `vehicle_telemetry` (`event_type='BATTERY'`) | `vin_hash` string, `event_date` date, `model_safe` string, `readings` bigint, `avg_charge_percent` double, `min_charge_percent` double, `peak_cell_temp` double, `avg_cell_temp_spread` double, `thermal_alert_count` bigint, `health_level_end_of_day` double, `peak_cell_temp_3d` double, `health_change` double | `event_date` (`BatteryHealthDaily.scala:10`) | [jobs/battery_health_daily.md](jobs/battery_health_daily.md) |
| `charging_sessions` (`…/analytics/ChargingSessions.scala`) | One row per charge session | `vehicle_telemetry` (`event_type='CHARGING'`) | `charge_session_id` string, `vin_hash` string, `model_safe` string, `session_start` timestamp, `session_end` timestamp, `events` bigint, `energy_added_kwh` double, `peak_power_kw` double, `soc_start` double, `soc_end` double, `end_state` string, `had_fault` boolean, `duration_minutes` double, `avg_power_kw` double, `session_date` date | `session_date` (`ChargingSessions.scala:9`) | [jobs/charging_sessions.md](jobs/charging_sessions.md) |
| `ota_rollout` (`…/analytics/OtaRollout.scala`) | OTA outcome per target version × model | `vehicle_telemetry` (`event_type='OTA'`) | `target_software_version` string, `model_safe` string, 7 × update-state count bigint, `vehicles` bigint, `success_rate_pct` double, `failure_rate_pct` double | none | [jobs/ota_rollout.md](jobs/ota_rollout.md) |
| `diagnostic_alerts` (`…/analytics/DiagnosticAlerts.scala`) | DTC impact per ECU/model/pack | `vehicle_telemetry` (`event_type='DIAGNOSTIC'`) + `vehicle_state` | `dtc_code` string, `ecu` string, `model_safe` string, `battery_pack_type` string, `events` bigint, `vehicles_affected` bigint, `warning_on_events` bigint, `first_seen` timestamp, `last_seen` timestamp, `fleet_pct_affected` double, `rank_in_ecu` int | none | [jobs/diagnostic_alerts.md](jobs/diagnostic_alerts.md) |

Types above are read from the committed Spark output fixtures in `rust_pipeline/tests/fixtures/expected/<job>/` (Parquet written with `TIMESTAMP_MICROS`, `…/analytics/BuildParityFixtures.scala:51`); partition columns match `scripts/load_demo_data.py` (`ANALYTICS_PARTITIONS`) and the Rust registry test `rust_pipeline/src/analytics/mod.rs:120-131`.

## 10. `BuildParityFixtures` (Scala utility)

- **What it does:** samples N vehicles (`orderBy("vin_hash").limit(vehicles)`) from the live Spark tables, writes them as the fixture inputs, and runs every job's `summarize` to write the expected outputs (`…/analytics/BuildParityFixtures.scala:19-24`, `34-44`, `46-65`). The state sample is filtered by hashing `vin` (`BuildParityFixtures.scala:41`).
- **Inputs:** `vehicle_telemetry`, `vehicle_state`. **Outputs:** `rust_pipeline/tests/fixtures/input/{telemetry,vehicle_state}`, `rust_pipeline/tests/fixtures/expected/<job>`.
- **Deployment:** run by hand when fixtures need refreshing; references all four analytics objects, so it couples them at compile time (see [dependencies.md](dependencies.md)).

## 11. `rust-pipeline analytics <job>` (Rust ports)

- **What it does:** opens `vehicle_telemetry_rust` and `vehicle_state_rust`, registers them as `telemetry` and `vehicle_state`, dispatches to the job's `summarize`, overwrites `<job>_rust` with the job's `partition_cols` (`rust_pipeline/src/analytics/mod.rs:69-111`).
- **Status:** every job file is a stub that `bail!`s with "not ported to Rust yet", and its parity test is `#[ignore]`d (`rust_pipeline/src/analytics/{battery_health_daily,charging_sessions,ota_rollout,diagnostic_alerts}.rs:16,22`).
- **Deployment:** CLI only (`rust_pipeline/src/main.rs:38-41`), launched from the dashboard Migration page (`dashboard/app.py:501-520`); no Kubernetes manifest.

## 12. `rust-pipeline validate`

- Compares a Spark table with its Rust counterpart: row/column/value parity, float tolerance `1e-9`, forbidden PII columns `vin`, `latitude`, `longitude`, `kafka_key` (`rust_pipeline/src/validate.rs:19-23`, `85-120`). Analytics comparisons also check column types (`rust_pipeline/src/analytics/mod.rs:36-45`). The fixture-based form runs in CI via `cargo test` (`rust_pipeline/src/analytics/parity.rs`, `.github/workflows/ci.yml:35`).

## 13. Dashboard (`dashboard/app.py`)

- Reads every Spark and Rust table listed in `TABLES` (`dashboard/app.py:212-225`) and renders the Fleet analytics page with a Spark/Rust engine switch (`dashboard/app.py:637-760`); launches Spark and Rust jobs (`dashboard/app.py:245-300`, `988-1041`); reads sidecar metrics and parity reports (`dashboard/metrics.py`).
- Deployment: Docker `dashboard` service (`docker-compose.yml:181`).

## 14. Metrics sidecars

- `spark_jobs/metrics_bridge.py` writes Spark job metrics as JSON to the shared `logs/` volume because DuckDB cannot be shared across the host/container boundary (module docstring); `spark_jobs/spark_metrics_collector.py` polls the Spark REST API at `localhost:4040` inside the Spark container (module docstring). Neither writes pipeline data; neither is used by the analytics jobs.
