# Vehicle Telemetry Pipeline — Spark → Rust Migration

A vehicle-to-cloud telemetry pipeline midway through a migration from Spark
Structured Streaming to Rust stream processors. Synthetic EV telemetry
(battery, charging, diagnostics, OTA update status) flows through Kafka,
keyed by VIN, into Delta Lake tables that downstream service, warranty and
OTA systems read.

Two jobs are already ported to Rust and proven equivalent by a parity
check. The batch analytics jobs are still Spark-only.

## Provenance

- **Forked from** [PragnaMohapatra/spark_to_rust_migration](https://github.com/PragnaMohapatra/spark_to_rust_migration)
  (Apache-2.0), a Spark vs Rust benchmark on a financial-transactions
  workload. This fork replaces that workload with vehicle telemetry.
- **Field names and enum values** (`charge_percent`, `battery_health_level`,
  `CHARGE_STATE_*`, `UPDATE_STATE_*`, `MODEL_AIR`, …) are modelled on the
  community-documented Lucid mobile-gateway protobufs in
  [nshp/python-lucidmotors](https://github.com/nshp/python-lucidmotors)
  (MIT; unofficial, no affiliation with Lucid Motors). Those describe the
  app-facing API, not an internal telemetry stream; the event envelope
  (`event_id`, `sequence_number`, `event_type`) is our own.
- **All data is synthetic.** VINs, locations and readings are generated.

## Architecture

```
data_generator ──► Kafka ─┬─ vehicle_telemetry (key = VIN) ─┬─► Spark  streaming_job.py   ─► delta_output/vehicle_telemetry
                          │                                  └─► Rust   rust-pipeline stream ─► delta_output/vehicle_telemetry_rust
                          │
                          └─ vehicle_state_updates (key = VIN, compacted)
                                                             ├─► Spark  vehicle_state_job.py ─► delta_output/vehicle_state
                                                             └─► Rust   rust-pipeline state  ─► delta_output/vehicle_state_rust

rust-pipeline validate  ──  compares each Spark table with its Rust twin (the migration acceptance gate)
```

## Migration status

| Job | Spark | Rust | Parity |
|---|---|---|---|
| Telemetry enrichment (`streaming_job.py`) | ✅ | ✅ `rust-pipeline stream` | `validate` |
| Vehicle state MERGE (`vehicle_state_job.py`) | ✅ | ✅ `rust-pipeline state` | `validate --state` |
| Batch analytics (`batch_analytics_job.py`) | still finance-schema; being converted | ❌ not ported | — |

## Guarantees the pipeline relies on

**Ordering.** Every message is keyed by VIN, so one vehicle's events stay in
one partition in order. The generator shards vehicles across producer
threads (one producer per VIN) with `max_in_flight_requests_per_connection=1`.
Each event also carries a per-VIN `sequence_number`.

**Exactly-once effect.** Spark uses streaming checkpoints. Rust commits Kafka
offsets to a local SQLite checkpoint after each Delta write and resumes from
it. The vehicle-state MERGE only applies a snapshot when its
`sequence_number` is newer than the stored one, so replays and out-of-order
arrivals cannot regress a vehicle's state.

**Privacy.** The enriched telemetry table never holds the raw VIN or precise
GPS: the VIN becomes `vin_hash` (SHA-256) plus `vin_wmi` (manufacturer
code), coordinates are rounded to 2 dp (~1 km), and the Kafka key (the VIN)
is dropped. `validate` fails if `vin`, `latitude`, `longitude` or
`kafka_key` appears in either table. The `vehicle_state` table is keyed by
VIN by design: it is the operational lookup for service and warranty, and
carries no location.

## Parity validation

```bash
./rust_pipeline/target/debug/rust-pipeline --config config/app_config.yaml validate --state
```

PASS requires no missing rows on either side, every compared column equal
on every row (doubles within 1e-9), and no raw PII columns. Reports are
written to `logs/validation_report_<table>.json`; the command exits
non-zero on FAIL so CI can gate on it.

Rust reproduces Spark's `round()` exactly (`transforms/rounding.rs`): Spark
rounds HALF_UP on the decimal form of the double, so `round(1.005, 2)` is
1.01, where naive float rounding gives 1.0.

## Quick start

Prerequisites: Docker, Rust (`rustup`), cmake, Python 3.10+.

```bash
# 1. Python env for the generator
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# 2. Kafka + Spark, then create topics
docker compose up -d zookeeper kafka spark-master spark-worker
docker compose up kafka-setup

# 3. Build the Rust pipeline (CMake 4 needs the policy floor for bundled librdkafka)
(cd rust_pipeline && CMAKE_POLICY_VERSION_MINIMUM=3.5 cargo build)

# 4. Generate ~50 MB of telemetry (2,000 vehicles)
.venv/bin/python -m data_generator.run --target-gb 0.05

# 5. Spark jobs (inside the cluster)
JARS=$(docker exec spark-master bash -c 'ls /opt/extra-jars/*.jar | paste -sd, -')
docker exec -d -w /opt spark-master bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --total-executor-cores 2 --jars $JARS --py-files /opt/spark-jobs/transforms.py \
  /opt/spark-jobs/streaming_job.py --config /opt/config/app_config.yaml > /opt/logs/spark_telemetry.log 2>&1"
docker exec -d -w /opt spark-master bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --total-executor-cores 2 --jars $JARS --py-files /opt/extra-jars/delta-spark_2.12-3.1.0.jar \
  /opt/spark-jobs/vehicle_state_job.py --config /opt/config/app_config.yaml > /opt/logs/spark_state.log 2>&1"

# 6. Rust jobs (run from the repo root so relative paths line up with Spark's)
./rust_pipeline/target/debug/rust-pipeline --config config/app_config.yaml stream &
./rust_pipeline/target/debug/rust-pipeline --config config/app_config.yaml state &

# 7. Once both sides have processed the data
./rust_pipeline/target/debug/rust-pipeline --config config/app_config.yaml validate --state
```

Unit tests: `(cd rust_pipeline && CMAKE_POLICY_VERSION_MINIMUM=3.5 cargo test)`.

## Event schema

Topic `vehicle_telemetry`, one JSON event per message. `event_type` decides
which group of fields is populated; the rest are null.

| Group | Fields |
|---|---|
| Envelope | `event_id`, `vin`, `event_type` (BATTERY · CHARGING · DIAGNOSTIC · OTA), `event_time` (UTC), `sequence_number` |
| Vehicle context | `model`, `model_variant`, `software_version`, `power_state`, `latitude`, `longitude`, `odometer_km` |
| BATTERY | `charge_percent`, `remaining_range`, `kwhr`, `capacity_kwhr`, `battery_health_level`, `max_cell_temp`, `min_cell_temp`, `preconditioning_status`, `battery_pack_type` |
| CHARGING | `charge_percent`, `charge_state`, `charge_session_id`, `charging_power_kw`, `energy_added_kwh` |
| DIAGNOSTIC | `ecu`, `dtc_code`, `warning_state` |
| OTA | `update_state`, `target_software_version`, `update_progress_percent` |

Topic `vehicle_state_updates`: one snapshot per event (`vin`, model,
software version, power state, odometer, charge, health, `last_event_type`,
`sequence_number`, `event_count`, `updated_at`, `first_seen_at`).

Enrichment (`spark_jobs/transforms.py`, mirrored in
`rust_pipeline/src/transforms/`): T1 null defaults · T2 time dimensions ·
T3 battery metrics (`soc_bucket`, `energy_pct`, `cell_temp_spread`,
`degradation_pct`, `km_per_percent`) · T4 health flags (`health_tier`,
`thermal_alert`, `is_fault`, `needs_service`, `severity_level`) · T5 GPS
coarsening · T6 VIN pseudonymisation · T7 `row_id`, `sw_major`, `sw_minor`.

## Not yet updated for telemetry

`dashboard/`, `delta_viewer/`, `cleanup/` and
`scripts/generate_pipeline_report.py` still assume the original
financial-transactions schema.

## License

Apache-2.0, inherited from the upstream project (see `LICENSE`).
