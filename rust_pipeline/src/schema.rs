use deltalake::arrow::array::*;
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use serde::Deserialize;
use std::sync::Arc;

// ── Input: telemetry event (JSON from Kafka, keyed by VIN) ───

#[derive(Debug, Deserialize, Clone)]
pub struct TelemetryInput {
    pub event_id: String,
    pub vin: String,
    pub event_type: String,
    pub event_time: String,
    pub sequence_number: i64,
    pub model: Option<String>,
    pub model_variant: Option<String>,
    pub software_version: Option<String>,
    pub power_state: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub odometer_km: Option<f64>,
    pub charge_percent: Option<f64>,
    pub remaining_range: Option<f64>,
    pub kwhr: Option<f64>,
    pub capacity_kwhr: Option<f64>,
    pub battery_health_level: Option<f64>,
    pub max_cell_temp: Option<f64>,
    pub min_cell_temp: Option<f64>,
    pub preconditioning_status: Option<String>,
    pub battery_pack_type: Option<String>,
    pub charge_state: Option<String>,
    pub charge_session_id: Option<String>,
    pub charging_power_kw: Option<f64>,
    pub energy_added_kwh: Option<f64>,
    pub ecu: Option<String>,
    pub dtc_code: Option<String>,
    pub warning_state: Option<String>,
    pub update_state: Option<String>,
    pub target_software_version: Option<String>,
    pub update_progress_percent: Option<i32>,
}

// ── Input: vehicle state snapshot ────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct VehicleStateInput {
    pub vin: String,
    pub model: Option<String>,
    pub model_variant: Option<String>,
    pub battery_pack_type: Option<String>,
    pub software_version: Option<String>,
    pub power_state: Option<String>,
    pub odometer_km: Option<f64>,
    pub charge_percent: Option<f64>,
    pub battery_health_level: Option<f64>,
    pub last_event_type: Option<String>,
    pub sequence_number: i64,
    pub event_count: Option<i32>,
    pub updated_at: Option<String>,
    pub first_seen_at: Option<String>,
}

// ── Kafka metadata attached to each record ───────────────────

#[derive(Debug, Clone)]
pub struct KafkaMeta {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
}

// ── Enriched telemetry output ────────────────────────────────
// Privacy: no vin, latitude, longitude or kafka_key (the VIN) field exists.

#[derive(Debug, Clone)]
pub struct TelemetryOutput {
    pub event_id: String,
    pub event_type: String,
    pub event_time: String,
    pub sequence_number: i64,
    pub model: Option<String>,
    pub model_variant: Option<String>,
    pub software_version: Option<String>,
    pub power_state: Option<String>,
    pub odometer_km: f64,
    pub charge_percent: Option<f64>,
    pub remaining_range: Option<f64>,
    pub kwhr: Option<f64>,
    pub capacity_kwhr: Option<f64>,
    pub battery_health_level: Option<f64>,
    pub max_cell_temp: Option<f64>,
    pub min_cell_temp: Option<f64>,
    pub preconditioning_status: Option<String>,
    pub battery_pack_type: Option<String>,
    pub charge_state: Option<String>,
    pub charge_session_id: Option<String>,
    pub charging_power_kw: Option<f64>,
    pub energy_added_kwh: Option<f64>,
    pub ecu: Option<String>,
    pub dtc_code: Option<String>,
    pub warning_state: Option<String>,
    pub update_state: Option<String>,
    pub target_software_version: Option<String>,
    pub update_progress_percent: Option<i32>,
    // Kafka metadata
    pub topic: String,
    pub kafka_partition: i32,
    pub kafka_offset: i64,
    pub kafka_timestamp: i64,
    // T1 null handling
    pub model_safe: String,
    pub software_version_safe: String,
    pub power_state_safe: String,
    // T2 time dimensions
    pub event_date: i32, // days since epoch (Arrow Date32)
    pub event_year: i32,
    pub event_month: i32,
    pub event_day: i32,
    pub event_hour: i32,
    pub event_dayofweek: i32,
    pub is_weekend: bool,
    pub event_quarter: i32,
    pub date_str: String,
    // T3 battery features
    pub soc_bucket: String,
    pub energy_pct: Option<f64>,
    pub cell_temp_spread: Option<f64>,
    pub degradation_pct: Option<f64>,
    pub km_per_percent: Option<f64>,
    // T4 health features
    pub health_tier: String,
    pub thermal_alert: bool,
    pub is_fault: bool,
    pub needs_service: bool,
    pub severity_level: i32,
    // T5 location (privacy)
    pub lat_coarse: Option<f64>,
    pub lon_coarse: Option<f64>,
    // T6 anonymized (privacy)
    pub vin_hash: String,
    pub vin_wmi: String,
    // T7 event features
    pub row_id: String,
    pub sw_major: Option<i32>,
    pub sw_minor: Option<i32>,
}

// ── Arrow schema: enriched telemetry Delta table ─────────────

pub fn telemetry_output_schema() -> Schema {
    use DataType::*;
    let f = |name: &str, dt: DataType, nullable: bool| Field::new(name, dt, nullable);
    Schema::new(vec![
        f("event_id", Utf8, false),
        f("event_type", Utf8, false),
        f("event_time", Utf8, false),
        f("sequence_number", Int64, false),
        f("model", Utf8, true),
        f("model_variant", Utf8, true),
        f("software_version", Utf8, true),
        f("power_state", Utf8, true),
        f("odometer_km", Float64, false),
        f("charge_percent", Float64, true),
        f("remaining_range", Float64, true),
        f("kwhr", Float64, true),
        f("capacity_kwhr", Float64, true),
        f("battery_health_level", Float64, true),
        f("max_cell_temp", Float64, true),
        f("min_cell_temp", Float64, true),
        f("preconditioning_status", Utf8, true),
        f("battery_pack_type", Utf8, true),
        f("charge_state", Utf8, true),
        f("charge_session_id", Utf8, true),
        f("charging_power_kw", Float64, true),
        f("energy_added_kwh", Float64, true),
        f("ecu", Utf8, true),
        f("dtc_code", Utf8, true),
        f("warning_state", Utf8, true),
        f("update_state", Utf8, true),
        f("target_software_version", Utf8, true),
        f("update_progress_percent", Int32, true),
        f("topic", Utf8, false),
        f("kafka_partition", Int32, false),
        f("kafka_offset", Int64, false),
        f("kafka_timestamp", Int64, false),
        f("model_safe", Utf8, false),
        f("software_version_safe", Utf8, false),
        f("power_state_safe", Utf8, false),
        f("event_date", Date32, false),
        f("event_year", Int32, false),
        f("event_month", Int32, false),
        f("event_day", Int32, false),
        f("event_hour", Int32, false),
        f("event_dayofweek", Int32, false),
        f("is_weekend", Boolean, false),
        f("event_quarter", Int32, false),
        f("date_str", Utf8, false),
        f("soc_bucket", Utf8, false),
        f("energy_pct", Float64, true),
        f("cell_temp_spread", Float64, true),
        f("degradation_pct", Float64, true),
        f("km_per_percent", Float64, true),
        f("health_tier", Utf8, false),
        f("thermal_alert", Boolean, false),
        f("is_fault", Boolean, false),
        f("needs_service", Boolean, false),
        f("severity_level", Int32, false),
        f("lat_coarse", Float64, true),
        f("lon_coarse", Float64, true),
        f("vin_hash", Utf8, false),
        f("vin_wmi", Utf8, false),
        f("row_id", Utf8, false),
        f("sw_major", Int32, true),
        f("sw_minor", Int32, true),
    ])
}

// ── Arrow schema: vehicle_state Delta table ──────────────────

pub fn vehicle_state_schema() -> Schema {
    use DataType::*;
    let f = |name: &str, dt: DataType, nullable: bool| Field::new(name, dt, nullable);
    Schema::new(vec![
        f("vin", Utf8, false),
        f("model", Utf8, true),
        f("model_variant", Utf8, true),
        f("battery_pack_type", Utf8, true),
        f("software_version", Utf8, true),
        f("power_state", Utf8, true),
        f("odometer_km", Float64, true),
        f("charge_percent", Float64, true),
        f("battery_health_level", Float64, true),
        f("last_event_type", Utf8, true),
        f("sequence_number", Int64, false),
        f("event_count", Int32, true),
        f("updated_at", Utf8, true),
        f("first_seen_at", Utf8, true),
    ])
}

// ── Column builders ──────────────────────────────────────────

macro_rules! col_utf8 {
    ($recs:expr, $f:ident) => {
        Arc::new(StringArray::from_iter_values(
            $recs.iter().map(|r| r.$f.as_str()),
        )) as ArrayRef
    };
}
macro_rules! col_utf8_opt {
    ($recs:expr, $f:ident) => {
        Arc::new(StringArray::from(
            $recs.iter().map(|r| r.$f.as_deref()).collect::<Vec<_>>(),
        )) as ArrayRef
    };
}
macro_rules! col_f64 {
    ($recs:expr, $f:ident) => {
        Arc::new(Float64Array::from_iter_values($recs.iter().map(|r| r.$f))) as ArrayRef
    };
}
macro_rules! col_f64_opt {
    ($recs:expr, $f:ident) => {
        Arc::new(Float64Array::from(
            $recs.iter().map(|r| r.$f).collect::<Vec<_>>(),
        )) as ArrayRef
    };
}
macro_rules! col_i32 {
    ($recs:expr, $f:ident) => {
        Arc::new(Int32Array::from_iter_values($recs.iter().map(|r| r.$f))) as ArrayRef
    };
}
macro_rules! col_i32_opt {
    ($recs:expr, $f:ident) => {
        Arc::new(Int32Array::from(
            $recs.iter().map(|r| r.$f).collect::<Vec<_>>(),
        )) as ArrayRef
    };
}
macro_rules! col_i64 {
    ($recs:expr, $f:ident) => {
        Arc::new(Int64Array::from_iter_values($recs.iter().map(|r| r.$f))) as ArrayRef
    };
}
macro_rules! col_bool {
    ($recs:expr, $f:ident) => {
        Arc::new(BooleanArray::from(
            $recs.iter().map(|r| r.$f).collect::<Vec<_>>(),
        )) as ArrayRef
    };
}

pub fn build_telemetry_batch(r: &[TelemetryOutput]) -> anyhow::Result<RecordBatch> {
    let event_date = Arc::new(Date32Array::from_iter_values(
        r.iter().map(|x| x.event_date),
    )) as ArrayRef;
    let columns: Vec<ArrayRef> = vec![
        col_utf8!(r, event_id),
        col_utf8!(r, event_type),
        col_utf8!(r, event_time),
        col_i64!(r, sequence_number),
        col_utf8_opt!(r, model),
        col_utf8_opt!(r, model_variant),
        col_utf8_opt!(r, software_version),
        col_utf8_opt!(r, power_state),
        col_f64!(r, odometer_km),
        col_f64_opt!(r, charge_percent),
        col_f64_opt!(r, remaining_range),
        col_f64_opt!(r, kwhr),
        col_f64_opt!(r, capacity_kwhr),
        col_f64_opt!(r, battery_health_level),
        col_f64_opt!(r, max_cell_temp),
        col_f64_opt!(r, min_cell_temp),
        col_utf8_opt!(r, preconditioning_status),
        col_utf8_opt!(r, battery_pack_type),
        col_utf8_opt!(r, charge_state),
        col_utf8_opt!(r, charge_session_id),
        col_f64_opt!(r, charging_power_kw),
        col_f64_opt!(r, energy_added_kwh),
        col_utf8_opt!(r, ecu),
        col_utf8_opt!(r, dtc_code),
        col_utf8_opt!(r, warning_state),
        col_utf8_opt!(r, update_state),
        col_utf8_opt!(r, target_software_version),
        col_i32_opt!(r, update_progress_percent),
        col_utf8!(r, topic),
        col_i32!(r, kafka_partition),
        col_i64!(r, kafka_offset),
        col_i64!(r, kafka_timestamp),
        col_utf8!(r, model_safe),
        col_utf8!(r, software_version_safe),
        col_utf8!(r, power_state_safe),
        event_date,
        col_i32!(r, event_year),
        col_i32!(r, event_month),
        col_i32!(r, event_day),
        col_i32!(r, event_hour),
        col_i32!(r, event_dayofweek),
        col_bool!(r, is_weekend),
        col_i32!(r, event_quarter),
        col_utf8!(r, date_str),
        col_utf8!(r, soc_bucket),
        col_f64_opt!(r, energy_pct),
        col_f64_opt!(r, cell_temp_spread),
        col_f64_opt!(r, degradation_pct),
        col_f64_opt!(r, km_per_percent),
        col_utf8!(r, health_tier),
        col_bool!(r, thermal_alert),
        col_bool!(r, is_fault),
        col_bool!(r, needs_service),
        col_i32!(r, severity_level),
        col_f64_opt!(r, lat_coarse),
        col_f64_opt!(r, lon_coarse),
        col_utf8!(r, vin_hash),
        col_utf8!(r, vin_wmi),
        col_utf8!(r, row_id),
        col_i32_opt!(r, sw_major),
        col_i32_opt!(r, sw_minor),
    ];
    Ok(RecordBatch::try_new(
        Arc::new(telemetry_output_schema()),
        columns,
    )?)
}

pub fn build_vehicle_state_batch(r: &[VehicleStateInput]) -> anyhow::Result<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        col_utf8!(r, vin),
        col_utf8_opt!(r, model),
        col_utf8_opt!(r, model_variant),
        col_utf8_opt!(r, battery_pack_type),
        col_utf8_opt!(r, software_version),
        col_utf8_opt!(r, power_state),
        col_f64_opt!(r, odometer_km),
        col_f64_opt!(r, charge_percent),
        col_f64_opt!(r, battery_health_level),
        col_utf8_opt!(r, last_event_type),
        col_i64!(r, sequence_number),
        col_i32_opt!(r, event_count),
        col_utf8_opt!(r, updated_at),
        col_utf8_opt!(r, first_seen_at),
    ];
    Ok(RecordBatch::try_new(
        Arc::new(vehicle_state_schema()),
        columns,
    )?)
}
