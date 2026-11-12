pub mod anonymized;
pub mod battery_features;
pub mod event_features;
pub mod health_features;
pub mod location_features;
pub mod null_handling;
pub mod rounding;
pub mod time_dimensions;

use crate::schema::{KafkaMeta, TelemetryInput, TelemetryOutput};

/// Apply all 7 transforms in order, matching Spark's apply_all_transforms.
pub fn apply_all_transforms(input: &TelemetryInput, meta: &KafkaMeta) -> TelemetryOutput {
    let nh = null_handling::apply(input); // T1
    let td = time_dimensions::apply(&input.event_time); // T2
    let bf = battery_features::apply(input); // T3
    let hf = health_features::apply(input, bf.cell_temp_spread); // T4 (uses T3)
    let loc = location_features::apply(input.latitude, input.longitude); // T5
    let anon = anonymized::apply(&input.vin); // T6
    let ev = event_features::apply(&input.event_id, &nh.software_version_safe); // T7

    TelemetryOutput {
        event_id: input.event_id.clone(),
        event_type: input.event_type.clone(),
        event_time: input.event_time.clone(),
        sequence_number: input.sequence_number,
        model: input.model.clone(),
        model_variant: input.model_variant.clone(),
        software_version: input.software_version.clone(),
        power_state: input.power_state.clone(),
        odometer_km: nh.odometer_km,
        charge_percent: input.charge_percent,
        remaining_range: input.remaining_range,
        kwhr: input.kwhr,
        capacity_kwhr: input.capacity_kwhr,
        battery_health_level: input.battery_health_level,
        max_cell_temp: input.max_cell_temp,
        min_cell_temp: input.min_cell_temp,
        preconditioning_status: input.preconditioning_status.clone(),
        battery_pack_type: input.battery_pack_type.clone(),
        charge_state: input.charge_state.clone(),
        charge_session_id: input.charge_session_id.clone(),
        charging_power_kw: input.charging_power_kw,
        energy_added_kwh: input.energy_added_kwh,
        ecu: input.ecu.clone(),
        dtc_code: input.dtc_code.clone(),
        warning_state: input.warning_state.clone(),
        update_state: input.update_state.clone(),
        target_software_version: input.target_software_version.clone(),
        update_progress_percent: input.update_progress_percent,
        topic: meta.topic.clone(),
        kafka_partition: meta.partition,
        kafka_offset: meta.offset,
        kafka_timestamp: meta.timestamp_ms,
        model_safe: nh.model_safe,
        software_version_safe: nh.software_version_safe,
        power_state_safe: nh.power_state_safe,
        event_date: td.event_date,
        event_year: td.event_year,
        event_month: td.event_month,
        event_day: td.event_day,
        event_hour: td.event_hour,
        event_dayofweek: td.event_dayofweek,
        is_weekend: td.is_weekend,
        event_quarter: td.event_quarter,
        date_str: td.date_str,
        soc_bucket: bf.soc_bucket,
        energy_pct: bf.energy_pct,
        cell_temp_spread: bf.cell_temp_spread,
        degradation_pct: bf.degradation_pct,
        km_per_percent: bf.km_per_percent,
        health_tier: hf.health_tier,
        thermal_alert: hf.thermal_alert,
        is_fault: hf.is_fault,
        needs_service: hf.needs_service,
        severity_level: hf.severity_level,
        lat_coarse: loc.lat_coarse,
        lon_coarse: loc.lon_coarse,
        vin_hash: anon.vin_hash,
        vin_wmi: anon.vin_wmi,
        row_id: ev.row_id,
        sw_major: ev.sw_major,
        sw_minor: ev.sw_minor,
    }
}
