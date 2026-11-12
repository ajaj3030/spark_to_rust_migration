use crate::schema::TelemetryInput;

const THERMAL_MAX_CELL_TEMP_C: f64 = 55.0;
const THERMAL_CELL_SPREAD_C: f64 = 15.0;
const SERVICE_HEALTH_THRESHOLD: f64 = 70.0;

const FAULT_CHARGE_STATES: &[&str] = &[
    "CHARGE_STATE_CHARGING_END_ERROR",
    "CHARGE_STATE_EVSE_MALFUNCTION",
    "CHARGE_STATE_TEMP_PROTECTION",
    "CHARGE_STATE_AUTHENTICATION_PNC_FAILURE",
    "CHARGE_STATE_CABLE_CONNECTION_FAULT",
    "CHARGE_STATE_INTERNAL_HARDWARE_FAULT",
];

const FAILED_UPDATE_STATES: &[&str] = &[
    "UPDATE_STATE_FAILED",
    "UPDATE_FAILED_DRIVE_ALLOWED",
    "UPDATE_FAILED_NO_ACTION",
];

pub struct HealthFeatures {
    pub health_tier: String,
    pub thermal_alert: bool,
    pub is_fault: bool,
    pub needs_service: bool,
    pub severity_level: i32,
}

/// Spark parity: add_health_features. A null comparison counts as false.
/// `cell_temp_spread` is the rounded value from battery_features (T3).
pub fn apply(input: &TelemetryInput, cell_temp_spread: Option<f64>) -> HealthFeatures {
    let thermal_alert = input.max_cell_temp.map_or(false, |t| t >= THERMAL_MAX_CELL_TEMP_C)
        || cell_temp_spread.map_or(false, |s| s >= THERMAL_CELL_SPREAD_C);

    let is_fault = input.warning_state.as_deref() == Some("WARNING_ON")
        || is_one_of(&input.charge_state, FAULT_CHARGE_STATES)
        || is_one_of(&input.update_state, FAILED_UPDATE_STATES);

    let needs_service = input
        .battery_health_level
        .map_or(false, |h| h < SERVICE_HEALTH_THRESHOLD)
        || thermal_alert
        || is_fault;

    let severity_level = if thermal_alert {
        3
    } else if is_fault {
        2
    } else if needs_service {
        1
    } else {
        0
    };

    HealthFeatures {
        health_tier: classify_health(input.battery_health_level).to_string(),
        thermal_alert,
        is_fault,
        needs_service,
        severity_level,
    }
}

fn is_one_of(value: &Option<String>, allowed: &[&str]) -> bool {
    value.as_deref().map_or(false, |v| allowed.contains(&v))
}

/// Replicates Spark UDF classify_health(battery_health_level)
fn classify_health(level: Option<f64>) -> &'static str {
    match level {
        None => "UNKNOWN",
        Some(h) if h >= 90.0 => "EXCELLENT",
        Some(h) if h >= 80.0 => "GOOD",
        Some(h) if h >= 70.0 => "FAIR",
        Some(_) => "SERVICE_REQUIRED",
    }
}
