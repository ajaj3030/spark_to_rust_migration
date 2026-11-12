use crate::schema::TelemetryInput;

pub struct NullHandled {
    pub model_safe: String,
    pub software_version_safe: String,
    pub power_state_safe: String,
    pub odometer_km: f64,
}

/// Spark parity: add_null_handling
/// - model_safe            ← coalesce(model, "MODEL_UNKNOWN")
/// - software_version_safe ← coalesce(software_version, "0.0.0")
/// - power_state_safe      ← coalesce(power_state, "POWER_STATE_UNKNOWN")
/// - odometer_km           ← na.fill(0.0)
pub fn apply(input: &TelemetryInput) -> NullHandled {
    NullHandled {
        model_safe: input
            .model
            .clone()
            .unwrap_or_else(|| "MODEL_UNKNOWN".into()),
        software_version_safe: input
            .software_version
            .clone()
            .unwrap_or_else(|| "0.0.0".into()),
        power_state_safe: input
            .power_state
            .clone()
            .unwrap_or_else(|| "POWER_STATE_UNKNOWN".into()),
        odometer_km: input.odometer_km.unwrap_or(0.0),
    }
}
