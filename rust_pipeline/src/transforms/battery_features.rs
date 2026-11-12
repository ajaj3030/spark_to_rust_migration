use super::rounding::spark_round;
use crate::schema::TelemetryInput;

pub struct BatteryFeatures {
    pub soc_bucket: String,
    pub energy_pct: Option<f64>,
    pub cell_temp_spread: Option<f64>,
    pub degradation_pct: Option<f64>,
    pub km_per_percent: Option<f64>,
}

/// Spark parity: add_battery_features. Every derived value is null when a
/// source field is null, matching Spark's null propagation.
pub fn apply(input: &TelemetryInput) -> BatteryFeatures {
    let soc = input.charge_percent;

    let soc_bucket = match soc {
        None => "UNKNOWN",
        Some(s) if s < 10.0 => "CRITICAL",
        Some(s) if s < 20.0 => "LOW",
        Some(s) if s < 80.0 => "NORMAL",
        Some(_) => "HIGH",
    }
    .to_string();

    let energy_pct = match (input.kwhr, input.capacity_kwhr) {
        (Some(k), Some(c)) if c > 0.0 => Some(spark_round(k / c * 100.0, 2)),
        _ => None,
    };

    let cell_temp_spread = match (input.max_cell_temp, input.min_cell_temp) {
        (Some(max), Some(min)) => Some(spark_round(max - min, 2)),
        _ => None,
    };

    let degradation_pct = input
        .battery_health_level
        .map(|h| spark_round(100.0 - h, 2));

    let km_per_percent = match (input.remaining_range, soc) {
        (Some(range), Some(s)) if s > 0.0 => Some(spark_round(range / s, 3)),
        _ => None,
    };

    BatteryFeatures {
        soc_bucket,
        energy_pct,
        cell_temp_spread,
        degradation_pct,
        km_per_percent,
    }
}
