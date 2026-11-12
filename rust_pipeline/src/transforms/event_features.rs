use super::anonymized::sha256_hex;

pub struct EventFeatures {
    pub row_id: String,
    pub sw_major: Option<i32>,
    pub sw_minor: Option<i32>,
}

/// Spark parity: add_event_features
/// - row_id   ← sha2(event_id, 256)
/// - sw_major ← split(software_version_safe, "\.")[0] cast int (null if not numeric)
/// - sw_minor ← split(software_version_safe, "\.")[1] cast int
pub fn apply(event_id: &str, software_version_safe: &str) -> EventFeatures {
    let mut parts = software_version_safe.split('.');
    EventFeatures {
        row_id: sha256_hex(event_id),
        sw_major: parts.next().and_then(|p| p.trim().parse().ok()),
        sw_minor: parts.next().and_then(|p| p.trim().parse().ok()),
    }
}
