use sha2::{Digest, Sha256};

pub struct AnonymizedColumns {
    pub vin_hash: String,
    pub vin_wmi: String,
}

/// Spark parity: add_anonymized_columns (privacy)
/// - vin_hash ← sha2(vin, 256)
/// - vin_wmi  ← substring(vin, 1, 3)  (manufacturer identifier, not personal)
///
/// The raw VIN is not carried into the output record.
pub fn apply(vin: &str) -> AnonymizedColumns {
    AnonymizedColumns {
        vin_hash: sha256_hex(vin),
        vin_wmi: vin.chars().take(3).collect(),
    }
}

pub fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}
