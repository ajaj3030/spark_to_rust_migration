use super::rounding::spark_round;

pub struct LocationFeatures {
    pub lat_coarse: Option<f64>,
    pub lon_coarse: Option<f64>,
}

/// Spark parity: add_location_features (privacy). Coarsens GPS to 2 dp
/// (~1 km). The precise coordinates are not carried into the output record.
pub fn apply(latitude: Option<f64>, longitude: Option<f64>) -> LocationFeatures {
    LocationFeatures {
        lat_coarse: latitude.map(|v| spark_round(v, 2)),
        lon_coarse: longitude.map(|v| spark_round(v, 2)),
    }
}
