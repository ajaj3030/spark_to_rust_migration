use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

/// Top-level config — mirrors config/app_config.yaml structure.
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub spark: SparkConfig,
    pub vehicle_state: VehicleStateConfig,
    pub delta: DeltaConfig,
    #[serde(default)]
    pub rust: RustConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub state_topic: String,
    pub partitions: i32,
    pub internal_bootstrap_servers: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SparkConfig {
    pub max_offsets_per_trigger: usize,
    #[serde(default = "default_trigger")]
    pub trigger_interval: String,
    pub shuffle_partitions: i32,
    pub delta_output_path: String,
    #[serde(default)]
    pub local_delta_output: String,
    pub cost_per_core_hour: f64,
    pub worker_cores: i32,
    pub worker_memory: String,
}

fn default_trigger() -> String { "10 seconds".into() }

#[derive(Debug, Deserialize, Clone)]
pub struct VehicleStateConfig {
    pub delta_output_path: String,
    #[serde(default)]
    pub local_delta_output: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DeltaConfig {
    pub table_path: String,
    pub state_table_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RustConfig {
    #[serde(default = "default_consumer_group")]
    pub consumer_group: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_flush_interval")]
    pub flush_interval_seconds: u64,
    #[serde(default = "default_rust_delta")]
    pub delta_output_path: String,
    #[serde(default = "default_rust_delta_local")]
    pub local_delta_output: String,
    #[serde(default = "default_rust_checkpoint")]
    pub checkpoint_db: String,
    #[serde(default = "default_rust_checkpoint_local")]
    pub local_checkpoint_db: String,
    #[serde(default = "default_rust_state_delta")]
    pub state_delta_output_path: String,
    #[serde(default = "default_rust_state_delta_local")]
    pub local_state_delta_output: String,
    #[serde(default = "default_rust_state_ckpt")]
    pub state_checkpoint_db: String,
    #[serde(default = "default_rust_state_ckpt_local")]
    pub local_state_checkpoint_db: String,
    #[serde(default = "default_state_consumer_group")]
    pub state_consumer_group: String,
    #[serde(default = "default_metrics_file")]
    pub metrics_file: String,
    #[serde(default = "default_state_metrics_file")]
    pub state_metrics_file: String,
}

fn default_consumer_group() -> String { "rust-vehicle-telemetry-consumer".into() }
fn default_batch_size() -> usize { 500_000 }
fn default_flush_interval() -> u64 { 10 }
fn default_rust_delta() -> String { "/opt/delta_output/vehicle_telemetry_rust".into() }
fn default_rust_delta_local() -> String { "./delta_output/vehicle_telemetry_rust".into() }
fn default_rust_checkpoint() -> String { "/opt/checkpoints/rust_vehicle_telemetry/offsets.db".into() }
fn default_rust_checkpoint_local() -> String { "./checkpoints/rust_vehicle_telemetry/offsets.db".into() }
fn default_rust_state_delta() -> String { "/opt/delta_output/vehicle_state_rust".into() }
fn default_rust_state_delta_local() -> String { "./delta_output/vehicle_state_rust".into() }
fn default_rust_state_ckpt() -> String { "/opt/checkpoints/rust_vehicle_state/offsets.db".into() }
fn default_rust_state_ckpt_local() -> String { "./checkpoints/rust_vehicle_state/offsets.db".into() }
fn default_state_consumer_group() -> String { "rust-vehicle-state-consumer".into() }
fn default_metrics_file() -> String { "/opt/logs/rust_metrics.json".into() }
fn default_state_metrics_file() -> String { "/opt/logs/rust_state_metrics.json".into() }

impl Default for RustConfig {
    fn default() -> Self {
        Self {
            consumer_group: default_consumer_group(),
            batch_size: default_batch_size(),
            flush_interval_seconds: default_flush_interval(),
            delta_output_path: default_rust_delta(),
            local_delta_output: default_rust_delta_local(),
            checkpoint_db: default_rust_checkpoint(),
            local_checkpoint_db: default_rust_checkpoint_local(),
            state_delta_output_path: default_rust_state_delta(),
            local_state_delta_output: default_rust_state_delta_local(),
            state_checkpoint_db: default_rust_state_ckpt(),
            local_state_checkpoint_db: default_rust_state_ckpt_local(),
            state_consumer_group: default_state_consumer_group(),
            metrics_file: default_metrics_file(),
            state_metrics_file: default_state_metrics_file(),
        }
    }
}

impl AppConfig {
    /// Returns true when running inside Docker (/opt paths exist).
    pub fn is_docker(&self) -> bool {
        Path::new("/opt/config").exists() || Path::new("/opt/spark-jobs").exists()
    }

    pub fn kafka_bootstrap(&self) -> &str {
        if self.is_docker() {
            &self.kafka.internal_bootstrap_servers
        } else {
            &self.kafka.bootstrap_servers
        }
    }

    pub fn rust_delta_table_path(&self) -> String {
        if self.is_docker() { self.rust.delta_output_path.clone() }
        else { self.rust.local_delta_output.clone() }
    }

    pub fn rust_checkpoint_path(&self) -> String {
        if self.is_docker() { self.rust.checkpoint_db.clone() }
        else { self.rust.local_checkpoint_db.clone() }
    }

    pub fn rust_state_delta_path(&self) -> String {
        if self.is_docker() { self.rust.state_delta_output_path.clone() }
        else { self.rust.local_state_delta_output.clone() }
    }

    pub fn rust_state_checkpoint_path(&self) -> String {
        if self.is_docker() { self.rust.state_checkpoint_db.clone() }
        else { self.rust.local_state_checkpoint_db.clone() }
    }

    pub fn rust_metrics_path(&self) -> String {
        if self.is_docker() { self.rust.metrics_file.clone() }
        else { "./logs/rust_metrics.json".into() }
    }

    pub fn rust_state_metrics_path(&self) -> String {
        if self.is_docker() { self.rust.state_metrics_file.clone() }
        else { "./logs/rust_state_metrics.json".into() }
    }

    pub fn delta_table_path(&self) -> String {
        if self.is_docker() { self.spark.delta_output_path.clone() }
        else { self.delta.table_path.clone() }
    }

    pub fn state_delta_path(&self) -> String {
        if self.is_docker() { self.vehicle_state.delta_output_path.clone() }
        else { self.delta.state_table_path.clone() }
    }
}

pub fn load_config(path: &Path) -> Result<AppConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config: {}", path.display()))?;
    let cfg: AppConfig = serde_yaml::from_str(&text)
        .with_context(|| "Failed to parse app_config.yaml")?;
    Ok(cfg)
}
