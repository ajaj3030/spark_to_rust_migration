mod checkpoint;
mod config;
mod consumer;
mod metrics;
mod schema;
mod transforms;
mod validate;
mod vehicle_state;
mod writer;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "rust-pipeline", about = "Rust Kafka→Delta vehicle telemetry pipeline (port of the Spark jobs)")]
struct Cli {
    /// Path to app_config.yaml (shared with Spark)
    #[arg(short, long)]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the vehicle-telemetry enrichment stream (port of streaming_job.py)
    Stream,
    /// Run the vehicle-state MERGE stream (port of vehicle_state_job.py)
    State,
    /// Validate Rust output against Spark output
    Validate {
        #[arg(long)]
        spark_path: Option<String>,
        #[arg(long)]
        rust_path: Option<String>,
        /// Also validate the vehicle_state tables
        #[arg(long, default_value_t = false)]
        state: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("rust_pipeline=info".parse()?))
        .init();

    let cli = Cli::parse();
    let cfg = config::load_config(&cli.config)?;

    match cli.command {
        Command::Stream => {
            tracing::info!("Starting vehicle-telemetry streaming pipeline");
            consumer::run_streaming_pipeline(&cfg).await?;
        }
        Command::State => {
            tracing::info!("Starting vehicle-state upsert pipeline");
            vehicle_state::run_vehicle_state_pipeline(&cfg).await?;
        }
        Command::Validate { spark_path, rust_path, state } => {
            let sp = spark_path.unwrap_or_else(|| cfg.delta_table_path());
            let rp = rust_path.unwrap_or_else(|| cfg.rust_delta_table_path());
            tracing::info!("Validating: spark={} rust={}", sp, rp);
            validate::run_validation(&sp, &rp).await?;
            if state {
                let sa = cfg.state_delta_path();
                let ra = cfg.rust_state_delta_path();
                tracing::info!("Validating vehicle_state: spark={} rust={}", sa, ra);
                validate::run_validation(&sa, &ra).await?;
            }
        }
    }
    Ok(())
}
