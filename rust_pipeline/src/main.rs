mod analytics;
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
#[command(
    name = "rust-pipeline",
    about = "Rust Kafka→Delta vehicle telemetry pipeline (port of the Spark jobs)"
)]
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
    /// Run one ported batch analytics job (port of a Scala job in spark_jobs/analytics/)
    Analytics {
        /// battery_health_daily | charging_sessions | ota_rollout | diagnostic_alerts
        job: String,
    },
    /// Validate Rust output against Spark output (telemetry by default)
    Validate {
        /// Override the Spark telemetry table path
        #[arg(long)]
        spark_path: Option<String>,
        /// Override the Rust telemetry table path
        #[arg(long)]
        rust_path: Option<String>,
        /// Also validate the vehicle_state tables
        #[arg(long, default_value_t = false)]
        state: bool,
        /// Validate one analytics job's output instead of telemetry
        #[arg(long)]
        job: Option<String>,
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
        Command::Analytics { job } => {
            analytics::run(analytics::find_job(&job)?, &cfg).await?;
        }
        Command::Validate {
            spark_path,
            rust_path,
            state,
            job: Some(job),
        } => {
            if spark_path.is_some() || rust_path.is_some() || state {
                anyhow::bail!("--job cannot be combined with --spark-path, --rust-path or --state");
            }
            let job = analytics::find_job(&job)?;
            validate::run_validation(
                &cfg.spark_analytics_path(job.name),
                &cfg.rust_analytics_path(job.name),
                &job.comparison(),
            )
            .await?;
        }
        Command::Validate {
            spark_path,
            rust_path,
            state,
            job: None,
        } => {
            let sp = spark_path.unwrap_or_else(|| cfg.delta_table_path());
            let rp = rust_path.unwrap_or_else(|| cfg.rust_delta_table_path());
            validate::run_validation(&sp, &rp, &validate::TELEMETRY).await?;
            if state {
                validate::run_validation(
                    &cfg.state_delta_path(),
                    &cfg.rust_state_delta_path(),
                    &validate::VEHICLE_STATE,
                )
                .await?;
            }
        }
    }
    Ok(())
}
