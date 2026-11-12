//! Vehicle state upsert: Kafka → Delta MERGE, one row per VIN.
//!
//! Spark parity: spark_jobs/vehicle_state_job.py
//! - Collapse each batch to one row per VIN: newest snapshot by
//!   sequence_number, event_count summed, earliest first_seen_at.
//! - MERGE only when source.sequence_number > target.sequence_number, so
//!   out-of-order snapshots never overwrite newer state and a replayed batch
//!   is a no-op.

use crate::checkpoint::Checkpoint;
use crate::config::AppConfig;
use crate::metrics::MetricsBridge;
use crate::schema::{self, VehicleStateInput};
use crate::writer;

use anyhow::Result;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::*;
use deltalake::DeltaOps;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::TopicPartitionList;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Fields replaced wholesale by the newest snapshot.
const LATEST_STATE_FIELDS: &[&str] = &[
    "model",
    "model_variant",
    "battery_pack_type",
    "software_version",
    "power_state",
    "odometer_km",
    "charge_percent",
    "battery_health_level",
    "last_event_type",
    "sequence_number",
    "updated_at",
];

const ALL_STATE_FIELDS: &[&str] = &[
    "vin",
    "model",
    "model_variant",
    "battery_pack_type",
    "software_version",
    "power_state",
    "odometer_km",
    "charge_percent",
    "battery_health_level",
    "last_event_type",
    "sequence_number",
    "event_count",
    "updated_at",
    "first_seen_at",
];

pub async fn run_vehicle_state_pipeline(cfg: &AppConfig) -> Result<()> {
    let topic = &cfg.kafka.state_topic;
    let flush_timeout = Duration::from_secs(cfg.rust.flush_interval_seconds);
    let delta_path = cfg.rust_state_delta_path();

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", cfg.kafka_bootstrap())
        .set("group.id", &cfg.rust.state_consumer_group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "500")
        .create()?;

    let checkpoint = Checkpoint::open(&cfg.rust_state_checkpoint_path())?;
    let mut tpl = TopicPartitionList::new();
    for p in 0..cfg.kafka.partitions {
        tpl.add_partition_offset(topic, p, checkpoint.get_offset(topic, p)?)?;
    }
    consumer.assign(&tpl)?;
    tracing::info!("Vehicle state: assigned {} partitions on {}", cfg.kafka.partitions, topic);

    writer::ensure_parent_dir(&delta_path)?;
    let mut metrics = MetricsBridge::new(&cfg.rust_state_metrics_path());
    metrics.update_status("running")?;
    let mut batch_id: u64 = 0;

    loop {
        let t_start = Instant::now();
        let messages = collect_batch(&consumer, cfg.rust.batch_size, flush_timeout).await;
        if messages.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        let snapshots: Vec<VehicleStateInput> = messages
            .iter()
            .filter_map(|m| serde_json::from_slice(&m.payload).ok())
            .collect();
        let rejected = messages.len() - snapshots.len();
        if rejected > 0 {
            tracing::warn!("State batch {}: {} messages failed to parse and were skipped", batch_id, rejected);
        }
        if snapshots.is_empty() {
            continue;
        }

        let snapshot_count = snapshots.len();
        let per_vehicle = collapse_batch_per_vehicle(snapshots);
        let vehicle_count = per_vehicle.len();
        let t_transform = t_start.elapsed();

        let t_write_start = Instant::now();
        let batch = schema::build_vehicle_state_batch(&per_vehicle)?;
        match deltalake::open_table(&delta_path).await {
            Ok(table) => merge_into_vehicle_state(table, batch).await?,
            Err(_) => writer::write_batch(&delta_path, batch, &["model".to_string()]).await?,
        }
        let t_write = t_write_start.elapsed();

        for ((top, part), off) in &compute_max_offsets(&messages) {
            checkpoint.commit_offset(top, *part, *off, batch_id)?;
        }

        let t_total = t_start.elapsed();
        metrics.record_batch(
            batch_id,
            snapshot_count,
            t_total.as_millis() as f64,
            t_transform.as_millis() as f64,
            t_write.as_millis() as f64,
        )?;
        tracing::info!(
            "State batch {}: {} snapshots → {} vehicles | merge {}ms | total {}ms",
            batch_id, snapshot_count, vehicle_count, t_write.as_millis(), t_total.as_millis(),
        );
        batch_id += 1;
    }
}

/// One row per VIN: newest snapshot by sequence_number, with this batch's
/// summed event_count and earliest first_seen_at.
pub fn collapse_batch_per_vehicle(snapshots: Vec<VehicleStateInput>) -> Vec<VehicleStateInput> {
    let mut by_vin: HashMap<String, VehicleStateInput> = HashMap::new();
    for snap in snapshots {
        match by_vin.get_mut(&snap.vin) {
            None => {
                by_vin.insert(snap.vin.clone(), snap);
            }
            Some(kept) => {
                let total = kept.event_count.unwrap_or(0) + snap.event_count.unwrap_or(0);
                let earliest = min_opt(kept.first_seen_at.take(), snap.first_seen_at.clone());
                if snap.sequence_number > kept.sequence_number {
                    *kept = snap;
                }
                kept.event_count = Some(total);
                kept.first_seen_at = earliest;
            }
        }
    }
    by_vin.into_values().collect()
}

fn min_opt(a: Option<String>, b: Option<String>) -> Option<String> {
    match (a, b) {
        (Some(x), Some(y)) => Some(if x <= y { x } else { y }),
        (x, None) => x,
        (None, y) => y,
    }
}

async fn merge_into_vehicle_state(table: deltalake::DeltaTable, source: RecordBatch) -> Result<()> {
    let ctx = SessionContext::new();
    let mem_table =
        deltalake::datafusion::datasource::MemTable::try_new(source.schema(), vec![vec![source]])?;
    let source_df = ctx.read_table(Arc::new(mem_table))?;

    DeltaOps(table)
        .merge(source_df, col("target.vin").eq(col("source.vin")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            let guarded = update
                .predicate(col("source.sequence_number").gt(col("target.sequence_number")))
                .update(
                    "event_count",
                    col("target.event_count") + col("source.event_count"),
                )
                .update(
                    "first_seen_at",
                    when(
                        col("target.first_seen_at").lt(col("source.first_seen_at")),
                        col("target.first_seen_at"),
                    )
                    .otherwise(col("source.first_seen_at"))
                    .expect("valid CASE expression"),
                );
            LATEST_STATE_FIELDS.iter().fold(guarded, |u, name| {
                u.update(*name, col(format!("source.{name}")))
            })
        })?
        .when_not_matched_insert(|insert| {
            ALL_STATE_FIELDS
                .iter()
                .fold(insert, |i, name| i.set(*name, col(format!("source.{name}"))))
        })?
        .await?;
    Ok(())
}

struct OwnedMsg {
    payload: Vec<u8>,
    topic: String,
    partition: i32,
    offset: i64,
}

async fn collect_batch(consumer: &StreamConsumer, max: usize, timeout: Duration) -> Vec<OwnedMsg> {
    let deadline = Instant::now() + timeout;
    let mut msgs = Vec::with_capacity(max.min(100_000));
    while msgs.len() < max {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(m)) => {
                if let Some(p) = m.payload() {
                    msgs.push(OwnedMsg {
                        payload: p.to_vec(),
                        topic: m.topic().to_string(),
                        partition: m.partition(),
                        offset: m.offset(),
                    });
                }
            }
            Ok(Err(e)) => {
                tracing::warn!("Kafka recv error: {}", e);
                break;
            }
            Err(_) => break, // timeout
        }
    }
    msgs
}

fn compute_max_offsets(messages: &[OwnedMsg]) -> HashMap<(String, i32), i64> {
    let mut offsets: HashMap<(String, i32), i64> = HashMap::new();
    for m in messages {
        let entry = offsets.entry((m.topic.clone(), m.partition)).or_insert(m.offset);
        *entry = (*entry).max(m.offset);
    }
    // Store offset + 1 (next offset to read)
    offsets.values_mut().for_each(|v| *v += 1);
    offsets
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(vin: &str, seq: i64, first_seen: &str) -> VehicleStateInput {
        VehicleStateInput {
            vin: vin.into(),
            model: Some("MODEL_AIR".into()),
            model_variant: None,
            battery_pack_type: None,
            software_version: Some(format!("v{seq}")),
            power_state: None,
            odometer_km: None,
            charge_percent: None,
            battery_health_level: None,
            last_event_type: None,
            sequence_number: seq,
            event_count: Some(1),
            updated_at: None,
            first_seen_at: Some(first_seen.into()),
        }
    }

    #[test]
    fn keeps_newest_snapshot_and_sums_counts_regardless_of_arrival_order() {
        let collapsed = collapse_batch_per_vehicle(vec![
            snap("VIN1", 2, "2026-01-02"),
            snap("VIN1", 3, "2026-01-03"),
            snap("VIN1", 1, "2026-01-01"),
            snap("VIN2", 7, "2026-02-01"),
        ]);
        let vin1 = collapsed.iter().find(|s| s.vin == "VIN1").unwrap();
        assert_eq!(vin1.sequence_number, 3);
        assert_eq!(vin1.software_version.as_deref(), Some("v3"));
        assert_eq!(vin1.event_count, Some(3));
        assert_eq!(vin1.first_seen_at.as_deref(), Some("2026-01-01"));
        assert_eq!(collapsed.len(), 2);
    }
}
