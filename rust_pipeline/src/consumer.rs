use crate::checkpoint::Checkpoint;
use crate::config::AppConfig;
use crate::metrics::MetricsBridge;
use crate::schema::{self, KafkaMeta, TelemetryInput, TelemetryOutput};
use crate::transforms;
use crate::writer;

use anyhow::Result;
use rayon::prelude::*;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::time::{Duration, Instant};

struct OwnedMessage {
    payload: Vec<u8>,
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_ms: i64,
}

pub async fn run_streaming_pipeline(cfg: &AppConfig) -> Result<()> {
    let topic = &cfg.kafka.topic;
    let num_partitions = cfg.kafka.partitions;
    let batch_size = cfg.rust.batch_size;
    let flush_timeout = Duration::from_secs(cfg.rust.flush_interval_seconds);
    let delta_path = cfg.rust_delta_table_path();
    let ckpt_path = cfg.rust_checkpoint_path();
    let metrics_path = cfg.rust_metrics_path();
    let partition_cols = vec![
        "event_year".to_string(),
        "event_month".to_string(),
        "event_type".to_string(),
    ];

    // Create consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", cfg.kafka_bootstrap())
        .set("group.id", &cfg.rust.consumer_group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("fetch.wait.max.ms", "500")
        .set("max.partition.fetch.bytes", "10485760")
        .create()?;

    // Open checkpoint DB
    let checkpoint = Checkpoint::open(&ckpt_path)?;

    // Assign partitions and seek to checkpointed offsets
    let mut tpl = TopicPartitionList::new();
    for p in 0..num_partitions {
        let off = checkpoint.get_offset(topic, p)?;
        tpl.add_partition_offset(topic, p, off)?;
    }
    consumer.assign(&tpl)?;
    tracing::info!("Assigned {} partitions on topic {}", num_partitions, topic);

    // Ensure delta table dir exists
    writer::ensure_parent_dir(&delta_path)?;

    let mut metrics = MetricsBridge::new(&metrics_path);
    metrics.update_status("running")?;
    let mut batch_id: u64 = 0;

    loop {
        let t_batch_start = Instant::now();

        // Collect batch
        let messages = collect_batch(&consumer, batch_size, flush_timeout).await;
        if messages.is_empty() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        let row_count = messages.len();

        // Deserialize + transform (parallel via rayon)
        let t_transform_start = Instant::now();
        let records: Vec<TelemetryOutput> = messages
            .par_iter()
            .filter_map(|msg| {
                let input: TelemetryInput = serde_json::from_slice(&msg.payload).ok()?;
                let meta = KafkaMeta {
                    topic: msg.topic.clone(),
                    partition: msg.partition,
                    offset: msg.offset,
                    timestamp_ms: msg.timestamp_ms,
                };
                Some(transforms::apply_all_transforms(&input, &meta))
            })
            .collect();
        let t_transform = t_transform_start.elapsed();

        let rejected = row_count - records.len();
        if rejected > 0 {
            tracing::warn!(
                "Batch {}: {} of {} messages failed to parse and were skipped",
                batch_id, rejected, row_count,
            );
        }

        if records.is_empty() {
            continue;
        }

        // Build Arrow batch and write to Delta
        let t_write_start = Instant::now();
        let arrow_batch = schema::build_telemetry_batch(&records)?;
        writer::write_batch(&delta_path, arrow_batch, &partition_cols).await?;
        let t_write = t_write_start.elapsed();

        // Checkpoint offsets
        let offsets = compute_max_offsets(&messages);
        for ((top, part), off) in &offsets {
            checkpoint.commit_offset(top, *part, *off, batch_id)?;
        }

        // Metrics
        let t_batch = t_batch_start.elapsed();
        metrics.record_batch(
            batch_id,
            row_count,
            t_batch.as_millis() as f64,
            t_transform.as_millis() as f64,
            t_write.as_millis() as f64,
        )?;

        tracing::info!(
            "Batch {}: {} rows | transform {}ms | write {}ms | total {}ms",
            batch_id,
            row_count,
            t_transform.as_millis(),
            t_write.as_millis(),
            t_batch.as_millis(),
        );

        batch_id += 1;
    }
}

async fn collect_batch(
    consumer: &StreamConsumer,
    max_messages: usize,
    timeout: Duration,
) -> Vec<OwnedMessage> {
    let deadline = Instant::now() + timeout;
    let mut messages = Vec::with_capacity(max_messages.min(100_000));

    while messages.len() < max_messages {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(payload) = msg.payload() {
                    messages.push(OwnedMessage {
                        payload: payload.to_vec(),
                        topic: msg.topic().to_string(),
                        partition: msg.partition(),
                        offset: msg.offset(),
                        timestamp_ms: msg.timestamp().to_millis().unwrap_or(0),
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

    messages
}

fn compute_max_offsets(messages: &[OwnedMessage]) -> HashMap<(String, i32), i64> {
    let mut offsets: HashMap<(String, i32), i64> = HashMap::new();
    for m in messages {
        let key = (m.topic.clone(), m.partition);
        let entry = offsets.entry(key).or_insert(m.offset);
        if m.offset > *entry {
            *entry = m.offset;
        }
    }
    // Store offset+1 (next offset to read)
    for v in offsets.values_mut() {
        *v += 1;
    }
    offsets
}
