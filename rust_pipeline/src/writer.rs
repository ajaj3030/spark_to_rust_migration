use anyhow::Result;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::protocol::SaveMode;
use deltalake::DeltaOps;
use std::path::Path;

/// Ensure the parent directory for a Delta table path exists.
pub fn ensure_parent_dir(path: &str) -> Result<()> {
    let p = Path::new(path);
    if !p.exists() {
        match std::fs::create_dir_all(p) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Append an Arrow RecordBatch to a Delta table (creating it if needed).
pub async fn write_batch(
    table_path: &str,
    batch: RecordBatch,
    partition_columns: &[String],
) -> Result<()> {
    let ops = DeltaOps::try_from_uri(table_path).await?;
    ops.write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .with_partition_columns(partition_columns.to_vec())
        .await?;
    Ok(())
}
