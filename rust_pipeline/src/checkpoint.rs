use anyhow::Result;
use rdkafka::Offset;
use rusqlite::Connection;
use std::path::Path;

pub struct Checkpoint {
    conn: Connection,
}

impl Checkpoint {
    pub fn open(path: &str) -> Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS checkpoints (
                topic       TEXT NOT NULL,
                partition   INTEGER NOT NULL,
                offset      INTEGER NOT NULL,
                batch_id    INTEGER NOT NULL,
                committed_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (topic, partition)
            );",
        )?;
        Ok(Self { conn })
    }

    /// Get the last committed offset for a (topic, partition).
    /// Returns Offset::Stored if a checkpoint exists, Offset::Beginning otherwise.
    pub fn get_offset(&self, topic: &str, partition: i32) -> Result<Offset> {
        let result: Option<i64> = self
            .conn
            .query_row(
                "SELECT offset FROM checkpoints WHERE topic = ?1 AND partition = ?2",
                rusqlite::params![topic, partition],
                |row| row.get(0),
            )
            .ok();

        match result {
            Some(off) => Ok(Offset::Offset(off)),
            None => Ok(Offset::Beginning),
        }
    }

    /// Commit an offset for a (topic, partition) in a transaction.
    pub fn commit_offset(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        batch_id: u64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO checkpoints (topic, partition, offset, batch_id)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(topic, partition) DO UPDATE SET
                offset = excluded.offset,
                batch_id = excluded.batch_id,
                committed_at = datetime('now')",
            rusqlite::params![topic, partition, offset, batch_id as i64],
        )?;
        Ok(())
    }
}
