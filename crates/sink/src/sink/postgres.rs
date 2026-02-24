/// PostgreSQL sink using `tokio-postgres` + `deadpool-postgres` connection pool.
///
/// This module persists Valkey key-value pairs to a PostgreSQL table using
/// `INSERT ... ON CONFLICT DO UPDATE` (upsert) for idempotent writes and
/// single-key `SELECT` for read-through lookups.
///
/// ## Table schema
///
/// The target table must have the following schema:
///
/// ```sql
/// CREATE TABLE IF NOT EXISTS <table_name> (
///     pk   BYTEA   PRIMARY KEY,
///     val  BYTEA   NOT NULL,
///     ts   BIGINT  NOT NULL,
///     ttl  BIGINT  NULL
/// );
/// ```
///
/// | Column | Type   | Description |
/// |--------|--------|-------------|
/// | `pk`   | BYTEA  | Valkey key bytes |
/// | `val`  | BYTEA  | Valkey value bytes |
/// | `ts`   | BIGINT | Unix timestamp in seconds of the write |
/// | `ttl`  | BIGINT | Absolute expiry time (Unix epoch seconds), NULL = no expiry |

use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use futures::stream::{self, StreamExt};
use tokio_postgres::NoTls;

use super::{Sink, SinkEntry, SinkError};

/// Maximum entries per batch INSERT statement.
///
/// PostgreSQL doesn't have a hard per-statement row limit, but binding too many
/// parameters in a single prepared statement can hurt planning time and memory.
/// 500 rows x 4 params = 2000 bind parameters — well within PG's limits while
/// keeping individual statements fast.
const BATCH_WRITE_MAX: usize = 500;

/// Persists key-value pairs to a PostgreSQL table.
///
/// Table schema:
///   pk  (BYTEA)  - primary key, raw Valkey key bytes
///   val (BYTEA)  - raw value bytes
///   ts  (BIGINT) - Unix timestamp in seconds
///   ttl (BIGINT) - absolute expiry epoch seconds, NULL = no expiry
pub struct PostgresSink {
    pool: Pool,
    table_name: String,
    write_concurrency: usize,
}

impl std::fmt::Debug for PostgresSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSink")
            .field("table_name", &self.table_name)
            .field("write_concurrency", &self.write_concurrency)
            .finish_non_exhaustive()
    }
}

impl PostgresSink {
    /// Create a new PostgresSink.
    ///
    /// # Arguments
    ///
    /// * `connection_string` — PostgreSQL connection URI
    ///   (e.g. `postgres://user:pass@host:5432/db`)
    /// * `table_name` — target table name (must already exist with the expected schema)
    /// * `write_concurrency` — maximum number of concurrent batch INSERT statements
    /// * `pool_size` — maximum number of connections in the pool
    pub fn new(
        connection_string: String,
        table_name: String,
        write_concurrency: usize,
        pool_size: u32,
    ) -> Result<Self, SinkError> {
        let pg_config: tokio_postgres::Config = connection_string
            .parse()
            .map_err(|e| SinkError::Fatal(format!("invalid connection string: {}", e)))?;

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(pg_config, NoTls, mgr_config);

        let pool = Pool::builder(mgr)
            .max_size(pool_size as usize)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| SinkError::Fatal(format!("failed to build connection pool: {}", e)))?;

        Ok(Self {
            pool,
            table_name,
            write_concurrency,
        })
    }

    /// Build a multi-row upsert statement for `n` entries.
    ///
    /// Generates SQL of the form:
    /// ```sql
    /// INSERT INTO <table> (pk, val, ts, ttl)
    /// VALUES ($1,$2,$3,$4), ($5,$6,$7,$8), ...
    /// ON CONFLICT (pk) DO UPDATE SET val=EXCLUDED.val, ts=EXCLUDED.ts, ttl=EXCLUDED.ttl
    /// ```
    fn build_upsert_sql(table: &str, n: usize) -> String {
        let mut sql = format!(
            "INSERT INTO {} (pk, val, ts, ttl) VALUES ",
            table
        );
        for i in 0..n {
            if i > 0 {
                sql.push(',');
            }
            let base = i * 4;
            sql.push_str(&format!(
                "(${},${},${},${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
            ));
        }
        sql.push_str(
            " ON CONFLICT (pk) DO UPDATE SET val=EXCLUDED.val, ts=EXCLUDED.ts, ttl=EXCLUDED.ttl",
        );
        sql
    }
}

impl PostgresSink {
    /// Write a batch from borrowed references (avoids cloning entries).
    ///
    /// This is the primary write path called by the SinkRouter. Entries are
    /// chunked into groups of [`BATCH_WRITE_MAX`] and executed concurrently
    /// using `buffer_unordered` up to `write_concurrency`.
    pub async fn write_batch_refs(&self, entries: &[&SinkEntry]) -> Result<(), SinkError> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        // Build all chunk futures upfront, then execute concurrently.
        let futs: Vec<_> = entries
            .chunks(BATCH_WRITE_MAX)
            .map(|chunk| {
                let ts = ts;
                async move {
                    let client = self.pool.get().await.map_err(|e| {
                        SinkError::Retriable(format!("PG pool connection failed: {}", e))
                    })?;

                    let sql = Self::build_upsert_sql(&self.table_name, chunk.len());

                    // Build the parameter list. Each entry contributes 4 params:
                    // pk (BYTEA), val (BYTEA), ts (BIGINT), ttl (BIGINT nullable).
                    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
                        Vec::with_capacity(chunk.len() * 4);

                    for entry in chunk {
                        params.push(Box::new(entry.key.clone()));
                        params.push(Box::new(entry.value.clone()));
                        params.push(Box::new(ts));
                        let ttl: Option<i64> = entry.expires_at.map(|e| e as i64);
                        params.push(Box::new(ttl));
                    }

                    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                        params.iter().map(|p| p.as_ref() as _).collect();

                    client.execute(&sql as &str, &param_refs).await.map_err(|e| {
                        SinkError::Retriable(format!("PG upsert failed: {}", e))
                    })?;

                    Ok(())
                }
            })
            .collect();

        let results: Vec<Result<(), SinkError>> = stream::iter(futs)
            .buffer_unordered(self.write_concurrency)
            .collect()
            .await;

        for result in results {
            result?;
        }

        Ok(())
    }

    /// Look up a single key and return its value and optional TTL.
    ///
    /// If the row exists but `ttl` is set and `now >= ttl`, the entry is treated
    /// as expired and `None` is returned.
    async fn lookup_impl(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Option<u64>)>, SinkError> {
        let client = self.pool.get().await.map_err(|e| {
            SinkError::Retriable(format!("PG pool connection failed: {}", e))
        })?;

        let sql = format!(
            "SELECT val, ttl FROM {} WHERE pk = $1",
            self.table_name
        );

        let row = client
            .query_opt(&sql as &str, &[&key])
            .await
            .map_err(|e| SinkError::Retriable(format!("PG lookup failed: {}", e)))?;

        match row {
            Some(row) => {
                let val: Vec<u8> = row.get(0);
                let ttl: Option<i64> = row.get(1);

                let expires_at = ttl.map(|t| t as u64);

                // If TTL is set and expired, treat as not found.
                if let Some(exp) = expires_at {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    if now >= exp {
                        return Ok(None);
                    }
                }

                Ok(Some((val, expires_at)))
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn write_batch(&self, entries: &[SinkEntry]) -> Result<(), SinkError> {
        let refs: Vec<&SinkEntry> = entries.iter().collect();
        self.write_batch_refs(&refs).await
    }

    async fn lookup(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Option<u64>)>, SinkError> {
        self.lookup_impl(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_upsert_sql_single_entry() {
        let sql = PostgresSink::build_upsert_sql("my_table", 1);
        assert_eq!(
            sql,
            "INSERT INTO my_table (pk, val, ts, ttl) VALUES ($1,$2,$3,$4) \
             ON CONFLICT (pk) DO UPDATE SET val=EXCLUDED.val, ts=EXCLUDED.ts, ttl=EXCLUDED.ttl"
        );
    }

    #[test]
    fn test_build_upsert_sql_three_entries() {
        let sql = PostgresSink::build_upsert_sql("sink_data", 3);
        assert_eq!(
            sql,
            "INSERT INTO sink_data (pk, val, ts, ttl) VALUES \
             ($1,$2,$3,$4),($5,$6,$7,$8),($9,$10,$11,$12) \
             ON CONFLICT (pk) DO UPDATE SET val=EXCLUDED.val, ts=EXCLUDED.ts, ttl=EXCLUDED.ttl"
        );
    }

    #[test]
    fn test_build_upsert_sql_placeholder_numbering() {
        let sql = PostgresSink::build_upsert_sql("t", 2);
        assert!(sql.contains("$1,$2,$3,$4"));
        assert!(sql.contains("$5,$6,$7,$8"));
        assert!(!sql.contains("$9"));
    }

    #[test]
    fn test_batch_chunking_exact() {
        let entries: Vec<SinkEntry> = (0..BATCH_WRITE_MAX)
            .map(|i| SinkEntry {
                key: format!("k{}", i).into_bytes(),
                value: b"v".to_vec(),
                expires_at: None,
            })
            .collect();
        let refs: Vec<&SinkEntry> = entries.iter().collect();
        let chunks: Vec<_> = refs.chunks(BATCH_WRITE_MAX).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), BATCH_WRITE_MAX);
    }

    #[test]
    fn test_batch_chunking_overflow_by_one() {
        let entries: Vec<SinkEntry> = (0..BATCH_WRITE_MAX + 1)
            .map(|i| SinkEntry {
                key: format!("k{}", i).into_bytes(),
                value: b"v".to_vec(),
                expires_at: None,
            })
            .collect();
        let refs: Vec<&SinkEntry> = entries.iter().collect();
        let chunks: Vec<_> = refs.chunks(BATCH_WRITE_MAX).collect();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), BATCH_WRITE_MAX);
        assert_eq!(chunks[1].len(), 1);
    }

    #[test]
    fn test_batch_chunking_double() {
        let entries: Vec<SinkEntry> = (0..BATCH_WRITE_MAX * 2)
            .map(|i| SinkEntry {
                key: format!("k{}", i).into_bytes(),
                value: b"v".to_vec(),
                expires_at: None,
            })
            .collect();
        let refs: Vec<&SinkEntry> = entries.iter().collect();
        let chunks: Vec<_> = refs.chunks(BATCH_WRITE_MAX).collect();
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn test_ttl_expiry_logic_expired() {
        let ttl: Option<i64> = Some(1000); // way in the past
        let expires_at = ttl.map(|t| t as u64);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let is_expired = if let Some(exp) = expires_at {
            now >= exp
        } else {
            false
        };
        assert!(is_expired);
    }

    #[test]
    fn test_ttl_expiry_logic_not_expired() {
        let ttl: Option<i64> = Some(9_999_999_999);
        let expires_at = ttl.map(|t| t as u64);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let is_expired = if let Some(exp) = expires_at {
            now >= exp
        } else {
            false
        };
        assert!(!is_expired);
    }

    #[test]
    fn test_ttl_expiry_logic_no_ttl() {
        let ttl: Option<i64> = None;
        let expires_at = ttl.map(|t| t as u64);
        let is_expired = if let Some(exp) = expires_at {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            now >= exp
        } else {
            false
        };
        assert!(!is_expired);
    }

    #[test]
    fn test_batch_write_max_constant() {
        assert_eq!(BATCH_WRITE_MAX * 4, 2000);
    }

    #[test]
    fn test_build_upsert_sql_on_conflict_clause() {
        let sql = PostgresSink::build_upsert_sql("t", 1);
        assert!(sql.contains("ON CONFLICT (pk) DO UPDATE SET"));
        assert!(sql.contains("val=EXCLUDED.val"));
        assert!(sql.contains("ts=EXCLUDED.ts"));
        assert!(sql.contains("ttl=EXCLUDED.ttl"));
    }

    #[test]
    fn test_build_upsert_sql_table_name_preserved() {
        let sql = PostgresSink::build_upsert_sql("my_schema.my_table", 1);
        assert!(sql.starts_with("INSERT INTO my_schema.my_table "));
    }

    #[test]
    fn test_expires_at_i64_u64_roundtrip() {
        let original: u64 = 1_700_000_000;
        let as_i64 = original as i64;
        let back_to_u64 = as_i64 as u64;
        assert_eq!(original, back_to_u64);
    }

    #[test]
    fn test_invalid_connection_string() {
        let result = PostgresSink::new(
            "not a valid connection string!!!".to_string(),
            "test_table".to_string(),
            4,
            8,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            SinkError::Fatal(msg) => {
                assert!(msg.contains("invalid connection string"), "got: {}", msg);
            }
            SinkError::Retriable(msg) => {
                panic!("expected Fatal, got Retriable: {}", msg);
            }
        }
    }
}
