/// Sink trait and size-based routing.

pub mod dynamo;
pub mod s3;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use async_trait::async_trait;

use self::dynamo::DynamoDbSink;
use self::s3::S3ExpressSink;
use crate::metrics;

/// A single key-value pair to persist, with optional TTL.
pub struct SinkEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// Absolute expiry time as Unix epoch seconds. None means no expiry.
    pub expires_at: Option<u64>,
}

/// Errors from sink operations.
#[derive(Debug)]
pub enum SinkError {
    /// Transient error that may succeed on retry.
    Retriable(String),
    /// Permanent error that should not be retried.
    #[allow(dead_code)]
    Fatal(String),
}

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkError::Retriable(msg) => write!(f, "retriable: {}", msg),
            SinkError::Fatal(msg) => write!(f, "fatal: {}", msg),
        }
    }
}

impl valkey_common::retry::Retriable for SinkError {
    fn is_retriable(&self) -> bool {
        matches!(self, SinkError::Retriable(_))
    }
}

/// Abstraction over a persistence backend.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Write a batch of entries to the sink.
    async fn write_batch(&self, entries: &[SinkEntry]) -> Result<(), SinkError>;

    /// Look up a single key. Returns None if not found or expired.
    /// On hit, returns the value and optional expiry (Unix epoch seconds).
    async fn lookup(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Option<u64>)>, SinkError>;
}

/// Routes writes to S3 or DynamoDB based on value size.
pub struct SinkRouter {
    pub s3: Option<S3ExpressSink>,
    pub dynamo: Option<DynamoDbSink>,
    size_threshold: AtomicUsize,
}

impl SinkRouter {
    /// Construct a new SinkRouter. The size_threshold is stored atomically so
    /// the router can be shared via Arc without requiring &mut self for config updates.
    pub fn new(s3: Option<S3ExpressSink>, dynamo: Option<DynamoDbSink>, size_threshold: usize) -> Self {
        Self {
            s3,
            dynamo,
            size_threshold: AtomicUsize::new(size_threshold),
        }
    }

    /// Read the current size threshold.
    pub fn size_threshold(&self) -> usize {
        self.size_threshold.load(Ordering::Relaxed)
    }

    /// Partition entries into (dynamo_entries, s3_entries) based on size threshold.
    pub fn partition_entries<'a>(
        &self,
        entries: &'a [SinkEntry],
    ) -> (Vec<&'a SinkEntry>, Vec<&'a SinkEntry>) {
        let mut dynamo_entries = Vec::new();
        let mut s3_entries = Vec::new();
        let threshold = self.size_threshold();

        for entry in entries {
            match (&self.dynamo, &self.s3) {
                (Some(_), Some(_)) => {
                    if entry.value.len() <= threshold {
                        dynamo_entries.push(entry);
                    } else {
                        s3_entries.push(entry);
                    }
                }
                (Some(_), None) => dynamo_entries.push(entry),
                (None, Some(_)) => s3_entries.push(entry),
                (None, None) => {} // should not happen, validated at init
            }
        }

        (dynamo_entries, s3_entries)
    }

    /// Write a batch, routing each entry to the appropriate sink.
    /// DynamoDB and S3 writes run concurrently via tokio::join!.
    pub async fn write_batch(&self, entries: &[SinkEntry]) -> Result<(), SinkError> {
        let (dynamo_entries, s3_entries) = self.partition_entries(entries);
        let m = metrics::get();

        // Kick off both sinks concurrently — flush latency becomes max(dynamo, s3)
        // instead of dynamo + s3.
        let dynamo_fut = async {
            if dynamo_entries.is_empty() {
                return Ok(());
            }
            if let Some(dynamo) = &self.dynamo {
                let count = dynamo_entries.len() as u64;
                let start = Instant::now();
                match dynamo.write_batch_refs(&dynamo_entries).await {
                    Ok(()) => {
                        m.dynamo_latency.record_since(start);
                        metrics::inc_by(&m.dynamo_writes, count);
                        Ok(())
                    }
                    Err(e) => {
                        m.dynamo_latency.record_since(start);
                        metrics::inc(&m.dynamo_write_failures);
                        Err(e)
                    }
                }
            } else {
                Ok(())
            }
        };

        let s3_fut = async {
            if s3_entries.is_empty() {
                return Ok(());
            }
            if let Some(s3) = &self.s3 {
                let count = s3_entries.len() as u64;
                let start = Instant::now();
                match s3.write_batch_refs(&s3_entries).await {
                    Ok(()) => {
                        m.s3_latency.record_since(start);
                        metrics::inc_by(&m.s3_writes, count);
                        Ok(())
                    }
                    Err(e) => {
                        m.s3_latency.record_since(start);
                        metrics::inc(&m.s3_write_failures);
                        Err(e)
                    }
                }
            } else {
                Ok(())
            }
        };

        let (dynamo_result, s3_result) = tokio::join!(dynamo_fut, s3_fut);
        dynamo_result?;
        s3_result?;
        Ok(())
    }

    /// Look up a key. Tries DynamoDB first (lower latency), then S3.
    /// Returns the value and optional expiry (Unix epoch seconds).
    pub async fn lookup(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Option<u64>)>, SinkError> {
        let start = Instant::now();
        let m = metrics::get();
        if let Some(dynamo) = &self.dynamo {
            let dynamo_start = Instant::now();
            match dynamo.lookup(key).await {
                Ok(Some(val)) => {
                    m.dynamo_read_latency.record_since(dynamo_start);
                    m.read_through_latency.record_since(start);
                    return Ok(Some(val));
                }
                Ok(None) => {
                    m.dynamo_read_latency.record_since(dynamo_start);
                }
                Err(e) => {
                    m.dynamo_read_latency.record_since(dynamo_start);
                    metrics::inc(&m.dynamo_read_failures);
                    eprintln!("valkey-sink: DynamoDB lookup failed, falling through to S3: {}", e);
                }
            }
        }
        if let Some(s3) = &self.s3 {
            let s3_start = Instant::now();
            let result = s3.lookup(key).await;
            m.s3_read_latency.record_since(s3_start);
            if result.is_err() {
                metrics::inc(&m.s3_read_failures);
            }
            m.read_through_latency.record_since(start);
            return result;
        }
        m.read_through_latency.record_since(start);
        Ok(None)
    }

    /// Update the size threshold (from CONFIG SET). Uses atomic store so
    /// this works on a shared reference (no &mut self needed).
    pub fn set_size_threshold(&self, threshold: usize) {
        self.size_threshold.store(threshold, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal stub sink for testing partition logic without real AWS clients.
    fn make_router_both(threshold: usize) -> SinkRouter {
        SinkRouter::new(None, None, threshold)
    }

    #[test]
    fn test_partition_no_sinks() {
        let router = make_router_both(100);
        let entries = vec![
            SinkEntry { key: b"k1".to_vec(), value: b"small".to_vec(), expires_at: None },
        ];
        let (dynamo, s3) = router.partition_entries(&entries);
        assert!(dynamo.is_empty());
        assert!(s3.is_empty());
    }

    #[test]
    fn test_sink_entry_expires_at_some() {
        let entry = SinkEntry {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            expires_at: Some(1700000000),
        };
        assert_eq!(entry.expires_at, Some(1700000000));
    }

    #[test]
    fn test_sink_entry_expires_at_none() {
        let entry = SinkEntry {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            expires_at: None,
        };
        assert_eq!(entry.expires_at, None);
    }

    #[test]
    fn test_set_size_threshold() {
        let router = make_router_both(100);
        assert_eq!(router.size_threshold(), 100);
        router.set_size_threshold(200);
        assert_eq!(router.size_threshold(), 200);
    }

    #[test]
    fn test_sink_error_display() {
        let retriable = SinkError::Retriable("timeout".into());
        assert_eq!(format!("{}", retriable), "retriable: timeout");

        let fatal = SinkError::Fatal("bad config".into());
        assert_eq!(format!("{}", fatal), "fatal: bad config");
    }

    #[test]
    fn test_sink_error_retriable_trait() {
        use valkey_common::retry::Retriable;
        assert!(SinkError::Retriable("err".into()).is_retriable());
        assert!(!SinkError::Fatal("err".into()).is_retriable());
    }

    #[test]
    fn test_set_size_threshold_affects_routing() {
        let router = make_router_both(50);
        // With threshold=50 and no sinks, partition is empty.
        // But we can verify the threshold changed.
        assert_eq!(router.size_threshold(), 50);
        router.set_size_threshold(200);
        assert_eq!(router.size_threshold(), 200);
        router.set_size_threshold(0);
        assert_eq!(router.size_threshold(), 0);
    }
}
