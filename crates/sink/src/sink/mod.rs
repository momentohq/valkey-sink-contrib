/// Sink trait and size-based routing.

pub mod dynamo;
pub mod postgres;
pub mod s3;

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Instant;

use async_trait::async_trait;

use self::dynamo::DynamoDbSink;
use self::postgres::PostgresSink;
use self::s3::S3ExpressSink;
use crate::metrics;

/// Identifies which sink backend to route entries to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SinkId {
    S3 = 0,
    Dynamo = 1,
    Postgres = 2,
    None = 3,
}

impl SinkId {
    /// Parse a string to a SinkId.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "s3" => Some(Self::S3),
            "dynamo" | "dynamodb" => Some(Self::Dynamo),
            "postgres" | "pg" | "postgresql" => Some(Self::Postgres),
            "none" => Some(Self::None),
            _ => None,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::S3,
            1 => Self::Dynamo,
            2 => Self::Postgres,
            _ => Self::None,
        }
    }
}

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

/// Routes writes to configured sinks based on value size.
///
/// Supports N sinks (S3, DynamoDB, PostgreSQL) with configurable routing:
/// - **Single-sink mode**: small_sink_id == large_sink_id, ALL values go to one sink
/// - **Dual-sink mode**: small values (<=threshold) go to small_sink, large values to large_sink
pub struct SinkRouter {
    pub s3: Option<S3ExpressSink>,
    pub dynamo: Option<DynamoDbSink>,
    pub postgres: Option<PostgresSink>,
    small_sink_id: AtomicU8,
    large_sink_id: AtomicU8,
    size_threshold: AtomicUsize,
}

impl SinkRouter {
    /// Construct a new SinkRouter.
    ///
    /// The size_threshold, small_sink_id, and large_sink_id are stored atomically
    /// so the router can be shared via Arc without requiring &mut self for config updates.
    pub fn new(
        s3: Option<S3ExpressSink>,
        dynamo: Option<DynamoDbSink>,
        postgres: Option<PostgresSink>,
        small_sink_id: SinkId,
        large_sink_id: SinkId,
        size_threshold: usize,
    ) -> Self {
        Self {
            s3,
            dynamo,
            postgres,
            small_sink_id: AtomicU8::new(small_sink_id as u8),
            large_sink_id: AtomicU8::new(large_sink_id as u8),
            size_threshold: AtomicUsize::new(size_threshold),
        }
    }

    /// Read the current size threshold.
    pub fn size_threshold(&self) -> usize {
        self.size_threshold.load(Ordering::Relaxed)
    }

    /// Read the current small sink id.
    pub fn small_sink_id(&self) -> SinkId {
        SinkId::from_u8(self.small_sink_id.load(Ordering::Relaxed))
    }

    /// Read the current large sink id.
    pub fn large_sink_id(&self) -> SinkId {
        SinkId::from_u8(self.large_sink_id.load(Ordering::Relaxed))
    }

    /// Partition entries into (small_sink_entries, large_sink_entries) based on size threshold.
    ///
    /// In single-sink mode (small_sink_id == large_sink_id), ALL entries go to small_sink_entries.
    /// In dual-sink mode, entries are split by size threshold.
    pub fn partition_entries<'a>(
        &self,
        entries: &'a [SinkEntry],
    ) -> (Vec<&'a SinkEntry>, Vec<&'a SinkEntry>) {
        let small_id = self.small_sink_id();
        let large_id = self.large_sink_id();

        // Single-sink mode: all entries go to the one sink (returned as "small").
        if small_id == large_id {
            let all: Vec<&SinkEntry> = entries.iter().collect();
            return (all, Vec::new());
        }

        // Dual-sink mode: partition by size threshold.
        let threshold = self.size_threshold();
        let mut small_entries = Vec::new();
        let mut large_entries = Vec::new();

        for entry in entries {
            if entry.value.len() <= threshold {
                small_entries.push(entry);
            } else {
                large_entries.push(entry);
            }
        }

        (small_entries, large_entries)
    }

    /// Dispatch a batch of entry references to the sink identified by `sink_id`.
    /// Returns Ok(()) if the sink is not configured (entries are silently dropped).
    async fn dispatch_write(
        &self,
        sink_id: SinkId,
        entries: &[&SinkEntry],
    ) -> Result<(), SinkError> {
        if entries.is_empty() {
            return Ok(());
        }
        let m = metrics::get();
        let count = entries.len() as u64;
        match sink_id {
            SinkId::S3 => {
                if let Some(s3) = &self.s3 {
                    let start = Instant::now();
                    match s3.write_batch_refs(entries).await {
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
            }
            SinkId::Dynamo => {
                if let Some(dynamo) = &self.dynamo {
                    let start = Instant::now();
                    match dynamo.write_batch_refs(entries).await {
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
            }
            SinkId::Postgres => {
                if let Some(pg) = &self.postgres {
                    let start = Instant::now();
                    match pg.write_batch_refs(entries).await {
                        Ok(()) => {
                            m.pg_latency.record_since(start);
                            metrics::inc_by(&m.pg_writes, count);
                            Ok(())
                        }
                        Err(e) => {
                            m.pg_latency.record_since(start);
                            metrics::inc(&m.pg_write_failures);
                            Err(e)
                        }
                    }
                } else {
                    Ok(())
                }
            }
            SinkId::None => Ok(()),
        }
    }

    /// Dispatch a lookup to a single sink identified by `sink_id`.
    async fn dispatch_lookup(
        &self,
        sink_id: SinkId,
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, Option<u64>)>, SinkError> {
        let m = metrics::get();
        match sink_id {
            SinkId::Dynamo => {
                if let Some(dynamo) = &self.dynamo {
                    let start = Instant::now();
                    let result = dynamo.lookup(key).await;
                    m.dynamo_read_latency.record_since(start);
                    if result.is_err() {
                        metrics::inc(&m.dynamo_read_failures);
                    }
                    result
                } else {
                    Ok(None)
                }
            }
            SinkId::S3 => {
                if let Some(s3) = &self.s3 {
                    let start = Instant::now();
                    let result = s3.lookup(key).await;
                    m.s3_read_latency.record_since(start);
                    if result.is_err() {
                        metrics::inc(&m.s3_read_failures);
                    }
                    result
                } else {
                    Ok(None)
                }
            }
            SinkId::Postgres => {
                if let Some(pg) = &self.postgres {
                    let start = Instant::now();
                    let result = pg.lookup(key).await;
                    m.pg_read_latency.record_since(start);
                    if result.is_err() {
                        metrics::inc(&m.pg_read_failures);
                    }
                    result
                } else {
                    Ok(None)
                }
            }
            SinkId::None => Ok(None),
        }
    }

    /// Write a batch, routing each entry to the appropriate sink.
    /// Up to two sinks run concurrently via tokio::join!.
    pub async fn write_batch(&self, entries: &[SinkEntry]) -> Result<(), SinkError> {
        let (small_entries, large_entries) = self.partition_entries(entries);
        let small_id = self.small_sink_id();
        let large_id = self.large_sink_id();

        // If both partitions route to the same sink, merge them.
        if small_id == large_id {
            return self.dispatch_write(small_id, &small_entries).await;
        }

        // Two different sinks: run concurrently.
        let small_fut = self.dispatch_write(small_id, &small_entries);
        let large_fut = self.dispatch_write(large_id, &large_entries);

        let (small_result, large_result) = tokio::join!(small_fut, large_fut);
        small_result?;
        large_result?;
        Ok(())
    }

    /// Look up a key. Tries small_sink first (likely lower latency for smaller values),
    /// then falls through to large_sink.
    /// Returns the value and optional expiry (Unix epoch seconds).
    pub async fn lookup(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Option<u64>)>, SinkError> {
        let start = Instant::now();
        let m = metrics::get();
        let small_id = self.small_sink_id();
        let large_id = self.large_sink_id();

        // Try small sink first.
        match self.dispatch_lookup(small_id, key).await {
            Ok(Some(val)) => {
                m.read_through_latency.record_since(start);
                return Ok(Some(val));
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!(
                    "valkey-sink: {:?} lookup failed, falling through to {:?}: {}",
                    small_id, large_id, e
                );
            }
        }

        // If small and large are the same sink, no point trying again.
        if small_id == large_id {
            m.read_through_latency.record_since(start);
            return Ok(None);
        }

        // Try large sink.
        let result = self.dispatch_lookup(large_id, key).await;
        m.read_through_latency.record_since(start);
        result
    }

    /// Update the size threshold (from CONFIG SET). Uses atomic store so
    /// this works on a shared reference (no &mut self needed).
    pub fn set_size_threshold(&self, threshold: usize) {
        self.size_threshold.store(threshold, Ordering::Relaxed);
    }

    /// Update which sink handles small values (from CONFIG SET).
    pub fn set_small_sink(&self, id: SinkId) {
        self.small_sink_id.store(id as u8, Ordering::Relaxed);
    }

    /// Update which sink handles large values (from CONFIG SET).
    pub fn set_large_sink(&self, id: SinkId) {
        self.large_sink_id.store(id as u8, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a router with Dynamo(small) + S3(large) routing and no real backends.
    fn make_router_dual(threshold: usize) -> SinkRouter {
        SinkRouter::new(None, None, None, SinkId::Dynamo, SinkId::S3, threshold)
    }

    /// Build a single-sink router where all entries go to one sink.
    fn make_router_single(id: SinkId) -> SinkRouter {
        SinkRouter::new(None, None, None, id, id, 100)
    }

    #[test]
    fn test_partition_dual_mode() {
        let router = make_router_dual(100);
        let entries = vec![
            SinkEntry { key: b"k1".to_vec(), value: vec![0; 50], expires_at: None },
            SinkEntry { key: b"k2".to_vec(), value: vec![0; 200], expires_at: None },
        ];
        let (small, large) = router.partition_entries(&entries);
        assert_eq!(small.len(), 1); // 50 bytes <= 100 threshold
        assert_eq!(large.len(), 1); // 200 bytes > 100 threshold
    }

    #[test]
    fn test_partition_single_mode_all_to_small() {
        let router = make_router_single(SinkId::Postgres);
        let entries = vec![
            SinkEntry { key: b"k1".to_vec(), value: vec![0; 50], expires_at: None },
            SinkEntry { key: b"k2".to_vec(), value: vec![0; 200], expires_at: None },
            SinkEntry { key: b"k3".to_vec(), value: vec![0; 999], expires_at: None },
        ];
        let (small, large) = router.partition_entries(&entries);
        // In single-sink mode, ALL entries go to the "small" bucket.
        assert_eq!(small.len(), 3);
        assert!(large.is_empty());
    }

    #[test]
    fn test_partition_boundary_value() {
        let router = make_router_dual(100);
        let entries = vec![
            SinkEntry { key: b"exact".to_vec(), value: vec![0; 100], expires_at: None },
        ];
        let (small, large) = router.partition_entries(&entries);
        // Value of exactly the threshold goes to small sink (<=).
        assert_eq!(small.len(), 1);
        assert!(large.is_empty());
    }

    #[test]
    fn test_partition_boundary_plus_one() {
        let router = make_router_dual(100);
        let entries = vec![
            SinkEntry { key: b"over".to_vec(), value: vec![0; 101], expires_at: None },
        ];
        let (small, large) = router.partition_entries(&entries);
        assert!(small.is_empty());
        assert_eq!(large.len(), 1);
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
        let router = make_router_dual(100);
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
        let router = make_router_dual(50);
        assert_eq!(router.size_threshold(), 50);
        router.set_size_threshold(200);
        assert_eq!(router.size_threshold(), 200);
        router.set_size_threshold(0);
        assert_eq!(router.size_threshold(), 0);
    }

    #[test]
    fn test_set_small_sink_and_large_sink() {
        let router = make_router_dual(100);
        assert_eq!(router.small_sink_id(), SinkId::Dynamo);
        assert_eq!(router.large_sink_id(), SinkId::S3);

        router.set_small_sink(SinkId::Postgres);
        assert_eq!(router.small_sink_id(), SinkId::Postgres);

        router.set_large_sink(SinkId::Postgres);
        assert_eq!(router.large_sink_id(), SinkId::Postgres);
    }

    #[test]
    fn test_sink_id_from_str() {
        assert_eq!(SinkId::from_str("s3"), Some(SinkId::S3));
        assert_eq!(SinkId::from_str("S3"), Some(SinkId::S3));
        assert_eq!(SinkId::from_str("dynamo"), Some(SinkId::Dynamo));
        assert_eq!(SinkId::from_str("dynamodb"), Some(SinkId::Dynamo));
        assert_eq!(SinkId::from_str("postgres"), Some(SinkId::Postgres));
        assert_eq!(SinkId::from_str("pg"), Some(SinkId::Postgres));
        assert_eq!(SinkId::from_str("postgresql"), Some(SinkId::Postgres));
        assert_eq!(SinkId::from_str("none"), Some(SinkId::None));
        assert_eq!(SinkId::from_str("unknown"), None);
    }

    #[test]
    fn test_sink_id_roundtrip() {
        for id in &[SinkId::S3, SinkId::Dynamo, SinkId::Postgres, SinkId::None] {
            assert_eq!(SinkId::from_u8(*id as u8), *id);
        }
    }

    #[test]
    fn test_dynamic_switch_from_dual_to_single() {
        let router = make_router_dual(100);
        let entries = vec![
            SinkEntry { key: b"k1".to_vec(), value: vec![0; 50], expires_at: None },
            SinkEntry { key: b"k2".to_vec(), value: vec![0; 200], expires_at: None },
        ];

        // Start dual: entries are split.
        let (small, large) = router.partition_entries(&entries);
        assert_eq!(small.len(), 1);
        assert_eq!(large.len(), 1);

        // Switch to single-sink mode at runtime.
        router.set_small_sink(SinkId::Postgres);
        router.set_large_sink(SinkId::Postgres);

        let (small, large) = router.partition_entries(&entries);
        assert_eq!(small.len(), 2); // all entries go to small (single-sink)
        assert!(large.is_empty());
    }
}
