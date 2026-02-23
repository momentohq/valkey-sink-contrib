/// Sink-specific metrics: atomic counters and latency tracking, exposed via SINK.INFO.

use std::sync::atomic::AtomicU64;
use std::sync::OnceLock;

// Re-export generic primitives from valkey-common.
pub use valkey_common::metrics::{inc, inc_by, load, set, LatencyTracker};

/// Global metrics singleton.
static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Atomic counters for observability.
pub struct Metrics {
    pub writes_coalesced: AtomicU64,
    pub writes_flushed: AtomicU64,
    pub write_failures: AtomicU64,
    pub read_throughs: AtomicU64,
    pub read_through_hits: AtomicU64,
    pub read_through_misses: AtomicU64,
    pub coalesce_map_size: AtomicU64,
    pub circuit_breaker_trips: AtomicU64,

    // Per-sink counters
    pub s3_writes: AtomicU64,
    pub s3_write_failures: AtomicU64,
    pub dynamo_writes: AtomicU64,
    pub dynamo_write_failures: AtomicU64,
    pub dynamo_read_failures: AtomicU64,
    pub s3_read_failures: AtomicU64,

    // Latency trackers (writes)
    pub s3_latency: LatencyTracker,
    pub dynamo_latency: LatencyTracker,
    pub flush_latency: LatencyTracker,
    // Latency trackers (reads)
    pub read_through_latency: LatencyTracker,
    pub s3_read_latency: LatencyTracker,
    pub dynamo_read_latency: LatencyTracker,
}

impl Metrics {
    fn new() -> Self {
        Self {
            writes_coalesced: AtomicU64::new(0),
            writes_flushed: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            read_throughs: AtomicU64::new(0),
            read_through_hits: AtomicU64::new(0),
            read_through_misses: AtomicU64::new(0),
            coalesce_map_size: AtomicU64::new(0),
            circuit_breaker_trips: AtomicU64::new(0),
            s3_writes: AtomicU64::new(0),
            s3_write_failures: AtomicU64::new(0),
            dynamo_writes: AtomicU64::new(0),
            dynamo_write_failures: AtomicU64::new(0),
            dynamo_read_failures: AtomicU64::new(0),
            s3_read_failures: AtomicU64::new(0),
            s3_latency: LatencyTracker::new(),
            dynamo_latency: LatencyTracker::new(),
            flush_latency: LatencyTracker::new(),
            read_through_latency: LatencyTracker::new(),
            s3_read_latency: LatencyTracker::new(),
            dynamo_read_latency: LatencyTracker::new(),
        }
    }
}

/// Get or initialize the global metrics.
pub fn get() -> &'static Metrics {
    METRICS.get_or_init(Metrics::new)
}

/// Format all metrics as a string suitable for Valkey INFO output.
///
/// Note: Because `get()` uses `OnceLock`, metrics are a process-wide singleton.
/// Tests that call `info_string()` or `get()` share state. Assertions must
/// account for values left by other tests running in the same process.
pub fn info_string() -> String {
    let m = get();
    let s3_lat = m.s3_latency.snapshot();
    let dynamo_lat = m.dynamo_latency.snapshot();
    let flush_lat = m.flush_latency.snapshot();
    let rt_lat = m.read_through_latency.snapshot();
    let s3_read_lat = m.s3_read_latency.snapshot();
    let dynamo_read_lat = m.dynamo_read_latency.snapshot();

    format!(
        concat!(
            "# valkey-sink\r\n",
            "sink_writes_coalesced:{}\r\n",
            "sink_writes_flushed:{}\r\n",
            "sink_write_failures:{}\r\n",
            "sink_read_throughs:{}\r\n",
            "sink_read_through_hits:{}\r\n",
            "sink_read_through_misses:{}\r\n",
            "sink_coalesce_map_size:{}\r\n",
            "sink_circuit_breaker_trips:{}\r\n",
            "# per-sink writes\r\n",
            "sink_s3_writes:{}\r\n",
            "sink_s3_write_failures:{}\r\n",
            "sink_dynamo_writes:{}\r\n",
            "sink_dynamo_write_failures:{}\r\n",
            "sink_dynamo_read_failures:{}\r\n",
            "sink_s3_read_failures:{}\r\n",
            "# write latency\r\n",
            "sink_s3_write_latency:{}\r\n",
            "sink_dynamo_write_latency:{}\r\n",
            "sink_flush_latency:{}\r\n",
            "# read latency\r\n",
            "sink_read_through_latency:{}\r\n",
            "sink_s3_read_latency:{}\r\n",
            "sink_dynamo_read_latency:{}\r\n",
        ),
        load(&m.writes_coalesced),
        load(&m.writes_flushed),
        load(&m.write_failures),
        load(&m.read_throughs),
        load(&m.read_through_hits),
        load(&m.read_through_misses),
        load(&m.coalesce_map_size),
        load(&m.circuit_breaker_trips),
        load(&m.s3_writes),
        load(&m.s3_write_failures),
        load(&m.dynamo_writes),
        load(&m.dynamo_write_failures),
        load(&m.dynamo_read_failures),
        load(&m.s3_read_failures),
        s3_lat.format(),
        dynamo_lat.format(),
        flush_lat.format(),
        rt_lat.format(),
        s3_read_lat.format(),
        dynamo_read_lat.format(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_returns_singleton() {
        let m1 = get();
        let m2 = get();
        // Both calls return the same &'static reference.
        assert!(std::ptr::eq(m1, m2));
    }

    #[test]
    fn test_info_string_contains_expected_fields() {
        let info = info_string();
        // Verify key metric names are present in the output.
        assert!(info.contains("sink_writes_coalesced:"));
        assert!(info.contains("sink_writes_flushed:"));
        assert!(info.contains("sink_s3_writes:"));
        assert!(info.contains("sink_dynamo_writes:"));
        assert!(info.contains("sink_read_throughs:"));
        assert!(info.contains("sink_read_through_hits:"));
        assert!(info.contains("sink_read_through_misses:"));
        assert!(info.contains("sink_s3_write_latency:"));
        assert!(info.contains("sink_dynamo_write_latency:"));
        assert!(info.contains("sink_flush_latency:"));
    }

    #[test]
    fn test_info_string_fresh_metrics_zero() {
        // Because metrics are a process-wide singleton, other tests may have
        // incremented counters. We verify the format is correct and that the
        // section header is present; zero-value checks use the structure.
        let info = info_string();
        assert!(info.starts_with("# valkey-sink\r\n"));
        // The latency fields for an unused tracker should show "count=0".
        // flush_latency is unlikely to be touched by other unit tests.
        assert!(info.contains("sink_flush_latency:count=0"));
    }
}
