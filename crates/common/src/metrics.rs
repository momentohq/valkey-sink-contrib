/// Atomic counter helpers and latency tracking primitives.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// Increment a counter by 1.
pub fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Increment a counter by a given amount.
pub fn inc_by(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

/// Set a gauge to a specific value.
pub fn set(gauge: &AtomicU64, val: u64) {
    gauge.store(val, Ordering::Relaxed);
}

/// Read a counter's current value.
pub fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

/// Tracks latency samples: count, min, max, sum, and a ring buffer for p50/p99.
pub struct LatencyTracker {
    inner: Mutex<LatencyInner>,
}

const RING_SIZE: usize = 1024;

struct LatencyInner {
    count: u64,
    sum_us: u64,
    min_us: u64,
    max_us: u64,
    /// Ring buffer of recent samples (microseconds) for percentile estimation.
    ring: Vec<u64>,
    ring_pos: usize,
}

impl LatencyTracker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(LatencyInner {
                count: 0,
                sum_us: 0,
                min_us: u64::MAX,
                max_us: 0,
                ring: Vec::with_capacity(RING_SIZE),
                ring_pos: 0,
            }),
        }
    }

    /// Record a latency sample in microseconds.
    pub fn record_us(&self, us: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.count += 1;
        inner.sum_us += us;
        if us < inner.min_us {
            inner.min_us = us;
        }
        if us > inner.max_us {
            inner.max_us = us;
        }
        if inner.ring.len() < RING_SIZE {
            inner.ring.push(us);
        } else {
            let pos = inner.ring_pos;
            inner.ring[pos] = us;
        }
        inner.ring_pos = (inner.ring_pos + 1) % RING_SIZE;
    }

    /// Record a duration since the given instant.
    pub fn record_since(&self, start: Instant) {
        let us = start.elapsed().as_micros() as u64;
        self.record_us(us);
    }

    /// Get a snapshot: (count, min_us, max_us, avg_us, p50_us, p99_us).
    pub fn snapshot(&self) -> LatencySnapshot {
        let inner = self.inner.lock().unwrap();
        if inner.count == 0 {
            return LatencySnapshot {
                count: 0,
                min_us: 0,
                max_us: 0,
                avg_us: 0,
                p50_us: 0,
                p99_us: 0,
            };
        }
        let avg_us = inner.sum_us / inner.count;

        let (p50_us, p99_us) = if inner.ring.is_empty() {
            (0, 0)
        } else {
            let mut sorted = inner.ring.clone();
            sorted.sort_unstable();
            let len = sorted.len();
            let p50 = sorted[len / 2];
            let p99 = sorted[(len * 99 / 100).min(len - 1)];
            (p50, p99)
        };

        LatencySnapshot {
            count: inner.count,
            min_us: if inner.min_us == u64::MAX { 0 } else { inner.min_us },
            max_us: inner.max_us,
            avg_us,
            p50_us,
            p99_us,
        }
    }
}

pub struct LatencySnapshot {
    pub count: u64,
    pub min_us: u64,
    pub max_us: u64,
    pub avg_us: u64,
    pub p50_us: u64,
    pub p99_us: u64,
}

impl LatencySnapshot {
    /// Format as "count=N min=Xms avg=Xms p50=Xms p99=Xms max=Xms"
    pub fn format(&self) -> String {
        if self.count == 0 {
            return "count=0".to_string();
        }
        format!(
            "count={} min={} avg={} p50={} p99={} max={}",
            self.count,
            fmt_us(self.min_us),
            fmt_us(self.avg_us),
            fmt_us(self.p50_us),
            fmt_us(self.p99_us),
            fmt_us(self.max_us),
        )
    }
}

/// Format microseconds as a human-readable string.
pub fn fmt_us(us: u64) -> String {
    if us < 1_000 {
        format!("{}us", us)
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fmt_us_microseconds() {
        assert_eq!(fmt_us(0), "0us");
        assert_eq!(fmt_us(500), "500us");
        assert_eq!(fmt_us(999), "999us");
    }

    #[test]
    fn test_fmt_us_milliseconds() {
        assert_eq!(fmt_us(1_000), "1.0ms");
        assert_eq!(fmt_us(5_500), "5.5ms");
        assert_eq!(fmt_us(999_999), "1000.0ms");
    }

    #[test]
    fn test_fmt_us_seconds() {
        assert_eq!(fmt_us(1_000_000), "1.00s");
        assert_eq!(fmt_us(2_500_000), "2.50s");
    }

    #[test]
    fn test_latency_tracker_empty() {
        let tracker = LatencyTracker::new();
        let snap = tracker.snapshot();
        assert_eq!(snap.count, 0);
        assert_eq!(snap.format(), "count=0");
    }

    #[test]
    fn test_latency_tracker_single_sample() {
        let tracker = LatencyTracker::new();
        tracker.record_us(5000); // 5ms
        let snap = tracker.snapshot();
        assert_eq!(snap.count, 1);
        assert_eq!(snap.min_us, 5000);
        assert_eq!(snap.max_us, 5000);
        assert_eq!(snap.avg_us, 5000);
    }

    #[test]
    fn test_latency_tracker_multiple_samples() {
        let tracker = LatencyTracker::new();
        tracker.record_us(1000);
        tracker.record_us(2000);
        tracker.record_us(3000);
        let snap = tracker.snapshot();
        assert_eq!(snap.count, 3);
        assert_eq!(snap.min_us, 1000);
        assert_eq!(snap.max_us, 3000);
        assert_eq!(snap.avg_us, 2000);
    }

    #[test]
    fn test_latency_tracker_ring_buffer_wraps() {
        let tracker = LatencyTracker::new();
        for i in 0..(RING_SIZE + 100) {
            tracker.record_us(i as u64);
        }
        let snap = tracker.snapshot();
        assert_eq!(snap.count, (RING_SIZE + 100) as u64);
        assert!(snap.p99_us > 0);
    }

    #[test]
    fn test_latency_tracker_record_since() {
        let tracker = LatencyTracker::new();
        let start = Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(1));
        tracker.record_since(start);
        let snap = tracker.snapshot();
        assert_eq!(snap.count, 1);
        assert!(snap.min_us > 0);
    }

    #[test]
    fn test_snapshot_format_includes_units() {
        let tracker = LatencyTracker::new();
        tracker.record_us(500);    // 500us
        tracker.record_us(5000);   // 5ms
        let formatted = tracker.snapshot().format();
        assert!(formatted.contains("count=2"));
        assert!(formatted.contains("min=500us"));
    }

    #[test]
    fn test_inc_by_multiple() {
        let counter = AtomicU64::new(0);
        inc_by(&counter, 10);
        assert_eq!(load(&counter), 10);
        inc_by(&counter, 25);
        assert_eq!(load(&counter), 35);
        inc_by(&counter, 0);
        assert_eq!(load(&counter), 35);
    }

    #[test]
    fn test_set_and_load() {
        let gauge = AtomicU64::new(0);
        set(&gauge, 42);
        assert_eq!(load(&gauge), 42);
        set(&gauge, 0);
        assert_eq!(load(&gauge), 0);
        set(&gauge, u64::MAX);
        assert_eq!(load(&gauge), u64::MAX);
    }

    #[test]
    fn test_fmt_us_boundary_values() {
        // 999us is the last value in the microsecond range
        assert_eq!(fmt_us(999), "999us");
        // 1000us is the first value in the millisecond range
        assert_eq!(fmt_us(1_000), "1.0ms");
        // 999999us is the last value in the millisecond range
        assert_eq!(fmt_us(999_999), "1000.0ms");
        // 1000000us is the first value in the second range
        assert_eq!(fmt_us(1_000_000), "1.00s");
    }
}
