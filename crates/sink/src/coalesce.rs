/// Write coalescing with debounce-window deduplication.
///
/// The coalesce map is the heart of the write-behind strategy. When a key is
/// SET multiple times within the debounce window (default 1s), only the final
/// value is written to the backend. This dramatically reduces backend I/O for
/// hot keys.
///
/// ## Example
///
/// If key "counter" is incremented 100 times in 500ms with a 1s debounce window:
/// - All 100 writes enter the map, each overwriting the previous value.
/// - After 1 second, `drain_ready()` returns a single entry with the final value.
/// - Result: 1 backend write instead of 100.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// A pending write: the latest value and when the key was first enqueued.
pub struct PendingWrite {
    pub value: Vec<u8>,
    /// Absolute expiry time as Unix epoch seconds. None means no expiry.
    pub expires_at: Option<u64>,
    /// When this key first entered the map. Preserved across upserts so the
    /// debounce deadline is anchored to the first write, not the last.
    pub first_seen: Instant,
}

/// Coalescing map that deduplicates writes within a debounce window.
pub struct CoalesceMap {
    pending: HashMap<Vec<u8>, PendingWrite>,
    /// How long a key must sit before it's eligible for flushing.
    pub debounce_window: Duration,
    /// Max entries before a forced flush (prevents unbounded memory growth).
    pub max_entries: usize,
}

impl CoalesceMap {
    pub fn new(debounce_window: Duration, max_entries: usize) -> Self {
        Self {
            pending: HashMap::new(),
            debounce_window,
            max_entries,
        }
    }

    /// Insert or update a key. If the key already exists, overwrite the value
    /// and expires_at but keep the original `first_seen` timestamp so the
    /// debounce window is anchored to the first write.
    pub fn upsert(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: Option<u64>) {
        self.pending
            .entry(key)
            .and_modify(|pw| {
                pw.value = value.clone();
                pw.expires_at = expires_at;
            })
            .or_insert(PendingWrite {
                value,
                expires_at,
                first_seen: Instant::now(),
            });
    }

    /// Drain all entries whose debounce window has elapsed.
    /// Returns (key, value, expires_at) tuples ready for flushing.
    pub fn drain_ready(&mut self) -> Vec<(Vec<u8>, Vec<u8>, Option<u64>)> {
        let now = Instant::now();
        let window = self.debounce_window;

        // Use retain to avoid cloning keys: extract ready entries in-place.
        let mut ready = Vec::new();
        self.pending.retain(|k, pw| {
            if now.duration_since(pw.first_seen) >= window {
                // Take value out via swap with empty vec (avoids clone).
                let value = std::mem::take(&mut pw.value);
                ready.push((k.clone(), value, pw.expires_at));
                false // remove from map
            } else {
                true // keep in map
            }
        });
        ready
    }

    /// Drain all entries regardless of debounce window (used on shutdown
    /// and when the map exceeds capacity).
    pub fn drain_all(&mut self) -> Vec<(Vec<u8>, Vec<u8>, Option<u64>)> {
        self.pending
            .drain()
            .map(|(k, pw)| (k, pw.value, pw.expires_at))
            .collect()
    }

    /// Re-enqueue entries that failed to flush.
    ///
    /// Only inserts keys that aren't already pending — if a newer write
    /// arrived while we were flushing, it takes priority.
    pub fn re_enqueue(&mut self, entries: Vec<(Vec<u8>, Vec<u8>, Option<u64>)>) {
        for (key, value, expires_at) in entries {
            self.pending.entry(key).or_insert(PendingWrite {
                value,
                expires_at,
                first_seen: Instant::now(),
            });
        }
    }

    /// Number of pending entries.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Whether the map has reached its capacity limit.
    pub fn is_over_capacity(&self) -> bool {
        self.pending.len() >= self.max_entries
    }

    /// Rough estimate of memory usage in bytes.
    #[allow(dead_code)]
    pub fn memory_usage_bytes(&self) -> usize {
        self.pending.iter().fold(0, |acc, (k, pw)| {
            // Vec overhead (24) + data for key, Vec overhead (24) + data for value,
            // Instant (8), HashMap bucket overhead (~64)
            acc + k.len() + 24 + pw.value.len() + 24 + 8 + 64
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upsert_new_key() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 100);
        map.upsert(b"key1".to_vec(), b"val1".to_vec(), None);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_upsert_overwrites_value() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 100);
        map.upsert(b"key1".to_vec(), b"val1".to_vec(), None);
        map.upsert(b"key1".to_vec(), b"val2".to_vec(), None);
        assert_eq!(map.len(), 1); // still 1 entry

        let drained = map.drain_all();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].1, b"val2"); // latest value wins
    }

    #[test]
    fn test_drain_ready_respects_window() {
        let mut map = CoalesceMap::new(Duration::from_millis(50), 100);
        map.upsert(b"key1".to_vec(), b"val1".to_vec(), None);

        // Immediately: nothing ready
        let ready = map.drain_ready();
        assert!(ready.is_empty());
        assert_eq!(map.len(), 1);

        // After waiting: entry is ready
        std::thread::sleep(Duration::from_millis(60));
        let ready = map.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_drain_all() {
        let mut map = CoalesceMap::new(Duration::from_secs(999), 100);
        map.upsert(b"k1".to_vec(), b"v1".to_vec(), None);
        map.upsert(b"k2".to_vec(), b"v2".to_vec(), None);

        let all = map.drain_all();
        assert_eq!(all.len(), 2);
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_re_enqueue_does_not_overwrite_newer() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 100);

        // Simulate: key was flushed but a new write arrived during flush.
        map.upsert(b"key1".to_vec(), b"new_value".to_vec(), None);

        // Re-enqueue the old failed value — should NOT overwrite the new one.
        map.re_enqueue(vec![(b"key1".to_vec(), b"old_value".to_vec(), None)]);

        let all = map.drain_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].1, b"new_value"); // new value preserved
    }

    #[test]
    fn test_re_enqueue_inserts_missing() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 100);

        map.re_enqueue(vec![(b"key1".to_vec(), b"val1".to_vec(), None)]);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_is_over_capacity() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 2);
        assert!(!map.is_over_capacity());

        map.upsert(b"k1".to_vec(), b"v1".to_vec(), None);
        assert!(!map.is_over_capacity());

        map.upsert(b"k2".to_vec(), b"v2".to_vec(), None);
        assert!(map.is_over_capacity());
    }

    #[test]
    fn test_many_upserts_same_key_only_one_entry() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 1000);
        for i in 0..100 {
            map.upsert(b"hotkey".to_vec(), format!("val{}", i).into_bytes(), None);
        }
        assert_eq!(map.len(), 1);
        let all = map.drain_all();
        assert_eq!(all[0].1, b"val99"); // last value
    }

    #[test]
    fn test_expires_at_preserved_through_upsert_drain() {
        let mut map = CoalesceMap::new(Duration::from_millis(10), 100);
        let exp = Some(1700000000u64);
        map.upsert(b"k".to_vec(), b"v1".to_vec(), exp);
        // Overwrite value but expires_at should update too
        let exp2 = Some(1800000000u64);
        map.upsert(b"k".to_vec(), b"v2".to_vec(), exp2);

        std::thread::sleep(Duration::from_millis(20));
        let ready = map.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].1, b"v2");
        assert_eq!(ready[0].2, exp2);
    }

    #[test]
    fn test_memory_usage_estimate() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 100);
        map.upsert(b"key".to_vec(), b"value".to_vec(), None);
        assert!(map.memory_usage_bytes() > 0);
    }

    #[test]
    fn test_expires_at_none_through_drain_all() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 100);
        map.upsert(b"k".to_vec(), b"v".to_vec(), None);
        let all = map.drain_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].2, None);
    }

    #[test]
    fn test_expires_at_preserved_through_re_enqueue() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 100);
        let exp = Some(1700000000u64);
        map.re_enqueue(vec![(b"k".to_vec(), b"v".to_vec(), exp)]);
        let all = map.drain_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].2, exp);
    }

    #[test]
    fn test_upsert_updates_expires_at() {
        let mut map = CoalesceMap::new(Duration::from_millis(10), 100);
        map.upsert(b"k".to_vec(), b"v1".to_vec(), Some(1000));
        map.upsert(b"k".to_vec(), b"v2".to_vec(), None);
        std::thread::sleep(Duration::from_millis(20));
        let ready = map.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].1, b"v2");
        assert_eq!(ready[0].2, None); // updated to None
    }

    #[test]
    fn test_drain_ready_returns_expires_at() {
        let mut map = CoalesceMap::new(Duration::from_millis(10), 100);
        let exp = Some(9999999999u64);
        map.upsert(b"k".to_vec(), b"v".to_vec(), exp);
        std::thread::sleep(Duration::from_millis(20));
        let ready = map.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].2, exp);
    }

    #[test]
    fn test_re_enqueue_does_not_overwrite_newer_with_expires() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 100);
        map.upsert(b"k".to_vec(), b"new".to_vec(), Some(2000));
        map.re_enqueue(vec![(b"k".to_vec(), b"old".to_vec(), Some(1000))]);
        let all = map.drain_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].1, b"new");
        assert_eq!(all[0].2, Some(2000));
    }

    #[test]
    fn test_multiple_keys_mixed_ttl() {
        let mut map = CoalesceMap::new(Duration::from_millis(10), 100);
        map.upsert(b"a".to_vec(), b"v1".to_vec(), Some(1000));
        map.upsert(b"b".to_vec(), b"v2".to_vec(), None);
        map.upsert(b"c".to_vec(), b"v3".to_vec(), Some(2000));
        std::thread::sleep(Duration::from_millis(20));
        let mut ready = map.drain_ready();
        ready.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(ready.len(), 3);
        assert_eq!(ready[0].2, Some(1000));
        assert_eq!(ready[1].2, None);
        assert_eq!(ready[2].2, Some(2000));
    }

    #[test]
    fn test_drain_ready_with_zero_debounce() {
        let mut map = CoalesceMap::new(Duration::ZERO, 100);
        map.upsert(b"k".to_vec(), b"v".to_vec(), None);
        let ready = map.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, b"k");
    }

    #[test]
    fn test_upsert_many_different_keys() {
        let mut map = CoalesceMap::new(Duration::from_secs(1), 1000);
        for i in 0..100u32 {
            map.upsert(i.to_be_bytes().to_vec(), b"v".to_vec(), None);
        }
        assert_eq!(map.len(), 100);
    }

    #[test]
    fn test_drain_all_returns_all_regardless_of_window() {
        let mut map = CoalesceMap::new(Duration::from_secs(3600), 100);
        map.upsert(b"a".to_vec(), b"v1".to_vec(), None);
        map.upsert(b"b".to_vec(), b"v2".to_vec(), None);
        // No sleep — entries are far from ready, but drain_all ignores the window.
        let all = map.drain_all();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_is_over_capacity_exact_boundary() {
        // is_over_capacity uses >= so len==max_entries is already over.
        let mut map = CoalesceMap::new(Duration::from_secs(1), 3);
        map.upsert(b"a".to_vec(), b"v".to_vec(), None);
        map.upsert(b"b".to_vec(), b"v".to_vec(), None);
        assert!(!map.is_over_capacity()); // 2 < 3
        map.upsert(b"c".to_vec(), b"v".to_vec(), None);
        assert!(map.is_over_capacity()); // 3 >= 3
    }
}
