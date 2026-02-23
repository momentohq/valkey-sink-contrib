/// Background tokio runtime: flush loop, message handler, sink I/O.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crossbeam_channel::Receiver;
use tokio::runtime::Runtime;

use crate::channel::{ConfigDelta, MainToBackground};
use crate::coalesce::CoalesceMap;
use crate::config::SinkConfig;
use crate::metrics;
use valkey_common::retry::{CircuitBreaker, RetryPolicy};
use crate::sink::{SinkEntry, SinkRouter};

/// Global handle to the background tokio runtime so we can shut it down on unload.
static BG_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Signal set by the background loop after the final shutdown flush completes.
static SHUTDOWN_COMPLETE: OnceLock<Arc<AtomicBool>> = OnceLock::new();

/// Spawn the background runtime and start the main processing loop.
///
/// This function is called once during module init. It creates a tokio runtime
/// and spawns the event loop that processes messages from the main thread.
pub fn start(config: &SinkConfig, rx: Receiver<MainToBackground>, router: SinkRouter) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.background_threads)
        .enable_all()
        .thread_name("valkey-sink-bg")
        .build()
        .expect("valkey-sink: failed to build tokio runtime");

    let debounce = Duration::from_millis(config.debounce_window_ms);
    let max_entries = config.coalesce_max_entries;
    let retry_policy = RetryPolicy::new(config.max_retries, config.retry_base_delay_ms);
    let circuit_breaker = CircuitBreaker::new(
        config.circuit_breaker_threshold,
        config.circuit_breaker_cooldown_ms,
    );

    // Initialize the shutdown completion signal.
    let _ = SHUTDOWN_COMPLETE.set(Arc::new(AtomicBool::new(false)));

    rt.spawn(async move {
        run_loop(rx, router, debounce, max_entries, retry_policy, circuit_breaker).await;
    });

    BG_RUNTIME
        .set(rt)
        .unwrap_or_else(|_| panic!("valkey-sink: background runtime already initialized"));
}

/// Shut down the background runtime (called on module unload).
/// Polls the shutdown-complete signal with a 5-second timeout, so we return
/// as soon as the final flush finishes rather than sleeping unconditionally.
pub fn shutdown() {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if let Some(flag) = SHUTDOWN_COMPLETE.get() {
            if flag.load(Ordering::Acquire) {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Main processing loop running inside the tokio runtime.
async fn run_loop(
    rx: Receiver<MainToBackground>,
    router: SinkRouter,
    initial_debounce: Duration,
    max_entries: usize,
    retry_policy: RetryPolicy,
    circuit_breaker: CircuitBreaker,
) {
    let router = Arc::new(router);
    let mut coalesce = CoalesceMap::new(initial_debounce, max_entries);

    // Tick at half the debounce window so entries are flushed within ~1.5x the window.
    let tick_interval = initial_debounce / 2;
    let mut timer = tokio::time::interval(tick_interval.max(Duration::from_millis(50)));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        // Process all pending channel messages (non-blocking drain).
        let mut shutdown_requested = false;
        loop {
            match rx.try_recv() {
                Ok(MainToBackground::KeyWritten {
                    key,
                    value,
                    expires_at,
                }) => {
                    metrics::inc(&metrics::get().writes_coalesced);
                    coalesce.upsert(key, value, expires_at);
                }
                Ok(MainToBackground::ReadThrough { key, reply }) => {
                    // Spawn the lookup as a separate tokio task so it doesn't
                    // block the drain loop. Multiple lookups can now run concurrently.
                    let router_clone = Arc::clone(&router);
                    tokio::spawn(async move {
                        let lookup_result = router_clone.lookup(&key).await;
                        let _ = reply.send(lookup_result.unwrap_or(None));
                    });
                }
                Ok(MainToBackground::ConfigUpdate(delta)) => {
                    apply_config_delta(&mut coalesce, &router, delta);
                }
                Ok(MainToBackground::Shutdown) => {
                    shutdown_requested = true;
                    break;
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    shutdown_requested = true;
                    break;
                }
            }
        }

        // Update the coalesce map size gauge.
        metrics::set(
            &metrics::get().coalesce_map_size,
            coalesce.len() as u64,
        );

        if shutdown_requested {
            // Flush everything on shutdown regardless of debounce window.
            let all = coalesce.drain_all();
            if !all.is_empty() {
                let entries: Vec<SinkEntry> = all
                    .into_iter()
                    .map(|(key, value, expires_at)| SinkEntry {
                        key,
                        value,
                        expires_at,
                    })
                    .collect();
                let _ = flush_with_retry(&router, &entries, &retry_policy, &circuit_breaker).await;
            }
            // Signal shutdown completion so lib.rs doesn't wait the full 5 seconds.
            if let Some(flag) = SHUTDOWN_COMPLETE.get() {
                flag.store(true, Ordering::Release);
            }
            return;
        }

        // Force-flush if over capacity.
        if coalesce.is_over_capacity() {
            let all = coalesce.drain_all();
            if !all.is_empty() {
                let entries: Vec<SinkEntry> = all
                    .into_iter()
                    .map(|(key, value, expires_at)| SinkEntry {
                        key,
                        value,
                        expires_at,
                    })
                    .collect();
                let result =
                    flush_with_retry(&router, &entries, &retry_policy, &circuit_breaker).await;
                if let Err(failed) = result {
                    coalesce.re_enqueue(failed);
                }
            }
        }

        // Wait for the next timer tick.
        timer.tick().await;

        // Drain entries that have waited long enough.
        if !circuit_breaker.is_open() {
            let ready = coalesce.drain_ready();
            if !ready.is_empty() {
                let entries: Vec<SinkEntry> = ready
                    .into_iter()
                    .map(|(key, value, expires_at)| SinkEntry {
                        key,
                        value,
                        expires_at,
                    })
                    .collect();
                let result =
                    flush_with_retry(&router, &entries, &retry_policy, &circuit_breaker).await;
                if let Err(failed) = result {
                    coalesce.re_enqueue(failed);
                }
            }
        }
    }
}

/// Flush entries through the retry policy and circuit breaker.
///
/// On success, returns Ok(()).
/// On failure, returns Err with the entries that should be re-enqueued.
async fn flush_with_retry(
    router: &SinkRouter,
    entries: &[SinkEntry],
    retry_policy: &RetryPolicy,
    circuit_breaker: &CircuitBreaker,
) -> Result<(), Vec<(Vec<u8>, Vec<u8>, Option<u64>)>> {
    let flush_start = Instant::now();
    let result = retry_policy
        .execute(|| async { router.write_batch(entries).await })
        .await;

    match result {
        Ok(()) => {
            metrics::get().flush_latency.record_since(flush_start);
            circuit_breaker.record_success();
            metrics::inc_by(
                &metrics::get().writes_flushed,
                entries.len() as u64,
            );
            Ok(())
        }
        Err(ref e) => {
            metrics::get().flush_latency.record_since(flush_start);
            eprintln!("valkey-sink: flush failed: {}", e);
            metrics::inc(&metrics::get().write_failures);
            let tripped = circuit_breaker.record_failure();
            if tripped {
                metrics::inc(&metrics::get().circuit_breaker_trips);
            }
            // Only clone on failure (rare path) for re-enqueue.
            let failed = entries
                .iter()
                .map(|e| (e.key.clone(), e.value.clone(), e.expires_at))
                .collect();
            Err(failed)
        }
    }
}

/// Apply a dynamic config change.
fn apply_config_delta(
    coalesce: &mut CoalesceMap,
    router: &SinkRouter,
    delta: ConfigDelta,
) {
    match delta {
        ConfigDelta::DebounceMs(ms) => {
            coalesce.debounce_window = Duration::from_millis(ms);
        }
        ConfigDelta::SizeThreshold(threshold) => {
            router.set_size_threshold(threshold);
        }
        ConfigDelta::ReadThroughEnabled(enabled) => {
            crate::READ_THROUGH_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
        }
    }
}
