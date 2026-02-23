# Reading Order: A Guided Tour

If you're reading the codebase for the first time, here's a guided path through the 14 source files. Follow this order and you'll understand the full system in about an hour.

## 1. Start with the entry point

**`crates/sink/src/lib.rs`**

Read the ASCII architecture diagram at the top of the file (lines 20-31). Then scroll to the bottom and read the `valkey_module!` macro invocation (lines 338-352). This tells you everything the module registers: two commands (`SINK.GET`, `SINK.INFO`), one event subscription (`@STRING`), init and deinit functions. Then read `init()` (line 101) top to bottom: it is the startup sequence. Finally read `keyspace_event_handler()` (line 202), the hot path that fires on every SET.

**Gotcha:** The `SKIP_ENQUEUE` thread-local flag (line 74) is subtle. It prevents re-enqueuing values that were just fetched from the backend during read-through. Without it, every SINK.GET cache populate would trigger a wasteful write-back to the sink.

## 2. Follow the write path

Starting from `keyspace_event_handler` in lib.rs:

1. **`crates/sink/src/channel.rs`** - the four message types (`MainToBackground`) and the `OnceLock` global sender. Small file (57 lines), read it entirely.
2. **`crates/sink/src/coalesce.rs`** - the write deduplication map. Focus on `upsert()`, `drain_ready()`, and `re_enqueue()`. The 20 unit tests at the bottom are an excellent specification of the coalescing behavior.
3. **`crates/sink/src/background.rs`** - the tokio event loop. Read `run_loop()` (line 71) for the main loop, then `flush_with_retry()` (line 200) for how failures are handled.
4. **`crates/sink/src/sink/mod.rs`** - the `Sink` trait, `SinkEntry`, `SinkError`, and the `SinkRouter` that partitions by size. Note that `write_batch` uses `tokio::join!` to run DynamoDB and S3 writes concurrently.
5. **`crates/sink/src/sink/dynamo.rs`** or **`crates/sink/src/sink/s3.rs`** - the actual HTTP calls. Read whichever backend you care about.

**Gotcha:** In `background.rs`, the `ReadThrough` handler (line 100) spawns a separate tokio task for each lookup. This means lookups run concurrently with the flush loop, not sequentially.

## 3. Follow the read path

1. **`crates/sink/src/readthrough.rs`** - the `SINK.GET` command handler. Read the entire 154-line file. Focus on the `std::thread::spawn` bridging pattern and the `SKIP_ENQUEUE` flag management.
2. Back to **`crates/sink/src/background.rs`** line 100 - the `ReadThrough` message handler that calls `router.lookup()`.
3. **`crates/sink/src/sink/mod.rs`** lines 171-206 - the `lookup()` method tries DynamoDB first (lower latency), then falls through to S3.

**Gotcha:** The bridging thread in readthrough.rs does `blocking_recv()` on a tokio oneshot. This is intentionally on a `std::thread`, not a tokio task, to avoid blocking a tokio worker thread.

## 4. Study the cross-cutting concerns

1. **`crates/common/src/aws.rs`** - SigV4 signing, credential loading, and the `RefreshableCredentials` provider with double-checked locking. The `TokenHeader` enum distinguishes S3 Express from standard SigV4.
2. **`crates/common/src/retry.rs`** - the `RetryPolicy` (exponential backoff with jitter) and `CircuitBreaker`. The `Retriable` trait lets the retry policy distinguish between transient and permanent errors.
3. **`crates/common/src/metrics.rs`** + **`crates/sink/src/metrics.rs`** - the common latency tracker (ring buffer, percentiles) and the sink-specific counters. `SINK.INFO` formats these into a Valkey-compatible INFO output.

**Gotcha:** The `RefreshableCredentials` in aws.rs chooses its TTL based on whether a session token is present: 50 minutes for temporary credentials (STS), 24 hours for long-lived IAM user credentials.

## 5. Finally, configuration and tooling

1. **`crates/sink/src/config.rs`** - all configuration parsing. The `from_args()` method handles both underscore and hyphen forms (e.g. `s3_bucket` or `s3-bucket`). Unknown keys are silently ignored, invalid numbers fall back to defaults.
2. **`tools/loadtest/src/main.rs`** - the sanity test tool (1,305 lines). It spins up a Valkey server via Docker, loads the module, and runs burst/sustained write tests with configurable concurrency.
