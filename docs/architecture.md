# Architecture deep dive

This document explains the interesting design decisions in valkey-sink, why they exist, and how the pieces fit together.

## The core problem

Valkey (like Redis) is single-threaded. Every operation on the main thread blocks all other clients. If we wrote to S3 or DynamoDB synchronously on every SET, a 5ms network call would stall all clients for 5ms — thousands of times per second. That's unacceptable.

We need to:
1. Never block the main thread
2. Batch and deduplicate writes (a key SET 100 times in 1s should be 1 backend write)
3. Handle backend failures without losing data
4. Support read-through on cache miss without blocking

## Non-blocking architecture

The solution is a producer-consumer pattern with a bounded channel:

```
Main thread (single-threaded)       Background (multi-threaded tokio)
+----------------------------+      +------------------------------+
| SET key value              |      |                              |
|   |                        |      |  crossbeam::Receiver         |
|   v                        |      |    |                         |
| keyspace_event_handler()   |      |    v                         |
|   |                        |      |  CoalesceMap.upsert(k, v)   |
|   v                        |      |    |                         |
| channel.try_send(msg)  ----+----->|  timer.tick()                |
|   |                        |      |    |                         |
|   v                        |      |    v                         |
| returns immediately        |      |  drain_ready() -> flush()   |
+----------------------------+      +------------------------------+
```

### Why crossbeam and not tokio channels?

Valkey's main thread is not async. It can't `.await` anything. We need a channel that works from synchronous code. `crossbeam-channel` provides:
- Lock-free `try_send` — O(1), never blocks
- Bounded capacity — backpressure by dropping writes (the next SET will re-enqueue)
- No allocation on send — the message is moved, not copied

### Why `try_send` instead of `send`?

`send` would block if the channel is full. In a 100K ops/sec Valkey server, blocking the main thread for even a microsecond is too much. `try_send` returns immediately with an error if full. The trade-off: if the channel fills up, some writes are dropped. But the next SET to the same key will re-enqueue it, so data is eventually consistent.

## Write coalescing (debounce)

The coalesce map is a `HashMap<Vec<u8>, PendingWrite>` where `PendingWrite` holds:
- `value: Vec<u8>` — the latest value
- `first_seen: Instant` — when the key first entered the map

### How it works

```
Time 0ms:   SET counter 1    -> upsert("counter", "1", first_seen=0)
Time 10ms:  SET counter 2    -> upsert("counter", "2", first_seen=0)  ← value overwritten, first_seen preserved
Time 100ms: SET counter 3    -> upsert("counter", "3", first_seen=0)
...
Time 1000ms: timer tick       -> drain_ready() finds "counter" (first_seen + 1000ms has elapsed)
                              -> flush("counter", "3") to backend  ← only 1 write!
```

### Why anchor first_seen to the first write?

If we reset `first_seen` on every upsert, a continuously hot key would never get flushed — the debounce window would keep sliding forward. By anchoring to the first write, we guarantee the key gets flushed within `debounce_ms` of its first modification.

### Timer tick frequency

The flush timer ticks at `debounce_window / 2`. With a 1s debounce, the timer fires every 500ms. This means entries are flushed within 1.0-1.5x the debounce window. We could tick faster for lower latency, but the overhead isn't worth it — sub-second precision is sufficient for write-behind.

## Size-based routing

In "both" mode, the `SinkRouter` partitions each batch by value size:

```rust
if entry.value.len() <= size_threshold {
    dynamo_entries.push(entry);   // small → DynamoDB (lower latency)
} else {
    s3_entries.push(entry);       // large → S3 Express (cheaper per byte)
}
```

**Why 64KB default?** DynamoDB items are limited to 400KB, but performance degrades above ~64KB. S3 Express has single-digit millisecond latency for any object size, and is much cheaper per GB than DynamoDB.

## S3 Express One Zone auth

S3 Express directory buckets use a different auth flow than regular S3:

```
1. CreateSession (signed with IAM creds, standard SigV4)
   → returns temporary session credentials (5 minute TTL)

2. PutObject / GetObject (signed with session creds)
   → uses x-amz-s3session-token header (NOT x-amz-security-token)
   → service name in scope is "s3express" (NOT "s3")
```

### Session caching

Creating a session on every request would be wasteful. We cache the session behind a `tokio::sync::RwLock`:

```rust
// Fast path: read lock, check if session is still valid
{
    let guard = self.session.read().await;
    if let Some(sess) = guard.as_ref() {
        if !sess.is_expired() {
            return Ok(sess.credentials.clone());  // cache hit
        }
    }
}

// Slow path: write lock, double-check, then refresh
let mut guard = self.session.write().await;
// ... double-check prevents thundering herd ...
let new_session = self.create_session().await?;
*guard = Some(new_session);
```

The 30-second refresh buffer ensures we refresh before expiry:
```rust
fn is_expired(&self) -> bool {
    self.obtained_at.elapsed() + Duration::from_secs(30) >= self.ttl
}
```

## Hand-rolled SigV4

The full AWS SDK (aws-sdk-s3 + aws-sdk-dynamodb + aws-config) adds:
- ~15MB to the binary
- ~60 seconds to compile
- Hundreds of transitive dependencies

Since we only need `PutObject`, `GetObject`, `CreateSession`, `BatchWriteItem`, and `GetItem`, we implemented SigV4 signing in ~160 lines using just `hmac` + `sha2` + `hex`.

### The SigV4 algorithm

```
1. payload_hash = SHA256(request_body)
2. canonical_request = METHOD + "\n" + path + "\n" + query + "\n" + headers + "\n" + signed_headers + "\n" + payload_hash
3. string_to_sign = "AWS4-HMAC-SHA256" + "\n" + datetime + "\n" + scope + "\n" + SHA256(canonical_request)
4. signing_key = HMAC(HMAC(HMAC(HMAC("AWS4" + secret, date), region), service), "aws4_request")
5. signature = HMAC(signing_key, string_to_sign)
6. Authorization: AWS4-HMAC-SHA256 Credential=AKID/scope, SignedHeaders=..., Signature=...
```

### A gotcha: query string normalization

S3 Express's `CreateSession` endpoint uses `?session` with no value. SigV4 requires every query parameter to have a `=` sign in the canonical form. We learned this the hard way — `?session` must be canonicalized as `session=`, not `session`.

## Read-through with BlockClient

Valkey's `BlockClient` API lets a command handler say "I'm not done yet, I'll reply later" without blocking the event loop:

```
1. Client sends SINK.GET key
2. Command handler calls native GET → nil (cache miss)
3. Handler calls ctx.block_client() → client is suspended
4. Handler sends ReadThrough message through channel
5. Handler returns ValkeyValue::NoReply (event loop continues serving other clients)

6. Background runtime receives ReadThrough message
7. Looks up key in DynamoDB, then S3 if not found
8. Sends result back via oneshot channel

9. Bridging thread receives result via oneshot
10. Populates cache with SET (so future GETs are fast)
11. Replies to blocked client via ThreadSafeContext
```

### Why a bridging thread?

The background runtime is async (tokio). The `ThreadSafeContext::reply()` API is synchronous. We can't call `blocking_recv()` on a tokio task without risking deadlock. The bridging thread:
- Is cheap (does zero I/O, just waits on a oneshot)
- Converts async → sync cleanly
- Lives for the duration of one lookup (~5-50ms)

## Retry and circuit breaker

### Exponential backoff with jitter

```
attempt 1: base_delay * 2^0 + random(0-100ms) = ~100ms
attempt 2: base_delay * 2^1 + random(0-100ms) = ~200ms
attempt 3: base_delay * 2^2 + random(0-100ms) = ~400ms
(capped at 10s)
```

The jitter prevents thundering herd: if 10 flush attempts fail simultaneously, they retry at slightly different times instead of all hitting the backend at once.

### Circuit breaker

After N consecutive failures (default 10), the circuit breaker "opens" and stops sending requests for a cooldown period (default 30s). This:
- Prevents hammering a failing backend
- Gives the backend time to recover
- Keeps the coalesce map growing (data isn't lost, just delayed)

When the cooldown expires, the breaker enters "half-open" state and allows the next flush through. If it succeeds, the breaker closes and normal operation resumes.

## Metrics

The latency tracker uses a ring buffer of the last 1024 samples:

```rust
struct LatencyInner {
    count: u64,          // total samples ever
    sum_us: u64,         // for computing avg
    min_us: u64,
    max_us: u64,
    ring: Vec<u64>,      // last 1024 samples for percentiles
    ring_pos: usize,     // circular write position
}
```

Percentiles (p50, p99) are computed on-demand by sorting the ring buffer. This gives accurate percentiles over recent traffic without unbounded memory growth.

## What's NOT in scope

- **Replication** — this module runs on a single Valkey instance. For HA, use Valkey's built-in replication; the module will run on the primary.
- **Transactions** — SET + sink write is not atomic. The sink is eventually consistent. A crash between SET and flush may lose the last debounce window of writes.
- **DELETE propagation** — DELs are not currently captured. A deleted key remains in the backend until overwritten.
- **TTL awareness** — expiring keys are not flushed on eviction. This could be added by subscribing to `@EXPIRED` events.
