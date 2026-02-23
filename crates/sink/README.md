# valkey-sink

> **Demo / Tutorial** — This module is a proof-of-concept built to explore write-behind caching patterns with Valkey. It is not production-hardened. Use it as a learning reference and starting point, not as a drop-in production component.

A Valkey module that provides transparent **write-behind persistence** to **S3 Express One Zone** and/or **DynamoDB**.

Every SET command is captured, coalesced, and flushed to the configured backend(s) on a debounce timer. On cache miss, `SINK.GET` reads through to the backend and populates the cache.

## Features

- **Write coalescing** — 100 writes to the same key in 1 second = 1 backend write
- **Dual-sink routing** — small values go to DynamoDB, large values to S3 Express
- **Non-blocking** — never blocks Valkey's main event loop
- **Read-through** — `SINK.GET` falls back to backends on cache miss, with TTL-aware filtering
- **TTL propagation** — key TTLs are stored in the backend and restored on cache populate
- **Replica-safe** — write-behind disabled on replicas, read-through still works
- **Lightweight** — ~5MB binary, uses `aws-sigv4` crate (not the full AWS SDK)
- **Observable** — per-sink counters, latency histograms (min/avg/p50/p99/max)
- **Resilient** — exponential backoff with jitter, circuit breaker, re-enqueue on failure

## Architecture

```
Valkey main thread              Background tokio runtime
+-----------------+             +----------------------------+
| SET key value   |--channel--->| CoalesceMap (debounce)     |
| (non-blocking)  |             |         |                  |
|                 |             |     timer tick              |
| SINK.GET key    |--channel--->|         v                  |
| (BlockClient)   |             | SinkRouter (by size)       |
+-----------------+             |  |- DynamoDB  (<64KB)      |
                                |  '- S3 Express (>=64KB)    |
                                +----------------------------+
```

1. **Keyspace notifications** — the module subscribes to `@STRING` events (primary only)
2. **Crossbeam channel** — bounded, lock-free, non-blocking `try_send`
3. **CoalesceMap** — HashMap with debounce window; upserts overwrite value but preserve first-seen time
4. **SinkRouter** — partitions entries by value size, writes to appropriate backend
5. **BlockClient** — Valkey's async reply mechanism for read-through without blocking the event loop
6. **Replica awareness** — write-behind disabled on replicas, `SINK.GET` read-through works on all instances

## Quick start

### Prerequisites

- Rust 1.70+ (for building)
- Valkey 7.2+ or Redis 7+ (with module support)
- AWS credentials with appropriate permissions
- S3 Express directory bucket and/or DynamoDB table

### Build

```bash
cargo build --release
# Output: target/release/libvalkey_sink.so (Linux) or .dylib (macOS)
```

### Load into Valkey

```bash
# S3 Express only
valkey-server --loadmodule ./target/release/libvalkey_sink.so \
  mode s3 \
  s3_bucket my-bucket--usw2-az3--x-s3 \
  s3_region us-west-2

# DynamoDB only
valkey-server --loadmodule ./target/release/libvalkey_sink.so \
  mode dynamo \
  dynamo_table my-table \

# Both (size-based routing)
valkey-server --loadmodule ./target/release/libvalkey_sink.so \
  mode both \
  s3_bucket my-bucket--usw2-az3--x-s3 \
  dynamo_table my-table \
  size_threshold 65536 \
  debounce_ms 1000
```

### Docker

```bash
docker run -d --name valkey \
  --network host \
  -e AWS_ACCESS_KEY_ID=AKIA... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -e AWS_REGION=us-west-2 \
  -v $(pwd)/target/release/libvalkey_sink.so:/modules/libvalkey_sink.so:ro \
  valkey/valkey:9 \
  valkey-server --enable-module-command yes \
    --loadmodule /modules/libvalkey_sink.so \
    mode both \
    s3_bucket my-bucket--usw2-az3--x-s3 \
    dynamo_table my-table
```

## Commands

### `SINK.GET key`

Like GET, but on cache miss, checks the configured backends (DynamoDB first, then S3). If found and not expired (TTL-aware), populates the cache via SET with the remaining TTL and returns the value. On replicas, the cache populate is skipped (READONLY) but the value is still returned.

```
> SET mykey "hello"
OK
> DEL mykey
(integer) 1
> GET mykey
(nil)
> SINK.GET mykey
"hello"
```

### `SINK.INFO`

Returns module metrics:

```
> SINK.INFO
# valkey-sink
sink_writes_coalesced:57        # SET events received
sink_writes_flushed:9           # actual backend writes
sink_write_failures:0
sink_read_throughs:2
sink_read_through_hits:2
sink_read_through_misses:0
sink_coalesce_map_size:0        # entries waiting to flush
sink_circuit_breaker_trips:0
# per-sink
sink_s3_writes:5
sink_s3_write_failures:0
sink_dynamo_writes:21
sink_dynamo_write_failures:0
# write latency
sink_s3_write_latency:count=11 min=5.4ms avg=14.5ms p50=6.4ms p99=73.3ms max=73.3ms
sink_dynamo_write_latency:count=11 min=2.9ms avg=6.1ms p50=3.6ms p99=29.1ms max=29.1ms
sink_flush_latency:count=11 min=8.4ms avg=20.6ms p50=11.7ms p99=102.5ms max=102.5ms
# read latency
sink_read_through_latency:count=3 min=2.0ms avg=10.9ms p50=14.0ms p99=16.6ms max=16.6ms
sink_s3_read_latency:count=1 min=5.2ms avg=5.2ms p50=5.2ms p99=5.2ms max=5.2ms
sink_dynamo_read_latency:count=3 min=1.8ms avg=3.1ms p50=2.4ms p99=5.0ms max=5.0ms
```

## Configuration

All config is passed as alternating key/value pairs on module load. Both underscore (`s3_bucket`) and hyphen (`s3-bucket`) forms are accepted.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `mode` | `s3` | `s3`, `dynamo`, or `both` |
| `s3_bucket` | (required for s3) | S3 Express directory bucket name |
| `s3_prefix` | `valkey-sink/` | Object key prefix |
| `s3_region` | `$AWS_REGION` or `us-east-1` | AWS region for S3 |
| `s3_endpoint` | (auto) | Custom endpoint (for LocalStack) |
| `dynamo_table` | (required for dynamo) | DynamoDB table name |
| `dynamo_region` | `$AWS_REGION` or `us-east-1` | AWS region for DynamoDB |
| `size_threshold` | `65536` (64KB) | Values above this go to S3 (in `both` mode) |
| `debounce_ms` | `1000` | Coalesce window in milliseconds |
| `background_threads` | `4` | Tokio worker threads |
| `channel_capacity` | `10000` | Bounded channel size |
| `s3_concurrency` | `16` | Max concurrent S3 uploads per batch |
| `dynamo_concurrency` | `32` | Max concurrent DynamoDB BatchWriteItem requests per flush |
| `max_retries` | `3` | Retry attempts per flush |
| `retry_base_delay_ms` | `100` | Base delay for exponential backoff |
| `circuit_breaker_threshold` | `10` | Failures before circuit opens |
| `circuit_breaker_cooldown_ms` | `30000` | Cooldown when circuit is open |
| `coalesce_max_entries` | `1000000` | Force-flush when map exceeds this |
| `read_through` | `true` | Enable/disable SINK.GET read-through |

## AWS setup

### IAM policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ExpressSession",
      "Effect": "Allow",
      "Action": "s3express:CreateSession",
      "Resource": "arn:aws:s3express:REGION:ACCOUNT:bucket/BUCKET_NAME"
    },
    {
      "Sid": "S3ExpressData",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3express:REGION:ACCOUNT:bucket/BUCKET_NAME/*"
    },
    {
      "Sid": "DynamoDB",
      "Effect": "Allow",
      "Action": ["dynamodb:BatchWriteItem", "dynamodb:GetItem"],
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/TABLE_NAME"
    }
  ]
}
```

### DynamoDB table

Create with partition key `pk` of type Binary (B). No sort key needed. Optionally enable DynamoDB native TTL on the `ttl` attribute for automatic background cleanup of expired items.

```bash
aws dynamodb create-table \
  --table-name valkey-table \
  --attribute-definitions AttributeName=pk,AttributeType=B \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# Optional: enable DynamoDB native TTL for automatic expired item cleanup
aws dynamodb update-time-to-live \
  --table-name valkey-table \
  --time-to-live-specification Enabled=true,AttributeName=ttl
```

### S3 Express directory bucket

```bash
aws s3api create-bucket \
  --bucket my-bucket--usw2-az3--x-s3 \
  --create-bucket-configuration \
    'Location={Type=AvailabilityZone,Name=usw2-az3},Bucket={DataRedundancy=SingleAvailabilityZone,Type=Directory}'
```

## Preliminary benchmark

Results from a single EC2 instance (us-west-2). These are preliminary numbers from a short test, not production baselines. Numbers are directional — real-world performance depends on instance size, DynamoDB capacity mode, network conditions, and workload patterns.

### Sustained write test (60 seconds)

**Setup:** 5,000 target writes/s, 80/20 small/large split, 30% with TTL, 32 writer connections, 1s debounce. DynamoDB on-demand mode, S3 Express single-AZ.

| Metric | DynamoDB | S3 Express | Total |
|--------|----------|------------|-------|
| Items flushed | 240,017 | 59,855 | 299,872 |
| Failures | 0 | 0 | 0 |
| Sustained TPS | ~4,000/s | ~998/s | ~4,998/s |
| Batch latency avg | 58.8ms | 213.8ms | 278.2ms (flush) |
| Batch latency p50 | 56.7ms | 216.5ms | 281.4ms (flush) |
| Batch latency p99 | 122.6ms | 345.5ms | 442.6ms (flush) |

DynamoDB BatchWriteItem runs up to 16 concurrent chunks of 25. S3 PutObject runs up to 16 concurrent uploads. The module coalesced ~300K writes into 120 flush cycles over 60 seconds with zero failures.

### Read-through backend latency (per call)

**Setup:** 5,000 keys, burst read-through after eviction, 128 concurrent connections.

| Metric | DynamoDB GetItem | S3 Express GetObject |
|--------|------------------|----------------------|
| Samples | 5,000 | 56 |
| avg | 1.5ms | 3.8ms |
| p50 | 1.4ms | 3.6ms |
| p99 | 4.7ms | 9.6ms |
| max | 19.5ms | 9.6ms |

### End-to-end SINK.GET

| Phase | avg | p50 | p99 | effective ops/s |
|-------|-----|-----|-----|-----------------|
| Cold (read-through) | 198.3ms | 193.0ms | 265.6ms | 618 |
| Warm (cache hit) | 0.8ms | 0.8ms | 1.6ms | 135,715 |

**Known limitation:** Cold read-through end-to-end latency (~198ms) is dominated by `std::thread::spawn` overhead per BlockClient request, not backend latency. The backend calls complete in 1.5-3.8ms; the remaining ~195ms is OS thread creation/teardown. A thread pool or async bridge would close this gap.

**Caveats:**
- Single EC2 instance, not a distributed cluster
- 60 second sustained write test, not hours-long soak
- ~5K writes/s total (~4K DynamoDB + ~1K S3) — not tested at higher TPS
- ~618 effective cold read ops/s (thread-spawn bottlenecked, not backend-limited)
- DynamoDB in on-demand mode (no provisioned throughput limits tested)
- S3 Express single-AZ bucket
- No network partitions, no concurrent writers, no failover tested

## Testing

```bash
# Unit tests (uses system allocator since ValkeyAlloc requires a running server)
cargo test --features enable-system-alloc
```

## How it works internally

See [docs/architecture.md](docs/architecture.md) for a deep dive into the design decisions.

## Project structure

```
src/
  lib.rs          - Module entry point, init/deinit, event handler
  config.rs       - Configuration parsing from module arguments
  channel.rs      - Message types for main thread -> background communication
  coalesce.rs     - Write coalescing with debounce window
  background.rs   - Tokio runtime, flush loop, retry orchestration
  metrics.rs      - Sink-specific counters, SINK.INFO output
  readthrough.rs  - SINK.GET command with BlockClient
  sink/
    mod.rs        - Sink trait, size-based router
    s3.rs         - S3 Express One Zone (CreateSession + SigV4-Express)
    dynamo.rs     - DynamoDB via raw HTTP + SigV4

# Shared code lives in the valkey-common crate (../common/):
#   aws.rs        - SigV4 signing (via aws-sigv4 crate), credential loading
#   retry.rs      - Exponential backoff + circuit breaker
#   metrics.rs    - LatencyTracker, counter helpers
```

## Caveats and future work

This module is a **demo/tutorial** — it demonstrates the pattern but is not production-ready. Key gaps:

- **Not durable under primary failover.** If the primary crashes, writes sitting in the in-memory coalesce map are lost. There is no replication of the pending-write queue, no WAL, and no acknowledgment protocol between the module and the backends. A production system would need to address this — for example, by writing to a local AOF or journal before acknowledging the SET, or by leveraging Valkey's own persistence as a backstop.
- **Limited load testing.** Current benchmarks cover ~5K writes/s for 60 seconds on a single instance. Sustained higher TPS (50K+), longer soak runs (hours/days), and degraded-mode scenarios (network partitions, throttled backends, node failures) have not been tested.
- **No cluster-aware coordination.** In a Valkey Cluster, each shard runs its own independent flush loop. There is no cross-shard deduplication or ordered delivery guarantee.
- **Thread-spawn bottleneck on read-through.** Cold read-through latency is ~198ms due to per-request `std::thread::spawn`. A thread pool or async bridge would bring this closer to the 1.5-3.8ms backend latency.
- **No delivery guarantees.** The write path is best-effort with retries and circuit breaker, but does not guarantee exactly-once or at-least-once delivery to backends across process restarts.

## License

Apache 2.0
