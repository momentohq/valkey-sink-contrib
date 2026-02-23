//! # valkey-sink
//!
//! A Valkey module that provides transparent write-behind persistence to
//! **S3 Express One Zone** and/or **DynamoDB**.
//!
//! ## How it works
//!
//! 1. The module subscribes to keyspace notifications for string commands (`@STRING`).
//! 2. On every SET, the key+value is copied and sent (non-blocking) through a bounded
//!    crossbeam channel to a background tokio runtime.
//! 3. The background runtime coalesces writes: if a key is updated 100 times within
//!    the debounce window (default 1s), only the final value is flushed once.
//! 4. On flush, entries are routed by size: small values go to DynamoDB,
//!    large values (>64KB default) go to S3 Express.
//! 5. The `SINK.GET` command provides read-through: on cache miss, it checks the
//!    active sinks and populates the cache if found.
//!
//! ## Architecture
//!
//! ```text
//! Valkey main thread          Background tokio runtime
//! ┌──────────────┐            ┌─────────────────────────┐
//! │  SET key val  │──channel──▶  CoalesceMap (debounce)  │
//! │  (non-block)  │           │         │                │
//! │               │           │     timer tick           │
//! │  SINK.GET key │──channel──▶         ▼                │
//! │  (BlockClient)│           │  SinkRouter (by size)    │
//! └──────────────┘            │   ├─ DynamoDbSink (<64K) │
//!                             │   └─ S3ExpressSink (≥64K)│
//!                             └─────────────────────────┘
//! ```
//!
//! ## Commands
//!
//! - `SINK.GET key` — GET with read-through fallback to sinks on cache miss
//! - `SINK.INFO` — module metrics (counters, per-sink stats, latency histograms)

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};

use linkme::distributed_slice;
use valkey_module::{
    alloc::ValkeyAlloc, server_events::ROLE_CHANGED_SERVER_EVENTS_LIST, valkey_module,
    CallOptionsBuilder, CallReply, CallResult, Context, ContextFlags, NotifyEvent, Status,
    ValkeyResult, ValkeyString, ValkeyValue,
};
use valkey_module::server_events::ServerRole;

/// True when this instance is a primary. Set on init and updated on role change.
/// On replicas, write-behind is disabled (no keyspace event enqueue) but
/// SINK.GET read-through still works.
static IS_PRIMARY: AtomicBool = AtomicBool::new(false);

/// Controls whether SINK.GET falls back to sinks on cache miss. When false,
/// SINK.GET behaves like a plain GET (returns nil on miss). Toggled at init
/// from SinkConfig and at runtime via ConfigDelta::ReadThroughEnabled.
static READ_THROUGH_ENABLED: AtomicBool = AtomicBool::new(true);

/// Role-change handler registered via distributed_slice. Fires on failover/promotion.
/// The `valkey_module!` macro calls `register_server_events()` BEFORE `init()`,
/// so this subscription is active before we first check the role.
#[distributed_slice(ROLE_CHANGED_SERVER_EVENTS_LIST)]
fn on_role_changed(_ctx: &Context, new_role: ServerRole) {
    IS_PRIMARY.store(new_role == ServerRole::Primary, Ordering::Relaxed);
}

// Thread-local flag to suppress keyspace re-enqueue during read-through cache populate.
// When SINK.GET populates the cache via SET, the keyspace notification handler fires
// synchronously on the same thread. Without this flag, the handler would re-enqueue
// the value that was just fetched from the backend, causing a wasteful write-back.
// The flag is set before the SET call and consumed (atomically read + cleared via
// `replace(false)`) in the handler. On SET failure the flag is cleared manually
// since the notification may not have fired.
thread_local! {
    pub(crate) static SKIP_ENQUEUE: Cell<bool> = const { Cell::new(false) };
}

mod background;
mod channel;
mod coalesce;
mod config;
mod metrics;
mod readthrough;
mod sink;

use channel::MainToBackground;
use config::{SinkConfig, SinkMode};
use sink::dynamo::DynamoDbSink;
use sink::s3::S3ExpressSink;
use sink::SinkRouter;

/// Module initialization. Called once when Valkey loads the module.
///
/// Sequence:
/// 1. Parse config from module load arguments (key/value pairs).
/// 2. Create the bounded crossbeam channel (main thread → background).
/// 3. Load AWS credentials from environment variables.
/// 4. Construct the SinkRouter with the configured backends.
/// 5. Start the background tokio runtime (flush loop).
/// 6. Auto-enable keyspace notifications if not already configured.
fn init(ctx: &Context, args: &[ValkeyString]) -> Status {
    let str_args: Vec<String> = args
        .iter()
        .map(|a| a.to_string_lossy().to_string())
        .collect();
    let config = SinkConfig::from_args(&str_args);
    READ_THROUGH_ENABLED.store(config.read_through_enabled, Ordering::Relaxed);

    let rx = channel::init_channel(config.channel_capacity);

    let creds = match valkey_common::aws::Credentials::from_env() {
        Ok(c) => c,
        Err(e) => {
            ctx.log_warning(&format!("valkey-sink: {}", e));
            return Status::Err;
        }
    };

    let http_client = reqwest::Client::builder()
        .pool_max_idle_per_host(64)
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .tcp_keepalive(std::time::Duration::from_secs(30))
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("valkey-sink: failed to build HTTP client");
    let router = build_router(&config, &creds, &http_client);

    background::start(&config, rx, router);

    // Auto-enable keyspace notifications for string commands.
    // K = keyspace events, $ = string commands (SET, SETEX, etc.).
    // Without these flags, the module's @STRING event handler won't fire.
    let current = match ctx.call("CONFIG", &["GET", "notify-keyspace-events"]) {
        Ok(ValkeyValue::Array(ref arr)) if arr.len() == 2 => match &arr[1] {
            ValkeyValue::BulkString(s) => s.clone(),
            _ => String::new(),
        },
        _ => String::new(),
    };
    if !current.contains('K') || !current.contains('$') {
        let mut flags = current.clone();
        if !flags.contains('K') {
            flags.push('K');
        }
        if !flags.contains('$') {
            flags.push('$');
        }
        let _ = ctx.call("CONFIG", &["SET", "notify-keyspace-events", &flags]);
        ctx.log_notice(&format!(
            "valkey-sink: enabled keyspace notifications (was='{}', now='{}')",
            current, flags,
        ));
    }

    // Detect current role. The role-change event handler is already registered
    // (the valkey_module! macro calls register_server_events before init), but
    // we still need to read the initial role since no event fires at startup.
    IS_PRIMARY.store(
        ctx.get_flags().contains(ContextFlags::MASTER),
        Ordering::Relaxed,
    );

    let role = if IS_PRIMARY.load(Ordering::Relaxed) {
        "primary"
    } else {
        "replica"
    };
    ctx.log_notice(&format!(
        "valkey-sink: loaded (mode={:?}, debounce={}ms, threshold={}B, role={})",
        config.mode, config.debounce_window_ms, config.size_threshold_bytes, role,
    ));

    Status::Ok
}

/// Module teardown. Sends a Shutdown message to flush pending writes,
/// then waits up to 5 seconds for the background runtime to drain.
fn deinit(ctx: &Context) -> Status {
    ctx.log_notice("valkey-sink: shutting down, flushing pending writes...");

    if let Some(sender) = channel::sender() {
        let _ = sender.send(MainToBackground::Shutdown);
    }

    // Wait for the background flush to complete, up to 5 seconds.
    background::shutdown();

    ctx.log_notice("valkey-sink: shutdown complete");
    Status::Ok
}

/// Keyspace notification handler for SET commands.
///
/// Valkey calls this on every `@STRING` event. We filter for "set" events,
/// copy the value via a native GET call, and send the key+value through
/// the channel to the background runtime.
///
/// This function MUST NOT block. The channel send is `try_send` (non-blocking):
/// if the channel is full, the write is silently dropped. The next SET to the
/// same key will re-enqueue it.
fn keyspace_event_handler(ctx: &Context, _event_type: NotifyEvent, event: &str, key: &[u8]) {
    if event != "set" {
        return;
    }

    // On replicas, skip write-behind enqueue entirely. SINK.GET read-through
    // still works (the SET to populate cache will fail with READONLY, which
    // is silently ignored — the client still gets the value from the backend).
    if !IS_PRIMARY.load(Ordering::Relaxed) {
        return;
    }

    // If this SET was triggered by read-through cache populate, skip re-enqueue.
    // replace(false) atomically reads the flag and clears it in one operation.
    if SKIP_ENQUEUE.with(|f| f.replace(false)) {
        return;
    }

    let sender = match channel::sender() {
        Some(s) => s,
        None => return,
    };

    // Keyspace notifications don't include the value, so we fetch it via GET.
    // We use call_ext instead of call to get the raw CallResult — the standard
    // call() path converts CallReply::String via to_string().unwrap() which
    // panics on non-UTF8 binary values (valkey-module 0.1 bug).
    // Pass raw &[u8] key directly to avoid silent empty-string fallback on non-UTF8 keys.
    let call_opts = CallOptionsBuilder::new().errors_as_replies().build();
    let get_result: CallResult = ctx.call_ext("GET", &call_opts, &[key]);
    let value = match get_result {
        Ok(CallReply::String(ref reply)) => reply.as_bytes().to_vec(),
        _ => return,
    };

    // Capture TTL via PTTL (available since Redis 2.6). If the key has an expiry,
    // compute the absolute Unix epoch seconds. PTTL returns -1 for no expiry,
    // -2 for key-not-found (shouldn't happen here), or the remaining TTL in ms.
    // Use call_ext with raw bytes so non-UTF8 keys are handled correctly.
    let pttl_result: CallResult = ctx.call_ext("PTTL", &call_opts, &[key]);
    let expires_at = match pttl_result {
        Ok(CallReply::I64(ref reply)) => {
            let ms = reply.to_i64();
            if ms > 0 {
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                // Round up to seconds since both sinks store seconds.
                Some(now_secs + ((ms as u64) + 999) / 1000)
            } else {
                None // no expiry (-1) or key-not-found (-2)
            }
        }
        _ => None, // error or unexpected reply type
    };

    let _ = sender.try_send(MainToBackground::KeyWritten {
        key: key.to_vec(),
        value,
        expires_at,
    });
}

/// Construct the SinkRouter based on config.
///
/// In "both" mode, both S3 and DynamoDB sinks are created and writes
/// are routed by value size. In single-sink modes, only that sink is created.
///
/// A single `Arc<RefreshableCredentials>` is shared across all sinks so that
/// credential refresh happens once and is visible to both backends.
fn build_router(
    config: &SinkConfig,
    creds: &valkey_common::aws::Credentials,
    http_client: &reqwest::Client,
) -> SinkRouter {
    let shared_creds = std::sync::Arc::new(
        valkey_common::aws::RefreshableCredentials::new(creds.clone()),
    );

    let s3 = match config.mode {
        SinkMode::S3Only | SinkMode::Both => {
            let bucket = config
                .s3_bucket
                .clone()
                .expect("valkey-sink: s3_bucket is required for S3 mode");
            let region = config
                .s3_region
                .clone()
                .unwrap_or_else(|| std::env::var("AWS_REGION").unwrap_or("us-east-1".into()));

            Some(S3ExpressSink::new(
                http_client.clone(),
                shared_creds.clone(),
                bucket,
                config.s3_prefix.clone(),
                region,
                config.s3_endpoint.clone(),
                config.s3_write_concurrency,
            ))
        }
        SinkMode::DynamoOnly => None,
    };

    let dynamo = match config.mode {
        SinkMode::DynamoOnly | SinkMode::Both => {
            let table = config
                .dynamo_table
                .clone()
                .expect("valkey-sink: dynamo_table is required for DynamoDB mode");
            let region = config
                .dynamo_region
                .clone()
                .unwrap_or_else(|| std::env::var("AWS_REGION").unwrap_or("us-east-1".into()));

            Some(DynamoDbSink::new(
                http_client.clone(),
                shared_creds.clone(),
                table,
                region,
                config.dynamo_write_concurrency,
            ))
        }
        SinkMode::S3Only => None,
    };

    SinkRouter::new(s3, dynamo, config.size_threshold_bytes)
}

/// `SINK.INFO` command handler. Returns all module metrics as a bulk string
/// formatted like Valkey's INFO command output.
fn sink_info_command(_ctx: &Context, _args: Vec<ValkeyString>) -> ValkeyResult {
    let info = metrics::info_string();
    Ok(ValkeyValue::BulkString(info))
}

valkey_module! {
    name: "valkey-sink",
    version: 1,
    allocator: (ValkeyAlloc, ValkeyAlloc),
    data_types: [],
    init: init,
    deinit: deinit,
    commands: [
        ["SINK.GET", readthrough::sink_get_command, "readonly", 1, 1, 1],
        ["SINK.INFO", sink_info_command, "readonly", 0, 0, 0],
    ],
    event_handlers: [
        [@STRING: keyspace_event_handler]
    ],
}
