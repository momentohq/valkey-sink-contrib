/// SINK.GET command handler with BlockClient for async read-through.

use valkey_module::{
    CallOptionsBuilder, CallReply, CallResult, Context, ThreadSafeContext, ValkeyResult,
    ValkeyString, ValkeyValue,
};

use crate::channel::{self, MainToBackground};
use crate::metrics;

/// Handler for the `SINK.GET key` command.
///
/// 1. Calls native GET on the key.
/// 2. If the value is present, returns it immediately (as raw bytes).
/// 3. On cache miss, sends a ReadThrough message to the background runtime,
///    blocks the client (not the event loop), and waits for the result.
pub fn sink_get_command(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() != 2 {
        return Err(valkey_module::ValkeyError::WrongArity);
    }

    let key_str = &args[1];

    // Step 1: Try native GET.
    // Use call_ext to get the raw CallResult — the standard call() path
    // converts CallReply::String via to_string().unwrap() which panics
    // on non-UTF8 binary values (valkey-module 0.1 bug).
    let call_opts = CallOptionsBuilder::new().errors_as_replies().build();
    let get_result: CallResult = ctx.call_ext("GET", &call_opts, &[key_str]);

    // Step 2: If non-nil, return directly as a StringBuffer (binary-safe).
    match &get_result {
        Ok(CallReply::String(reply)) => {
            return Ok(ValkeyValue::StringBuffer(reply.as_bytes().to_vec()));
        }
        Ok(CallReply::Null(_)) => {} // cache miss, fall through
        _ => return Ok(ValkeyValue::Null),
    }

    // Step 3: Cache miss -- initiate read-through.
    metrics::inc(&metrics::get().read_throughs);

    // If read-through is disabled, return nil on cache miss instead of
    // falling through to the sink backends.
    if !crate::READ_THROUGH_ENABLED.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(ValkeyValue::Null);
    }

    let sender = match channel::sender() {
        Some(s) => s,
        None => {
            // Module not fully initialized or shutting down.
            return Ok(ValkeyValue::Null);
        }
    };

    let key = key_str.as_slice().to_vec();
    let (tx, rx) = tokio::sync::oneshot::channel();

    let msg = MainToBackground::ReadThrough {
        key: key.clone(),
        reply: tx,
    };

    if sender.try_send(msg).is_err() {
        // Channel full or closed -- return nil as graceful degradation.
        return Ok(ValkeyValue::Null);
    }

    // Step 4: Block the client so the event loop can keep serving others.
    let blocked_client = ctx.block_client();

    // Step 5: Spawn a bridging thread that waits on the oneshot and unblocks.
    // This thread does zero I/O; it only bridges the async oneshot to the
    // synchronous BlockClient unblock API via ThreadSafeContext.
    std::thread::spawn(move || {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);

        match rx.blocking_recv() {
            Ok(Some((value, expires_at))) => {
                metrics::inc(&metrics::get().read_through_hits);

                // Check if the data is already expired.
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if let Some(exp) = expires_at {
                    if now_secs >= exp {
                        // Data is expired — don't populate cache, return nil.
                        metrics::inc(&metrics::get().read_through_misses);
                        thread_ctx.reply(Ok(ValkeyValue::Null));
                        return;
                    }
                }

                // Populate cache so future GETs are served locally.
                // Set the skip flag so the keyspace handler doesn't re-enqueue
                // this value back to the sink (it was just read from there).
                // On replicas, the SET will fail with READONLY — this is fine,
                // the client still gets the value from the backend below.
                //
                // We use call() with byte slice args (&[&[u8]]) which is
                // binary-safe — StrCallArgs handles &[u8] via AsRef<[u8]>.
                {
                    let guard = thread_ctx.lock();
                    crate::SKIP_ENQUEUE.with(|f| f.set(true));

                    let set_result = if let Some(exp) = expires_at {
                        // SET with EX (remaining seconds).
                        let remaining = exp.saturating_sub(now_secs);
                        if remaining > 0 {
                            let remaining_str = remaining.to_string();
                            let args: &[&[u8]] = &[
                                key.as_slice(),
                                value.as_slice(),
                                b"EX",
                                remaining_str.as_bytes(),
                            ];
                            guard.call("SET", args)
                        } else {
                            // Expired during processing — skip populate.
                            crate::SKIP_ENQUEUE.with(|f| f.set(false));
                            thread_ctx.reply(Ok(ValkeyValue::Null));
                            return;
                        }
                    } else {
                        let args: &[&[u8]] = &[key.as_slice(), value.as_slice()];
                        guard.call("SET", args)
                    };

                    // On failure the notification may not have fired, so clear manually.
                    if set_result.is_err() {
                        crate::SKIP_ENQUEUE.with(|f| f.set(false));
                    }
                }

                // Reply to the blocked client with the raw bytes (binary-safe).
                thread_ctx.reply(Ok(ValkeyValue::StringBuffer(value)));
            }
            Ok(None) => {
                metrics::inc(&metrics::get().read_through_misses);
                thread_ctx.reply(Ok(ValkeyValue::Null));
            }
            Err(_) => {
                metrics::inc(&metrics::get().read_through_misses);
                thread_ctx.reply(Ok(ValkeyValue::Null));
            }
        }
    });

    // Return NoReply since the client is blocked and will be replied to later.
    Ok(ValkeyValue::NoReply)
}
