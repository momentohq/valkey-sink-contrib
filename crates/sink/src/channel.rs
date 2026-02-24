/// Channel types connecting Valkey's main thread to the background runtime.

use std::sync::OnceLock;

use crossbeam_channel::{bounded, Sender, Receiver};

/// Messages sent from Valkey's main thread to the background runtime.
#[allow(dead_code)]
pub enum MainToBackground {
    /// A key was written (SET-family event). Key, value, and optional TTL are copied.
    KeyWritten {
        key: Vec<u8>,
        value: Vec<u8>,
        /// Absolute expiry time as Unix epoch seconds. None means no expiry.
        expires_at: Option<u64>,
    },

    /// A cache miss on SINK.GET. Background should look up in sink(s)
    /// and reply on the provided oneshot channel with (value, expires_at).
    ReadThrough {
        key: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<Option<(Vec<u8>, Option<u64>)>>,
    },

    /// Dynamic config change from CONFIG SET.
    ConfigUpdate(ConfigDelta),

    /// Module is unloading. Flush pending writes and shut down.
    Shutdown,
}

/// Incremental config changes that can be applied at runtime.
#[allow(dead_code)]
pub enum ConfigDelta {
    DebounceMs(u64),
    SizeThreshold(usize),
    ReadThroughEnabled(bool),
    SmallSink(String),
    LargeSink(String),
}

/// Global sender handle, initialized once during module load.
static SENDER: OnceLock<Sender<MainToBackground>> = OnceLock::new();

/// Create the bounded channel pair and store the sender globally.
/// Returns the receiver for the background runtime to consume.
pub fn init_channel(capacity: usize) -> Receiver<MainToBackground> {
    let (tx, rx) = bounded(capacity);
    SENDER
        .set(tx)
        .unwrap_or_else(|_| panic!("valkey-sink: channel already initialized"));
    rx
}

/// Get the global sender. Returns None if the channel hasn't been initialized.
pub fn sender() -> Option<&'static Sender<MainToBackground>> {
    SENDER.get()
}
