/// Sink configuration parsed from module arguments and CONFIG SET.
///
/// Configuration is passed as alternating key/value pairs when loading:
/// ```text
/// MODULE LOAD /path/to/module.so mode both s3_bucket my-bucket debounce_ms 500
/// ```
/// Both underscore and hyphen forms are accepted (e.g. `s3_bucket` or `s3-bucket`).

/// Which sink backends are enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkMode {
    /// Only S3 Express One Zone.
    S3Only,
    /// Only DynamoDB.
    DynamoOnly,
    /// Both — routes by value size (small → DynamoDB, large → S3).
    Both,
}

impl SinkMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "s3" | "s3only" => Some(Self::S3Only),
            "dynamo" | "dynamoonly" => Some(Self::DynamoOnly),
            "both" => Some(Self::Both),
            _ => None,
        }
    }
}

/// Full configuration for the valkey-sink module.
#[derive(Debug, Clone)]
pub struct SinkConfig {
    /// Which backends to use.
    pub mode: SinkMode,

    // -- S3 Express settings --
    /// S3 Express directory bucket name (e.g. "my-bucket--usw2-az3--x-s3").
    pub s3_bucket: Option<String>,
    /// Object key prefix (default: "valkey-sink/").
    pub s3_prefix: String,
    /// AWS region for S3 Express (falls back to AWS_REGION env var).
    pub s3_region: Option<String>,
    /// Custom S3 endpoint URL (for testing with LocalStack, MinIO, etc.).
    pub s3_endpoint: Option<String>,

    // -- DynamoDB settings --
    /// DynamoDB table name.
    pub dynamo_table: Option<String>,
    /// AWS region for DynamoDB (falls back to AWS_REGION env var).
    pub dynamo_region: Option<String>,

    // -- Routing --
    /// Values larger than this (bytes) go to S3; smaller go to DynamoDB.
    /// Only applies in "both" mode. Default: 64KB.
    pub size_threshold_bytes: usize,

    // -- Coalescing --
    /// How long to wait before flushing a key (milliseconds). Default: 1000.
    pub debounce_window_ms: u64,

    // -- Performance --
    /// Number of tokio worker threads in the background runtime.
    pub background_threads: usize,
    /// Bounded channel capacity (main → background). Backpressure drops on overflow.
    pub channel_capacity: usize,
    /// Max concurrent S3 PutObject requests per batch.
    pub s3_write_concurrency: usize,
    /// Max concurrent DynamoDB BatchWriteItem requests per flush.
    pub dynamo_write_concurrency: usize,
    /// Max retries per flush attempt before giving up.
    pub max_retries: u32,
    /// Base delay for exponential backoff (milliseconds).
    pub retry_base_delay_ms: u64,

    // -- Circuit breaker --
    /// Consecutive failures before the circuit breaker trips open.
    pub circuit_breaker_threshold: u32,
    /// Cooldown period when circuit is open (milliseconds).
    pub circuit_breaker_cooldown_ms: u64,

    // -- Coalesce map limits --
    /// Max entries in the coalesce map before a forced flush.
    pub coalesce_max_entries: usize,

    // -- Read-through --
    /// Whether SINK.GET should fall back to sinks on cache miss.
    pub read_through_enabled: bool,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            mode: SinkMode::S3Only,
            s3_bucket: None,
            s3_prefix: "valkey-sink/".to_string(),
            s3_region: None,
            s3_endpoint: None,
            dynamo_table: None,
            dynamo_region: None,
            size_threshold_bytes: 65_536, // 64KB
            debounce_window_ms: 1000,
            background_threads: 4,
            channel_capacity: 10_000,
            s3_write_concurrency: 16,
            dynamo_write_concurrency: 32,
            max_retries: 3,
            retry_base_delay_ms: 100,
            circuit_breaker_threshold: 10,
            circuit_breaker_cooldown_ms: 30_000,
            coalesce_max_entries: 1_000_000,
            read_through_enabled: true,
        }
    }
}

impl SinkConfig {
    /// Parse config from module argument strings.
    /// Expected format: alternating key/value pairs.
    /// e.g. ["mode", "both", "s3_bucket", "my-bucket", ...]
    pub fn from_args(args: &[String]) -> Self {
        let mut config = Self::default();
        let mut i = 0;
        while i + 1 < args.len() {
            let key = args[i].to_lowercase();
            let val = &args[i + 1];
            match key.as_str() {
                "mode" => {
                    if let Some(m) = SinkMode::from_str(val) {
                        config.mode = m;
                    }
                }
                "s3_bucket" | "s3-bucket" => config.s3_bucket = Some(val.clone()),
                "s3_prefix" | "s3-prefix" => config.s3_prefix = val.clone(),
                "s3_region" | "s3-region" => config.s3_region = Some(val.clone()),
                "s3_endpoint" | "s3-endpoint" => config.s3_endpoint = Some(val.clone()),
                "dynamo_table" | "dynamo-table" => config.dynamo_table = Some(val.clone()),
                "dynamo_region" | "dynamo-region" => config.dynamo_region = Some(val.clone()),
                "size_threshold" | "size-threshold" => {
                    if let Ok(v) = val.parse() {
                        config.size_threshold_bytes = v;
                    }
                }
                "debounce_ms" | "debounce-ms" => {
                    if let Ok(v) = val.parse() {
                        config.debounce_window_ms = v;
                    }
                }
                "background_threads" | "background-threads" => {
                    if let Ok(v) = val.parse() {
                        config.background_threads = v;
                    }
                }
                "channel_capacity" | "channel-capacity" => {
                    if let Ok(v) = val.parse() {
                        config.channel_capacity = v;
                    }
                }
                "s3_concurrency" | "s3-concurrency" => {
                    if let Ok(v) = val.parse() {
                        config.s3_write_concurrency = v;
                    }
                }
                "dynamo_concurrency" | "dynamo-concurrency" => {
                    if let Ok(v) = val.parse() {
                        config.dynamo_write_concurrency = v;
                    }
                }
                "max_retries" | "max-retries" => {
                    if let Ok(v) = val.parse() {
                        config.max_retries = v;
                    }
                }
                "retry_base_delay_ms" | "retry-base-delay-ms" => {
                    if let Ok(v) = val.parse() {
                        config.retry_base_delay_ms = v;
                    }
                }
                "circuit_breaker_threshold" | "circuit-breaker-threshold" => {
                    if let Ok(v) = val.parse() {
                        config.circuit_breaker_threshold = v;
                    }
                }
                "circuit_breaker_cooldown_ms" | "circuit-breaker-cooldown-ms" => {
                    if let Ok(v) = val.parse() {
                        config.circuit_breaker_cooldown_ms = v;
                    }
                }
                "coalesce_max_entries" | "coalesce-max-entries" => {
                    if let Ok(v) = val.parse() {
                        config.coalesce_max_entries = v;
                    }
                }
                "read_through" | "read-through" => {
                    config.read_through_enabled = val == "true" || val == "1" || val == "yes";
                }
                _ => {} // ignore unknown keys
            }
            i += 2;
        }
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let config = SinkConfig::default();
        assert_eq!(config.mode, SinkMode::S3Only);
        assert_eq!(config.debounce_window_ms, 1000);
        assert_eq!(config.size_threshold_bytes, 65_536);
        assert_eq!(config.max_retries, 3);
        assert!(config.read_through_enabled);
    }

    #[test]
    fn test_parse_mode() {
        assert_eq!(SinkMode::from_str("s3"), Some(SinkMode::S3Only));
        assert_eq!(SinkMode::from_str("S3Only"), Some(SinkMode::S3Only));
        assert_eq!(SinkMode::from_str("dynamo"), Some(SinkMode::DynamoOnly));
        assert_eq!(SinkMode::from_str("DynamoOnly"), Some(SinkMode::DynamoOnly));
        assert_eq!(SinkMode::from_str("both"), Some(SinkMode::Both));
        assert_eq!(SinkMode::from_str("BOTH"), Some(SinkMode::Both));
        assert_eq!(SinkMode::from_str("invalid"), None);
    }

    #[test]
    fn test_from_args_basic() {
        let args: Vec<String> = vec![
            "mode", "both",
            "s3_bucket", "my-bucket--az1--x-s3",
            "dynamo_table", "my-table",
            "debounce_ms", "500",
            "size_threshold", "1024",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let config = SinkConfig::from_args(&args);
        assert_eq!(config.mode, SinkMode::Both);
        assert_eq!(config.s3_bucket, Some("my-bucket--az1--x-s3".into()));
        assert_eq!(config.dynamo_table, Some("my-table".into()));
        assert_eq!(config.debounce_window_ms, 500);
        assert_eq!(config.size_threshold_bytes, 1024);
    }

    #[test]
    fn test_from_args_hyphen_form() {
        let args: Vec<String> = vec![
            "s3-bucket", "bucket",
            "dynamo-table", "table",
            "debounce-ms", "200",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let config = SinkConfig::from_args(&args);
        assert_eq!(config.s3_bucket, Some("bucket".into()));
        assert_eq!(config.dynamo_table, Some("table".into()));
        assert_eq!(config.debounce_window_ms, 200);
    }

    #[test]
    fn test_from_args_unknown_keys_ignored() {
        let args: Vec<String> = vec!["unknown_key", "value", "mode", "dynamo"]
            .into_iter()
            .map(String::from)
            .collect();

        let config = SinkConfig::from_args(&args);
        assert_eq!(config.mode, SinkMode::DynamoOnly);
    }

    #[test]
    fn test_from_args_invalid_numbers_ignored() {
        let args: Vec<String> = vec!["debounce_ms", "not_a_number"]
            .into_iter()
            .map(String::from)
            .collect();

        let config = SinkConfig::from_args(&args);
        assert_eq!(config.debounce_window_ms, 1000); // stays at default
    }

    #[test]
    fn test_read_through_parsing() {
        for val in &["true", "1", "yes"] {
            let args: Vec<String> = vec!["read_through", val]
                .into_iter()
                .map(|s| s.to_string())
                .collect();
            assert!(SinkConfig::from_args(&args).read_through_enabled);
        }
        for val in &["false", "0", "no"] {
            let args: Vec<String> = vec!["read_through", val]
                .into_iter()
                .map(|s| s.to_string())
                .collect();
            assert!(!SinkConfig::from_args(&args).read_through_enabled);
        }
    }

    #[test]
    fn test_empty_args() {
        let config = SinkConfig::from_args(&[]);
        assert_eq!(config.mode, SinkMode::S3Only);
        assert_eq!(config.debounce_window_ms, 1000);
    }

    #[test]
    fn test_odd_number_of_args() {
        let args: Vec<String> = vec!["mode", "both", "orphan"]
            .into_iter()
            .map(String::from)
            .collect();
        let config = SinkConfig::from_args(&args);
        assert_eq!(config.mode, SinkMode::Both); // "orphan" is ignored
    }

    #[test]
    fn test_all_numeric_config_fields() {
        let args: Vec<String> = vec![
            "background_threads", "8",
            "channel_capacity", "5000",
            "s3_concurrency", "32",
            "dynamo_concurrency", "64",
            "max_retries", "5",
            "retry_base_delay_ms", "200",
            "circuit_breaker_threshold", "20",
            "circuit_breaker_cooldown_ms", "60000",
            "coalesce_max_entries", "500000",
        ].into_iter().map(String::from).collect();
        let config = SinkConfig::from_args(&args);
        assert_eq!(config.background_threads, 8);
        assert_eq!(config.channel_capacity, 5000);
        assert_eq!(config.s3_write_concurrency, 32);
        assert_eq!(config.dynamo_write_concurrency, 64);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_base_delay_ms, 200);
        assert_eq!(config.circuit_breaker_threshold, 20);
        assert_eq!(config.circuit_breaker_cooldown_ms, 60000);
        assert_eq!(config.coalesce_max_entries, 500000);
    }

    #[test]
    fn test_all_mode_string_variations() {
        for s in &["s3", "s3only", "S3", "S3Only", "S3ONLY"] {
            assert_eq!(SinkMode::from_str(s), Some(SinkMode::S3Only), "failed for {}", s);
        }
        for s in &["dynamo", "dynamoonly", "DynamoOnly", "DYNAMOONLY"] {
            assert_eq!(SinkMode::from_str(s), Some(SinkMode::DynamoOnly), "failed for {}", s);
        }
        for s in &["both", "Both", "BOTH"] {
            assert_eq!(SinkMode::from_str(s), Some(SinkMode::Both), "failed for {}", s);
        }
    }

    #[test]
    fn test_s3_endpoint_config() {
        for key in &["s3_endpoint", "s3-endpoint"] {
            let args: Vec<String> = vec![key, "http://localhost:4566"]
                .into_iter().map(|s| s.to_string()).collect();
            let config = SinkConfig::from_args(&args);
            assert_eq!(config.s3_endpoint, Some("http://localhost:4566".into()));
        }
    }

    #[test]
    fn test_s3_prefix_override() {
        let args: Vec<String> = vec!["s3_prefix", "custom/prefix/"]
            .into_iter().map(String::from).collect();
        let config = SinkConfig::from_args(&args);
        assert_eq!(config.s3_prefix, "custom/prefix/");
    }
}
