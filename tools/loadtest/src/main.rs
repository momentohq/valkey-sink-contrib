//! valkey-sink load tester
//!
//! Connects to a running Valkey instance, loads the valkey-sink module,
//! populates keys of various sizes (triggering write-behind to sinks),
//! waits for the debounce flush, evicts keys, and then runs thousands of
//! SINK.GET lookups to measure read-through latency from both backends.
//!
//! Configuration is read from a TOML file. CLI flags override TOML values.
//!
//! Usage:
//!   cargo run --release -- --config loadtest.toml --start-valkey
//!   cargo run --release -- --config loadtest.toml --skip-load
//!   cargo run --release -- --config loadtest.toml --emit-docker

use std::collections::BTreeMap;
use std::process::Command;
use std::time::{Duration, Instant};

use clap::Parser;
use rand::{Rng, SeedableRng};
use redis::AsyncCommands;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// TOML schema
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
#[serde(default)]
struct TomlConfig {
    valkey: TomlValkey,
    aws: TomlAws,
    docker: TomlDocker,
    module: TomlModule,
    bench: TomlBench,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct TomlAws {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    region: String,
    /// Fall back to EC2 IMDS if explicit creds are empty.
    imds_creds: bool,
}

#[derive(Deserialize)]
#[serde(default)]
struct TomlValkey {
    url: String,
}
impl Default for TomlValkey {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".into(),
        }
    }
}

#[derive(Deserialize)]
#[serde(default)]
struct TomlDocker {
    image: String,
    container_name: String,
    network: String,
    host_module_path: String,
    container_module_path: String,
}
impl Default for TomlDocker {
    fn default() -> Self {
        Self {
            image: "valkey/valkey:9".into(),
            container_name: "valkey".into(),
            network: "host".into(),
            host_module_path: String::new(),
            container_module_path: "/modules/libvalkey_sink.so".into(),
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct TomlModule {
    path: Option<String>,
    skip_load: bool,
    unload: bool,
    config: BTreeMap<String, toml::Value>,
}

#[derive(Deserialize)]
#[serde(default)]
struct TomlBench {
    keys: usize,
    large_fraction: f64,
    ttl_fraction: f64,
    ttl_secs: u64,
    drain_secs: u64,
    concurrency: usize,
    small_key: TomlKeyRange,
    large_key: TomlKeyRange,
    /// If > 0, run sustained writes for this many seconds instead of burst populate.
    sustained_secs: u64,
    /// Target writes per second for sustained mode. 0 = unlimited.
    target_tps: u64,
    /// Seconds between periodic stat prints in sustained mode.
    report_interval: u64,
}
impl Default for TomlBench {
    fn default() -> Self {
        Self {
            keys: 3000,
            large_fraction: 0.2,
            ttl_fraction: 0.3,
            ttl_secs: 300,
            drain_secs: 3,
            concurrency: 16,
            small_key: TomlKeyRange { min: 64, max: 4096 },
            large_key: TomlKeyRange {
                min: 65_536,
                max: 131_072,
            },
            sustained_secs: 0,
            target_tps: 0,
            report_interval: 10,
        }
    }
}

#[derive(Deserialize, Clone)]
struct TomlKeyRange {
    min: usize,
    max: usize,
}

// ---------------------------------------------------------------------------
// CLI args — override TOML values
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "valkey-sink-loadtest")]
#[command(about = "End-to-end load tester and benchmark for valkey-sink")]
struct Args {
    /// Path to the TOML config file
    #[arg(long, short)]
    config: Option<String>,

    /// Print the `docker run` command (from TOML) and exit
    #[arg(long)]
    emit_docker: bool,

    /// Start Valkey via Docker (from TOML config) before running the benchmark
    #[arg(long)]
    start_valkey: bool,

    /// Stop and remove the Valkey Docker container, then exit
    #[arg(long)]
    stop_valkey: bool,

    /// Valkey URL (e.g. redis://127.0.0.1:6379)
    #[arg(long)]
    url: Option<String>,

    /// Path to the compiled module .so/.dylib
    #[arg(long)]
    module_path: Option<String>,

    /// Skip MODULE LOAD (module is already loaded)
    #[arg(long)]
    skip_load: bool,

    /// Total number of keys to test
    #[arg(long)]
    keys: Option<usize>,

    /// Fraction of keys with large values (routes to S3 sink)
    #[arg(long)]
    large_fraction: Option<f64>,

    /// Fraction of keys with a TTL
    #[arg(long)]
    ttl_fraction: Option<f64>,

    /// TTL in seconds for keys that have one
    #[arg(long)]
    ttl_secs: Option<u64>,

    /// Seconds to wait for debounce flush before evicting
    #[arg(long)]
    drain_secs: Option<u64>,

    /// Number of concurrent connections for the read-through phase
    #[arg(long)]
    concurrency: Option<usize>,

    /// Small key value size min (bytes)
    #[arg(long)]
    small_min: Option<usize>,

    /// Small key value size max (bytes)
    #[arg(long)]
    small_max: Option<usize>,

    /// Large key value size min (bytes)
    #[arg(long)]
    large_min: Option<usize>,

    /// Large key value size max (bytes)
    #[arg(long)]
    large_max: Option<usize>,

    /// Unload the module when done
    #[arg(long)]
    unload: bool,

    /// Run sustained writes for N seconds (instead of burst populate+drain+read-through)
    #[arg(long)]
    sustained_secs: Option<u64>,

    /// Target writes per second in sustained mode (0 = unlimited)
    #[arg(long)]
    target_tps: Option<u64>,

    /// Seconds between periodic stat reports in sustained mode
    #[arg(long)]
    report_interval: Option<u64>,
}

// ---------------------------------------------------------------------------
// Merged config
// ---------------------------------------------------------------------------

struct Config {
    url: String,
    module_path: Option<String>,
    skip_load: bool,
    unload: bool,
    module_args: Vec<String>,
    keys: usize,
    large_fraction: f64,
    ttl_fraction: f64,
    ttl_secs: u64,
    drain_secs: u64,
    concurrency: usize,
    small_range: (usize, usize),
    large_range: (usize, usize),
    size_threshold: usize,
    sustained_secs: u64,
    target_tps: u64,
    report_interval: u64,
}

impl Config {
    fn from_args_and_toml(args: &Args, toml: &TomlConfig) -> Self {
        let mut module_args = Vec::new();
        for (k, v) in &toml.module.config {
            module_args.push(k.clone());
            match v {
                toml::Value::String(s) => module_args.push(s.clone()),
                toml::Value::Integer(n) => module_args.push(n.to_string()),
                toml::Value::Float(f) => module_args.push(f.to_string()),
                toml::Value::Boolean(b) => module_args.push(b.to_string()),
                other => module_args.push(other.to_string()),
            }
        }

        let size_threshold = toml
            .module
            .config
            .get("size_threshold")
            .and_then(|v| match v {
                toml::Value::Integer(n) => Some(*n as usize),
                toml::Value::String(s) => s.parse().ok(),
                _ => None,
            })
            .unwrap_or(65_536);

        let small_min = args.small_min.unwrap_or(toml.bench.small_key.min);
        let small_max = args.small_max.unwrap_or(toml.bench.small_key.max);
        let large_min = args.large_min.unwrap_or(toml.bench.large_key.min);
        let large_max = args.large_max.unwrap_or(toml.bench.large_key.max);

        Self {
            url: args.url.clone().unwrap_or_else(|| toml.valkey.url.clone()),
            module_path: args
                .module_path
                .clone()
                .or_else(|| toml.module.path.clone()),
            skip_load: args.skip_load || toml.module.skip_load,
            unload: args.unload || toml.module.unload,
            module_args,
            keys: args.keys.unwrap_or(toml.bench.keys),
            large_fraction: args.large_fraction.unwrap_or(toml.bench.large_fraction),
            ttl_fraction: args.ttl_fraction.unwrap_or(toml.bench.ttl_fraction),
            ttl_secs: args.ttl_secs.unwrap_or(toml.bench.ttl_secs),
            drain_secs: args.drain_secs.unwrap_or(toml.bench.drain_secs),
            concurrency: args.concurrency.unwrap_or(toml.bench.concurrency),
            small_range: (small_min, small_max),
            large_range: (large_min, large_max),
            size_threshold,
            sustained_secs: args.sustained_secs.unwrap_or(toml.bench.sustained_secs),
            target_tps: args.target_tps.unwrap_or(toml.bench.target_tps),
            report_interval: args.report_interval.unwrap_or(toml.bench.report_interval),
        }
    }
}

// ---------------------------------------------------------------------------
// Docker helpers
// ---------------------------------------------------------------------------

/// Resolved AWS credentials: (access_key_id, secret_access_key, session_token).
/// Returns None if no credentials are available.
struct AwsCreds {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    region: String,
}

/// Resolve AWS credentials from the TOML config.
/// Priority: explicit [aws] creds > IMDS > none.
fn resolve_aws_creds(toml: &TomlConfig) -> Result<Option<AwsCreds>, Box<dyn std::error::Error>> {
    let aws = &toml.aws;

    // Region: explicit [aws].region > [module.config] s3_region/dynamo_region
    let region = if !aws.region.is_empty() {
        aws.region.clone()
    } else if let Some(toml::Value::String(r)) = toml.module.config.get("s3_region") {
        r.clone()
    } else if let Some(toml::Value::String(r)) = toml.module.config.get("dynamo_region") {
        r.clone()
    } else {
        String::new()
    };

    // Explicit creds in TOML
    if !aws.access_key_id.is_empty() && !aws.secret_access_key.is_empty() {
        return Ok(Some(AwsCreds {
            access_key_id: aws.access_key_id.clone(),
            secret_access_key: aws.secret_access_key.clone(),
            session_token: aws.session_token.clone(),
            region,
        }));
    }

    // IMDS fallback
    if aws.imds_creds {
        println!("[AWS] Fetching credentials from IMDS...");
        let (ak, sk, st) = fetch_imds_creds()?;
        return Ok(Some(AwsCreds {
            access_key_id: ak,
            secret_access_key: sk,
            session_token: st,
            region,
        }));
    }

    Ok(None)
}

/// Build the `docker run` command line from TOML config.
/// For --emit-docker: shows placeholders for IMDS creds, real values for explicit creds.
fn build_docker_cmd(toml: &TomlConfig) -> Vec<String> {
    let d = &toml.docker;
    let aws = &toml.aws;
    let module_path = toml
        .module
        .path
        .as_deref()
        .unwrap_or(&d.container_module_path);

    let mut cmd = vec![
        "docker".into(),
        "run".into(),
        "-d".into(),
        "--name".into(),
        d.container_name.clone(),
    ];

    if !d.network.is_empty() {
        cmd.push("--network".into());
        cmd.push(d.network.clone());
    }

    // Mount the .so
    if !d.host_module_path.is_empty() {
        cmd.push("-v".into());
        cmd.push(format!(
            "{}:{}:ro",
            d.host_module_path, d.container_module_path
        ));
    }

    // AWS env vars
    if !aws.access_key_id.is_empty() {
        cmd.push("-e".into());
        cmd.push(format!("AWS_ACCESS_KEY_ID={}", aws.access_key_id));
        cmd.push("-e".into());
        cmd.push(format!("AWS_SECRET_ACCESS_KEY={}", aws.secret_access_key));
        if !aws.session_token.is_empty() {
            cmd.push("-e".into());
            cmd.push(format!("AWS_SESSION_TOKEN={}", aws.session_token));
        }
    } else if aws.imds_creds {
        cmd.push("-e".into());
        cmd.push("AWS_ACCESS_KEY_ID=<from-imds>".into());
        cmd.push("-e".into());
        cmd.push("AWS_SECRET_ACCESS_KEY=<from-imds>".into());
        cmd.push("-e".into());
        cmd.push("AWS_SESSION_TOKEN=<from-imds>".into());
    }

    // Region
    let region = if !aws.region.is_empty() {
        Some(aws.region.clone())
    } else if let Some(toml::Value::String(r)) = toml.module.config.get("s3_region") {
        Some(r.clone())
    } else if let Some(toml::Value::String(r)) = toml.module.config.get("dynamo_region") {
        Some(r.clone())
    } else {
        None
    };
    if let Some(region) = region {
        cmd.push("-e".into());
        cmd.push(format!("AWS_REGION={}", region));
    }

    // Image
    cmd.push(d.image.clone());

    // Valkey server + --loadmodule with module args
    cmd.push("valkey-server".into());
    cmd.push("--loadmodule".into());
    cmd.push(module_path.into());
    for (k, v) in &toml.module.config {
        cmd.push(k.clone());
        match v {
            toml::Value::String(s) => cmd.push(s.clone()),
            toml::Value::Integer(n) => cmd.push(n.to_string()),
            toml::Value::Float(f) => cmd.push(f.to_string()),
            toml::Value::Boolean(b) => cmd.push(b.to_string()),
            other => cmd.push(other.to_string()),
        }
    }

    cmd
}

/// Format the docker command as a shell string for display.
fn format_docker_cmd(cmd: &[String]) -> String {
    let mut out = String::new();
    for (i, part) in cmd.iter().enumerate() {
        if i > 0 {
            // Break lines at flag boundaries for readability
            if part.starts_with('-') || part == "valkey-server" {
                out.push_str(" \\\n  ");
            } else {
                out.push(' ');
            }
        }
        if part.contains(' ') || part.contains('$') {
            out.push('"');
            out.push_str(part);
            out.push('"');
        } else {
            out.push_str(part);
        }
    }
    out
}

/// Fetch IMDS credentials and return (access_key, secret_key, session_token).
fn fetch_imds_creds() -> Result<(String, String, String), Box<dyn std::error::Error>> {
    // IMDSv2: get token first
    let token = Command::new("curl")
        .args([
            "-s",
            "-X",
            "PUT",
            "http://169.254.169.254/latest/api/token",
            "-H",
            "X-aws-ec2-metadata-token-ttl-seconds: 21600",
        ])
        .output()?;
    let token = String::from_utf8_lossy(&token.stdout).trim().to_string();

    // Get role name
    let role = Command::new("curl")
        .args([
            "-s",
            "-H",
            &format!("X-aws-ec2-metadata-token: {}", token),
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
        ])
        .output()?;
    let role = String::from_utf8_lossy(&role.stdout).trim().to_string();

    // Get credentials JSON
    let creds = Command::new("curl")
        .args([
            "-s",
            "-H",
            &format!("X-aws-ec2-metadata-token: {}", token),
            &format!(
                "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}",
                role
            ),
        ])
        .output()?;
    let creds_json: serde_json::Value =
        serde_json::from_slice(&creds.stdout).map_err(|e| format!("IMDS JSON parse: {}", e))?;

    let ak = creds_json["AccessKeyId"]
        .as_str()
        .ok_or("missing AccessKeyId")?
        .to_string();
    let sk = creds_json["SecretAccessKey"]
        .as_str()
        .ok_or("missing SecretAccessKey")?
        .to_string();
    let st = creds_json["Token"]
        .as_str()
        .ok_or("missing Token")?
        .to_string();

    Ok((ak, sk, st))
}

/// Start Valkey via Docker using the TOML config.
fn start_valkey(toml: &TomlConfig) -> Result<(), Box<dyn std::error::Error>> {
    let d = &toml.docker;
    let module_path = toml
        .module
        .path
        .as_deref()
        .unwrap_or(&d.container_module_path);

    // Remove existing container if present
    println!("[DOCKER] Removing existing '{}' container...", d.container_name);
    let _ = Command::new("docker")
        .args(["rm", "-f", &d.container_name])
        .output();

    // Build the real command
    let mut cmd = Command::new("docker");
    cmd.args(["run", "-d", "--name", &d.container_name]);

    if !d.network.is_empty() {
        cmd.args(["--network", &d.network]);
    }

    if !d.host_module_path.is_empty() {
        cmd.args([
            "-v",
            &format!("{}:{}:ro", d.host_module_path, d.container_module_path),
        ]);
    }

    // AWS creds: explicit > IMDS > none
    if let Some(creds) = resolve_aws_creds(toml)? {
        cmd.args(["-e", &format!("AWS_ACCESS_KEY_ID={}", creds.access_key_id)]);
        cmd.args(["-e", &format!("AWS_SECRET_ACCESS_KEY={}", creds.secret_access_key)]);
        if !creds.session_token.is_empty() {
            cmd.args(["-e", &format!("AWS_SESSION_TOKEN={}", creds.session_token)]);
        }
        if !creds.region.is_empty() {
            cmd.args(["-e", &format!("AWS_REGION={}", creds.region)]);
        }
    }

    // Image + valkey-server --loadmodule
    cmd.arg(&d.image);
    cmd.args(["valkey-server", "--loadmodule", module_path]);
    for (k, v) in &toml.module.config {
        cmd.arg(k);
        match v {
            toml::Value::String(s) => cmd.arg(s),
            toml::Value::Integer(n) => cmd.arg(n.to_string()),
            toml::Value::Float(f) => cmd.arg(f.to_string()),
            toml::Value::Boolean(b) => cmd.arg(b.to_string()),
            other => cmd.arg(other.to_string()),
        };
    }

    println!("[DOCKER] Starting Valkey...");
    let output = cmd.output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("docker run failed: {}", stderr).into());
    }

    let container_id = String::from_utf8_lossy(&output.stdout)
        .trim()
        .chars()
        .take(12)
        .collect::<String>();
    println!("[DOCKER] Container started: {}", container_id);

    // Wait for Valkey to be ready
    println!("[DOCKER] Waiting for Valkey to be ready...");
    for _ in 0..30 {
        std::thread::sleep(Duration::from_millis(500));
        let ping = Command::new("docker")
            .args(["exec", &d.container_name, "valkey-cli", "PING"])
            .output();
        if let Ok(out) = ping {
            let reply = String::from_utf8_lossy(&out.stdout);
            if reply.trim() == "PONG" {
                println!("[DOCKER] Valkey is ready.");
                return Ok(());
            }
        }
    }

    Err("Valkey did not become ready within 15 seconds".into())
}

/// Stop and remove the Valkey Docker container.
fn stop_valkey(toml: &TomlConfig) -> Result<(), Box<dyn std::error::Error>> {
    let name = &toml.docker.container_name;
    println!("[DOCKER] Stopping '{}'...", name);
    let output = Command::new("docker")
        .args(["rm", "-f", name])
        .output()?;
    if output.status.success() {
        println!("[DOCKER] Container '{}' removed.", name);
    } else {
        println!(
            "[DOCKER] Container '{}' not found or already removed.",
            name
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct KeyMeta {
    key: String,
    size: usize,
    ttl: Option<u64>,
}

fn report_latencies(label: &str, samples: &mut [Duration]) {
    if samples.is_empty() {
        println!("  {}: no samples", label);
        return;
    }
    samples.sort();
    let n = samples.len();
    let sum: Duration = samples.iter().sum();
    let avg = sum / n as u32;
    let min = samples[0];
    let p50 = samples[n * 50 / 100];
    let p90 = samples[n * 90 / 100];
    let p99 = samples[n * 99 / 100];
    let max = samples[n - 1];
    let wall_clock = sum.as_secs_f64();
    let throughput = n as f64 / wall_clock;

    println!(
        "  {:<20} n={:<6} min={:<10} avg={:<10} p50={:<10} p90={:<10} p99={:<10} max={:<10} {:.0} ops/s",
        label, n,
        fmt_dur(min), fmt_dur(avg), fmt_dur(p50), fmt_dur(p90), fmt_dur(p99), fmt_dur(max),
        throughput,
    );
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{}us", us)
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

fn random_value(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen::<u8>()).collect()
}

fn key_name(i: usize) -> String {
    format!("loadtest:sink:{:06}", i)
}

// ---------------------------------------------------------------------------
// Sustained write mode
// ---------------------------------------------------------------------------

/// Parse a u64 from a SINK.INFO line like "sink_dynamo_writes:1234"
fn parse_info_val(info: &str, prefix: &str) -> u64 {
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(prefix) {
            return rest.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// Parse a latency line like "count=10 min=1.0ms avg=2.0ms p50=1.5ms p99=5.0ms max=10.0ms"
fn parse_info_latency(info: &str, prefix: &str) -> String {
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(prefix) {
            return rest.trim().to_string();
        }
    }
    "count=0".to_string()
}

/// Run sustained writes at a target TPS for a given duration.
/// Uses `concurrency` parallel tokio tasks, each with its own Redis connection,
/// writing unique keys at a rate-limited pace.
async fn run_sustained(
    cfg: &Config,
    conn: &mut redis::aio::MultiplexedConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    let duration = Duration::from_secs(cfg.sustained_secs);
    let report_interval = Duration::from_secs(cfg.report_interval);

    println!("===============================================================================");
    println!("  valkey-sink SUSTAINED LOAD TEST");
    println!("===============================================================================");
    println!("  target:       {}", cfg.url);
    println!("  duration:     {}s", cfg.sustained_secs);
    println!(
        "  target TPS:   {} (total writes/s)",
        if cfg.target_tps == 0 {
            "unlimited".to_string()
        } else {
            format!("{}", cfg.target_tps)
        }
    );
    println!("  concurrency:  {}", cfg.concurrency);
    println!(
        "  large frac:   {:.0}% | ttl frac: {:.0}%",
        cfg.large_fraction * 100.0,
        cfg.ttl_fraction * 100.0,
    );
    println!(
        "  small range:  {}-{} bytes | large range: {}-{} bytes",
        cfg.small_range.0, cfg.small_range.1, cfg.large_range.0, cfg.large_range.1,
    );
    if !cfg.module_args.is_empty() {
        println!("  module args:  {}", cfg.module_args.join(" "));
    }
    println!();

    // Snapshot initial metrics
    let info_before: String = redis::cmd("SINK.INFO").query_async(conn).await?;
    let dynamo_writes_before = parse_info_val(&info_before, "sink_dynamo_writes:");
    let s3_writes_before = parse_info_val(&info_before, "sink_s3_writes:");
    let dynamo_failures_before = parse_info_val(&info_before, "sink_dynamo_write_failures:");
    let s3_failures_before = parse_info_val(&info_before, "sink_s3_write_failures:");

    // Shared atomic counter for total writes issued
    let writes_issued = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Compute per-task rate limit
    let tps_per_task = if cfg.target_tps > 0 {
        cfg.target_tps / cfg.concurrency as u64
    } else {
        0 // unlimited
    };
    let interval_per_write = if tps_per_task > 0 {
        Duration::from_secs_f64(1.0 / tps_per_task as f64)
    } else {
        Duration::ZERO
    };

    // Spawn writer tasks
    let mut handles = Vec::new();
    for task_id in 0..cfg.concurrency {
        let url = cfg.url.clone();
        let writes_issued = writes_issued.clone();
        let stop = stop.clone();
        let large_fraction = cfg.large_fraction;
        let ttl_fraction = cfg.ttl_fraction;
        let ttl_secs = cfg.ttl_secs;
        let small_range = cfg.small_range;
        let large_range = cfg.large_range;
        let interval = interval_per_write;

        let handle = tokio::spawn(async move {
            let client = redis::Client::open(url.as_str()).unwrap();
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();
            let mut rng = rand::rngs::StdRng::from_entropy();
            let mut local_count = 0u64;
            let mut next_write = Instant::now();

            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                // Rate limit
                if !interval.is_zero() {
                    let now = Instant::now();
                    if now < next_write {
                        tokio::time::sleep(next_write - now).await;
                    }
                    next_write += interval;
                }

                // Generate unique key using task_id + local_count
                let key = format!("loadtest:sustained:{}:{:08}", task_id, local_count);
                let is_large = rng.gen::<f64>() < large_fraction;
                let size = if is_large {
                    rng.gen_range(large_range.0..large_range.1)
                } else {
                    rng.gen_range(small_range.0..small_range.1)
                };
                let value = random_value(size);
                let has_ttl = rng.gen::<f64>() < ttl_fraction;

                let result = if has_ttl {
                    redis::cmd("SET")
                        .arg(&key)
                        .arg(value)
                        .arg("EX")
                        .arg(ttl_secs)
                        .query_async::<()>(&mut conn)
                        .await
                } else {
                    redis::cmd("SET")
                        .arg(&key)
                        .arg(value)
                        .query_async::<()>(&mut conn)
                        .await
                };

                if result.is_ok() {
                    local_count += 1;
                    writes_issued.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    // Periodic reporting loop
    let start = Instant::now();
    let mut last_report = Instant::now();
    let mut last_dynamo = dynamo_writes_before;
    let mut last_s3 = s3_writes_before;
    let mut last_issued = 0u64;

    println!("{:<8} {:>10} {:>10} {:>10} {:>12} {:>12} {:>12}",
        "elapsed", "issued", "ddb_flush", "s3_flush", "ddb_tps", "s3_tps", "issue_tps");
    println!("{}", "-".repeat(80));

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let elapsed = start.elapsed();

        if elapsed >= duration {
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            break;
        }

        if last_report.elapsed() >= report_interval {
            let info: String = redis::cmd("SINK.INFO").query_async(conn).await?;
            let dynamo_now = parse_info_val(&info, "sink_dynamo_writes:");
            let s3_now = parse_info_val(&info, "sink_s3_writes:");
            let issued_now = writes_issued.load(std::sync::atomic::Ordering::Relaxed);

            let dt = last_report.elapsed().as_secs_f64();
            let ddb_tps = (dynamo_now - last_dynamo) as f64 / dt;
            let s3_tps = (s3_now - last_s3) as f64 / dt;
            let issue_tps = (issued_now - last_issued) as f64 / dt;

            println!("{:<8.0}s {:>10} {:>10} {:>10} {:>12.0} {:>12.0} {:>12.0}",
                elapsed.as_secs_f64(),
                issued_now,
                dynamo_now - dynamo_writes_before,
                s3_now - s3_writes_before,
                ddb_tps, s3_tps, issue_tps);

            last_dynamo = dynamo_now;
            last_s3 = s3_now;
            last_issued = issued_now;
            last_report = Instant::now();
        }
    }

    // Wait for all writer tasks to finish
    for handle in handles {
        let _ = handle.await;
    }

    // Final drain
    println!();
    println!("[DRAIN] Waiting {}s for final flush...", cfg.drain_secs);
    tokio::time::sleep(Duration::from_secs(cfg.drain_secs)).await;

    // Final metrics
    let info: String = redis::cmd("SINK.INFO").query_async(conn).await?;
    let total_elapsed = start.elapsed();
    let total_issued = writes_issued.load(std::sync::atomic::Ordering::Relaxed);
    let dynamo_total = parse_info_val(&info, "sink_dynamo_writes:") - dynamo_writes_before;
    let s3_total = parse_info_val(&info, "sink_s3_writes:") - s3_writes_before;
    let dynamo_fail = parse_info_val(&info, "sink_dynamo_write_failures:") - dynamo_failures_before;
    let s3_fail = parse_info_val(&info, "sink_s3_write_failures:") - s3_failures_before;
    let write_failures = parse_info_val(&info, "sink_write_failures:");
    let coalesce_size = parse_info_val(&info, "sink_coalesce_map_size:");

    println!();
    println!("===============================================================================");
    println!("  SUSTAINED LOAD TEST RESULTS");
    println!("===============================================================================");
    println!("  Duration:              {:.1}s", total_elapsed.as_secs_f64());
    println!("  Writes issued:         {}", total_issued);
    println!("  Avg issue rate:        {:.0} writes/s", total_issued as f64 / cfg.sustained_secs as f64);
    println!();
    println!("  DynamoDB flushed:      {}", dynamo_total);
    println!("  DynamoDB failures:     {}", dynamo_fail);
    println!("  DynamoDB avg TPS:      {:.0}", dynamo_total as f64 / cfg.sustained_secs as f64);
    println!();
    println!("  S3 Express flushed:    {}", s3_total);
    println!("  S3 Express failures:   {}", s3_fail);
    println!("  S3 Express avg TPS:    {:.0}", s3_total as f64 / cfg.sustained_secs as f64);
    println!();
    println!("  Flush failures:        {}", write_failures);
    println!("  Coalesce map pending:  {}", coalesce_size);
    println!();
    println!("  --- Write Latency ---");
    println!("  DynamoDB:  {}", parse_info_latency(&info, "sink_dynamo_write_latency:"));
    println!("  S3:        {}", parse_info_latency(&info, "sink_s3_write_latency:"));
    println!("  Flush:     {}", parse_info_latency(&info, "sink_flush_latency:"));
    println!();
    println!("  --- Read Latency (from prior lookups) ---");
    println!("  DynamoDB:  {}", parse_info_latency(&info, "sink_dynamo_read_latency:"));
    println!("  S3:        {}", parse_info_latency(&info, "sink_s3_read_latency:"));
    println!("  Combined:  {}", parse_info_latency(&info, "sink_read_through_latency:"));
    println!();

    // Full SINK.INFO
    println!("=== Final SINK.INFO ===");
    println!("{}", info);

    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load TOML config (or use defaults)
    let toml_cfg: TomlConfig = if let Some(path) = &args.config {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file '{}': {}", path, e))?;
        toml::from_str(&text)
            .map_err(|e| format!("failed to parse config file '{}': {}", path, e))?
    } else {
        TomlConfig::default()
    };

    // --emit-docker: print the docker run command and exit
    if args.emit_docker {
        let cmd = build_docker_cmd(&toml_cfg);
        println!("{}", format_docker_cmd(&cmd));
        return Ok(());
    }

    // --stop-valkey: remove the container and exit
    if args.stop_valkey {
        stop_valkey(&toml_cfg)?;
        return Ok(());
    }

    // --start-valkey: start Valkey via Docker before benchmarking
    if args.start_valkey {
        start_valkey(&toml_cfg)?;
        println!();
    }

    let cfg = Config::from_args_and_toml(&args, &toml_cfg);

    println!("===============================================================================");
    println!("  valkey-sink load test");
    println!("===============================================================================");
    println!("  target:      {}", cfg.url);
    println!(
        "  keys:        {} | large: {:.0}% | ttl: {:.0}% | concurrency: {}",
        cfg.keys,
        cfg.large_fraction * 100.0,
        cfg.ttl_fraction * 100.0,
        cfg.concurrency,
    );
    println!(
        "  small range: {}-{} bytes | large range: {}-{} bytes | threshold: {} bytes",
        cfg.small_range.0, cfg.small_range.1, cfg.large_range.0, cfg.large_range.1,
        cfg.size_threshold,
    );
    if !cfg.module_args.is_empty() {
        println!("  module args: {}", cfg.module_args.join(" "));
    }
    println!();

    let client = redis::Client::open(cfg.url.as_str())?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // -- Load module (via MODULE LOAD, not --loadmodule) --
    if !cfg.skip_load && !args.start_valkey {
        let module_path = cfg.module_path.as_deref().ok_or(
            "--module-path (or [module] path in TOML) is required unless --skip-load or --start-valkey is set",
        )?;

        println!("[SETUP] Loading module: {}", module_path);
        let mut cmd = redis::cmd("MODULE");
        cmd.arg("LOAD").arg(module_path);
        for part in &cfg.module_args {
            cmd.arg(part);
        }
        match cmd.query_async::<String>(&mut conn).await {
            Ok(_) => println!("  Module loaded successfully."),
            Err(e) => {
                let msg = format!("{}", e);
                if msg.contains("already loaded") || msg.contains("ERR Error loading") {
                    println!("  Module appears already loaded: {}", msg);
                } else {
                    return Err(e.into());
                }
            }
        }
        println!();
    }

    // -- Verify module is responding --
    let info: String = redis::cmd("SINK.INFO")
        .query_async(&mut conn)
        .await
        .map_err(|e| format!("SINK.INFO failed -- is valkey-sink module loaded? ({})", e))?;
    println!("[SETUP] Module responding. Initial SINK.INFO:");
    for line in info.lines().take(6) {
        println!("  {}", line);
    }
    println!("  ...");
    println!();

    // =========================================================================
    // Sustained mode — runs a different flow entirely
    // =========================================================================
    if cfg.sustained_secs > 0 {
        run_sustained(&cfg, &mut conn).await?;

        if cfg.unload {
            println!("Unloading module...");
            let _: Result<String, _> = redis::cmd("MODULE")
                .arg("UNLOAD")
                .arg("valkey-sink")
                .query_async(&mut conn)
                .await;
        }
        return Ok(());
    }

    // =========================================================================
    // Phase 1: POPULATE (burst mode)
    // =========================================================================
    println!("[1/5] POPULATE: SET {} keys with varying sizes...", cfg.keys);
    let mut rng = rand::thread_rng();
    let metas: Vec<KeyMeta> = (0..cfg.keys)
        .map(|i| {
            let is_large = rng.gen::<f64>() < cfg.large_fraction;
            let size = if is_large {
                rng.gen_range(cfg.large_range.0..cfg.large_range.1)
            } else {
                rng.gen_range(cfg.small_range.0..cfg.small_range.1)
            };
            let has_ttl = rng.gen::<f64>() < cfg.ttl_fraction;
            let ttl = if has_ttl { Some(cfg.ttl_secs) } else { None };
            KeyMeta {
                key: key_name(i),
                size,
                ttl,
            }
        })
        .collect();

    let mut small_count = 0usize;
    let mut large_count = 0usize;
    let mut ttl_count = 0usize;
    let mut total_bytes = 0usize;
    let mut populate_samples = Vec::with_capacity(cfg.keys);

    for meta in &metas {
        let value = random_value(meta.size);
        total_bytes += meta.size;
        if meta.size >= cfg.size_threshold {
            large_count += 1;
        } else {
            small_count += 1;
        }

        let start = Instant::now();
        if let Some(ttl) = meta.ttl {
            ttl_count += 1;
            redis::cmd("SET")
                .arg(&meta.key)
                .arg(value)
                .arg("EX")
                .arg(ttl)
                .query_async::<()>(&mut conn)
                .await?;
        } else {
            conn.set::<_, _, ()>(&meta.key, value).await?;
        }
        populate_samples.push(start.elapsed());
    }

    println!(
        "  {} small (<{}->DynamoDB), {} large (>={}->S3), {} with TTL, {:.1} MB total",
        small_count,
        cfg.size_threshold,
        large_count,
        cfg.size_threshold,
        ttl_count,
        total_bytes as f64 / (1024.0 * 1024.0),
    );
    report_latencies("SET", &mut populate_samples);
    println!();

    // =========================================================================
    // Phase 2: DRAIN — wait for debounce flush
    // =========================================================================
    println!(
        "[2/5] DRAIN: waiting {}s for debounce flush to sinks...",
        cfg.drain_secs
    );
    tokio::time::sleep(Duration::from_secs(cfg.drain_secs)).await;

    let info: String = redis::cmd("SINK.INFO").query_async(&mut conn).await?;
    for line in info.lines() {
        if line.contains("flushed")
            || line.contains("coalesce")
            || line.contains("s3_writes")
            || line.contains("dynamo_writes")
        {
            println!("  {}", line.trim());
        }
    }
    println!();

    // =========================================================================
    // Phase 3: EVICT — DEL all keys
    // =========================================================================
    println!("[3/5] EVICT: DEL {} keys from Valkey...", cfg.keys);
    let mut evict_samples = Vec::with_capacity(cfg.keys);
    for meta in &metas {
        let start = Instant::now();
        conn.del::<_, ()>(&meta.key).await?;
        evict_samples.push(start.elapsed());
    }
    report_latencies("DEL", &mut evict_samples);
    println!();

    // =========================================================================
    // Phase 4: READ-THROUGH (cold — every key is a cache miss)
    // =========================================================================
    println!(
        "[4/5] READ-THROUGH: SINK.GET {} keys (cold — all cache misses)...",
        cfg.keys
    );
    let overall_start = Instant::now();
    let mut rt_samples = run_sink_get_phase(&cfg.url, &metas, cfg.concurrency).await?;
    let rt_wall = overall_start.elapsed();

    let info: String = redis::cmd("SINK.INFO").query_async(&mut conn).await?;
    let mut hits = 0usize;
    let mut misses = 0usize;
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix("sink_read_through_hits:") {
            hits = rest.trim().parse().unwrap_or(0);
        }
        if let Some(rest) = line.strip_prefix("sink_read_through_misses:") {
            misses = rest.trim().parse().unwrap_or(0);
        }
    }
    println!(
        "  wall clock: {:.2?} | backend hits: {} | misses: {}",
        rt_wall, hits, misses,
    );
    report_latencies("SINK.GET (cold)", &mut rt_samples);
    println!();

    // =========================================================================
    // Phase 5: CACHE-HIT (warm — every key should be in local cache)
    // =========================================================================
    println!(
        "[5/5] CACHE-HIT: SINK.GET {} keys (warm — should all be cache hits)...",
        cfg.keys
    );
    let overall_start = Instant::now();
    let mut ch_samples = run_sink_get_phase(&cfg.url, &metas, cfg.concurrency).await?;
    let ch_wall = overall_start.elapsed();

    println!("  wall clock: {:.2?}", ch_wall);
    report_latencies("SINK.GET (warm)", &mut ch_samples);
    println!();

    // =========================================================================
    // Cleanup
    // =========================================================================
    println!("Cleaning up {} keys...", cfg.keys);
    for meta in &metas {
        conn.del::<_, ()>(&meta.key).await?;
    }

    // =========================================================================
    // Summary
    // =========================================================================
    println!();
    println!("===============================================================================");
    println!("  RESULTS SUMMARY");
    println!("===============================================================================");
    println!("  Keys tested:          {}", cfg.keys);
    println!(
        "  Data volume:          {:.1} MB ({} small + {} large)",
        total_bytes as f64 / (1024.0 * 1024.0),
        small_count,
        large_count,
    );
    println!("  Keys with TTL:        {}", ttl_count);
    println!("  Read-through hits:    {}", hits);
    println!("  Read-through misses:  {}", misses);
    println!(
        "  Cold read wall time:  {:.2?} ({:.0} effective ops/s)",
        rt_wall,
        cfg.keys as f64 / rt_wall.as_secs_f64(),
    );
    println!(
        "  Warm read wall time:  {:.2?} ({:.0} effective ops/s)",
        ch_wall,
        cfg.keys as f64 / ch_wall.as_secs_f64(),
    );
    println!();

    // Final SINK.INFO
    println!("=== Final SINK.INFO ===");
    let info: String = redis::cmd("SINK.INFO").query_async(&mut conn).await?;
    println!("{}", info);

    // -- Optionally unload --
    if cfg.unload {
        println!("Unloading module...");
        let _: Result<String, _> = redis::cmd("MODULE")
            .arg("UNLOAD")
            .arg("valkey-sink")
            .query_async(&mut conn)
            .await;
    }

    Ok(())
}

/// Run SINK.GET across all keys using `concurrency` parallel tokio tasks,
/// each with its own Redis connection.
async fn run_sink_get_phase(
    url: &str,
    metas: &[KeyMeta],
    concurrency: usize,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>> {
    let keys: Vec<String> = metas.iter().map(|m| m.key.clone()).collect();
    let chunk_size = (keys.len() + concurrency - 1) / concurrency;
    let chunks: Vec<Vec<String>> = keys.chunks(chunk_size).map(|c| c.to_vec()).collect();

    let mut handles = Vec::new();
    for chunk in chunks {
        let url = url.to_string();
        let handle = tokio::spawn(async move {
            let client = redis::Client::open(url.as_str()).unwrap();
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();
            let mut samples = Vec::with_capacity(chunk.len());

            for key in &chunk {
                let start = Instant::now();
                let _: Option<Vec<u8>> = redis::cmd("SINK.GET")
                    .arg(key)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(None);
                samples.push(start.elapsed());
            }

            samples
        });
        handles.push(handle);
    }

    let mut all_samples = Vec::with_capacity(keys.len());
    for handle in handles {
        let samples = handle.await?;
        all_samples.extend(samples);
    }

    Ok(all_samples)
}
