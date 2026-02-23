# valkey-modules

Valkey modules for transparent persistence to AWS backends.

| Crate | Description |
|-------|-------------|
| [valkey-common](crates/common/) | Shared: AWS SigV4 signing, retry + circuit breaker, latency tracking |
| [valkey-sink](crates/sink/) | Write-behind persistence to S3 Express One Zone and DynamoDB |

| Tool | Description |
|------|-------------|
| [loadtest](tools/loadtest/) | Benchmark and load testing |

## License

Apache 2.0 — see [LICENSE](LICENSE)
