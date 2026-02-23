/// DynamoDB sink using raw HTTP + SigV4 signing.
///
/// Instead of pulling in the AWS SDK for DynamoDB (~10MB+ binary impact),
/// this module makes direct HTTP POST requests to the DynamoDB JSON API,
/// signed with SigV4. It only uses two operations:
///
/// - `BatchWriteItem` -- batch PutItem for flushing coalesced writes
/// - `GetItem` -- single-key lookup for read-through on cache miss
///
/// ## Table schema
///
/// The DynamoDB table must have a single partition key named `pk` of type Binary (B).
/// No sort key is needed.
///
/// | Attribute | Type | Description |
/// |-----------|------|-------------|
/// | `pk`      | B    | Valkey key bytes (base64-encoded) |
/// | `val`     | B    | Valkey value bytes (base64-encoded) |
/// | `ts`      | N    | Unix timestamp of the write |

use async_trait::async_trait;
use base64::engine::{general_purpose::STANDARD as B64, Engine};
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

use super::{Sink, SinkEntry, SinkError};
use valkey_common::aws::{self, RefreshableCredentials, SignRequest, TokenHeader};

/// Maximum items per DynamoDB BatchWriteItem request.
const BATCH_WRITE_MAX: usize = 25;

/// Persists key-value pairs to a DynamoDB table.
///
/// Table schema:
///   pk  (B) - partition key, raw Valkey key bytes (base64)
///   val (B) - raw value bytes (base64)
///   ts  (N) - Unix timestamp in seconds
pub struct DynamoDbSink {
    client: reqwest::Client,
    creds: Arc<RefreshableCredentials>,
    table_name: String,
    region: String,
    endpoint_url: String,
    write_concurrency: usize,
}

impl DynamoDbSink {
    pub fn new(
        client: reqwest::Client,
        creds: Arc<RefreshableCredentials>,
        table_name: String,
        region: String,
        write_concurrency: usize,
    ) -> Self {
        let endpoint_url = format!("https://dynamodb.{}.amazonaws.com/", region);
        Self {
            client,
            creds,
            table_name,
            region,
            endpoint_url,
            write_concurrency,
        }
    }

    /// Make a signed DynamoDB API call.
    async fn call_dynamo(&self, target: &str, body: &Value) -> Result<Value, SinkError> {
        let body_bytes = serde_json::to_vec(body)
            .map_err(|e| SinkError::Fatal(format!("JSON serialize failed: {}", e)))?;

        let creds = self
            .creds
            .get()
            .map_err(|e| SinkError::Retriable(format!("credential refresh failed: {}", e)))?;

        let mut extra_headers = BTreeMap::new();
        extra_headers.insert(
            "content-type".to_string(),
            "application/x-amz-json-1.0".to_string(),
        );
        extra_headers.insert("x-amz-target".to_string(), target.to_string());

        let signed = aws::sign_request(
            &creds,
            &SignRequest {
                method: "POST",
                url: &self.endpoint_url,
                region: &self.region,
                service: "dynamodb",
                headers: &extra_headers,
                payload: &body_bytes,
                token_header: TokenHeader::Standard,
            },
        );

        let mut req = self
            .client
            .post(&self.endpoint_url)
            .header("Authorization", &signed.authorization)
            .header("x-amz-date", &signed.x_amz_date)
            .header("x-amz-content-sha256", &signed.x_amz_content_sha256)
            .header("content-type", "application/x-amz-json-1.0")
            .header("x-amz-target", target);

        if let Some((name, val)) = &signed.session_token {
            req = req.header(name.as_str(), val.as_str());
        }

        let resp = req
            .body(body_bytes)
            .send()
            .await
            .map_err(|e| SinkError::Retriable(format!("DynamoDB request failed: {}", e)))?;

        if resp.status().is_success() {
            let body: Value = resp
                .json()
                .await
                .map_err(|e| {
                    SinkError::Retriable(format!("DynamoDB response parse failed: {}", e))
                })?;
            Ok(body)
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if status.as_u16() == 400
                && (text.contains("ProvisionedThroughputExceededException")
                    || text.contains("ThrottlingException"))
            {
                Err(SinkError::Retriable(format!(
                    "DynamoDB throttled: {}",
                    text.chars().take(200).collect::<String>()
                )))
            } else {
                Err(SinkError::Retriable(format!(
                    "DynamoDB returned {}: {}",
                    status,
                    text.chars().take(200).collect::<String>()
                )))
            }
        }
    }

    /// Base64-encode bytes for DynamoDB Binary attribute.
    fn b64(data: &[u8]) -> String {
        B64.encode(data)
    }
}

impl DynamoDbSink {
    /// Write a batch from borrowed references (avoids cloning entries).
    pub async fn write_batch_refs(&self, entries: &[&SinkEntry]) -> Result<(), SinkError> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Build all chunk requests upfront, then execute concurrently.
        let futs: Vec<_> = entries
            .chunks(BATCH_WRITE_MAX)
            .map(|chunk| {
                let requests: Vec<Value> = chunk
                    .iter()
                    .map(|entry| {
                        let mut item = serde_json::Map::new();
                        item.insert(
                            "pk".to_string(),
                            json!({ "B": DynamoDbSink::b64(&entry.key) }),
                        );
                        item.insert(
                            "val".to_string(),
                            json!({ "B": DynamoDbSink::b64(&entry.value) }),
                        );
                        item.insert("ts".to_string(), json!({ "N": ts.to_string() }));
                        if let Some(exp) = entry.expires_at {
                            item.insert("ttl".to_string(), json!({ "N": exp.to_string() }));
                        }
                        json!({ "PutRequest": { "Item": Value::Object(item) } })
                    })
                    .collect();

                let body = json!({
                    "RequestItems": {
                        &self.table_name: requests
                    }
                });

                async move {
                    let max_inner_retries = 5u32;
                    let mut current_body = body;

                    for attempt in 0..max_inner_retries {
                        let result = self
                            .call_dynamo("DynamoDB_20120810.BatchWriteItem", &current_body)
                            .await?;

                        // Check for unprocessed items and retry only those.
                        let has_unprocessed = result
                            .get("UnprocessedItems")
                            .and_then(|u| u.get(&self.table_name))
                            .and_then(|items| items.as_array())
                            .filter(|arr| !arr.is_empty());

                        match has_unprocessed {
                            Some(arr) => {
                                if attempt + 1 >= max_inner_retries {
                                    return Err(SinkError::Retriable(format!(
                                        "DynamoDB BatchWriteItem had {} unprocessed items after {} retries",
                                        arr.len(),
                                        max_inner_retries
                                    )));
                                }
                                // Exponential backoff: 50ms, 100ms, 200ms, 400ms...
                                let delay_ms = 50u64 * (1u64 << attempt);
                                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                                // Rebuild body with only the unprocessed items.
                                current_body = json!({
                                    "RequestItems": {
                                        &self.table_name: arr.clone()
                                    }
                                });
                            }
                            None => return Ok(()),
                        }
                    }

                    Ok(())
                }
            })
            .collect();

        let results: Vec<Result<(), SinkError>> = stream::iter(futs)
            .buffer_unordered(self.write_concurrency)
            .collect()
            .await;

        for result in results {
            result?;
        }

        Ok(())
    }

    async fn lookup_impl(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Option<u64>)>, SinkError> {
        let body = json!({
            "TableName": &self.table_name,
            "Key": {
                "pk": { "B": DynamoDbSink::b64(key) }
            }
        });

        let result = self
            .call_dynamo("DynamoDB_20120810.GetItem", &body)
            .await?;

        match result.get("Item") {
            Some(item) => {
                if let Some(val_obj) = item.get("val") {
                    if let Some(b64_str) = val_obj.get("B").and_then(|v| v.as_str()) {
                        let decoded = B64.decode(b64_str).map_err(|e| {
                            SinkError::Retriable(format!("base64 decode failed: {}", e))
                        })?;

                        // Read TTL attribute. If present and expired, treat as not found.
                        let expires_at = item
                            .get("ttl")
                            .and_then(|t| t.get("N"))
                            .and_then(|n| n.as_str())
                            .and_then(|s| s.parse::<u64>().ok());

                        if let Some(exp) = expires_at {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            if now >= exp {
                                return Ok(None);
                            }
                        }

                        return Ok(Some((decoded, expires_at)));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl Sink for DynamoDbSink {
    async fn write_batch(&self, entries: &[SinkEntry]) -> Result<(), SinkError> {
        let refs: Vec<&SinkEntry> = entries.iter().collect();
        self.write_batch_refs(&refs).await
    }

    async fn lookup(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Option<u64>)>, SinkError> {
        self.lookup_impl(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64_roundtrip() {
        let inputs: &[&[u8]] = &[
            b"",
            b"f",
            b"fo",
            b"foo",
            b"foob",
            b"fooba",
            b"foobar",
            b"\x00\xff\x80",
            b"Hello, World!",
        ];
        for input in inputs {
            let encoded = DynamoDbSink::b64(input);
            let decoded = B64.decode(&encoded).unwrap();
            assert_eq!(&decoded, input, "roundtrip failed for {:?}", input);
        }
    }

    #[test]
    fn test_batch_item_json_with_ttl() {
        // Simulate the JSON construction from write_batch to verify TTL is included.
        let entry = SinkEntry {
            key: b"mykey".to_vec(),
            value: b"myvalue".to_vec(),
            expires_at: Some(1700000000),
        };
        let ts = 1699999900u64;

        let mut item = serde_json::Map::new();
        item.insert("pk".to_string(), json!({ "B": DynamoDbSink::b64(&entry.key) }));
        item.insert("val".to_string(), json!({ "B": DynamoDbSink::b64(&entry.value) }));
        item.insert("ts".to_string(), json!({ "N": ts.to_string() }));
        if let Some(exp) = entry.expires_at {
            item.insert("ttl".to_string(), json!({ "N": exp.to_string() }));
        }
        let put_req = json!({ "PutRequest": { "Item": Value::Object(item) } });

        // Verify ttl attribute is present
        let ttl = put_req["PutRequest"]["Item"]["ttl"]["N"].as_str().unwrap();
        assert_eq!(ttl, "1700000000");
        // Verify pk and val are base64-encoded
        let pk_b64 = put_req["PutRequest"]["Item"]["pk"]["B"].as_str().unwrap();
        let decoded_key = B64.decode(pk_b64).unwrap();
        assert_eq!(decoded_key, b"mykey");
    }

    #[test]
    fn test_batch_item_json_without_ttl() {
        let entry = SinkEntry {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            expires_at: None,
        };
        let ts = 1699999900u64;

        let mut item = serde_json::Map::new();
        item.insert("pk".to_string(), json!({ "B": DynamoDbSink::b64(&entry.key) }));
        item.insert("val".to_string(), json!({ "B": DynamoDbSink::b64(&entry.value) }));
        item.insert("ts".to_string(), json!({ "N": ts.to_string() }));
        if let Some(exp) = entry.expires_at {
            item.insert("ttl".to_string(), json!({ "N": exp.to_string() }));
        }
        let put_req = json!({ "PutRequest": { "Item": Value::Object(item) } });

        // Verify ttl attribute is NOT present when expires_at is None
        assert!(put_req["PutRequest"]["Item"]["ttl"].is_null());
    }

    #[test]
    fn test_lookup_response_parsing_with_ttl() {
        // Simulate a DynamoDB GetItem response with a ttl attribute.
        let response = json!({
            "Item": {
                "pk": { "B": DynamoDbSink::b64(b"key1") },
                "val": { "B": DynamoDbSink::b64(b"value1") },
                "ts": { "N": "1700000000" },
                "ttl": { "N": "9999999999" }
            }
        });

        // Parse the way lookup() does
        let item = response.get("Item").unwrap();
        let b64_str = item["val"]["B"].as_str().unwrap();
        let decoded = B64.decode(b64_str).unwrap();
        assert_eq!(decoded, b"value1");

        let expires_at = item.get("ttl")
            .and_then(|t| t.get("N"))
            .and_then(|n| n.as_str())
            .and_then(|s| s.parse::<u64>().ok());
        assert_eq!(expires_at, Some(9999999999));
    }

    #[test]
    fn test_lookup_response_parsing_without_ttl() {
        let response = json!({
            "Item": {
                "pk": { "B": DynamoDbSink::b64(b"key1") },
                "val": { "B": DynamoDbSink::b64(b"value1") },
                "ts": { "N": "1700000000" }
            }
        });

        let item = response.get("Item").unwrap();
        let expires_at = item.get("ttl")
            .and_then(|t| t.get("N"))
            .and_then(|n| n.as_str())
            .and_then(|s| s.parse::<u64>().ok());
        assert_eq!(expires_at, None);
    }

    #[test]
    fn test_lookup_response_expired_item() {
        // If ttl < now, lookup should treat as not found.
        let past_ttl = 1000u64; // way in the past
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        assert!(now >= past_ttl, "test requires current time > 1000 epoch seconds");

        let response = json!({
            "Item": {
                "pk": { "B": DynamoDbSink::b64(b"key1") },
                "val": { "B": DynamoDbSink::b64(b"value1") },
                "ttl": { "N": past_ttl.to_string() }
            }
        });

        let item = response.get("Item").unwrap();
        let expires_at = item.get("ttl")
            .and_then(|t| t.get("N"))
            .and_then(|n| n.as_str())
            .and_then(|s| s.parse::<u64>().ok());

        // Simulate the filtering logic from lookup()
        let is_expired = if let Some(exp) = expires_at {
            now >= exp
        } else {
            false
        };
        assert!(is_expired);
    }

    #[test]
    fn test_batch_chunking_exact_25() {
        let entries: Vec<SinkEntry> = (0..25)
            .map(|i| SinkEntry {
                key: format!("k{}", i).into_bytes(),
                value: b"v".to_vec(),
                expires_at: None,
            })
            .collect();
        let refs: Vec<&SinkEntry> = entries.iter().collect();
        let chunks: Vec<_> = refs.chunks(BATCH_WRITE_MAX).collect();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 25);
    }

    #[test]
    fn test_batch_chunking_26_entries() {
        let entries: Vec<SinkEntry> = (0..26)
            .map(|i| SinkEntry {
                key: format!("k{}", i).into_bytes(),
                value: b"v".to_vec(),
                expires_at: None,
            })
            .collect();
        let refs: Vec<&SinkEntry> = entries.iter().collect();
        let chunks: Vec<_> = refs.chunks(BATCH_WRITE_MAX).collect();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 25);
        assert_eq!(chunks[1].len(), 1);
    }

    #[test]
    fn test_batch_chunking_50_entries() {
        let entries: Vec<SinkEntry> = (0..50)
            .map(|i| SinkEntry {
                key: format!("k{}", i).into_bytes(),
                value: b"v".to_vec(),
                expires_at: None,
            })
            .collect();
        let refs: Vec<&SinkEntry> = entries.iter().collect();
        let chunks: Vec<_> = refs.chunks(BATCH_WRITE_MAX).collect();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 25);
        assert_eq!(chunks[1].len(), 25);
    }

    #[test]
    fn test_base64_all_zeros() {
        let zeros = vec![0u8; 16];
        let encoded = DynamoDbSink::b64(&zeros);
        let decoded = B64.decode(&encoded).unwrap();
        assert_eq!(decoded, zeros);
    }

    #[test]
    fn test_base64_all_ones() {
        let ones = vec![0xFFu8; 16];
        let encoded = DynamoDbSink::b64(&ones);
        let decoded = B64.decode(&encoded).unwrap();
        assert_eq!(decoded, ones);
    }
}
