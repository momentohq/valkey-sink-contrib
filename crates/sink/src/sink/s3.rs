/// S3 Express One Zone sink using CreateSession auth + SigV4-Express.
///
/// S3 Express directory buckets require a two-step auth flow:
/// 1. CreateSession: get short-lived session credentials (5 min TTL)
/// 2. Use those credentials with `x-amz-s3session-token` for data requests
///
/// This sink caches the session and auto-refreshes before expiry.

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

use super::{Sink, SinkEntry, SinkError};
use valkey_common::aws::{self, Credentials, RefreshableCredentials, SignRequest, TokenHeader};

/// How long before expiry to refresh the session (30s buffer).
const REFRESH_BUFFER: Duration = Duration::from_secs(30);

/// S3 Express session credentials returned by CreateSession.
struct ExpressSession {
    credentials: Credentials,
    obtained_at: Instant,
    ttl: Duration,
}

impl ExpressSession {
    fn is_expired(&self) -> bool {
        self.obtained_at.elapsed() + REFRESH_BUFFER >= self.ttl
    }
}

/// Persists key-value pairs to an S3 Express One Zone directory bucket.
///
/// Key mapping: `{prefix}{hex(key)}` to avoid special-character issues.
/// Value is stored as the raw object body.
pub struct S3ExpressSink {
    client: reqwest::Client,
    /// Shared refreshable IAM credentials used to call CreateSession.
    iam_creds: Arc<RefreshableCredentials>,
    /// Cached S3 Express session credentials (auto-refreshed).
    session: RwLock<Option<ExpressSession>>,
    /// Serializes session refresh attempts to avoid thundering-herd.
    refresh_mutex: Mutex<()>,
    prefix: String,
    region: String,
    /// Full base URL, e.g. https://bucket--az--x-s3.s3express-az.region.amazonaws.com
    base_url: String,
    write_concurrency: usize,
}

impl S3ExpressSink {
    pub fn new(
        client: reqwest::Client,
        iam_creds: Arc<RefreshableCredentials>,
        bucket: String,
        prefix: String,
        region: String,
        endpoint: Option<String>,
        write_concurrency: usize,
    ) -> Self {
        // S3 Express endpoint format:
        //   https://{bucket}.s3express-{az}.{region}.amazonaws.com
        // The AZ is embedded in the bucket name: bucket--{az}--x-s3
        let base_url = match endpoint {
            Some(ep) => format!("{}/{}", ep.trim_end_matches('/'), bucket),
            None => {
                // Extract AZ from bucket name: "mybucket--use1-az1--x-s3" -> "use1-az1"
                let az = extract_az_from_bucket(&bucket).unwrap_or("use1-az1");
                format!(
                    "https://{}.s3express-{}.{}.amazonaws.com",
                    bucket, az, region
                )
            }
        };

        Self {
            client,
            iam_creds,
            session: RwLock::new(None),
            refresh_mutex: Mutex::new(()),
            prefix,
            region,
            base_url,
            write_concurrency,
        }
    }

    /// Get valid session credentials, refreshing via CreateSession if needed.
    async fn get_session_creds(&self) -> Result<Credentials, SinkError> {
        // Fast path: read lock, check if session is valid.
        {
            let guard = self.session.read().await;
            if let Some(sess) = guard.as_ref() {
                if !sess.is_expired() {
                    return Ok(sess.credentials.clone());
                }
            }
        }

        // Serialize refresh: only one task enters this section at a time,
        // preventing a thundering-herd of CreateSession calls.
        let _refresh_guard = self.refresh_mutex.lock().await;

        // Re-check under refresh lock (another task may have just refreshed).
        {
            let guard = self.session.read().await;
            if let Some(sess) = guard.as_ref() {
                if !sess.is_expired() {
                    return Ok(sess.credentials.clone());
                }
            }
        }

        // This task is the refresher. Do the HTTP call outside the write lock.
        let new_session = self.create_session().await?;
        let creds = new_session.credentials.clone();

        // Brief write lock just to store the new session.
        {
            let mut guard = self.session.write().await;
            *guard = Some(new_session);
        }

        Ok(creds)
    }

    /// Call CreateSession on the directory bucket to get session credentials.
    ///
    /// POST /{bucket}?session
    /// Signed with IAM credentials using standard SigV4 (service=s3express).
    async fn create_session(&self) -> Result<ExpressSession, SinkError> {
        let url = format!("{}?session", self.base_url);
        let headers = BTreeMap::new();

        let iam_creds = self
            .iam_creds
            .get()
            .map_err(|e| SinkError::Retriable(format!("credential refresh failed: {}", e)))?;

        let signed = aws::sign_request(
            &iam_creds,
            &SignRequest {
                method: "GET",
                url: &url,
                region: &self.region,
                service: "s3express",
                headers: &headers,
                payload: &[],
                token_header: TokenHeader::Standard, // CreateSession uses IAM creds
            },
        );

        let mut req = self
            .client
            .get(&url)
            .header("Authorization", &signed.authorization)
            .header("x-amz-date", &signed.x_amz_date)
            .header("x-amz-content-sha256", &signed.x_amz_content_sha256);

        if let Some((name, val)) = &signed.session_token {
            req = req.header(name.as_str(), val.as_str());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SinkError::Retriable(format!("CreateSession request failed: {}", e)))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(SinkError::Retriable(format!(
                "CreateSession returned {}: {}",
                status,
                text.chars().take(500).collect::<String>()
            )));
        }

        // Response is XML:
        // <Credentials>
        //   <AccessKeyId>...</AccessKeyId>
        //   <SecretAccessKey>...</SecretAccessKey>
        //   <SessionToken>...</SessionToken>
        //   <Expiration>...</Expiration>
        // </Credentials>
        let body = resp
            .text()
            .await
            .map_err(|e| SinkError::Retriable(format!("CreateSession body read failed: {}", e)))?;

        let access_key = extract_xml_value(&body, "AccessKeyId")
            .ok_or_else(|| SinkError::Retriable("CreateSession: missing AccessKeyId".into()))?;
        let secret_key = extract_xml_value(&body, "SecretAccessKey")
            .ok_or_else(|| {
                SinkError::Retriable("CreateSession: missing SecretAccessKey".into())
            })?;
        let session_token = extract_xml_value(&body, "SessionToken");

        Ok(ExpressSession {
            credentials: Credentials {
                access_key,
                secret_key,
                session_token,
            },
            obtained_at: Instant::now(),
            ttl: Duration::from_secs(300), // S3 Express sessions are 5 minutes
        })
    }

    /// Map a raw Valkey key to an S3 object key.
    fn object_key(&self, key: &[u8]) -> String {
        format!("{}{}", self.prefix, hex::encode(key))
    }

    /// Full URL for an object.
    fn object_url(&self, object_key: &str) -> String {
        format!("{}/{}", self.base_url, object_key)
    }

    /// Sign and execute a PUT request using S3 Express session credentials.
    async fn put_object(
        &self,
        object_key: &str,
        body: Vec<u8>,
        expires_at: Option<u64>,
    ) -> Result<(), SinkError> {
        let session_creds = self.get_session_creds().await?;
        let url = self.object_url(object_key);
        let mut headers = BTreeMap::new();
        if let Some(exp) = expires_at {
            headers.insert(
                "x-amz-meta-expires-at".to_string(),
                exp.to_string(),
            );
        }

        let signed = aws::sign_request(
            &session_creds,
            &SignRequest {
                method: "PUT",
                url: &url,
                region: &self.region,
                service: "s3express",
                headers: &headers,
                payload: &body,
                token_header: TokenHeader::S3Express,
            },
        );

        let mut req = self
            .client
            .put(&url)
            .header("Authorization", &signed.authorization)
            .header("x-amz-date", &signed.x_amz_date)
            .header("x-amz-content-sha256", &signed.x_amz_content_sha256);

        // Add caller-supplied headers (e.g. x-amz-meta-expires-at) that were
        // included in the signature. Without these the request fails with 403.
        for (name, val) in &headers {
            req = req.header(name.as_str(), val.as_str());
        }

        if let Some((name, val)) = &signed.session_token {
            req = req.header(name.as_str(), val.as_str());
        }

        let resp = req
            .body(body)
            .send()
            .await
            .map_err(|e| SinkError::Retriable(format!("S3 Express PUT failed: {}", e)))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            Err(SinkError::Retriable(format!(
                "S3 Express PUT returned {}: {}",
                status,
                text.chars().take(200).collect::<String>()
            )))
        }
    }

    /// Sign and execute a GET request using S3 Express session credentials.
    /// Returns the value and optional expiry (from x-amz-meta-expires-at header).
    async fn get_object(
        &self,
        object_key: &str,
    ) -> Result<Option<(Vec<u8>, Option<u64>)>, SinkError> {
        let session_creds = self.get_session_creds().await?;
        let url = self.object_url(object_key);
        let headers = BTreeMap::new();

        let signed = aws::sign_request(
            &session_creds,
            &SignRequest {
                method: "GET",
                url: &url,
                region: &self.region,
                service: "s3express",
                headers: &headers,
                payload: &[],
                token_header: TokenHeader::S3Express,
            },
        );

        let mut req = self
            .client
            .get(&url)
            .header("Authorization", &signed.authorization)
            .header("x-amz-date", &signed.x_amz_date)
            .header("x-amz-content-sha256", &signed.x_amz_content_sha256);

        if let Some((name, val)) = &signed.session_token {
            req = req.header(name.as_str(), val.as_str());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SinkError::Retriable(format!("S3 Express GET failed: {}", e)))?;

        if resp.status().is_success() {
            // Read TTL from S3 object metadata header.
            let expires_at = resp
                .headers()
                .get("x-amz-meta-expires-at")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());

            // If expired, treat as not found.
            if let Some(exp) = expires_at {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if now >= exp {
                    return Ok(None);
                }
            }

            let bytes = resp
                .bytes()
                .await
                .map_err(|e| SinkError::Retriable(format!("S3 Express body read failed: {}", e)))?;
            Ok(Some((bytes.to_vec(), expires_at)))
        } else if resp.status().as_u16() == 404 {
            Ok(None)
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            Err(SinkError::Retriable(format!(
                "S3 Express GET returned {}: {}",
                status,
                text.chars().take(200).collect::<String>()
            )))
        }
    }
}

impl S3ExpressSink {
    /// Write a batch from borrowed references (avoids cloning entries).
    pub async fn write_batch_refs(&self, entries: &[&SinkEntry]) -> Result<(), SinkError> {
        // Pre-fetch session creds once for the batch (they're cached).
        self.get_session_creds().await?;

        let futs: Vec<_> = entries
            .iter()
            .map(|entry| {
                let obj_key = self.object_key(&entry.key);
                let body = entry.value.clone(); // value must be owned for reqwest body
                let expires_at = entry.expires_at;
                async move { self.put_object(&obj_key, body, expires_at).await }
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
}

#[async_trait]
impl Sink for S3ExpressSink {
    async fn write_batch(&self, entries: &[SinkEntry]) -> Result<(), SinkError> {
        let refs: Vec<&SinkEntry> = entries.iter().collect();
        self.write_batch_refs(&refs).await
    }

    async fn lookup(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Option<u64>)>, SinkError> {
        let obj_key = self.object_key(key);
        self.get_object(&obj_key).await
    }
}

/// Extract the AZ from an S3 Express bucket name.
/// "my-bucket--use1-az1--x-s3" -> "use1-az1"
fn extract_az_from_bucket(bucket: &str) -> Option<&str> {
    // Format: {name}--{az-id}--x-s3
    let rest = bucket.strip_suffix("--x-s3")?;
    let idx = rest.rfind("--")?;
    Some(&rest[idx + 2..])
}

/// Extract the text content of an XML element by tag name using quick-xml.
/// Handles attributes, namespaces, CDATA, and XML entities correctly.
fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml);
    let tag_bytes = tag.as_bytes();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) if e.name().as_ref() == tag_bytes => {
                match reader.read_event() {
                    Ok(Event::Text(t)) => {
                        let text = t.unescape().ok()?;
                        let trimmed = text.trim();
                        if trimmed.is_empty() {
                            return None;
                        }
                        // Require a matching close tag (rejects truncated XML).
                        match reader.read_event() {
                            Ok(Event::End(end)) if end.name().as_ref() == tag_bytes => {
                                return Some(trimmed.to_string());
                            }
                            _ => return None,
                        }
                    }
                    _ => return None,
                }
            }
            Ok(Event::Eof) | Err(_) => return None,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_az() {
        assert_eq!(
            extract_az_from_bucket("my-bucket--use1-az1--x-s3"),
            Some("use1-az1")
        );
        assert_eq!(
            extract_az_from_bucket("data--usw2-az3--x-s3"),
            Some("usw2-az3")
        );
        assert_eq!(extract_az_from_bucket("regular-bucket"), None);
    }

    #[test]
    fn test_extract_xml() {
        let xml = r#"<Credentials><AccessKeyId>AKIA123</AccessKeyId><SecretAccessKey>secret</SecretAccessKey></Credentials>"#;
        assert_eq!(
            extract_xml_value(xml, "AccessKeyId"),
            Some("AKIA123".into())
        );
        assert_eq!(
            extract_xml_value(xml, "SecretAccessKey"),
            Some("secret".into())
        );
        assert_eq!(extract_xml_value(xml, "Missing"), None);
    }

    #[test]
    fn test_extract_az_empty_string() {
        assert_eq!(extract_az_from_bucket(""), None);
    }

    #[test]
    fn test_extract_az_no_double_dash() {
        assert_eq!(extract_az_from_bucket("simple-bucket"), None);
        assert_eq!(extract_az_from_bucket("bucket-with-dashes"), None);
    }

    #[test]
    fn test_extract_xml_whitespace_in_value() {
        let xml = "<Tag>  value  </Tag>";
        assert_eq!(extract_xml_value(xml, "Tag"), Some("value".into()));
    }

    #[test]
    fn test_extract_xml_empty_value() {
        let xml = "<Tag></Tag>";
        assert_eq!(extract_xml_value(xml, "Tag"), None);
    }

    #[test]
    fn test_extract_xml_with_attributes() {
        let xml = r#"<Foo attr="bar">value</Foo>"#;
        assert_eq!(extract_xml_value(xml, "Foo"), Some("value".into()));
    }

    #[test]
    fn test_extract_xml_missing_close_tag() {
        let xml = "<Tag>value";
        assert_eq!(extract_xml_value(xml, "Tag"), None);
    }
}
