/// AWS credential loading and SigV4 request signing via `aws-sigv4`.
///
/// Uses the maintained `aws-sigv4` crate for signing, replacing the previous
/// hand-rolled implementation. The public API (`Credentials`, `SignRequest`,
/// `SignedHeaders`, `TokenHeader`) is unchanged so callers need zero changes.
///
/// ## S3 Express differences
///
/// S3 Express directory buckets use a different token header name:
/// - Standard SigV4: `x-amz-security-token`
/// - S3 Express: `x-amz-s3session-token`
///
/// And the service name in the signing scope is `s3express` (not `s3`).

use std::collections::BTreeMap;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

/// AWS credentials (access key, secret key, optional session token).
#[derive(Clone, Debug)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    /// Present when using temporary credentials (STS, instance roles, S3 Express sessions).
    pub session_token: Option<String>,
}

impl Credentials {
    /// Load from environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN.
    pub fn from_env() -> Result<Self, String> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| "AWS_ACCESS_KEY_ID not set".to_string())?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| "AWS_SECRET_ACCESS_KEY not set".to_string())?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
        Ok(Self {
            access_key,
            secret_key,
            session_token,
        })
    }
}

/// Credential provider that periodically refreshes from environment.
/// Safe to share across threads via `Arc`.
///
/// Uses a double-checked locking pattern with `RwLock` for the fast read path
/// and `Mutex` to serialize refresh attempts (thundering-herd prevention).
///
/// TTL is chosen based on credential type:
/// - Temporary (STS/IMDS, i.e. session token present): 50 minutes (refresh 10 min before typical 1-hour expiry)
/// - Long-lived IAM user credentials: 24 hours
pub struct RefreshableCredentials {
    current: RwLock<Credentials>,
    refresh_lock: Mutex<()>,
    obtained_at: RwLock<Instant>,
    ttl: Duration,
}

impl RefreshableCredentials {
    pub fn new(initial: Credentials) -> Self {
        // If session token present, credentials are temporary (refresh every 50min).
        // Otherwise, long-lived IAM user credentials (refresh every 24h).
        let ttl = if initial.session_token.is_some() {
            Duration::from_secs(50 * 60)
        } else {
            Duration::from_secs(24 * 3600)
        };
        Self {
            current: RwLock::new(initial),
            refresh_lock: Mutex::new(()),
            obtained_at: RwLock::new(Instant::now()),
            ttl,
        }
    }

    /// Get current credentials, refreshing from env if stale.
    pub fn get(&self) -> Result<Credentials, String> {
        // Fast path: read lock, check if still fresh.
        {
            let obtained = self.obtained_at.read().unwrap();
            if obtained.elapsed() < self.ttl {
                return Ok(self.current.read().unwrap().clone());
            }
        }

        // Serialize refresh attempts (only one thread refreshes at a time).
        let _guard = self.refresh_lock.lock().unwrap();

        // Double-check after acquiring the mutex (another thread may have refreshed).
        {
            let obtained = self.obtained_at.read().unwrap();
            if obtained.elapsed() < self.ttl {
                return Ok(self.current.read().unwrap().clone());
            }
        }

        let new_creds = Credentials::from_env()?;
        *self.current.write().unwrap() = new_creds.clone();
        *self.obtained_at.write().unwrap() = Instant::now();
        Ok(new_creds)
    }
}

/// Which HTTP header to use for the session token when signing.
#[derive(Clone, Copy)]
pub enum TokenHeader {
    /// Standard SigV4: `x-amz-security-token` (DynamoDB, regular S3, CreateSession)
    Standard,
    /// S3 Express data requests: `x-amz-s3session-token`
    S3Express,
}

/// Parameters for signing an AWS request.
pub struct SignRequest<'a> {
    pub method: &'a str,
    pub url: &'a str,
    pub region: &'a str,
    /// Service name used in the signing scope (e.g. "s3express", "dynamodb").
    pub service: &'a str,
    /// Additional headers to include in the signature (e.g. content-type, x-amz-target).
    pub headers: &'a BTreeMap<String, String>,
    /// Request body bytes (used to compute the payload hash).
    pub payload: &'a [u8],
    pub token_header: TokenHeader,
}

/// Output of signing: headers to attach to the HTTP request.
pub struct SignedHeaders {
    pub authorization: String,
    pub x_amz_date: String,
    pub x_amz_content_sha256: String,
    /// Session token header (name, value) if credentials include a token.
    pub session_token: Option<(String, String)>,
}

/// Sign an AWS request using SigV4.
///
/// Delegates to the `aws-sigv4` crate, which handles the full SigV4 process:
/// canonical request construction, string-to-sign, signing key derivation,
/// and signature computation. The `session_token_name_override` setting
/// handles S3 Express's non-standard token header natively.
pub fn sign_request(creds: &Credentials, req: &SignRequest) -> SignedHeaders {
    use aws_credential_types::Credentials as AwsCreds;
    use aws_sigv4::http_request::{
        sign, SignableBody, SignableRequest, SigningSettings, UriPathNormalizationMode,
    };
    use aws_sigv4::sign::v4;
    use aws_smithy_runtime_api::client::identity::Identity;
    use sha2::{Digest, Sha256};

    // Compute payload hash upfront — the aws-sigv4 crate uses it internally
    // for signing but doesn't emit it as an output header.
    let payload_hash = hex::encode(Sha256::digest(req.payload));

    let mut settings = SigningSettings::default();
    // Disable path normalization for S3 compatibility (S3 paths can have
    // consecutive slashes, dots, etc. that must not be normalized).
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;

    // S3 Express uses a non-standard session token header name.
    if matches!(req.token_header, TokenHeader::S3Express) {
        settings.session_token_name_override = Some("x-amz-s3session-token");
    }

    let aws_creds = AwsCreds::new(
        creds.access_key.clone(),
        creds.secret_key.clone(),
        creds.session_token.clone(),
        None, // no expiry
        "valkey-sink",
    );
    let identity: Identity = aws_creds.into();

    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(req.region)
        .name(req.service)
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .expect("valid signing params")
        .into();

    // Pass caller-supplied headers (e.g. content-type, x-amz-target for DynamoDB).
    // The sign function handles host (from URL) and date automatically.
    // We pass the precomputed payload hash so the crate uses it consistently.
    let signable = SignableRequest::new(
        req.method,
        req.url,
        req.headers.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        SignableBody::Precomputed(payload_hash.clone()),
    )
    .expect("valid signable request");

    let (instructions, _sig) = sign(signable, &params)
        .expect("signing succeeded")
        .into_parts();

    // Extract the headers we need from the signing output.
    // x-amz-content-sha256 is not emitted by the crate; we computed it above.
    let mut authorization = String::new();
    let mut x_amz_date = String::new();
    let mut session_token: Option<(String, String)> = None;

    for (name, value) in instructions.headers() {
        match name {
            "authorization" => authorization = value.to_string(),
            "x-amz-date" => x_amz_date = value.to_string(),
            "x-amz-security-token" | "x-amz-s3session-token" => {
                session_token = Some((name.to_string(), value.to_string()));
            }
            _ => {}
        }
    }

    SignedHeaders {
        authorization,
        x_amz_date,
        x_amz_content_sha256: payload_hash,
        session_token,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_request_produces_auth_header() {
        let creds = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        };
        let headers = BTreeMap::new();
        let signed = sign_request(
            &creds,
            &SignRequest {
                method: "GET",
                url: "https://example.com/test",
                region: "us-east-1",
                service: "s3",
                headers: &headers,
                payload: &[],
                token_header: TokenHeader::Standard,
            },
        );

        assert!(signed.authorization.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"));
        assert!(signed.authorization.contains("SignedHeaders="));
        assert!(signed.authorization.contains("Signature="));
        assert!(!signed.x_amz_date.is_empty());
        assert!(!signed.x_amz_content_sha256.is_empty());
        assert!(signed.session_token.is_none());
    }

    #[test]
    fn test_sign_request_s3express_token_header() {
        let creds = Credentials {
            access_key: "AKID".into(),
            secret_key: "SECRET".into(),
            session_token: Some("SESSION".into()),
        };
        let headers = BTreeMap::new();
        let signed = sign_request(
            &creds,
            &SignRequest {
                method: "PUT",
                url: "https://bucket.s3express.amazonaws.com/key",
                region: "us-west-2",
                service: "s3express",
                headers: &headers,
                payload: b"hello",
                token_header: TokenHeader::S3Express,
            },
        );

        let (name, _) = signed.session_token.unwrap();
        assert_eq!(name, "x-amz-s3session-token");
    }

    #[test]
    fn test_sign_request_standard_token_header() {
        let creds = Credentials {
            access_key: "AKID".into(),
            secret_key: "SECRET".into(),
            session_token: Some("TOKEN123".into()),
        };
        let headers = BTreeMap::new();
        let signed = sign_request(
            &creds,
            &SignRequest {
                method: "POST",
                url: "https://dynamodb.us-east-1.amazonaws.com/",
                region: "us-east-1",
                service: "dynamodb",
                headers: &headers,
                payload: b"{}",
                token_header: TokenHeader::Standard,
            },
        );

        let (name, value) = signed.session_token.unwrap();
        assert_eq!(name, "x-amz-security-token");
        assert_eq!(value, "TOKEN123");
    }

    #[test]
    fn test_sign_request_no_token_when_absent() {
        let creds = Credentials {
            access_key: "AKID".into(),
            secret_key: "SECRET".into(),
            session_token: None,
        };
        let headers = BTreeMap::new();
        let signed = sign_request(
            &creds,
            &SignRequest {
                method: "GET",
                url: "https://s3.us-east-1.amazonaws.com/bucket/key",
                region: "us-east-1",
                service: "s3",
                headers: &headers,
                payload: &[],
                token_header: TokenHeader::Standard,
            },
        );
        assert!(signed.session_token.is_none());
    }

    #[test]
    fn test_sign_request_with_extra_headers() {
        let creds = Credentials {
            access_key: "AKID".into(),
            secret_key: "SECRET".into(),
            session_token: None,
        };
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "application/x-amz-json-1.0".to_string());
        headers.insert("x-amz-target".to_string(), "DynamoDB_20120810.GetItem".to_string());

        let signed = sign_request(
            &creds,
            &SignRequest {
                method: "POST",
                url: "https://dynamodb.us-east-1.amazonaws.com/",
                region: "us-east-1",
                service: "dynamodb",
                headers: &headers,
                payload: b"{}",
                token_header: TokenHeader::Standard,
            },
        );

        // Extra headers should be included in the signed headers list
        assert!(signed.authorization.contains("SignedHeaders="));
        assert!(!signed.authorization.is_empty());
    }

    #[test]
    fn test_sign_request_payload_hash_varies_with_body() {
        let creds = Credentials {
            access_key: "AKID".into(),
            secret_key: "SECRET".into(),
            session_token: None,
        };
        let headers = BTreeMap::new();

        let signed1 = sign_request(&creds, &SignRequest {
            method: "PUT",
            url: "https://bucket.s3.amazonaws.com/key",
            region: "us-east-1",
            service: "s3",
            headers: &headers,
            payload: b"body1",
            token_header: TokenHeader::Standard,
        });

        let signed2 = sign_request(&creds, &SignRequest {
            method: "PUT",
            url: "https://bucket.s3.amazonaws.com/key",
            region: "us-east-1",
            service: "s3",
            headers: &headers,
            payload: b"body2",
            token_header: TokenHeader::Standard,
        });

        // Different payloads should produce different content hashes
        assert_ne!(signed1.x_amz_content_sha256, signed2.x_amz_content_sha256);
    }

    #[test]
    fn test_credentials_from_env() {
        // This test just validates the error paths since we can't
        // reliably set env vars in parallel tests.
        // If AWS_ACCESS_KEY_ID is not set, it should return an error.
        let saved = std::env::var("AWS_ACCESS_KEY_ID").ok();
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        let result = Credentials::from_env();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("AWS_ACCESS_KEY_ID"));
        // Restore
        if let Some(val) = saved {
            std::env::set_var("AWS_ACCESS_KEY_ID", val);
        }
    }

    #[test]
    fn test_refreshable_creds_get_returns_current() {
        let creds = Credentials {
            access_key: "AKID".into(),
            secret_key: "SECRET".into(),
            session_token: None,
        };
        let refreshable = RefreshableCredentials::new(creds);
        let got = refreshable.get().unwrap();
        assert_eq!(got.access_key, "AKID");
        assert_eq!(got.secret_key, "SECRET");
        assert!(got.session_token.is_none());
    }

    #[test]
    fn test_refreshable_creds_detects_session_token() {
        // Without session token: 24-hour TTL
        let long_lived = Credentials {
            access_key: "AKID".into(),
            secret_key: "SECRET".into(),
            session_token: None,
        };
        let r1 = RefreshableCredentials::new(long_lived);
        assert_eq!(r1.ttl, Duration::from_secs(24 * 3600));

        // With session token: 50-minute TTL
        let temporary = Credentials {
            access_key: "AKID".into(),
            secret_key: "SECRET".into(),
            session_token: Some("TOKEN".into()),
        };
        let r2 = RefreshableCredentials::new(temporary);
        assert_eq!(r2.ttl, Duration::from_secs(50 * 60));
    }
}
