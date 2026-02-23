/// Retry policy with exponential backoff + jitter, and circuit breaker.

use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Trait for error types that can be classified as retriable or fatal.
pub trait Retriable: std::fmt::Display {
    /// Returns true if the error is transient and the operation should be retried.
    fn is_retriable(&self) -> bool;
}

/// Retry policy: exponential backoff with jitter, capped retries.
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl RetryPolicy {
    pub fn new(max_retries: u32, base_delay_ms: u64) -> Self {
        Self {
            max_retries,
            base_delay: Duration::from_millis(base_delay_ms),
            max_delay: Duration::from_secs(10),
        }
    }

    /// Execute an async closure with retries on retriable errors.
    /// Fatal errors are returned immediately without retrying.
    pub async fn execute<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: Retriable,
    {
        let mut attempt = 0u32;
        loop {
            match f().await {
                Ok(val) => return Ok(val),
                Err(e) => {
                    if !e.is_retriable() {
                        return Err(e);
                    }
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(e);
                    }
                    let delay = self.base_delay * 2u32.pow(attempt - 1);
                    let delay = delay.min(self.max_delay);
                    let jitter = Duration::from_millis(rand::random::<u64>() % 100);
                    tokio::time::sleep(delay + jitter).await;
                }
            }
        }
    }
}

/// Circuit breaker: trips after N consecutive failures, pauses for cooldown.
pub struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    threshold: u32,
    cooldown: Duration,
    /// If Some, the circuit is open until this instant.
    open_until: Mutex<Option<Instant>>,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cooldown_ms: u64) -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            threshold,
            cooldown: Duration::from_millis(cooldown_ms),
            open_until: Mutex::new(None),
        }
    }

    /// Check if the circuit is currently open (i.e., in cooldown).
    pub fn is_open(&self) -> bool {
        let guard = self.open_until.lock().unwrap_or_else(|e| e.into_inner());
        match *guard {
            Some(until) => {
                if Instant::now() >= until {
                    // Cooldown expired, but we leave closing to record_success
                    // or the next check. For simplicity, treat as half-open: allow.
                    false
                } else {
                    true
                }
            }
            None => false,
        }
    }

    /// Record a successful operation. Resets the failure counter and closes the circuit.
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        let mut guard = self.open_until.lock().unwrap_or_else(|e| e.into_inner());
        *guard = None;
    }

    /// Record a failed operation. If the threshold is reached, open the circuit.
    /// Returns true if the circuit just tripped open.
    pub fn record_failure(&self) -> bool {
        let prev = self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        let new_count = prev + 1;
        if new_count >= self.threshold {
            let mut guard = self.open_until.lock().unwrap_or_else(|e| e.into_inner());
            if guard.is_none() {
                *guard = Some(Instant::now() + self.cooldown);
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test error type implementing Retriable.
    #[derive(Debug)]
    enum TestError {
        Retriable(String),
        Fatal(String),
    }

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                TestError::Retriable(msg) => write!(f, "retriable: {}", msg),
                TestError::Fatal(msg) => write!(f, "fatal: {}", msg),
            }
        }
    }

    impl Retriable for TestError {
        fn is_retriable(&self) -> bool {
            matches!(self, TestError::Retriable(_))
        }
    }

    // -- CircuitBreaker tests --

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new(3, 1000);
        assert!(!cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_opens_at_threshold() {
        let cb = CircuitBreaker::new(3, 1000);
        assert!(!cb.record_failure()); // 1
        assert!(!cb.record_failure()); // 2
        assert!(cb.record_failure());  // 3 -- trips open
        assert!(cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_success_resets() {
        let cb = CircuitBreaker::new(3, 1000);
        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // resets counter
        assert!(!cb.is_open());
        // Now need 3 more failures to trip
        assert!(!cb.record_failure());
        assert!(!cb.record_failure());
        assert!(cb.record_failure());
    }

    #[test]
    fn test_circuit_breaker_cooldown_expires() {
        let cb = CircuitBreaker::new(1, 10); // 10ms cooldown
        assert!(cb.record_failure()); // trips
        assert!(cb.is_open());
        std::thread::sleep(Duration::from_millis(20));
        assert!(!cb.is_open()); // cooldown expired -> half-open
    }

    #[test]
    fn test_circuit_breaker_does_not_re_trip_while_open() {
        let cb = CircuitBreaker::new(2, 5000);
        cb.record_failure();
        assert!(cb.record_failure()); // trips
        // Further failures should not return true again (already open)
        assert!(!cb.record_failure());
        assert!(!cb.record_failure());
    }

    #[test]
    fn test_circuit_breaker_success_closes_after_cooldown() {
        let cb = CircuitBreaker::new(1, 10);
        cb.record_failure(); // trips
        std::thread::sleep(Duration::from_millis(20));
        // Half-open: not open anymore
        assert!(!cb.is_open());
        cb.record_success(); // fully close
        assert!(!cb.is_open());
    }

    // -- RetryPolicy tests --

    #[test]
    fn test_retry_policy_new() {
        let rp = RetryPolicy::new(5, 200);
        assert_eq!(rp.max_retries, 5);
        assert_eq!(rp.base_delay, Duration::from_millis(200));
        assert_eq!(rp.max_delay, Duration::from_secs(10));
    }

    #[tokio::test]
    async fn test_retry_policy_success_first_try() {
        let rp = RetryPolicy::new(3, 10);
        let result = rp.execute(|| async { Ok::<_, TestError>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_policy_fatal_not_retried() {
        let rp = RetryPolicy::new(3, 10);
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = rp
            .execute(|| {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async { Err::<(), _>(TestError::Fatal("permanent".into())) }
            })
            .await;
        assert!(matches!(result, Err(TestError::Fatal(_))));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_retry_policy_retries_then_succeeds() {
        let rp = RetryPolicy::new(3, 1); // 1ms base delay for fast test
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = rp
            .execute(|| {
                let n = attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async move {
                    if n < 2 {
                        Err::<u32, _>(TestError::Retriable("try again".into()))
                    } else {
                        Ok(99)
                    }
                }
            })
            .await;
        assert_eq!(result.unwrap(), 99);
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_policy_exhausts_retries() {
        let rp = RetryPolicy::new(2, 1);
        let result = rp
            .execute(|| async { Err::<(), _>(TestError::Retriable("fail".into())) })
            .await;
        assert!(matches!(result, Err(TestError::Retriable(_))));
    }

    #[test]
    fn test_circuit_breaker_threshold_1() {
        let cb = CircuitBreaker::new(1, 5000);
        assert!(!cb.is_open());
        // Single failure should trip the breaker
        assert!(cb.record_failure());
        assert!(cb.is_open());
    }

    #[tokio::test]
    async fn test_retry_policy_zero_retries() {
        let rp = RetryPolicy::new(0, 10);
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = rp
            .execute(|| {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async { Err::<(), _>(TestError::Retriable("fail".into())) }
            })
            .await;
        assert!(matches!(result, Err(TestError::Retriable(_))));
        // With max_retries=0, should_retry is false on the first attempt
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
