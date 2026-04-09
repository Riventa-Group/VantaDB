use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Token-bucket rate limiter with per-key tracking.
pub struct RateLimiter {
    buckets: DashMap<String, Bucket>,
    max_tokens: f64,
    refill_rate: f64, // tokens per second
}

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    /// Create a rate limiter with `max_tokens` capacity and `refill_rate` tokens/sec.
    pub fn new(max_tokens: f64, refill_rate: f64) -> Self {
        Self {
            buckets: DashMap::new(),
            max_tokens,
            refill_rate,
        }
    }

    /// Try to consume one token for the given key. Returns true if allowed.
    pub fn allow(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut entry = self.buckets.entry(key.to_string()).or_insert_with(|| Bucket {
            tokens: self.max_tokens,
            last_refill: now,
        });

        let bucket = entry.value_mut();
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_rate).min(self.max_tokens);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Global rate limiter: single shared bucket (not per-key).
pub struct GlobalRateLimiter {
    tokens: parking_lot::Mutex<(f64, Instant)>,
    max_tokens: f64,
    refill_rate: f64,
}

impl GlobalRateLimiter {
    pub fn new(max_tokens: f64, refill_rate: f64) -> Self {
        Self {
            tokens: parking_lot::Mutex::new((max_tokens, Instant::now())),
            max_tokens,
            refill_rate,
        }
    }

    pub fn allow(&self) -> bool {
        let now = Instant::now();
        let mut guard = self.tokens.lock();
        let (ref mut tokens, ref mut last) = *guard;

        let elapsed = now.duration_since(*last).as_secs_f64();
        *tokens = (*tokens + elapsed * self.refill_rate).min(self.max_tokens);
        *last = now;

        if *tokens >= 1.0 {
            *tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_per_key_allows_up_to_capacity() {
        let limiter = RateLimiter::new(3.0, 3.0);
        assert!(limiter.allow("ip1"));
        assert!(limiter.allow("ip1"));
        assert!(limiter.allow("ip1"));
        // 4th should be denied (no time to refill)
        assert!(!limiter.allow("ip1"));
        // Different key still has tokens
        assert!(limiter.allow("ip2"));
    }

    #[test]
    fn test_global_allows_up_to_capacity() {
        let limiter = GlobalRateLimiter::new(5.0, 10.0);
        for _ in 0..5 {
            assert!(limiter.allow());
        }
        assert!(!limiter.allow());
    }

    #[test]
    fn test_refill_after_wait() {
        let limiter = RateLimiter::new(1.0, 1000.0); // 1000 tokens/sec = instant refill
        assert!(limiter.allow("k"));
        assert!(!limiter.allow("k"));
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(limiter.allow("k"));
    }
}
