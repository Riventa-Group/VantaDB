use dashmap::DashMap;
use std::time::{Duration, Instant};

const MAX_ATTEMPTS: u32 = 5;
const WINDOW: Duration = Duration::from_secs(5 * 60);     // 5 minutes
const LOCKOUT: Duration = Duration::from_secs(15 * 60);   // 15 minutes

struct LoginRecord {
    attempts: Vec<Instant>,
    locked_until: Option<Instant>,
}

/// Tracks failed login attempts per username and enforces lockout.
pub struct LockoutTracker {
    records: DashMap<String, LoginRecord>,
}

impl LockoutTracker {
    pub fn new() -> Self {
        Self {
            records: DashMap::new(),
        }
    }

    /// Check if a user is currently locked out. Returns the remaining lockout
    /// duration if locked, or None if login is allowed.
    pub fn check(&self, username: &str) -> Option<Duration> {
        let record = self.records.get(username)?;
        if let Some(until) = record.locked_until {
            let now = Instant::now();
            if now < until {
                return Some(until - now);
            }
        }
        None
    }

    /// Record a failed login attempt. Returns true if the account is now locked out.
    pub fn record_failure(&self, username: &str) -> bool {
        let now = Instant::now();
        let mut entry = self.records.entry(username.to_string()).or_insert_with(|| LoginRecord {
            attempts: Vec::new(),
            locked_until: None,
        });

        let record = entry.value_mut();

        // If currently locked out, don't add more attempts
        if let Some(until) = record.locked_until {
            if now < until {
                return true;
            }
            // Lockout expired — reset
            record.locked_until = None;
            record.attempts.clear();
        }

        // Prune attempts outside the window
        record.attempts.retain(|t| now.duration_since(*t) < WINDOW);
        record.attempts.push(now);

        if record.attempts.len() as u32 >= MAX_ATTEMPTS {
            record.locked_until = Some(now + LOCKOUT);
            return true;
        }

        false
    }

    /// Clear lockout and attempts for a user (e.g., after successful login).
    pub fn clear(&self, username: &str) {
        self.records.remove(username);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_lockout_under_threshold() {
        let tracker = LockoutTracker::new();
        assert!(tracker.check("alice").is_none());

        for _ in 0..4 {
            assert!(!tracker.record_failure("alice"));
        }
        assert!(tracker.check("alice").is_none());
    }

    #[test]
    fn test_lockout_at_threshold() {
        let tracker = LockoutTracker::new();
        for _ in 0..4 {
            assert!(!tracker.record_failure("bob"));
        }
        // 5th attempt triggers lockout
        assert!(tracker.record_failure("bob"));
        assert!(tracker.check("bob").is_some());
    }

    #[test]
    fn test_clear_resets_lockout() {
        let tracker = LockoutTracker::new();
        for _ in 0..5 {
            tracker.record_failure("carol");
        }
        assert!(tracker.check("carol").is_some());

        tracker.clear("carol");
        assert!(tracker.check("carol").is_none());
    }

    #[test]
    fn test_independent_users() {
        let tracker = LockoutTracker::new();
        for _ in 0..5 {
            tracker.record_failure("dave");
        }
        assert!(tracker.check("dave").is_some());
        assert!(tracker.check("eve").is_none());
    }
}
