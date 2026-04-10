use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Tracks read lease validity on follower nodes.
///
/// The leader piggybacks lease grants on AppendEntries responses.
/// While the lease is valid, the follower can serve linearizable reads
/// locally without forwarding to the leader (because no election
/// can have occurred within the lease window).
pub struct LeaseTracker {
    valid_until: Arc<RwLock<Instant>>,
    duration: Duration,
}

impl LeaseTracker {
    pub fn new(lease_duration_secs: u64) -> Self {
        Self {
            // Start with an expired lease
            valid_until: Arc::new(RwLock::new(Instant::now())),
            duration: Duration::from_secs(lease_duration_secs),
        }
    }

    /// Renew the lease (called when AppendEntries succeeds from leader).
    pub fn renew(&self) {
        *self.valid_until.write() = Instant::now() + self.duration;
    }

    /// Check if the lease is currently valid.
    pub fn is_valid(&self) -> bool {
        Instant::now() < *self.valid_until.read()
    }

    /// Get remaining lease time, or None if expired.
    pub fn remaining(&self) -> Option<Duration> {
        let until = *self.valid_until.read();
        let now = Instant::now();
        if now < until {
            Some(until - now)
        } else {
            None
        }
    }

    /// Get a handle for external updates.
    pub fn handle(&self) -> Arc<RwLock<Instant>> {
        Arc::clone(&self.valid_until)
    }
}

/// Read consistency level specified by the client.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReadConsistency {
    /// Always route to leader (default, strongest).
    Strong,
    /// Serve from any node with a valid lease (linearizable within lease window).
    Lease,
}

impl ReadConsistency {
    /// Parse from gRPC metadata header value.
    pub fn from_header(value: &str) -> Self {
        match value.to_lowercase().as_str() {
            "lease" => ReadConsistency::Lease,
            _ => ReadConsistency::Strong,
        }
    }
}

/// Extract read consistency from a gRPC request's metadata.
pub fn extract_consistency<T>(request: &tonic::Request<T>) -> ReadConsistency {
    request
        .metadata()
        .get("x-read-consistency")
        .and_then(|v| v.to_str().ok())
        .map(ReadConsistency::from_header)
        .unwrap_or(ReadConsistency::Strong)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_starts_expired() {
        let tracker = LeaseTracker::new(5);
        assert!(!tracker.is_valid());
        assert!(tracker.remaining().is_none());
    }

    #[test]
    fn test_lease_renew_and_valid() {
        let tracker = LeaseTracker::new(5);
        tracker.renew();
        assert!(tracker.is_valid());
        let remaining = tracker.remaining().unwrap();
        assert!(remaining.as_secs() >= 4); // at least 4 of the 5 seconds
    }

    #[test]
    fn test_lease_expires() {
        let tracker = LeaseTracker::new(0); // 0 second lease = expires immediately
        tracker.renew();
        std::thread::sleep(Duration::from_millis(10));
        assert!(!tracker.is_valid());
    }

    #[test]
    fn test_consistency_parsing() {
        assert_eq!(ReadConsistency::from_header("lease"), ReadConsistency::Lease);
        assert_eq!(ReadConsistency::from_header("Lease"), ReadConsistency::Lease);
        assert_eq!(ReadConsistency::from_header("strong"), ReadConsistency::Strong);
        assert_eq!(ReadConsistency::from_header("anything"), ReadConsistency::Strong);
    }
}
