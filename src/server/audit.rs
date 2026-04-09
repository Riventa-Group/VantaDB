use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::storage::StorageEngine;

const AUDIT_TABLE: &str = "_vanta_audit";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub username: String,
    pub role: String,
    pub operation: String,
    pub database: Option<String>,
    pub collection: Option<String>,
    pub detail: String,
    pub success: bool,
    pub error: Option<String>,
}

/// Filter for querying the audit log.
#[derive(Default)]
pub struct AuditFilter {
    pub username: Option<String>,
    pub operation: Option<String>,
    pub database: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub limit: usize,
}

pub struct AuditLogger {
    engine: Arc<StorageEngine>,
}

impl AuditLogger {
    pub fn new(engine: Arc<StorageEngine>) -> std::io::Result<Self> {
        if !engine.table_exists(AUDIT_TABLE) {
            engine.create_table(AUDIT_TABLE)?;
        }
        Ok(Self { engine })
    }

    /// Log an audit event. Fire-and-forget — callers should not block on this.
    pub fn log(&self, event: AuditEvent) {
        let data = match serde_json::to_vec(&event) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("  audit: failed to serialize event: {}", e);
                return;
            }
        };
        // Key: reverse timestamp + UUID for newest-first ordering
        // Reverse timestamp = i64::MAX - unix_millis
        let ts = i64::MAX - event.timestamp.timestamp_millis();
        let key = format!("{:020}_{}", ts, event.id);
        if let Err(e) = self.engine.put(AUDIT_TABLE, &key, &data) {
            eprintln!("  audit: failed to persist event: {}", e);
        }
    }

    /// Create and log an event in one call.
    pub fn record(
        &self,
        username: &str,
        role: &str,
        operation: &str,
        database: Option<&str>,
        collection: Option<&str>,
        detail: &str,
        success: bool,
        error: Option<&str>,
    ) {
        self.log(AuditEvent {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            username: username.to_string(),
            role: role.to_string(),
            operation: operation.to_string(),
            database: database.map(String::from),
            collection: collection.map(String::from),
            detail: detail.to_string(),
            success,
            error: error.map(String::from),
        });
    }

    /// Query the audit log with optional filters.
    pub fn query(&self, filter: &AuditFilter) -> Vec<AuditEvent> {
        let limit = if filter.limit == 0 { 100 } else { filter.limit };
        let mut keys = self.engine.list_keys(AUDIT_TABLE);
        // Sort keys lexicographically — reverse-timestamp prefix ensures newest first
        keys.sort();
        let mut results = Vec::new();

        for key in keys {
            if results.len() >= limit {
                break;
            }
            let data = match self.engine.get(AUDIT_TABLE, &key) {
                Some(d) => d,
                None => continue,
            };
            let event: AuditEvent = match serde_json::from_slice(&data) {
                Ok(e) => e,
                Err(_) => continue,
            };

            // Apply filters
            if let Some(ref u) = filter.username {
                if event.username != *u {
                    continue;
                }
            }
            if let Some(ref op) = filter.operation {
                if event.operation != *op {
                    continue;
                }
            }
            if let Some(ref db) = filter.database {
                if event.database.as_deref() != Some(db.as_str()) {
                    continue;
                }
            }
            if let Some(since) = filter.since {
                if event.timestamp < since {
                    continue;
                }
            }

            results.push(event);
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, AuditLogger) {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let logger = AuditLogger::new(engine).unwrap();
        (dir, logger)
    }

    #[test]
    fn test_log_and_query() {
        let (_dir, logger) = setup();
        logger.record("alice", "admin", "insert", Some("db1"), Some("col1"), "{}", true, None);
        logger.record("bob", "rw", "delete", Some("db1"), Some("col1"), "{}", true, None);

        let all = logger.query(&AuditFilter { limit: 100, ..Default::default() });
        assert_eq!(all.len(), 2);
        // Newest first
        assert_eq!(all[0].username, "alice");
    }

    #[test]
    fn test_filter_by_username() {
        let (_dir, logger) = setup();
        logger.record("alice", "admin", "insert", None, None, "{}", true, None);
        logger.record("bob", "rw", "insert", None, None, "{}", true, None);
        logger.record("alice", "admin", "delete", None, None, "{}", true, None);

        let filtered = logger.query(&AuditFilter {
            username: Some("alice".to_string()),
            limit: 100,
            ..Default::default()
        });
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|e| e.username == "alice"));
    }

    #[test]
    fn test_filter_by_operation() {
        let (_dir, logger) = setup();
        logger.record("alice", "admin", "insert", None, None, "{}", true, None);
        logger.record("alice", "admin", "delete", None, None, "{}", true, None);
        logger.record("alice", "admin", "insert", None, None, "{}", true, None);

        let filtered = logger.query(&AuditFilter {
            operation: Some("insert".to_string()),
            limit: 100,
            ..Default::default()
        });
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_limit() {
        let (_dir, logger) = setup();
        for i in 0..10 {
            logger.record("alice", "admin", "insert", None, None, &format!("{}", i), true, None);
        }

        let limited = logger.query(&AuditFilter { limit: 5, ..Default::default() });
        assert_eq!(limited.len(), 5);
    }
}
