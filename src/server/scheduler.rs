use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

use crate::db::DatabaseManager;
use super::metrics::MetricsCollector;

const REAP_INTERVAL: Duration = Duration::from_secs(10);
const GC_INTERVAL: Duration = Duration::from_secs(60);
const COMPACT_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Background scheduler for periodic maintenance tasks:
/// - Transaction reaping (every 10s)
/// - MVCC garbage collection (every 60s)
/// - WAL compaction (every 5min)
pub struct BackgroundScheduler {
    handle: JoinHandle<()>,
}

impl BackgroundScheduler {
    pub fn start(
        db_manager: Arc<DatabaseManager>,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut reap_interval = tokio::time::interval(REAP_INTERVAL);
            let mut gc_interval = tokio::time::interval(GC_INTERVAL);
            let mut compact_interval = tokio::time::interval(COMPACT_INTERVAL);

            // Don't run immediately on startup — tick once to consume the initial instant
            reap_interval.tick().await;
            gc_interval.tick().await;
            compact_interval.tick().await;

            loop {
                tokio::select! {
                    _ = reap_interval.tick() => {
                        let reaped = db_manager.tx_manager.reap_expired();
                        if reaped > 0 {
                            eprintln!("  scheduler: reaped {} expired transactions", reaped);
                            for _ in 0..reaped {
                                metrics.inc("tx_reaped");
                            }
                        }
                    }
                    _ = gc_interval.tick() => {
                        let stores = db_manager.all_stores();
                        let mut total_pruned = 0;
                        for store in &stores {
                            total_pruned += store.gc();
                        }
                        if total_pruned > 0 {
                            metrics.inc("gc_runs");
                            for _ in 0..total_pruned {
                                metrics.inc("versions_pruned");
                            }
                        }
                    }
                    _ = compact_interval.tick() => {
                        let stores = db_manager.all_stores();
                        let mut total_compacted = 0;
                        for store in &stores {
                            match store.compact_all() {
                                Ok(n) => total_compacted += n,
                                Err(e) => {
                                    eprintln!("  scheduler: compaction error: {}", e);
                                }
                            }
                        }
                        if total_compacted > 0 {
                            metrics.inc("wal_compactions");
                        }
                    }
                }
            }
        });

        Self { handle }
    }

    /// Abort the scheduler task (idempotent).
    pub fn stop(&self) {
        self.handle.abort();
    }
}

impl Drop for BackgroundScheduler {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_scheduler_starts_and_stops() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("data");
        std::fs::create_dir_all(&data_path).unwrap();
        let mgr = Arc::new(DatabaseManager::new(&data_path).unwrap());
        let metrics = Arc::new(MetricsCollector::new());

        let scheduler = BackgroundScheduler::start(mgr, metrics);

        // Let it run briefly
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Stop should not panic
        scheduler.stop();
    }

    #[tokio::test]
    async fn test_reaper_cleans_expired() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("data");
        std::fs::create_dir_all(&data_path).unwrap();
        let mgr = Arc::new(DatabaseManager::new(&data_path).unwrap());

        // Begin a transaction (default 30s timeout — won't expire)
        let _id = mgr.begin_transaction();
        assert_eq!(mgr.tx_manager.active_count(), 1);

        // Reaping should find 0 expired
        let reaped = mgr.tx_manager.reap_expired();
        assert_eq!(reaped, 0);
        assert_eq!(mgr.tx_manager.active_count(), 1);

        // Rollback to clean up
        mgr.rollback_transaction(&_id).unwrap();
        assert_eq!(mgr.tx_manager.active_count(), 0);
    }
}
