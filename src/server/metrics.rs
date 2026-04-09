use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const LATENCY_WINDOW: usize = 1000;

/// Lock-free metrics collector for server instrumentation.
pub struct MetricsCollector {
    start_time: Instant,
    counters: DashMap<String, AtomicU64>,
    gauges: DashMap<String, AtomicU64>,
    latencies: DashMap<String, Mutex<VecDeque<u64>>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            counters: DashMap::new(),
            gauges: DashMap::new(),
            latencies: DashMap::new(),
        }
    }

    // ---- Counters -----------------------------------------------

    pub fn inc(&self, name: &str) {
        self.counters
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn counter(&self, name: &str) -> u64 {
        self.counters
            .get(name)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn all_counters(&self) -> Vec<(String, u64)> {
        self.counters
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect()
    }

    // ---- Gauges -------------------------------------------------

    pub fn gauge_set(&self, name: &str, value: u64) {
        self.gauges
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .store(value, Ordering::Relaxed);
    }

    pub fn gauge_inc(&self, name: &str) {
        self.gauges
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn gauge_dec(&self, name: &str) {
        self.gauges
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_sub(1, Ordering::Relaxed);
    }

    pub fn gauge(&self, name: &str) -> u64 {
        self.gauges
            .get(name)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn all_gauges(&self) -> Vec<(String, u64)> {
        let mut g: Vec<_> = self
            .gauges
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        g.push(("uptime_seconds".to_string(), self.start_time.elapsed().as_secs()));
        g
    }

    // ---- Latency ------------------------------------------------

    /// Record a latency sample in microseconds for a given operation.
    pub fn record_latency(&self, op: &str, micros: u64) {
        let entry = self
            .latencies
            .entry(op.to_string())
            .or_insert_with(|| Mutex::new(VecDeque::with_capacity(LATENCY_WINDOW)));
        let mut window = entry.lock();
        if window.len() >= LATENCY_WINDOW {
            window.pop_front();
        }
        window.push_back(micros);
    }

    /// Get p50, p95, p99 latencies in microseconds for a given operation.
    pub fn percentiles(&self, op: &str) -> Option<(u64, u64, u64)> {
        let entry = self.latencies.get(op)?;
        let window = entry.lock();
        if window.is_empty() {
            return None;
        }
        let mut sorted: Vec<u64> = window.iter().copied().collect();
        sorted.sort_unstable();
        Some(compute_percentiles(&sorted))
    }

    /// Get all latency operations and their percentiles.
    pub fn all_latencies(&self) -> Vec<(String, u64, u64, u64)> {
        self.latencies
            .iter()
            .filter_map(|e| {
                let op = e.key().clone();
                let window = e.value().lock();
                if window.is_empty() {
                    return None;
                }
                let mut sorted: Vec<u64> = window.iter().copied().collect();
                sorted.sort_unstable();
                let (p50, p95, p99) = compute_percentiles(&sorted);
                Some((op, p50, p95, p99))
            })
            .collect()
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

fn percentile_index(len: usize, p: f64) -> usize {
    if len == 0 { return 0; }
    let idx = ((p / 100.0) * len as f64).ceil() as usize;
    idx.saturating_sub(1).min(len - 1)
}

fn compute_percentiles(sorted: &[u64]) -> (u64, u64, u64) {
    let len = sorted.len();
    let p50 = sorted[percentile_index(len, 50.0)];
    let p95 = sorted[percentile_index(len, 95.0)];
    let p99 = sorted[percentile_index(len, 99.0)];
    (p50, p95, p99)
}

/// Record an operation's outcome: increment counter and record latency.
pub fn record_op(metrics: &MetricsCollector, op: &str, start: Instant, success: bool) {
    let micros = start.elapsed().as_micros() as u64;
    metrics.inc(&format!("ops.{}", op));
    if !success {
        metrics.inc(&format!("ops.{}.errors", op));
    }
    metrics.record_latency(op, micros);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counters() {
        let m = MetricsCollector::new();
        assert_eq!(m.counter("ops.insert"), 0);
        m.inc("ops.insert");
        m.inc("ops.insert");
        m.inc("ops.insert");
        assert_eq!(m.counter("ops.insert"), 3);
    }

    #[test]
    fn test_gauges() {
        let m = MetricsCollector::new();
        m.gauge_set("active_tx", 5);
        assert_eq!(m.gauge("active_tx"), 5);
        m.gauge_inc("active_tx");
        assert_eq!(m.gauge("active_tx"), 6);
        m.gauge_dec("active_tx");
        assert_eq!(m.gauge("active_tx"), 5);
    }

    #[test]
    fn test_latency_percentiles() {
        let m = MetricsCollector::new();
        // Record 100 samples: 1, 2, 3, ..., 100
        for i in 1..=100 {
            m.record_latency("insert", i);
        }
        let (p50, p95, p99) = m.percentiles("insert").unwrap();
        assert_eq!(p50, 50);
        assert_eq!(p95, 95);
        assert_eq!(p99, 99);
    }

    #[test]
    fn test_record_op() {
        let m = MetricsCollector::new();
        let start = Instant::now();
        record_op(&m, "insert", start, true);
        assert_eq!(m.counter("ops.insert"), 1);
        assert_eq!(m.counter("ops.insert.errors"), 0);

        record_op(&m, "insert", start, false);
        assert_eq!(m.counter("ops.insert"), 2);
        assert_eq!(m.counter("ops.insert.errors"), 1);
    }

    #[test]
    fn test_uptime() {
        let m = MetricsCollector::new();
        // Just verify it doesn't panic and returns >=0
        assert!(m.uptime_seconds() < 5);
    }
}
