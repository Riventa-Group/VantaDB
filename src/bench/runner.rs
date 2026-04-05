use colored::*;
use serde_json::{json, Value};
use std::hint::black_box;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::storage::{StorageEngine, Table};

// ─────────────────────────────────────────────────────────
// Latency collector — samples per-op latencies for percentile analysis
// ─────────────────────────────────────────────────────────

struct LatencyCollector {
    samples: Vec<u64>, // nanoseconds
}

impl LatencyCollector {
    fn new() -> Self {
        Self {
            samples: Vec::new(),
        }
    }

    fn with_capacity(cap: usize) -> Self {
        Self {
            samples: Vec::with_capacity(cap),
        }
    }

    #[inline(always)]
    fn record(&mut self, ns: u64) {
        self.samples.push(ns);
    }

    fn merge(collectors: Vec<Self>) -> Self {
        let total: usize = collectors.iter().map(|c| c.samples.len()).sum();
        let mut merged = Self::with_capacity(total);
        for mut c in collectors {
            merged.samples.append(&mut c.samples);
        }
        merged.samples.sort_unstable();
        merged
    }

    fn percentile(&self, p: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let idx = ((p / 100.0) * (self.samples.len() as f64 - 1.0)).ceil() as usize;
        self.samples[idx.min(self.samples.len() - 1)]
    }

    fn p50(&self) -> u64 {
        self.percentile(50.0)
    }
    fn p95(&self) -> u64 {
        self.percentile(95.0)
    }
    fn p99(&self) -> u64 {
        self.percentile(99.0)
    }
    fn p999(&self) -> u64 {
        self.percentile(99.9)
    }
    fn min(&self) -> u64 {
        self.samples.first().copied().unwrap_or(0)
    }
    fn max(&self) -> u64 {
        self.samples.last().copied().unwrap_or(0)
    }
    fn avg(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        self.samples.iter().sum::<u64>() as f64 / self.samples.len() as f64
    }
    fn count(&self) -> usize {
        self.samples.len()
    }
}

// ─────────────────────────────────────────────────────────
// Resource utilization tracker
// ─────────────────────────────────────────────────────────

fn get_rss_bytes() -> u64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<u64>() {
                        return kb * 1024;
                    }
                }
            }
        }
    }
    0
}

// ─────────────────────────────────────────────────────────
// Bench result with full metrics
// ─────────────────────────────────────────────────────────

struct BenchResult {
    name: String,
    ops: u64,
    elapsed: Duration,
    data_size: u64,
    latency: Option<LatencyCollector>,
    read_ops: u64,
    write_ops: u64,
    mem_before: u64,
    mem_after: u64,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.elapsed.as_secs_f64()
    }

    fn avg_latency_ns(&self) -> f64 {
        if let Some(ref l) = self.latency {
            l.avg()
        } else {
            self.elapsed.as_nanos() as f64 / self.ops as f64
        }
    }

    fn throughput_mb(&self) -> f64 {
        if self.data_size == 0 {
            return 0.0;
        }
        (self.data_size as f64 / (1024.0 * 1024.0)) / self.elapsed.as_secs_f64()
    }

    fn mem_delta_mb(&self) -> f64 {
        if self.mem_after > self.mem_before {
            (self.mem_after - self.mem_before) as f64 / (1024.0 * 1024.0)
        } else {
            0.0
        }
    }

    fn rw_ratio_str(&self) -> String {
        if self.read_ops == 0 && self.write_ops == 0 {
            return "—".to_string();
        }
        if self.read_ops == 0 {
            return "0:100 (R:W)".to_string();
        }
        if self.write_ops == 0 {
            return "100:0 (R:W)".to_string();
        }
        let total = (self.read_ops + self.write_ops) as f64;
        let r_pct = (self.read_ops as f64 / total * 100.0).round() as u64;
        let w_pct = 100 - r_pct;
        format!("{}:{} (R:W)", r_pct, w_pct)
    }
}

// ─────────────────────────────────────────────────────────
// Formatting helpers
// ─────────────────────────────────────────────────────────

fn format_ops(n: f64) -> String {
    if n >= 1_000_000.0 {
        format!("{:.2}M", n / 1_000_000.0)
    } else if n >= 1_000.0 {
        format!("{:.2}K", n / 1_000.0)
    } else {
        format!("{:.0}", n)
    }
}

fn format_latency(ns: f64) -> String {
    if ns >= 1_000_000.0 {
        format!("{:.2}ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:.2}μs", ns / 1_000.0)
    } else {
        format!("{:.0}ns", ns)
    }
}

fn format_latency_u64(ns: u64) -> String {
    format_latency(ns as f64)
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn progress_bar(pct: f64, width: usize) -> String {
    let filled = (pct * width as f64) as usize;
    let empty = width.saturating_sub(filled);
    let bar_filled: String = "█".repeat(filled);
    let bar_empty: String = "░".repeat(empty);
    format!(
        "{}{}",
        bar_filled.truecolor(120, 80, 255),
        bar_empty.truecolor(40, 40, 60)
    )
}

fn print_result(result: &BenchResult, idx: usize, total: usize) {
    let ops_str = format_ops(result.ops_per_sec());
    let latency_str = format_latency(result.avg_latency_ns());
    let throughput_str = if result.data_size > 0 {
        format!("{:.1} MB/s", result.throughput_mb())
    } else {
        "—".to_string()
    };

    println!(
        "  {} {}",
        format!("[{}/{}]", idx, total).dimmed(),
        result.name.bold()
    );
    println!(
        "        {} {} ops/sec   {} {} avg   {} {}",
        "▸".truecolor(120, 80, 255),
        ops_str.truecolor(120, 255, 120).bold(),
        "▸".truecolor(120, 80, 255),
        latency_str.truecolor(255, 200, 100),
        "▸".truecolor(120, 80, 255),
        throughput_str.truecolor(180, 180, 255),
    );

    // Latency percentiles
    if let Some(ref lat) = result.latency {
        if lat.count() > 0 {
            println!(
                "        {} P50 {}  P95 {}  P99 {}  P99.9 {}",
                "▸".truecolor(120, 80, 255),
                format_latency_u64(lat.p50()).truecolor(100, 255, 200),
                format_latency_u64(lat.p95()).truecolor(200, 255, 100),
                format_latency_u64(lat.p99()).truecolor(255, 200, 100).bold(),
                format_latency_u64(lat.p999()).truecolor(255, 120, 80),
            );
            println!(
                "        {} min {}  max {}  samples {}",
                "▸".truecolor(80, 80, 120),
                format_latency_u64(lat.min()).dimmed(),
                format_latency_u64(lat.max()).dimmed(),
                lat.count().to_string().dimmed(),
            );
        }
    }

    // Read/write ratio
    if result.read_ops > 0 || result.write_ops > 0 {
        println!(
            "        {} R/W ratio: {}  (reads: {} writes: {})",
            "▸".truecolor(80, 80, 120),
            result.rw_ratio_str().truecolor(180, 200, 255),
            result.read_ops.to_string().dimmed(),
            result.write_ops.to_string().dimmed(),
        );
    }

    // Resource utilization
    if result.mem_after > 0 {
        println!(
            "        {} Memory: {} → {} (Δ {:.1} MB)",
            "▸".truecolor(80, 80, 120),
            format_bytes(result.mem_before).dimmed(),
            format_bytes(result.mem_after).dimmed(),
            result.mem_delta_mb(),
        );
    }

    println!(
        "        {} {} ops in {:.2}ms",
        "↳".dimmed(),
        result.ops,
        result.elapsed.as_secs_f64() * 1000.0
    );
    println!();
}

fn make_doc(i: u64) -> Value {
    json!({
        "_id": format!("id_{}", i),
        "username": format!("user_{}", i),
        "email": format!("user_{}@vantadb.io", i),
        "age": (i % 80) + 18,
        "balance": (i as f64) * 1.37,
        "active": i % 2 == 0,
        "role": if i % 10 == 0 { "admin" } else { "user" },
        "metadata": {
            "created": "2026-01-01T00:00:00Z",
            "region": format!("region_{}", i % 5),
            "tier": format!("tier_{}", i % 3)
        }
    })
}

fn precompute_keys(prefix: &str, count: u64) -> Vec<String> {
    (0..count).map(|i| format!("{}_{}", prefix, i)).collect()
}

fn fill_table(table: &Table, keys: &[String], value: &Arc<[u8]>) {
    for key in keys {
        table.insert(key.clone(), Arc::clone(value));
    }
}

/// Sampling rate for latency collection. We sample 1-in-N ops to avoid
/// measurement overhead dominating the benchmark on very fast paths.
const LATENCY_SAMPLE_RATE: u64 = 16;

pub fn run_benchmark(data_dir: &Path) -> io::Result<()> {
    let bench_dir = data_dir.join("_benchmark_temp");
    if bench_dir.exists() {
        std::fs::remove_dir_all(&bench_dir)?;
    }

    let num_threads = num_cpus();
    let global_start = Instant::now();
    let initial_rss = get_rss_bytes();

    println!();
    println!(
        "  {} {}",
        "⚡".truecolor(255, 200, 50),
        "VANTADB BENCHMARK SUITE".bold().truecolor(120, 80, 255)
    );
    println!(
        "  {}",
        "══════════════════════════════════════════════════"
            .truecolor(60, 60, 80)
    );
    println!(
        "  {} Pushing the engine to its absolute limits...",
        "→".truecolor(120, 80, 255)
    );
    println!(
        "  {} Using {} threads across all cores",
        "→".truecolor(120, 80, 255),
        num_threads.to_string().bold()
    );
    println!(
        "  {} Initial RSS: {}",
        "→".truecolor(120, 80, 255),
        format_bytes(initial_rss).dimmed()
    );
    println!();

    let mut results: Vec<BenchResult> = Vec::new();
    let total_tests = 12;

    // ─────────────────────────────────────────────────
    // TEST 1: Parallel memory writes (all threads, pre-allocated)
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(1.0 / total_tests as f64, 20),
            "Memory writes...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let engine = StorageEngine::open(&bench_dir.join("t1"))?;
        let ops_per_thread = 500_000u64 / num_threads as u64;
        let total_ops = ops_per_thread * num_threads as u64;
        engine.create_table_with_capacity("bench", total_ops as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;

        let mut thread_keys: Vec<Option<Vec<String>>> = (0..num_threads)
            .map(|t| {
                Some(
                    (0..ops_per_thread)
                        .map(|i| format!("key_{}_{}", t, i))
                        .collect(),
                )
            })
            .collect();

        let start = Instant::now();
        let collectors: Vec<LatencyCollector> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..num_threads {
                let table = &table;
                let raw = doc_bytes.as_slice();
                let keys = thread_keys[t].take().unwrap();
                handles.push(s.spawn(move || {
                    let doc: Arc<[u8]> = Arc::from(raw);
                    let mut lc = LatencyCollector::with_capacity(
                        (ops_per_thread / LATENCY_SAMPLE_RATE) as usize + 1,
                    );
                    for (i, key) in keys.into_iter().enumerate() {
                        let op_start = Instant::now();
                        table.insert(key, Arc::clone(&doc));
                        if i as u64 % LATENCY_SAMPLE_RATE == 0 {
                            lc.record(op_start.elapsed().as_nanos() as u64);
                        }
                    }
                    lc
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let elapsed = start.elapsed();
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(1.0 / total_tests as f64, 20),
            "Memory writes ✓".green()
        );

        results.push(BenchResult {
            name: format!("Parallel memory writes ({} threads, 500K docs)", num_threads),
            ops: total_ops,
            elapsed,
            data_size: total_ops * doc_size,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: 0,
            write_ops: total_ops,
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 2: Parallel memory reads (snapshot, lock-free)
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(2.0 / total_tests as f64, 20),
            "Memory reads...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let engine = StorageEngine::open(&bench_dir.join("t2"))?;
        let data_count = 500_000u64;
        engine.create_table_with_capacity("bench", data_count as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;
        let doc: Arc<[u8]> = Arc::from(doc_bytes.into_boxed_slice());

        let keys = precompute_keys("key", data_count);
        fill_table(&table, &keys, &doc);
        drop(table);

        let snapshot = Arc::new(engine.snapshot("bench").unwrap());
        let ops_per_thread = 500_000u64 / num_threads as u64;
        let total_ops = ops_per_thread * num_threads as u64;
        let keys = Arc::new(keys);

        let start = Instant::now();
        let collectors: Vec<LatencyCollector> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..num_threads {
                let snapshot = Arc::clone(&snapshot);
                let keys = Arc::clone(&keys);
                handles.push(s.spawn(move || {

                    let mut lc = LatencyCollector::with_capacity(
                        (ops_per_thread / LATENCY_SAMPLE_RATE) as usize + 1,
                    );
                    let key_count = keys.len();
                    let offset = t * ops_per_thread as usize;
                    for i in 0..ops_per_thread as usize {
                        let op_start = Instant::now();
                        black_box(snapshot.get(keys[(offset + i) % key_count].as_str()));
                        if i as u64 % LATENCY_SAMPLE_RATE == 0 {
                            lc.record(op_start.elapsed().as_nanos() as u64);
                        }
                    }
                    lc
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let elapsed = start.elapsed();
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(2.0 / total_tests as f64, 20),
            "Memory reads ✓".green()
        );

        results.push(BenchResult {
            name: format!(
                "Parallel snapshot reads ({} threads, 500K lookups)",
                num_threads
            ),
            ops: total_ops,
            elapsed,
            data_size: total_ops * doc_size,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: total_ops,
            write_ops: 0,
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 3: Writes with disk persistence
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(3.0 / total_tests as f64, 20),
            "Persisted writes...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let engine = StorageEngine::open(&bench_dir.join("t3"))?;
        engine.create_table_with_capacity("bench", 10_000)?;

        let count = 10_000u64;
        let doc = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc.len() as u64;
        let keys = precompute_keys("key", count);

        let mut lc = LatencyCollector::with_capacity(count as usize);
        let start = Instant::now();
        for key in &keys {
            let op_start = Instant::now();
            engine.put("bench", key, &doc)?;
            lc.record(op_start.elapsed().as_nanos() as u64);
        }
        let elapsed = start.elapsed();
        lc.samples.sort_unstable();
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(3.0 / total_tests as f64, 20),
            "Persisted writes ✓".green()
        );

        results.push(BenchResult {
            name: "Persisted writes with fsync (10K docs)".to_string(),
            ops: count,
            elapsed,
            data_size: count * doc_size,
            latency: Some(lc),
            read_ops: 0,
            write_ops: count,
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 4: Parallel batch insert (all threads)
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(4.0 / total_tests as f64, 20),
            "Batch insert...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let engine = StorageEngine::open(&bench_dir.join("t4"))?;
        let count = 200_000u64;
        engine.create_table_with_capacity("bench", count as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;

        let ops_per_thread = count / num_threads as u64;
        let mut thread_keys: Vec<Option<Vec<String>>> = (0..num_threads)
            .map(|t| {
                Some(
                    (0..ops_per_thread)
                        .map(|i| format!("key_{}_{}", t, i))
                        .collect(),
                )
            })
            .collect();

        let start = Instant::now();
        let collectors: Vec<LatencyCollector> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..num_threads {
                let table = &table;
                let raw = doc_bytes.as_slice();
                let keys = thread_keys[t].take().unwrap();
                handles.push(s.spawn(move || {
                    let doc: Arc<[u8]> = Arc::from(raw);
                    let mut lc = LatencyCollector::with_capacity(
                        (ops_per_thread / LATENCY_SAMPLE_RATE) as usize + 1,
                    );
                    for (i, key) in keys.into_iter().enumerate() {
                        let op_start = Instant::now();
                        table.insert(key, Arc::clone(&doc));
                        if i as u64 % LATENCY_SAMPLE_RATE == 0 {
                            lc.record(op_start.elapsed().as_nanos() as u64);
                        }
                    }
                    lc
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        engine.compact("bench")?;
        let elapsed = start.elapsed();
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(4.0 / total_tests as f64, 20),
            "Batch insert ✓".green()
        );

        results.push(BenchResult {
            name: format!(
                "Parallel batch insert + flush ({} threads, 200K docs)",
                num_threads
            ),
            ops: count,
            elapsed,
            data_size: count * doc_size,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: 0,
            write_ops: count,
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 5: Concurrent snapshot reads (high volume)
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(5.0 / total_tests as f64, 20),
            "Concurrent reads...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let engine = StorageEngine::open(&bench_dir.join("t5"))?;
        let data_count = 200_000u64;
        engine.create_table_with_capacity("bench", data_count as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;
        let doc: Arc<[u8]> = Arc::from(doc_bytes.into_boxed_slice());

        let keys = precompute_keys("key", data_count);
        fill_table(&table, &keys, &doc);
        drop(table);

        let snapshot = Arc::new(engine.snapshot("bench").unwrap());
        let ops_per_thread = 200_000u64;
        let total_ops = ops_per_thread * num_threads as u64;
        let keys = Arc::new(keys);

        let start = Instant::now();
        let collectors: Vec<LatencyCollector> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..num_threads {
                let snapshot = Arc::clone(&snapshot);
                let keys = Arc::clone(&keys);
                handles.push(s.spawn(move || {

                    let mut lc = LatencyCollector::with_capacity(
                        (ops_per_thread / LATENCY_SAMPLE_RATE) as usize + 1,
                    );
                    let key_count = keys.len();
                    let offset = t * ops_per_thread as usize;
                    for i in 0..ops_per_thread as usize {
                        let op_start = Instant::now();
                        black_box(snapshot.get(keys[(offset + i) % key_count].as_str()));
                        if i as u64 % LATENCY_SAMPLE_RATE == 0 {
                            lc.record(op_start.elapsed().as_nanos() as u64);
                        }
                    }
                    lc
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let elapsed = start.elapsed();
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(5.0 / total_tests as f64, 20),
            "Concurrent reads ✓".green()
        );

        results.push(BenchResult {
            name: format!(
                "Concurrent snapshot reads ({} threads, {}K ops)",
                num_threads,
                total_ops / 1000
            ),
            ops: total_ops,
            elapsed,
            data_size: total_ops * doc_size,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: total_ops,
            write_ops: 0,
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 6: Concurrent writes (all threads, DashMap)
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(6.0 / total_tests as f64, 20),
            "Concurrent writes...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let engine = StorageEngine::open(&bench_dir.join("t6"))?;

        let ops_per_thread = 100_000u64;
        let total_entries = num_threads as u64 * ops_per_thread;
        engine.create_table_with_capacity("bench", total_entries as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;

        let mut thread_keys: Vec<Option<Vec<String>>> = (0..num_threads)
            .map(|t| {
                Some(
                    (0..ops_per_thread)
                        .map(|i| format!("t{}_{}", t, i))
                        .collect(),
                )
            })
            .collect();

        let start = Instant::now();
        let collectors: Vec<LatencyCollector> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..num_threads {
                let table = &table;
                let raw = doc_bytes.as_slice();
                let keys = thread_keys[t].take().unwrap();
                handles.push(s.spawn(move || {
                    let doc: Arc<[u8]> = Arc::from(raw);
                    let mut lc = LatencyCollector::with_capacity(
                        (ops_per_thread / LATENCY_SAMPLE_RATE) as usize + 1,
                    );
                    for (i, key) in keys.into_iter().enumerate() {
                        let op_start = Instant::now();
                        table.insert(key, Arc::clone(&doc));
                        if i as u64 % LATENCY_SAMPLE_RATE == 0 {
                            lc.record(op_start.elapsed().as_nanos() as u64);
                        }
                    }
                    lc
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let elapsed = start.elapsed();
        let actual_ops = ops_per_thread * num_threads as u64;
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(6.0 / total_tests as f64, 20),
            "Concurrent writes ✓".green()
        );

        results.push(BenchResult {
            name: format!(
                "Concurrent memory writes ({} threads, {}K ops)",
                num_threads,
                actual_ops / 1000
            ),
            ops: actual_ops,
            elapsed,
            data_size: actual_ops * doc_size,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: 0,
            write_ops: actual_ops,
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 7: Parallel mixed read/write (all threads)
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(7.0 / total_tests as f64, 20),
            "Mixed read/write...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let engine = StorageEngine::open(&bench_dir.join("t7"))?;
        let ops_per_thread = 200_000u64 / num_threads as u64;
        let total_ops = ops_per_thread * num_threads as u64;
        engine.create_table_with_capacity("bench", total_ops as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;
        let prefill_doc: Arc<[u8]> = Arc::from(doc_bytes.as_slice());

        let prefill_keys = precompute_keys("pre", total_ops / 2);
        fill_table(&table, &prefill_keys, &prefill_doc);
        let prefill_keys = Arc::new(prefill_keys);

        let write_keys: Vec<Vec<String>> = (0..num_threads)
            .map(|t| {
                (0..ops_per_thread)
                    .map(|i| format!("w_{}_{}", t, i))
                    .collect()
            })
            .collect();

        let total_reads = AtomicU64::new(0);
        let total_writes = AtomicU64::new(0);

        let start = Instant::now();
        let collectors: Vec<LatencyCollector> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..num_threads {
                let table = &table;
                let raw = doc_bytes.as_slice();
                let wkeys = &write_keys[t];
                let rkeys = Arc::clone(&prefill_keys);
                let total_reads = &total_reads;
                let total_writes = &total_writes;
                handles.push(s.spawn(move || {

                    let doc: Arc<[u8]> = Arc::from(raw);
                    let mut lc = LatencyCollector::with_capacity(
                        (ops_per_thread / LATENCY_SAMPLE_RATE) as usize + 1,
                    );
                    let rkey_count = rkeys.len();
                    let mut reads = 0u64;
                    let mut writes = 0u64;
                    for i in 0..ops_per_thread as usize {
                        let op_start = Instant::now();
                        if i % 3 == 0 {
                            table.insert(wkeys[i].clone(), Arc::clone(&doc));
                            writes += 1;
                        } else {
                            black_box(table.get(rkeys[i % rkey_count].as_str()));
                            reads += 1;
                        }
                        if i as u64 % LATENCY_SAMPLE_RATE == 0 {
                            lc.record(op_start.elapsed().as_nanos() as u64);
                        }
                    }
                    total_reads.fetch_add(reads, Ordering::Relaxed);
                    total_writes.fetch_add(writes, Ordering::Relaxed);
                    lc
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let elapsed = start.elapsed();
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(7.0 / total_tests as f64, 20),
            "Mixed read/write ✓".green()
        );

        results.push(BenchResult {
            name: format!(
                "Parallel mixed 33%w/67%r ({} threads, 200K ops)",
                num_threads
            ),
            ops: total_ops,
            elapsed,
            data_size: total_ops * doc_size,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: total_reads.load(Ordering::Relaxed),
            write_ops: total_writes.load(Ordering::Relaxed),
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 8: Parallel JSON serialize + deserialize
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(8.0 / total_tests as f64, 20),
            "JSON serde...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let ops_per_thread = 500_000u64 / num_threads as u64;
        let total_ops = ops_per_thread * num_threads as u64;
        let doc = Arc::new(make_doc(42));

        let start = Instant::now();
        let (thread_bytes, collectors): (Vec<u64>, Vec<LatencyCollector>) =
            std::thread::scope(|s| {
                let mut handles = Vec::new();
                for t in 0..num_threads {
                    let doc = Arc::clone(&doc);
                    handles.push(s.spawn(move || {
                        let mut bytes = 0u64;
                        let mut lc = LatencyCollector::with_capacity(
                            (ops_per_thread / LATENCY_SAMPLE_RATE) as usize + 1,
                        );
                        // Reuse buffer to avoid per-iteration allocation
                        let mut buf = Vec::with_capacity(256);
                        for i in 0..ops_per_thread {
                            let op_start = Instant::now();
                            buf.clear();
                            serde_json::to_writer(&mut buf, &*doc).unwrap();
                            bytes += buf.len() as u64;
                            let _: Value =
                                serde_json::from_slice(black_box(&buf)).unwrap();
                            if i % LATENCY_SAMPLE_RATE == 0 {
                                lc.record(op_start.elapsed().as_nanos() as u64);
                            }
                        }
                        (bytes, lc)
                    }));
                }
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .unzip()
            });
        let elapsed = start.elapsed();
        let total_bytes: u64 = thread_bytes.iter().sum();
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(8.0 / total_tests as f64, 20),
            "JSON serde ✓".green()
        );

        results.push(BenchResult {
            name: format!(
                "Parallel JSON serde ({} threads, 500K round-trips)",
                num_threads
            ),
            ops: total_ops,
            elapsed,
            data_size: total_bytes,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: 0,
            write_ops: 0,
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 9: Parallel delete performance
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(9.0 / total_tests as f64, 20),
            "Delete perf...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let engine = StorageEngine::open(&bench_dir.join("t9"))?;
        let count = 200_000u64;
        engine.create_table_with_capacity("bench", count as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();

        let ops_per_thread = count / num_threads as u64;
        let thread_keys: Vec<Vec<String>> = (0..num_threads)
            .map(|t| {
                (0..ops_per_thread)
                    .map(|i| format!("key_{}_{}", t, i))
                    .collect()
            })
            .collect();

        // Fill in parallel
        std::thread::scope(|s| {
            for t in 0..num_threads {
                let table = &table;
                let raw = doc_bytes.as_slice();
                let keys = &thread_keys[t];
                s.spawn(move || {

                    let doc: Arc<[u8]> = Arc::from(raw);
                    for key in keys {
                        table.insert(key.clone(), Arc::clone(&doc));
                    }
                });
            }
        });

        // Delete in parallel with latency tracking
        let start = Instant::now();
        let collectors: Vec<LatencyCollector> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..num_threads {
                let table = &table;
                let keys = &thread_keys[t];
                handles.push(s.spawn(move || {

                    let mut lc = LatencyCollector::with_capacity(
                        (ops_per_thread / LATENCY_SAMPLE_RATE) as usize + 1,
                    );
                    for (i, key) in keys.iter().enumerate() {
                        let op_start = Instant::now();
                        table.remove(key.as_str());
                        if i as u64 % LATENCY_SAMPLE_RATE == 0 {
                            lc.record(op_start.elapsed().as_nanos() as u64);
                        }
                    }
                    lc
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let elapsed = start.elapsed();
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(9.0 / total_tests as f64, 20),
            "Delete perf ✓".green()
        );

        results.push(BenchResult {
            name: format!("Parallel deletes ({} threads, 200K keys)", num_threads),
            ops: count,
            elapsed,
            data_size: 0,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: 0,
            write_ops: count,
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 10: Parallel large document writes
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(10.0 / total_tests as f64, 20),
            "Large doc stress...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let engine = StorageEngine::open(&bench_dir.join("t10"))?;
        let count = 100_000u64;
        engine.create_table_with_capacity("bench", count as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let large_doc = json!({
            "_id": "bench",
            "data": "X".repeat(3000),
            "tags": (0..50).map(|i| format!("tag_{}", i)).collect::<Vec<_>>(),
            "nested": {
                "a": "X".repeat(500),
                "b": (0..20).collect::<Vec<i32>>(),
            }
        });
        let serialized = serde_json::to_vec(&large_doc).unwrap();
        let doc_size = serialized.len() as u64;

        let ops_per_thread = count / num_threads as u64;
        let mut thread_keys: Vec<Option<Vec<String>>> = (0..num_threads)
            .map(|t| {
                Some(
                    (0..ops_per_thread)
                        .map(|i| format!("key_{}_{}", t, i))
                        .collect(),
                )
            })
            .collect();

        let start = Instant::now();
        let collectors: Vec<LatencyCollector> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..num_threads {
                let table = &table;
                let raw = serialized.as_slice();
                let keys = thread_keys[t].take().unwrap();
                handles.push(s.spawn(move || {
                    let doc: Arc<[u8]> = Arc::from(raw);
                    let mut lc = LatencyCollector::with_capacity(
                        (ops_per_thread / LATENCY_SAMPLE_RATE) as usize + 1,
                    );
                    for (i, key) in keys.into_iter().enumerate() {
                        let op_start = Instant::now();
                        table.insert(key, Arc::clone(&doc));
                        if i as u64 % LATENCY_SAMPLE_RATE == 0 {
                            lc.record(op_start.elapsed().as_nanos() as u64);
                        }
                    }
                    lc
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let elapsed = start.elapsed();
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(10.0 / total_tests as f64, 20),
            "Large doc stress ✓".green()
        );

        results.push(BenchResult {
            name: format!(
                "Parallel large doc writes ({} threads, 200K x ~{}KB)",
                num_threads,
                doc_size / 1024
            ),
            ops: count,
            elapsed,
            data_size: count * doc_size,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: 0,
            write_ops: count,
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 11: Scalability — single-thread vs N-thread write comparison
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(11.0 / total_tests as f64, 20),
            "Scalability test...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let total_ops = 800_000u64;
        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;

        // Single-thread baseline (consume keys, no clone overhead)
        let engine_st = StorageEngine::open(&bench_dir.join("t11_st"))?;
        engine_st.create_table_with_capacity("bench", total_ops as usize)?;
        let table_st = engine_st.table_handle("bench").unwrap();
        let keys_st = precompute_keys("st_key", total_ops);

        let local_doc: Arc<[u8]> = Arc::from(doc_bytes.as_slice());

        let start_st = Instant::now();
        for key in keys_st {
            table_st.insert(key, Arc::clone(&local_doc));
        }
        let elapsed_st = start_st.elapsed();
        let single_ops_sec = total_ops as f64 / elapsed_st.as_secs_f64();
        drop(local_doc);

        // Multi-thread (each thread gets its own Arc — no shared refcount contention)
        let engine_mt = StorageEngine::open(&bench_dir.join("t11_mt"))?;
        engine_mt.create_table_with_capacity("bench", total_ops as usize)?;
        let table_mt = engine_mt.table_handle("bench").unwrap();

        let ops_per_thread = total_ops / num_threads as u64;
        let mut thread_keys: Vec<Option<Vec<String>>> = (0..num_threads)
            .map(|t| {
                Some(
                    (0..ops_per_thread)
                        .map(|i| format!("mt_key_{}_{}", t, i))
                        .collect(),
                )
            })
            .collect();

        let start_mt = Instant::now();
        let collectors: Vec<LatencyCollector> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..num_threads {
                let table_mt = &table_mt;
                let raw = doc_bytes.as_slice();
                let keys = thread_keys[t].take().unwrap();
                handles.push(s.spawn(move || {
                    let doc: Arc<[u8]> = Arc::from(raw);
                    let mut lc = LatencyCollector::with_capacity(
                        (ops_per_thread / LATENCY_SAMPLE_RATE) as usize + 1,
                    );
                    for (i, key) in keys.into_iter().enumerate() {
                        let op_start = Instant::now();
                        table_mt.insert(key, Arc::clone(&doc));
                        if i as u64 % LATENCY_SAMPLE_RATE == 0 {
                            lc.record(op_start.elapsed().as_nanos() as u64);
                        }
                    }
                    lc
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let elapsed_mt = start_mt.elapsed();
        let multi_ops_sec = total_ops as f64 / elapsed_mt.as_secs_f64();
        let scaling_efficiency =
            (multi_ops_sec / single_ops_sec / num_threads as f64) * 100.0;
        let mem_after = get_rss_bytes();

        println!(
            "\r  {} {}",
            progress_bar(11.0 / total_tests as f64, 20),
            "Scalability test ✓".green()
        );

        results.push(BenchResult {
            name: format!(
                "Scalability: 1→{} threads (efficiency {:.1}%, {:.1}x speedup)",
                num_threads,
                scaling_efficiency,
                multi_ops_sec / single_ops_sec
            ),
            ops: total_ops,
            elapsed: elapsed_mt,
            data_size: total_ops * doc_size,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: 0,
            write_ops: total_ops,
            mem_before,
            mem_after,
        });
    }

    // ─────────────────────────────────────────────────
    // TEST 12: Query response time — mixed lookups simulating real query patterns
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(12.0 / total_tests as f64, 20),
            "Query response time...".dimmed()
        );
        let mem_before = get_rss_bytes();
        let engine = StorageEngine::open(&bench_dir.join("t12"))?;
        let data_count = 100_000u64;
        engine.create_table_with_capacity("bench", data_count as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;
        let doc: Arc<[u8]> = Arc::from(doc_bytes.into_boxed_slice());

        let keys = precompute_keys("qkey", data_count);
        fill_table(&table, &keys, &doc);
        drop(table);

        let snapshot = Arc::new(engine.snapshot("bench").unwrap());
        let ops_per_thread = 100_000u64;
        let total_ops = ops_per_thread * num_threads as u64;
        let keys = Arc::new(keys);
        let total_reads = AtomicU64::new(0);
        let total_misses = AtomicU64::new(0);

        // Pre-compute miss keys to avoid format! allocation in hot loop
        let miss_keys: Arc<Vec<Vec<String>>> = Arc::new(
            (0..num_threads)
                .map(|t| {
                    (0..ops_per_thread as usize / 10 + 1)
                        .map(|i| format!("missing_{}_{}", t, i))
                        .collect()
                })
                .collect(),
        );

        let start = Instant::now();
        let collectors: Vec<LatencyCollector> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for t in 0..num_threads {
                let snapshot = Arc::clone(&snapshot);
                let keys = Arc::clone(&keys);
                let miss_keys = Arc::clone(&miss_keys);
                let total_reads = &total_reads;
                let total_misses = &total_misses;
                handles.push(s.spawn(move || {

                    // Record EVERY op for accurate percentile distribution
                    let mut lc =
                        LatencyCollector::with_capacity(ops_per_thread as usize);
                    let key_count = keys.len();
                    let my_miss_keys = &miss_keys[t];
                    let mut reads = 0u64;
                    let mut misses = 0u64;
                    let mut miss_idx = 0usize;
                    for i in 0..ops_per_thread as usize {
                        let op_start = Instant::now();
                        if i % 10 == 0 {
                            // 10% miss lookups — query non-existent keys
                            black_box(
                                snapshot.get(my_miss_keys[miss_idx].as_str()),
                            );
                            miss_idx += 1;
                            misses += 1;
                        } else {
                            // 90% hit lookups
                            let idx = (t * ops_per_thread as usize + i) % key_count;
                            black_box(snapshot.get(keys[idx].as_str()));
                            reads += 1;
                        }
                        lc.record(op_start.elapsed().as_nanos() as u64);
                    }
                    total_reads.fetch_add(reads, Ordering::Relaxed);
                    total_misses.fetch_add(misses, Ordering::Relaxed);
                    lc
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let elapsed = start.elapsed();
        let mem_after = get_rss_bytes();
        let reads = total_reads.load(Ordering::Relaxed);
        let misses = total_misses.load(Ordering::Relaxed);

        println!(
            "\r  {} {}",
            progress_bar(1.0, 20),
            "Query response time ✓".green()
        );

        results.push(BenchResult {
            name: format!(
                "Query response time ({} threads, {}K queries, {:.0}% hit rate)",
                num_threads,
                total_ops / 1000,
                reads as f64 / total_ops as f64 * 100.0
            ),
            ops: total_ops,
            elapsed,
            data_size: total_ops * doc_size,
            latency: Some(LatencyCollector::merge(collectors)),
            read_ops: reads + misses,
            write_ops: 0,
            mem_before,
            mem_after,
        });
    }

    // ═══════════════════════════════════════════════
    // RESULTS
    // ═══════════════════════════════════════════════
    let global_elapsed = global_start.elapsed();
    let final_rss = get_rss_bytes();

    println!();
    println!(
        "  {} {}",
        "⚡".truecolor(255, 200, 50),
        "BENCHMARK RESULTS".bold().truecolor(120, 80, 255)
    );
    println!(
        "  {}",
        "══════════════════════════════════════════════════"
            .truecolor(60, 60, 80)
    );
    println!();

    for (i, result) in results.iter().enumerate() {
        print_result(result, i + 1, results.len());
    }

    // ═══════════════════════════════════════════════
    // AGGREGATE SUMMARY
    // ═══════════════════════════════════════════════
    let total_ops: u64 = results.iter().map(|r| r.ops).sum();
    let total_time: f64 = results.iter().map(|r| r.elapsed.as_secs_f64()).sum();
    let total_data: u64 = results.iter().map(|r| r.data_size).sum();
    let total_reads: u64 = results.iter().map(|r| r.read_ops).sum();
    let total_writes: u64 = results.iter().map(|r| r.write_ops).sum();
    let peak_ops = results
        .iter()
        .map(|r| r.ops_per_sec())
        .fold(0.0f64, f64::max);

    // Aggregate P99 across all tests with latency data
    let all_latencies = LatencyCollector::merge(
        results
            .iter()
            .filter_map(|r| {
                r.latency.as_ref().map(|l| LatencyCollector {
                    samples: l.samples.clone(),
                })
            })
            .collect(),
    );

    println!(
        "  {}",
        "══════════════════════════════════════════════════"
            .truecolor(60, 60, 80)
    );
    println!(
        "  {} {}",
        "⚡".truecolor(255, 200, 50),
        "SUMMARY".bold().truecolor(120, 80, 255)
    );
    println!(
        "  {}",
        "──────────────────────────────────────────────────"
            .truecolor(60, 60, 80)
    );

    // Throughput
    println!(
        "  {} {}  {}",
        "Total ops:".dimmed(),
        format_ops(total_ops as f64).bold().truecolor(120, 255, 120),
        format!("({})", total_ops).dimmed()
    );
    println!(
        "  {} {} ops/sec",
        "Peak throughput:".dimmed(),
        format_ops(peak_ops).bold().truecolor(255, 100, 100)
    );
    println!(
        "  {} {}",
        "Data processed:".dimmed(),
        format!("{:.1} MB", total_data as f64 / (1024.0 * 1024.0))
            .bold()
            .truecolor(180, 180, 255)
    );

    println!(
        "  {}",
        "──────────────────────────────────────────────────"
            .truecolor(60, 60, 80)
    );

    // Latency percentiles (aggregate)
    println!(
        "  {} {}",
        "⏱".truecolor(255, 200, 50),
        "LATENCY DISTRIBUTION (aggregate)"
            .bold()
            .truecolor(120, 80, 255)
    );
    println!(
        "  {} {}",
        "  P50:".dimmed(),
        format_latency_u64(all_latencies.p50())
            .bold()
            .truecolor(100, 255, 200)
    );
    println!(
        "  {} {}",
        "  P95:".dimmed(),
        format_latency_u64(all_latencies.p95())
            .bold()
            .truecolor(200, 255, 100)
    );
    println!(
        "  {} {}",
        "  P99:".dimmed(),
        format_latency_u64(all_latencies.p99())
            .bold()
            .truecolor(255, 200, 100)
    );
    println!(
        "  {} {}",
        "P99.9:".dimmed(),
        format_latency_u64(all_latencies.p999())
            .bold()
            .truecolor(255, 120, 80)
    );
    println!(
        "  {}  {}",
        "  Min:".dimmed(),
        format_latency_u64(all_latencies.min()).dimmed()
    );
    println!(
        "  {}  {}",
        "  Max:".dimmed(),
        format_latency_u64(all_latencies.max()).dimmed()
    );
    println!(
        "  {}  {}",
        "  Avg:".dimmed(),
        format_latency(all_latencies.avg()).dimmed()
    );

    println!(
        "  {}",
        "──────────────────────────────────────────────────"
            .truecolor(60, 60, 80)
    );

    // Read/write ratio
    println!(
        "  {} {}",
        "📊".truecolor(255, 200, 50),
        "READ/WRITE RATIO (aggregate)"
            .bold()
            .truecolor(120, 80, 255)
    );
    let rw_total = (total_reads + total_writes) as f64;
    if rw_total > 0.0 {
        let r_pct = total_reads as f64 / rw_total * 100.0;
        let w_pct = total_writes as f64 / rw_total * 100.0;
        println!(
            "  {} reads  {} writes",
            format!("{:.1}%", r_pct).bold().truecolor(100, 200, 255),
            format!("{:.1}%", w_pct).bold().truecolor(255, 150, 100),
        );
        println!(
            "  {} reads: {}  writes: {}",
            "↳".dimmed(),
            total_reads.to_string().dimmed(),
            total_writes.to_string().dimmed(),
        );
    }

    println!(
        "  {}",
        "──────────────────────────────────────────────────"
            .truecolor(60, 60, 80)
    );

    // Resource utilization
    println!(
        "  {} {}",
        "🖥".truecolor(255, 200, 50),
        "RESOURCE UTILIZATION".bold().truecolor(120, 80, 255)
    );
    println!(
        "  {} {}",
        "CPU threads:".dimmed(),
        num_threads.to_string().bold()
    );
    println!(
        "  {} {}",
        "Initial RSS:".dimmed(),
        format_bytes(initial_rss).bold()
    );
    println!(
        "  {} {}",
        "Final RSS:  ".dimmed(),
        format_bytes(final_rss).bold()
    );
    if final_rss > initial_rss {
        println!(
            "  {} {}",
            "Δ Memory:   ".dimmed(),
            format!(
                "+{:.1} MB",
                (final_rss - initial_rss) as f64 / 1_048_576.0
            )
            .bold()
            .truecolor(255, 200, 100)
        );
    }
    println!(
        "  {} {}",
        "Wall clock: ".dimmed(),
        format!("{:.2}s", global_elapsed.as_secs_f64())
            .bold()
            .truecolor(255, 200, 100)
    );
    println!(
        "  {} {}",
        "Sum of test:".dimmed(),
        format!("{:.2}s", total_time).bold().truecolor(255, 200, 100)
    );

    println!(
        "  {}",
        "──────────────────────────────────────────────────"
            .truecolor(60, 60, 80)
    );

    // Scalability summary
    println!(
        "  {} {}",
        "📈".truecolor(255, 200, 50),
        "SCALABILITY".bold().truecolor(120, 80, 255)
    );
    if let Some(scale_result) = results.iter().find(|r| r.name.starts_with("Scalability")) {
        println!(
            "  {} {}",
            "Result:".dimmed(),
            scale_result.name.bold().truecolor(120, 255, 120)
        );
    }
    let write_tests: Vec<&BenchResult> = results.iter().filter(|r| r.write_ops > 0).collect();
    let read_tests: Vec<&BenchResult> = results.iter().filter(|r| r.read_ops > 0).collect();
    if !write_tests.is_empty() {
        let avg_write_ops: f64 =
            write_tests.iter().map(|r| r.ops_per_sec()).sum::<f64>() / write_tests.len() as f64;
        println!(
            "  {} {} ops/sec",
            "Avg write throughput:".dimmed(),
            format_ops(avg_write_ops).bold().truecolor(255, 150, 100)
        );
    }
    if !read_tests.is_empty() {
        let avg_read_ops: f64 =
            read_tests.iter().map(|r| r.ops_per_sec()).sum::<f64>() / read_tests.len() as f64;
        println!(
            "  {} {} ops/sec",
            "Avg read throughput: ".dimmed(),
            format_ops(avg_read_ops).bold().truecolor(100, 200, 255)
        );
    }

    println!();
    println!(
        "  {}",
        "══════════════════════════════════════════════════"
            .truecolor(60, 60, 80)
    );

    let score = ((peak_ops / 1000.0) + (total_ops as f64 / total_time / 1000.0)) as u64;
    println!(
        "  {} {} {}",
        "VANTADB SCORE:".bold().truecolor(120, 80, 255),
        score.to_string().bold().truecolor(255, 200, 50),
        "pts".dimmed()
    );

    let rating = if score > 100000 {
        "TRANSCENDENT ⚡⚡⚡⚡⚡"
    } else if score > 50000 {
        "GODLIKE ⚡⚡⚡⚡"
    } else if score > 20000 {
        "MYTHICAL ⚡⚡⚡"
    } else if score > 5000 {
        "LEGENDARY ⚡⚡"
    } else if score > 2000 {
        "BLAZING ⚡"
    } else {
        "WARMING UP"
    };
    println!(
        "  {} {}",
        "Rating:".dimmed(),
        rating.bold().truecolor(255, 200, 50)
    );
    println!();

    if bench_dir.exists() {
        let _ = std::fs::remove_dir_all(&bench_dir);
    }

    Ok(())
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
