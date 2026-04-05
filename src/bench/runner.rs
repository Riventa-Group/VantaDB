use colored::*;
use dashmap::DashMap;
use serde_json::{json, Value};
use std::hint::black_box;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::storage::StorageEngine;

struct BenchResult {
    name: String,
    ops: u64,
    elapsed: Duration,
    data_size: u64,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.elapsed.as_secs_f64()
    }

    fn avg_latency_ns(&self) -> f64 {
        self.elapsed.as_nanos() as f64 / self.ops as f64
    }

    fn throughput_mb(&self) -> f64 {
        if self.data_size == 0 {
            return 0.0;
        }
        (self.data_size as f64 / (1024.0 * 1024.0)) / self.elapsed.as_secs_f64()
    }
}

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

fn fill_table(table: &DashMap<String, Arc<[u8]>>, keys: &[String], value: &Arc<[u8]>) {
    for key in keys {
        table.insert(key.clone(), Arc::clone(value));
    }
}

pub fn run_benchmark(data_dir: &Path) -> io::Result<()> {
    let bench_dir = data_dir.join("_benchmark_temp");
    if bench_dir.exists() {
        std::fs::remove_dir_all(&bench_dir)?;
    }

    let num_threads = num_cpus();

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
    println!();

    let mut results: Vec<BenchResult> = Vec::new();
    let total_tests = 10;

    // ─────────────────────────────────────────────────
    // TEST 1: Parallel memory writes (all threads, pre-allocated)
    // ─────────────────────────────────────────────────
    {
        print!(
            "  {} {}",
            progress_bar(1.0 / total_tests as f64, 20),
            "Memory writes...".dimmed()
        );
        let engine = StorageEngine::open(&bench_dir.join("t1"))?;
        let ops_per_thread = 500_000u64 / num_threads as u64;
        let total_ops = ops_per_thread * num_threads as u64;
        engine.create_table_with_capacity("bench", total_ops as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;
        let doc: Arc<[u8]> = Arc::from(doc_bytes.into_boxed_slice());

        // Pre-compute keys per thread
        let thread_keys: Vec<Vec<String>> = (0..num_threads)
            .map(|t| {
                (0..ops_per_thread)
                    .map(|i| format!("key_{}_{}", t, i))
                    .collect()
            })
            .collect();

        let start = Instant::now();
        std::thread::scope(|s| {
            for t in 0..num_threads {
                let table = &table;
                let doc = &doc;
                let keys = &thread_keys[t];
                s.spawn(move || {
                    for key in keys {
                        table.insert(key.clone(), Arc::clone(doc));
                    }
                });
            }
        });
        let elapsed = start.elapsed();

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

        // Lock-free snapshot for maximum read throughput
        let snapshot = Arc::new(engine.snapshot("bench").unwrap());
        let ops_per_thread = 500_000u64 / num_threads as u64;
        let total_ops = ops_per_thread * num_threads as u64;
        let keys = Arc::new(keys);

        let start = Instant::now();
        std::thread::scope(|s| {
            for t in 0..num_threads {
                let snapshot = Arc::clone(&snapshot);
                let keys = Arc::clone(&keys);
                s.spawn(move || {
                    let key_count = keys.len();
                    let offset = t * ops_per_thread as usize;
                    for i in 0..ops_per_thread as usize {
                        black_box(snapshot.get(keys[(offset + i) % key_count].as_str()));
                    }
                });
            }
        });
        let elapsed = start.elapsed();

        println!(
            "\r  {} {}",
            progress_bar(2.0 / total_tests as f64, 20),
            "Memory reads ✓".green()
        );

        results.push(BenchResult {
            name: format!("Parallel snapshot reads ({} threads, 500K lookups)", num_threads),
            ops: total_ops,
            elapsed,
            data_size: total_ops * doc_size,
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
        let engine = StorageEngine::open(&bench_dir.join("t3"))?;
        engine.create_table_with_capacity("bench", 10_000)?;

        let count = 10_000u64;
        let doc = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc.len() as u64;
        let keys = precompute_keys("key", count);

        let start = Instant::now();
        for key in &keys {
            engine.put("bench", key, &doc)?;
        }
        let elapsed = start.elapsed();

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
        let engine = StorageEngine::open(&bench_dir.join("t4"))?;
        let count = 200_000u64;
        engine.create_table_with_capacity("bench", count as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;
        let doc: Arc<[u8]> = Arc::from(doc_bytes.into_boxed_slice());

        let ops_per_thread = count / num_threads as u64;
        let thread_keys: Vec<Vec<String>> = (0..num_threads)
            .map(|t| {
                (0..ops_per_thread)
                    .map(|i| format!("key_{}_{}", t, i))
                    .collect()
            })
            .collect();

        let start = Instant::now();
        std::thread::scope(|s| {
            for t in 0..num_threads {
                let table = &table;
                let doc = &doc;
                let keys = &thread_keys[t];
                s.spawn(move || {
                    for key in keys {
                        table.insert(key.clone(), Arc::clone(doc));
                    }
                });
            }
        });
        // Single flush at the end
        engine.put("bench", "__flush__", &[])?;
        let elapsed = start.elapsed();

        println!(
            "\r  {} {}",
            progress_bar(4.0 / total_tests as f64, 20),
            "Batch insert ✓".green()
        );

        results.push(BenchResult {
            name: format!("Parallel batch insert + flush ({} threads, 200K docs)", num_threads),
            ops: count,
            elapsed,
            data_size: count * doc_size,
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
        std::thread::scope(|s| {
            for t in 0..num_threads {
                let snapshot = Arc::clone(&snapshot);
                let keys = Arc::clone(&keys);
                s.spawn(move || {
                    let key_count = keys.len();
                    let offset = t * ops_per_thread as usize;
                    for i in 0..ops_per_thread as usize {
                        black_box(snapshot.get(keys[(offset + i) % key_count].as_str()));
                    }
                });
            }
        });
        let elapsed = start.elapsed();

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
        let engine = StorageEngine::open(&bench_dir.join("t6"))?;

        let ops_per_thread = 100_000u64;
        let total_entries = num_threads as u64 * ops_per_thread;
        engine.create_table_with_capacity("bench", total_entries as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;
        let doc: Arc<[u8]> = Arc::from(doc_bytes.into_boxed_slice());

        let thread_keys: Vec<Vec<String>> = (0..num_threads)
            .map(|t| {
                (0..ops_per_thread)
                    .map(|i| format!("t{}_{}", t, i))
                    .collect()
            })
            .collect();

        let start = Instant::now();
        std::thread::scope(|s| {
            for t in 0..num_threads {
                let table = &table;
                let doc = &doc;
                let keys = &thread_keys[t];
                s.spawn(move || {
                    for key in keys {
                        table.insert(key.clone(), Arc::clone(doc));
                    }
                });
            }
        });
        let elapsed = start.elapsed();
        let actual_ops = ops_per_thread * num_threads as u64;

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
        let engine = StorageEngine::open(&bench_dir.join("t7"))?;
        let ops_per_thread = 200_000u64 / num_threads as u64;
        let total_ops = ops_per_thread * num_threads as u64;
        engine.create_table_with_capacity("bench", total_ops as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc_size = doc_bytes.len() as u64;
        let doc: Arc<[u8]> = Arc::from(doc_bytes.into_boxed_slice());

        // Pre-fill with data all threads can read
        let prefill_keys = precompute_keys("pre", total_ops / 2);
        fill_table(&table, &prefill_keys, &doc);
        let prefill_keys = Arc::new(prefill_keys);

        // Per-thread write keys
        let write_keys: Vec<Vec<String>> = (0..num_threads)
            .map(|t| {
                (0..ops_per_thread)
                    .map(|i| format!("w_{}_{}", t, i))
                    .collect()
            })
            .collect();

        let start = Instant::now();
        std::thread::scope(|s| {
            for t in 0..num_threads {
                let table = &table;
                let doc = &doc;
                let wkeys = &write_keys[t];
                let rkeys = Arc::clone(&prefill_keys);
                s.spawn(move || {
                    let rkey_count = rkeys.len();
                    for i in 0..ops_per_thread as usize {
                        if i % 3 == 0 {
                            table.insert(wkeys[i].clone(), Arc::clone(doc));
                        } else {
                            black_box(table.get(rkeys[i % rkey_count].as_str()));
                        }
                    }
                });
            }
        });
        let elapsed = start.elapsed();

        println!(
            "\r  {} {}",
            progress_bar(7.0 / total_tests as f64, 20),
            "Mixed read/write ✓".green()
        );

        results.push(BenchResult {
            name: format!("Parallel mixed 33%w/67%r ({} threads, 200K ops)", num_threads),
            ops: total_ops,
            elapsed,
            data_size: total_ops * doc_size,
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
        let ops_per_thread = 500_000u64 / num_threads as u64;
        let total_ops = ops_per_thread * num_threads as u64;
        let doc = Arc::new(make_doc(42));
        let doc_bytes = serde_json::to_vec(&*doc).unwrap();
        let single_doc_bytes = doc_bytes.len() as u64;

        let start = Instant::now();
        let thread_bytes: Vec<u64> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for _t in 0..num_threads {
                let doc = Arc::clone(&doc);
                handles.push(s.spawn(move || {
                    let mut bytes = 0u64;
                    for _ in 0..ops_per_thread {
                        let serialized = serde_json::to_vec(&*doc).unwrap();
                        bytes += serialized.len() as u64;
                        let _: Value = serde_json::from_slice(black_box(&serialized)).unwrap();
                    }
                    bytes
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let elapsed = start.elapsed();
        let total_bytes: u64 = thread_bytes.iter().sum();

        println!(
            "\r  {} {}",
            progress_bar(8.0 / total_tests as f64, 20),
            "JSON serde ✓".green()
        );

        results.push(BenchResult {
            name: format!("Parallel JSON serde ({} threads, 500K round-trips)", num_threads),
            ops: total_ops,
            elapsed,
            data_size: total_bytes,
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
        let engine = StorageEngine::open(&bench_dir.join("t9"))?;
        let count = 200_000u64;
        engine.create_table_with_capacity("bench", count as usize)?;
        let table = engine.table_handle("bench").unwrap();

        let doc_bytes = serde_json::to_vec(&make_doc(0)).unwrap();
        let doc: Arc<[u8]> = Arc::from(doc_bytes.into_boxed_slice());

        // Pre-compute keys per thread for both fill and delete
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
                let doc = &doc;
                let keys = &thread_keys[t];
                s.spawn(move || {
                    for key in keys {
                        table.insert(key.clone(), Arc::clone(doc));
                    }
                });
            }
        });

        // Delete in parallel
        let start = Instant::now();
        std::thread::scope(|s| {
            for t in 0..num_threads {
                let table = &table;
                let keys = &thread_keys[t];
                s.spawn(move || {
                    for key in keys {
                        table.remove(key.as_str());
                    }
                });
            }
        });
        let elapsed = start.elapsed();

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
        let doc: Arc<[u8]> = Arc::from(serialized.into_boxed_slice());

        let ops_per_thread = count / num_threads as u64;
        let thread_keys: Vec<Vec<String>> = (0..num_threads)
            .map(|t| {
                (0..ops_per_thread)
                    .map(|i| format!("key_{}_{}", t, i))
                    .collect()
            })
            .collect();

        let start = Instant::now();
        std::thread::scope(|s| {
            for t in 0..num_threads {
                let table = &table;
                let doc = &doc;
                let keys = &thread_keys[t];
                s.spawn(move || {
                    for key in keys {
                        table.insert(key.clone(), Arc::clone(doc));
                    }
                });
            }
        });
        let elapsed = start.elapsed();

        println!(
            "\r  {} {}",
            progress_bar(1.0, 20),
            "Large doc stress ✓".green()
        );

        results.push(BenchResult {
            name: format!("Parallel large doc writes ({} threads, 100K x ~{}KB)", num_threads, doc_size / 1024),
            ops: count,
            elapsed,
            data_size: count * doc_size,
        });
    }

    // ═══════════════════════════════════════════════
    // RESULTS
    // ═══════════════════════════════════════════════
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

    let total_ops: u64 = results.iter().map(|r| r.ops).sum();
    let total_time: f64 = results.iter().map(|r| r.elapsed.as_secs_f64()).sum();
    let total_data: u64 = results.iter().map(|r| r.data_size).sum();
    let peak_ops = results
        .iter()
        .map(|r| r.ops_per_sec())
        .fold(0.0f64, f64::max);

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
    println!(
        "  {} {}  {}",
        "Total ops:".dimmed(),
        format_ops(total_ops as f64).bold().truecolor(120, 255, 120),
        format!("({})", total_ops).dimmed()
    );
    println!(
        "  {} {}",
        "Total time:".dimmed(),
        format!("{:.2}s", total_time).bold().truecolor(255, 200, 100)
    );
    println!(
        "  {} {}",
        "Data processed:".dimmed(),
        format!("{:.1} MB", total_data as f64 / (1024.0 * 1024.0))
            .bold()
            .truecolor(180, 180, 255)
    );
    println!(
        "  {} {} ops/sec",
        "Peak throughput:".dimmed(),
        format_ops(peak_ops).bold().truecolor(255, 100, 100)
    );
    println!(
        "  {} {}",
        "CPU threads:".dimmed(),
        num_threads.to_string().bold()
    );
    println!();

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
