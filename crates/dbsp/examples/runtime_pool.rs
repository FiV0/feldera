//! Runnable evaluation of pinned, step-scheduled query circuits.
//!
//! cargo run -p dbsp --example runtime_pool -- --circuits 10000 --rounds 4 --spill

use anyhow::Result;
use clap::Parser;
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig, StorageConfig, StorageOptions};
use dbsp::utils::Tup2;
use dbsp::{OrdZSet, PoolConfig, Runtime, RuntimePool};
use futures::future::join_all;
use serde_json::json;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = 1000)]
    circuits: usize,
    #[arg(long, default_value_t = 8)]
    threads: usize,
    #[arg(long, default_value_t = 4)]
    merger_threads: usize,
    #[arg(long, default_value_t = 256)]
    cache_mib: usize,
    #[arg(long, default_value_t = 20)]
    rounds: usize,
    /// Force each circuit's state to spill into its own temporary directory.
    #[arg(long)]
    spill: bool,
    /// Every Nth query is a join with many microsteps; zero selects only small queries.
    #[arg(long, default_value_t = 0)]
    heavy_every: usize,
    /// Use one dedicated runtime per circuit for a small-population baseline.
    #[arg(long)]
    dedicated: bool,
    /// Parent directory for a disposable evaluation directory.
    #[arg(long)]
    storage_dir: Option<std::path::PathBuf>,
}

fn rss() -> Option<usize> {
    memory_stats::memory_stats().map(|stats| stats.physical_mem)
}
fn threads() -> Option<usize> {
    std::fs::read_dir("/proc/self/task")
        .ok()
        .map(|entries| entries.count())
}
fn descriptors() -> Option<usize> {
    std::fs::read_dir("/proc/self/fd")
        .ok()
        .map(|entries| entries.count())
}

fn main() -> Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        args.circuits > 0 && args.rounds > 0,
        "circuits and rounds must be positive"
    );
    // Thousands of independent spill namespaces can require many open files.
    let fd_limit = match fdlimit::raise_fd_limit() {
        Ok(fdlimit::Outcome::LimitRaised { from, to }) => json!({"from": from, "to": to}),
        Ok(fdlimit::Outcome::Unsupported) => json!("unsupported"),
        Err(error) => json!({"error": error.to_string()}),
    };
    let baseline_rss = rss();
    let baseline_threads = threads();
    let baseline_descriptors = descriptors();
    let directory = match &args.storage_dir {
        Some(path) => tempfile::tempdir_in(path)?,
        None => tempfile::tempdir()?,
    };
    let pool = if args.dedicated {
        None
    } else {
        Some(RuntimePool::start(
            PoolConfig::with_threads(args.threads)
                .with_merger_threads(args.merger_threads)
                .with_cache_mib(args.cache_mib),
        )?)
    };
    let start = Instant::now();
    let mut queries = Vec::with_capacity(args.circuits);
    for id in 0..args.circuits {
        let heavy = args.heavy_every != 0 && id % args.heavy_every == 0;
        let keys = if heavy { 8u64 } else { 1u64 };
        let mut config = CircuitConfig::with_workers(1)
            .with_splitter_chunk_size_records(if heavy { 1 } else { 50_000 });
        if let Some(pool) = &pool {
            config = config.with_runtime_pool(pool.clone());
        }
        if args.spill {
            let path = directory.path().join(id.to_string());
            std::fs::create_dir(&path)?;
            config = config.with_storage(Some(CircuitStorageConfig::for_config(
                StorageConfig {
                    path: path.to_string_lossy().into_owned(),
                    cache: Default::default(),
                },
                StorageOptions {
                    min_storage_bytes: Some(0),
                    min_step_storage_bytes: Some(0),
                    cache_mib: args.dedicated.then_some(args.cache_mib),
                    ..Default::default()
                },
            )?));
        }
        let (handle, (input, output)) = Runtime::init_circuit(config, move |c| {
            let (stream, input) = c.add_input_zset::<u64>();
            let output = if heavy {
                let indexed = stream.map_index(|x| (0u64, *x));
                indexed
                    .join(&indexed, move |_, left, right| left * keys + right)
                    .distinct()
                    .accumulate_output()
            } else {
                stream.distinct().accumulate_output()
            };
            Ok((input, output))
        })?;
        queries.push((handle, input, output, keys, heavy));
    }
    let initialization = start.elapsed();
    let idle_rss = rss();
    let idle_threads = threads();
    let idle_descriptors = descriptors();
    let mut peak_rss = idle_rss;
    let mut peak_threads = idle_threads;
    let mut latencies = Vec::with_capacity(args.circuits * args.rounds);
    let execution = Instant::now();
    for round in 0..args.rounds {
        let weight = if round % 2 == 0 { 1 } else { -1 };
        for (_, input, _, keys, _) in &mut queries {
            input.append(&mut (0..*keys).map(|key| Tup2(key, weight)).collect());
        }
        // One submitting thread polls all completions; there is no per-query waiter thread.
        let round_latencies = futures::executor::block_on(async {
            join_all(queries.iter_mut().map(|(handle, _, _, _, _)| async move {
                let start = Instant::now();
                handle.transaction_async().await.map(|()| start.elapsed())
            }))
            .await
        });
        for result in round_latencies {
            latencies.push(result?);
        }
        for (_, _, output, keys, heavy) in &queries {
            let count = if *heavy { keys * keys } else { *keys };
            let expected =
                OrdZSet::from_keys((), (0..count).map(|key| Tup2(key, weight)).collect());
            anyhow::ensure!(
                output.concat().consolidate() == expected,
                "incorrect query delta in round {round}"
            );
        }
        peak_rss = peak_rss.max(rss());
        peak_threads = peak_threads.max(threads());
    }
    let execution = execution.elapsed();
    if args.spill {
        for (handle, _, _, _, _) in &mut queries {
            handle.start_compaction()?;
            handle.wait_for_compaction(Duration::from_secs(30))?;
        }
    }
    let stats = pool.as_ref().map(RuntimePool::stats);
    let before_teardown_descriptors = descriptors();
    let teardown = Instant::now();
    drop(queries);
    if let Some(pool) = &pool {
        anyhow::ensure!(
            pool.stats().registered_circuits == 0,
            "circuit registrations survived teardown"
        );
        pool.shutdown()
            .map_err(|_| anyhow::anyhow!("pool shutdown panicked"))?;
    }
    drop(pool);
    let teardown = teardown.elapsed();
    latencies.sort_unstable();
    let percentile = |percent: usize| {
        latencies[((latencies.len() - 1) * percent / 100).min(latencies.len() - 1)].as_secs_f64()
            * 1000.0
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "circuits": args.circuits, "rounds": args.rounds, "spill": args.spill,
            "heavy_every": args.heavy_every, "dedicated": args.dedicated,
            "foreground_threads": if args.dedicated { args.circuits } else { args.threads },
            "merger_threads": if args.dedicated { args.circuits } else { args.merger_threads },
            "cache_mib": args.cache_mib,
            "cache_budget_scope": if args.dedicated { "per_circuit" } else { "pool" },
            "os": std::env::consts::OS, "architecture": std::env::consts::ARCH,
            "available_parallelism": std::thread::available_parallelism().ok().map(|n| n.get()),
            "fd_limit": fd_limit,
            "initialization_seconds": initialization.as_secs_f64(), "execution_seconds": execution.as_secs_f64(),
            "transactions_per_second": args.circuits as f64 * args.rounds as f64 / execution.as_secs_f64(),
            "latency_ms": { "p50": percentile(50), "p95": percentile(95), "p99": percentile(99) },
            "rss_bytes": {"baseline": baseline_rss, "idle": idle_rss, "peak_sampled": peak_rss, "after_teardown": rss()},
            "process_threads": {"baseline": baseline_threads, "idle": idle_threads, "peak_sampled": peak_threads, "after_teardown": threads()},
            "file_descriptors": {"baseline": baseline_descriptors, "idle": idle_descriptors, "active": before_teardown_descriptors, "after_teardown": descriptors()},
            "steps": stats.as_ref().map(|s| s.completed_steps),
            "queue_seconds": stats.as_ref().map(|s| s.queue_time.as_secs_f64()),
            "worker_execution_seconds": stats.as_ref().map(|s| s.execution_time.as_secs_f64()),
            "teardown_seconds": teardown.as_secs_f64(), "verified": true
        }))?
    );
    Ok(())
}
