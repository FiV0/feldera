# Shared runtime pool

A runtime pool runs independent single-worker DBSP circuits on a fixed set of
foreground threads. Each circuit is built, stepped, and destroyed on its assigned
thread. It keeps its own graph, local Tokio executor, logical clock, storage
namespace, and operator configuration.

```rust
use dbsp::{PoolConfig, Runtime, RuntimePool};
use dbsp::circuit::CircuitConfig;

let pool = RuntimePool::start(
    PoolConfig::with_threads(8)
        .with_merger_threads(4)
        .with_cache_mib(256),
)?;
let config = CircuitConfig::with_workers(1).with_runtime_pool(pool.clone());
let (mut query, input) = Runtime::init_circuit(config, |circuit| {
    let (stream, input) = circuit.add_input_zset::<u64>();
    stream.distinct().accumulate_output();
    Ok(input)
})?;
input.push(42, 1);
query.transaction_async().await?;
```

The existing synchronous `transaction()` method remains available. Omitting the
pool retains dedicated runtime construction. The low-level `Runtime::run` API
cannot be attached to a pool.

## Scheduling and outputs

Registration chooses the physical worker with the fewest registered circuits,
with ties resolved by worker index. Placement remains fixed. A worker serves a
FIFO queue of ready circuits. A transaction that needs another step goes to the
back of that queue after its current step finishes. A slow step, including time
waiting for I/O, holds its worker until completion. Construction and administrative
commands also execute to completion.

The logical worker index and worker count are always 0 and 1, regardless of the
physical pool size. Circuits on different physical workers can execute
concurrently. A circuit never has two commands executing concurrently.

Use `accumulate_output()` when consuming complete transaction deltas after
`transaction()` or `transaction_async()`. The existing `output()` API exposes the
most recent *step*; a transaction can contain multiple steps. Pooling does not
change these output semantics.

Dropping a transaction future leaves its submitted work running. Before a later
command executes, the handle resolves that pending transaction. Read its output
before starting another transaction if that output is needed. Killing or dropping
the circuit cancels outstanding work.

## Resources and lifecycle

Foreground workers, merger threads, cache budgets, slab allocators, and the RSS
monitor belong to the pool. The default merger thread count equals the foreground
thread count. The default **total** buffer-cache budget is 256 MiB, shared across
foreground/background worker pairs. This budget does not cap operator state,
queued input, or spill files.

Storage backends and directory locks remain circuit-specific. Existing
`CircuitStorageConfig` configures disk spill, including compression and spill
thresholds. Sharing merger threads uses task-local circuit context; switching
foreground circuits installs context for the complete operation.

Circuit handles keep the pool alive. Dropping one circuit removes its
registration and drains its merger work without stopping its peers. Explicit
`pool.shutdown()` cancels all circuits and joins the workers. Dropping the last
owner also initiates shutdown. Cleanup is deferred when an owner is released
inside its own pool worker so a worker never joins itself.

Ordinary circuit execution errors terminate that circuit and preserve the error
returned by its operator or scheduler. A constructor, foreground operator, or
merger panic makes the entire pool fail. Callers waiting for work receive an
error. Shutdown is cooperative: an operator that never returns can prevent it
from completing.

## First-version boundaries

- Only single-host, single-worker circuits are accepted.
- Storage supports disk spill and compaction. Checkpoint, restore, and bootstrap
  operations are rejected before changing circuit state.
- Configure physical resources on `PoolConfig`. Per-circuit CPU pinning, RSS
  limits, merger counts, cache budgets, and cache/allocator policies are rejected.
- Registration and command submission from workers of the same pool are rejected
  to prevent blocking a worker on its own queue. Input handles remain usable.
- No circuit migration, priorities, or scheduling within a step.
- No Triplox changes are included.

## Runnable evaluation

The example validates every transaction's output, including retractions, and
prints a JSON report with initialization time, sampled RSS, process thread and
file-descriptor counts, throughput, latency percentiles, queue time, execution
time, and teardown resource counts. Its caller polls asynchronous completions on
one thread. It does not allocate one waiting thread per circuit.

```sh
cargo run -p dbsp --example runtime_pool -- --circuits 10000 --rounds 4 --spill
cargo run -p dbsp --example runtime_pool -- --circuits 1000 --rounds 4 --spill --heavy-every 100
cargo run -p dbsp --example runtime_pool -- --circuits 100 --rounds 4 --spill --dedicated
```

`--heavy-every N` makes every Nth circuit a join that generates multiple steps.
Use `--storage-dir PATH` to select the parent for disposable spill directories.
The example attempts to raise its file-descriptor limit and reports the result.
Storage contents are removed when the evaluation exits normally.

Use the same build profile when comparing runs. These workloads establish a
runtime-overhead baseline; they are not a Triplox throughput or latency guarantee.

## Evaluation recorded on 2026-09-09

These runs used an unoptimized debug build on Linux x86-64 with 16 available
logical CPUs, based on upstream commit `8fc1b417d`. Each query processed four
alternating insertion/retraction transactions with forced spill. Pool runs used
eight foreground threads, four merger threads, and a total 256 MiB cache budget.
The machine was also building and running tests, so timings are illustrative,
not a controlled performance comparison. Local builds used `CFLAGS=-std=gnu17`
for the pinned mimalloc dependency with GCC 15; no repository toolchain settings
were changed.

| Circuits | Workload | Process threads | Init (s) | Transactions/s | Idle / sampled peak RSS (MiB) |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | pool, distinct | 14 | 0.00 | 231 | 27.7 / 30.3 |
| 100 | pool, distinct | 14 | 0.19 | 865 | 30.6 / 56.4 |
| 1,000 | pool, distinct | 14 | 2.12 | 1,621 | 78.2 / 286.3 |
| 10,000 | pool, distinct | 14 | 19.00 | 1,777 | 527.4 / 2202.0 |
| 1,000 | pool, 1% joins | 14 | 1.84 | 587 | 76.8 / 333.0 |
| 100 | dedicated, distinct | 301 | 0.50 | 1,689 | 142.3 / 220.2 |

Every output was verified. All runs returned to one process thread and six open
file descriptors after teardown, with zero registered circuits. The 10,000-query
run completed 40,000 steps; the mixed run completed 9,120 steps, exercising
transactions that return to the scheduling queue between steps.

The 10,000-query run used 40,010 open file descriptors before teardown and took
3.90 seconds to tear down. Sampled RSS remained about 2,171 MiB afterward despite
releasing the pool, so these measurements do not establish that all memory is
returned to the OS. Allocator retention and live-allocation profiling need a
separate investigation. The cache budget limits cache contents, not total query
memory. The dedicated baseline used 100 foreground, 100 merger, and 100 RSS
monitor threads, with a separate 256 MiB cache budget per circuit.

A repeat with the final code again verified all 40,000 transactions and the same
thread/file-descriptor counts. Initialization took 20.84 seconds, execution 37.16
seconds, and teardown 35.84 seconds while other builds and tests were active.
This variation reinforces that these runs establish correctness and thread sharing,
not a latency bound. The repeat report is `pool-10000-final.json`.

Full local JSON reports and validation logs are saved under the ignored
`target/runtime-pooling-evaluation/` directory.

## Regression coverage

The pool integration suite covers dedicated/pooled output parity and retractions,
worker affinity, per-circuit configuration, concurrent physical workers,
step-boundary fairness, spill/compaction, constructor and scheduler errors,
pool-wide panic propagation, canceled transaction futures, deferred cleanup from
a pool worker, shutdown during registration, concurrent shutdown callers, and repeated
registration/removal.
The existing circuit-wait-time integration suite also passes. Internal merger
tests exercise task-local context across yields and background panic propagation.

Clippy completed for the DBSP library, example, and integration tests. Rust 1.95
reports six existing warnings in circuit metadata, row-number evaluation, and
storage readers; no warnings remain in the added or changed pool code.

The workspace-wide compile check stopped at the native `rdkafka-sys` dependency:
this environment lacks `pkg-config`, which it needs to locate librdkafka 2.12.1.
DBSP compiled in that check, but a full workspace build is not verified.

The final integration run passed 19 tests (16 pool tests and 3 existing profiler
wait-time tests). Both internal merger tests passed when run directly. Rustdoc
passed 58 examples, with 18 existing examples ignored.

The full 649-test DBSP unit suite completed with **646 passed, 3 existing tests
ignored, and no failures**. After 287 tests completed with two test workers, the
unfinished tests were resumed using the same compiled binary with six test
workers and unchanged property-test case counts. An exact-name coverage ledger
verified that all 649 tests were accounted for across the two runs; see
`unit-test-validation.json` and `unit-test-resume.json` in the report directory.
