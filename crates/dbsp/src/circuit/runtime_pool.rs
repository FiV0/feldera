//! Pinned single-worker circuits sharing foreground and merger threads.

use super::CircuitConfig;
use super::dbsp_handle::{Command, Response, StatusSender};
use super::runtime::{RuntimeHandle, WorkerPanicInfo};
use crate::profile::Profiler;
use crate::{DBSPHandle, Error, RootCircuit, Runtime, RuntimeError};
use crossbeam::channel::{Receiver, Sender, bounded, unbounded};
use feldera_buffer_cache::ThreadType;
use futures::task::AtomicWaker;
use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Physical resources shared by all circuits attached to a pool.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    threads: usize,
    merger_threads: usize,
    cache_mib: usize,
    max_rss_bytes: Option<u64>,
    pin_cpus: Vec<usize>,
}

impl PoolConfig {
    /// Create a pool configuration with `threads` foreground and merger threads.
    pub fn with_threads(threads: usize) -> Self {
        Self {
            threads,
            merger_threads: threads,
            cache_mib: 256,
            max_rss_bytes: None,
            pin_cpus: Vec::new(),
        }
    }
    /// Set the shared merger thread count.
    pub fn with_merger_threads(mut self, threads: usize) -> Self {
        self.merger_threads = threads;
        self
    }
    /// Set the total foreground/background buffer-cache budget in MiB.
    pub fn with_cache_mib(mut self, mib: usize) -> Self {
        self.cache_mib = mib;
        self
    }
    /// Set the process RSS limit used by the shared memory-pressure monitor.
    pub fn with_max_rss_bytes(mut self, bytes: Option<u64>) -> Self {
        self.max_rss_bytes = bytes;
        self
    }
    /// Pin physical foreground and merger threads using DBSP's CPU mapping.
    pub fn with_pin_cpus(mut self, cpus: Vec<usize>) -> Self {
        self.pin_cpus = cpus;
        self
    }
}

/// Aggregate pool counters. Queue time and execution time are cumulative.
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub registered_circuits: usize,
    pub foreground_threads: usize,
    pub merger_threads: usize,
    pub completed_steps: u64,
    pub queue_time: Duration,
    pub execution_time: Duration,
}

#[derive(Default, Debug)]
struct Counters {
    steps: AtomicU64,
    queue_nanos: AtomicU64,
    execution_nanos: AtomicU64,
}

type Constructor = Box<dyn FnOnce() -> Option<Entry> + Send>;
enum Message {
    Create(u64, Constructor),
    Wake(u64),
    Remove(u64, Sender<()>),
    Shutdown,
}

struct PoolInner {
    config: PoolConfig,
    handle: Mutex<Option<RuntimeHandle>>,
    runtime: Runtime,
    senders: Vec<Sender<Message>>,
    loads: Arc<Vec<AtomicUsize>>,
    placement: Mutex<()>,
    next_id: AtomicU64,
    counters: Arc<Counters>,
}

impl PoolInner {
    fn shutdown(&self) -> std::thread::Result<()> {
        if Runtime::runtime().is_some_and(|runtime| runtime.belongs_to_pool(&self.runtime)) {
            return Err(Box::new(
                "cannot synchronously shut down a pool from its own worker",
            ));
        }
        // Keep concurrent shutdown callers waiting until the threads are joined.
        let mut slot = self.handle.lock().unwrap();
        let Some(handle) = slot.take() else {
            return Ok(());
        };
        handle.kill_async();
        for sender in &self.senders {
            let _ = sender.send(Message::Shutdown);
        }
        // Tokio disallows dropping a runtime from an asynchronous executor.
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::spawn(move || handle.join()).join()?
        } else {
            handle.join()
        }
    }
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        if Runtime::runtime().is_some_and(|runtime| runtime.belongs_to_pool(&self.runtime)) {
            if let Some(handle) = self.handle.get_mut().unwrap().take() {
                handle.kill_async();
                for sender in &self.senders {
                    let _ = sender.send(Message::Shutdown);
                }
                // A constructor/operator may release the last owner on a pool thread.
                std::thread::spawn(move || {
                    let _ = handle.join();
                });
            }
        } else {
            let _ = self.shutdown();
        }
    }
}

/// An owning, cloneable handle to shared DBSP execution resources.
///
/// Circuits retain the pool. Dropping the last pool/circuit handle shuts it down.
/// A circuit panic stops the entire pool; ordinary execution errors affect only
/// the circuit that failed. Scheduling is cooperative at complete step boundaries.
#[derive(Clone)]
pub struct RuntimePool(Arc<PoolInner>);

impl std::fmt::Debug for RuntimePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimePool")
            .field("stats", &self.stats())
            .finish()
    }
}

impl RuntimePool {
    /// Start the pool. Circuits attach through `CircuitConfig::with_runtime_pool`.
    pub fn start(config: PoolConfig) -> Result<Self, Error> {
        if config.threads == 0
            || config.merger_threads == 0
            || config.merger_threads > u16::MAX as usize
        {
            return Err(invalid(
                "foreground/merger thread counts must be positive and merger threads must fit u16",
            ));
        }
        let (senders, receivers): (Vec<_>, Vec<_>) =
            (0..config.threads).map(|_| unbounded()).unzip();
        let loads = Arc::new(
            (0..config.threads)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>(),
        );
        let counters = Arc::new(Counters::default());
        let mut circuit_config = CircuitConfig::with_workers(config.threads)
            .with_max_rss_bytes(config.max_rss_bytes)
            .with_pin_cpus(config.pin_cpus.clone());
        circuit_config.dev_tweaks.merger_threads = Some(config.merger_threads as u16);
        let worker_counters = counters.clone();
        let handle = Runtime::run_with_cache(
            circuit_config,
            move |_| {
                let worker = Runtime::worker_index();
                let receiver = receivers.into_iter().nth(worker).unwrap();
                worker_loop(receiver, &worker_counters);
            },
            Some(config.cache_mib),
        )?;
        let runtime = handle.runtime().clone();
        Ok(Self(Arc::new(PoolInner {
            config,
            handle: Mutex::new(Some(handle)),
            runtime,
            senders,
            loads,
            placement: Mutex::new(()),
            next_id: AtomicU64::new(0),
            counters,
        })))
    }

    /// Stop all circuits and join pool threads. Repeated calls are harmless.
    /// An operator that never returns can prevent shutdown from completing.
    pub fn shutdown(&self) -> std::thread::Result<()> {
        self.0.shutdown()
    }

    /// Read aggregate scheduling counters without stopping workers.
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            registered_circuits: self
                .0
                .loads
                .iter()
                .map(|load| load.load(Ordering::Relaxed))
                .sum(),
            foreground_threads: self.0.config.threads,
            merger_threads: self.0.config.merger_threads,
            completed_steps: self.0.counters.steps.load(Ordering::Relaxed),
            queue_time: Duration::from_nanos(self.0.counters.queue_nanos.load(Ordering::Relaxed)),
            execution_time: Duration::from_nanos(
                self.0.counters.execution_nanos.load(Ordering::Relaxed),
            ),
        }
    }

    pub(super) fn panic_info(&self) -> Vec<(usize, ThreadType, WorkerPanicInfo)> {
        (0..self.0.config.threads)
            .flat_map(|worker| {
                [ThreadType::Foreground, ThreadType::Background]
                    .into_iter()
                    .filter_map(move |kind| {
                        self.0
                            .runtime
                            .worker_panic_info(worker, kind)
                            .map(|info| (worker, kind, info))
                    })
            })
            .collect()
    }
    pub(super) fn panicked(&self) -> bool {
        !self.panic_info().is_empty()
    }

    pub(super) fn init<F, T>(
        &self,
        config: CircuitConfig,
        constructor: F,
    ) -> Result<(DBSPHandle, T), Error>
    where
        F: FnOnce(&mut RootCircuit) -> Result<T, anyhow::Error> + Clone + Send + 'static,
        T: Send + 'static,
    {
        validate(&config)?;
        if Runtime::runtime().is_some_and(|runtime| runtime.belongs_to_pool(&self.0.runtime)) {
            return Err(unsupported(
                "registering a circuit from a worker of the same pool",
            ));
        }
        if self.0.runtime.stopped() {
            return Err(terminated());
        }
        let worker = {
            let _placement = self.0.placement.lock().unwrap();
            let worker = self
                .0
                .loads
                .iter()
                .enumerate()
                .min_by_key(|(i, load)| (load.load(Ordering::Relaxed), *i))
                .unwrap()
                .0;
            self.0.loads[worker].fetch_add(1, Ordering::Relaxed);
            worker
        };
        let load = LoadLease {
            loads: self.0.loads.clone(),
            worker,
        };
        let runtime = Runtime::for_pool(config, self.0.runtime.clone(), worker)?;
        let id = self.0.next_id.fetch_add(1, Ordering::Relaxed);
        let (commands, receiver) = bounded(1);
        let (status, responses) = bounded(1);
        let waker = Arc::new(AtomicWaker::new());
        let status = StatusSender::new(status, waker.clone());
        let (initialized, result) = bounded(1);
        let context = runtime.clone();
        let build = Box::new(move || {
            let _guard = context.enter();
            let built = RootCircuit::build(|circuit| {
                let profiler = Profiler::new(circuit);
                // Convert a constructor unwind to an error so RootCircuit clears its Rc cycles.
                // The panic hook has already marked the pool failed.
                catch_unwind(AssertUnwindSafe(|| constructor(circuit)))
                    .unwrap_or_else(|_| {
                        Err(anyhow::anyhow!(
                            "circuit constructor panicked; pool terminated"
                        ))
                    })
                    .map(|io| (io, profiler))
            });
            match built {
                Ok((circuit, (io, profiler))) => {
                    let fingerprint = circuit.fingerprint();
                    let entry = Entry {
                        _load: load,
                        runtime: context.clone(),
                        circuit: Some(circuit),
                        profiler,
                        commands: receiver,
                        status,
                        transaction: false,
                        queued: None,
                        elapsed: Duration::ZERO,
                        response: None,
                        stepped: false,
                    };
                    if initialized.send(Ok((io, fingerprint))).is_ok() {
                        Some(entry)
                    } else {
                        None
                    }
                }
                Err(error) => {
                    let _ = initialized.send(Err(error));
                    None
                }
            }
        });
        if self.0.senders[worker]
            .send(Message::Create(id, build))
            .is_err()
        {
            return Err(terminated());
        }
        let registration = Registration {
            pool: self.clone(),
            worker,
            id,
        };
        let (io, fingerprint) = loop {
            match result.recv_timeout(Duration::from_millis(100)) {
                Ok(Ok(result)) => break result,
                Ok(Err(error)) => return Err(error),
                Err(crossbeam::channel::RecvTimeoutError::Timeout) if !self.0.runtime.stopped() => {
                }
                Err(_) => {
                    return Err(if self.panicked() {
                        Error::Runtime(RuntimeError::WorkerPanic {
                            panic_info: self.panic_info(),
                        })
                    } else {
                        terminated()
                    });
                }
            }
        };
        let handle = RuntimeHandle::pooled(runtime, registration);
        let dbsp = DBSPHandle::new(
            None,
            handle,
            vec![commands],
            vec![responses],
            fingerprint,
            waker,
        )?;
        Ok((dbsp, io))
    }
}

fn invalid(message: &str) -> Error {
    Error::Runtime(RuntimeError::InvalidPoolConfig(message.into()))
}
pub(super) fn unsupported(message: &str) -> Error {
    Error::Runtime(RuntimeError::UnsupportedPoolOperation(message.into()))
}
fn terminated() -> Error {
    Error::Runtime(RuntimeError::Terminated)
}

fn validate(config: &CircuitConfig) -> Result<(), Error> {
    if config.layout.is_multihost() || config.layout.n_workers() != 1 {
        return Err(invalid(
            "pooled circuits require a single-host layout with exactly one worker",
        ));
    }
    let tweaks = &config.dev_tweaks;
    if config.max_rss_bytes.is_some()
        || !config.pin_cpus.is_empty()
        || config.exchange_listener.is_some()
        || tweaks.merger_threads.is_some()
        || tweaks.buffer_cache_strategy.is_some()
        || tweaks.buffer_max_buckets.is_some()
        || tweaks.buffer_cache_allocation_strategy.is_some()
        || tweaks.fbuf_slab_bytes_per_class.is_some()
        || config
            .storage
            .as_ref()
            .is_some_and(|storage| storage.options.cache_mib.is_some())
    {
        return Err(invalid(
            "configure thread, memory, cache and allocator resources on the pool",
        ));
    }
    if config
        .storage
        .as_ref()
        .is_some_and(|storage| storage.init_checkpoint.is_some() || storage.defer_restore)
    {
        return Err(unsupported("checkpoint restore and bootstrap"));
    }
    Ok(())
}

#[derive(Debug)]
pub(super) struct Registration {
    pub(super) pool: RuntimePool,
    worker: usize,
    id: u64,
}
impl Registration {
    pub(super) fn wake(&self) {
        let _ = self.pool.0.senders[self.worker].send(Message::Wake(self.id));
    }
    pub(super) fn on_pool_thread(&self) -> bool {
        Runtime::runtime().is_some_and(|runtime| runtime.belongs_to_pool(&self.pool.0.runtime))
    }
    pub(super) fn remove(&self) {
        let (sender, receiver) = bounded(1);
        if self.pool.0.senders[self.worker]
            .send(Message::Remove(self.id, sender))
            .is_ok()
            && !self.on_pool_thread()
        {
            let _ = receiver.recv();
        }
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        let (ack, _) = bounded(1);
        let _ = self.pool.0.senders[self.worker].send(Message::Remove(self.id, ack));
    }
}

struct LoadLease {
    loads: Arc<Vec<AtomicUsize>>,
    worker: usize,
}
impl Drop for LoadLease {
    fn drop(&mut self) {
        self.loads[self.worker].fetch_sub(1, Ordering::Relaxed);
    }
}

struct Entry {
    _load: LoadLease,
    runtime: Runtime,
    circuit: Option<super::CircuitHandle>,
    profiler: Profiler,
    commands: Receiver<Command>,
    status: StatusSender,
    transaction: bool,
    queued: Option<Instant>,
    elapsed: Duration,
    response: Option<Response>,
    stepped: bool,
}

impl Drop for Entry {
    fn drop(&mut self) {
        let _guard = self.runtime.enter();
        self.runtime.stop();
        // A panic during evaluation may leave clock-end invariants unsatisfied.
        let _ = catch_unwind(AssertUnwindSafe(|| drop(self.circuit.take())));
        self.runtime.wait_for_mergers();
        self.runtime.local_store().clear();
    }
}

impl Entry {
    fn turn(&mut self) -> Result<bool, Error> {
        self.stepped = false;
        let circuit = self.circuit.as_ref().unwrap();
        if self.transaction {
            circuit.step()?;
            self.stepped = true;
            if !circuit.is_commit_complete() {
                return Ok(true);
            }
            self.transaction = false;
            self.response = Some(Response::Unit);
            return Ok(false);
        }
        let Ok(command) = self.commands.try_recv() else {
            return Ok(false);
        };
        let response = match command {
            Command::Transaction => {
                circuit.start_transaction()?;
                circuit.start_commit_transaction()?;
                if !circuit.is_commit_complete() {
                    self.transaction = true;
                    return Ok(true);
                }
                Response::Unit
            }
            Command::StartTransaction => {
                circuit.start_transaction()?;
                Response::Unit
            }
            Command::CommitTransaction => {
                circuit.start_commit_transaction()?;
                Response::Unit
            }
            Command::Step => {
                circuit.step()?;
                self.stepped = true;
                Response::CommitComplete(circuit.is_commit_complete())
            }
            Command::CommitProgress => Response::CommitProgress(circuit.commit_progress()),
            Command::EnableProfiler => {
                self.profiler.enable_cpu_profiler(circuit.runtime_idle());
                Response::Unit
            }
            Command::DumpProfile { .. } => {
                Response::ProfileDump(self.profiler.dump_profile(self.elapsed))
            }
            Command::RetrieveGraph => Response::ProfileDump(self.profiler.dump_graph()),
            Command::RetrieveProfile { .. } => {
                Response::Profile(self.profiler.profile(self.elapsed))
            }
            Command::GetLir => Response::Lir(circuit.lir()),
            Command::SetAutoRebalance(enable) => {
                circuit.set_auto_rebalance(enable)?;
                Response::Unit
            }
            Command::SetBalancerHintsByGlobalId(hints) => Response::SetBalancerHints(
                hints
                    .into_iter()
                    .map(|(id, hint)| circuit.set_balancer_hint_by_global_id(&id, hint))
                    .collect(),
            ),
            Command::SetBalancerHints(hints) => Response::SetBalancerHints(
                hints
                    .into_iter()
                    .map(|(id, hint)| circuit.set_balancer_hint(&id, hint))
                    .collect(),
            ),
            Command::GetCurrentBalancerPolicies => {
                Response::CurrentBalancerPolicies(circuit.get_current_balancer_policies())
            }
            Command::GetCurrentBalancerPolicy(id) => {
                Response::CurrentBalancerPolicy(circuit.get_current_balancer_policy(&id))
            }
            Command::Rebalance => {
                circuit.rebalance();
                Response::Unit
            }
            Command::StartCompaction => {
                circuit.start_compaction();
                Response::Unit
            }
            Command::IsCompactionComplete => {
                Response::IsCompactionComplete(circuit.is_compaction_complete())
            }
            _ => return Err(unsupported("checkpoint restore and bootstrap")),
        };
        self.response = Some(response);
        Ok(false)
    }
}

fn worker_loop(receiver: Receiver<Message>, counters: &Counters) {
    let mut entries: HashMap<u64, Entry> = HashMap::new();
    let mut ready = VecDeque::new();
    while !Runtime::kill_in_progress() {
        // Process a bounded number of incoming messages so producers cannot starve steps.
        for index in 0..64 {
            let message = if ready.is_empty() && index == 0 {
                receiver.recv_timeout(Duration::from_millis(100)).ok()
            } else {
                receiver.try_recv().ok()
            };
            let Some(message) = message else {
                break;
            };
            match message {
                Message::Create(id, build) => {
                    if let Ok(Some(entry)) = catch_unwind(AssertUnwindSafe(build)) {
                        entries.insert(id, entry);
                    }
                }
                Message::Wake(id) => {
                    if let Some(entry) = entries.get_mut(&id) {
                        if entry.runtime.stopped() {
                            entries.remove(&id);
                        } else if entry.queued.is_none() {
                            entry.queued = Some(Instant::now());
                            ready.push_back(id);
                        }
                    }
                }
                Message::Remove(id, ack) => {
                    entries.remove(&id);
                    let _ = ack.send(());
                }
                Message::Shutdown => {
                    Runtime::runtime().unwrap().stop();
                }
            }
        }
        if Runtime::kill_in_progress() {
            break;
        }
        if let Some(id) = ready.pop_front()
            && let Some(entry) = entries.get_mut(&id)
        {
            if let Some(queued) = entry.queued.take() {
                counters
                    .queue_nanos
                    .fetch_add(queued.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
            let _guard = entry.runtime.enter();
            let start = Instant::now();
            let result = catch_unwind(AssertUnwindSafe(|| entry.turn()));
            let elapsed = start.elapsed();
            entry.elapsed += elapsed;
            entry.runtime.record_execution(elapsed);
            counters
                .execution_nanos
                .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
            if entry.stepped {
                counters.steps.fetch_add(1, Ordering::Relaxed);
            }
            match result {
                Ok(Ok(true)) => {
                    entry.queued = Some(Instant::now());
                    ready.push_back(id);
                }
                Ok(Ok(false)) => {
                    if let Some(response) = entry.response.take() {
                        let _ = entry.status.send(Ok(response));
                    }
                }
                Ok(Err(error)) => {
                    let _ = entry.status.send(Err(error));
                    entries.remove(&id);
                }
                Err(_) => {
                    Runtime::runtime().unwrap().stop();
                }
            }
        }
    }
    // Drop every graph on its owning thread before the shared merger executor exits.
    drop(entries);
    // Dropping queued constructors/reply senders releases callers waiting on initialization/removal.
}
