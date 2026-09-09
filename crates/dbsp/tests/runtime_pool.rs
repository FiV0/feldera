use dbsp::circuit::{CircuitConfig, CircuitStorageConfig, StorageConfig, StorageOptions};
use dbsp::utils::Tup2;
use dbsp::{DBSPHandle, OrdZSet, OutputHandle, PoolConfig, Runtime, RuntimePool, ZSetHandle, zset};
use futures::{FutureExt, future::join_all};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn pool(threads: usize) -> RuntimePool {
    RuntimePool::start(
        PoolConfig::with_threads(threads)
            .with_merger_threads(2)
            .with_cache_mib(8),
    )
    .unwrap()
}

fn circuit(config: CircuitConfig) -> (DBSPHandle, (ZSetHandle<u64>, OutputHandle<OrdZSet<u64>>)) {
    Runtime::init_circuit(config, |c| {
        let (stream, input) = c.add_input_zset::<u64>();
        Ok((input, stream.distinct().output()))
    })
    .unwrap()
}

#[test]
fn pooled_and_dedicated_retractions_match() {
    let pool = pool(2);
    let mut queries = (0..8)
        .map(|_| circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool.clone())))
        .collect::<Vec<_>>();
    let (mut dedicated, (input, output)) = circuit(CircuitConfig::with_workers(2));
    for data in [
        vec![Tup2(1, 1), Tup2(2, 2)],
        vec![Tup2(2, -1)],
        vec![Tup2(1, -1), Tup2(2, -1)],
    ] {
        input.append(&mut data.clone());
        for (_, (input, _)) in &mut queries {
            input.append(&mut data.clone());
        }
        futures::executor::block_on(async {
            let (dedicated_result, pooled_results) = futures::join!(
                dedicated.transaction_async(),
                join_all(queries.iter_mut().map(|(h, _)| h.transaction_async()))
            );
            dedicated_result.unwrap();
            for result in pooled_results {
                result.unwrap();
            }
        });
        let expected = output.consolidate();
        for (_, (_, output)) in &queries {
            assert_eq!(output.consolidate(), expected);
        }
    }
    assert_eq!(pool.stats().registered_circuits, 8);
    drop(queries);
    assert_eq!(pool.stats().registered_circuits, 0);
}

#[test]
fn context_and_affinity_are_per_circuit() {
    let pool = pool(2);
    let observations = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for id in 0..6 {
        let observations = observations.clone();
        let (handle, _) = Runtime::init_circuit(
            CircuitConfig::with_workers(1)
                .with_runtime_pool(pool.clone())
                .with_splitter_chunk_size_records(id + 1),
            move |c| {
                let thread = std::thread::current().id();
                assert_eq!(Runtime::worker_index(), 0);
                let (stream, input) = c.add_input_zset::<u64>();
                stream.inspect(move |_| {
                    assert_eq!(std::thread::current().id(), thread);
                    assert_eq!(Runtime::worker_index(), 0);
                    assert_eq!(Runtime::num_workers(), 1);
                    assert_eq!(
                        dbsp::circuit::splitter_output_chunk_size(),
                        (id + 1) as usize
                    );
                    observations.lock().unwrap().push((id, thread));
                });
                Ok(input)
            },
        )
        .unwrap();
        handles.push(handle);
    }
    for _ in 0..3 {
        for h in &mut handles {
            h.transaction().unwrap();
        }
    }
    let seen = observations.lock().unwrap();
    assert!(seen.len() >= 18);
    let threads: std::collections::HashSet<_> = seen.iter().map(|(_, t)| t).collect();
    assert_eq!(threads.len(), 2);
}

#[test]
fn invalid_configuration_and_unsupported_operations_are_nonfatal() {
    let pool = pool(1);
    assert!(
        Runtime::init_circuit(
            CircuitConfig::with_workers(2).with_runtime_pool(pool.clone()),
            |_| Ok(())
        )
        .is_err()
    );
    assert!(
        Runtime::init_circuit(
            CircuitConfig::with_workers(1)
                .with_runtime_pool(pool.clone())
                .with_max_rss_bytes(Some(1024)),
            |_| Ok(())
        )
        .is_err()
    );
    assert!(
        Runtime::run(
            CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()),
            |_| {}
        )
        .is_err()
    );
    let (mut query, _) = circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()));
    assert!(query.create_bootstrap_circuit().is_err());
    assert!(query.checkpoint().run().is_err());
    assert!(query.list_checkpoints().is_err());
    query.transaction().unwrap();
}

#[test]
fn constructor_and_execution_errors_leave_peers_alive() {
    let pool = pool(1);
    let result = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()),
        |_| -> Result<(), anyhow::Error> { anyhow::bail!("constructor failed") },
    );
    assert!(result.is_err());
    let (mut bad, _) = circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()));
    let (mut good, _) = circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()));
    // A step outside a transaction is a scheduler error.
    assert!(matches!(
        bad.step(),
        Err(dbsp::Error::Scheduler(
            dbsp::SchedulerError::StepWithoutTransaction
        ))
    ));
    good.transaction().unwrap();
    assert_eq!(pool.stats().registered_circuits, 1);
}

#[test]
fn drop_one_circuit_and_shutdown_pool() {
    let pool = pool(1);
    let (one, _) = circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()));
    let (mut two, _) = circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()));
    drop(one);
    two.transaction().unwrap();
    pool.shutdown().unwrap();
    assert!(two.transaction().is_err());
    assert!(
        Runtime::init_circuit(
            CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()),
            |_| Ok(())
        )
        .is_err()
    );
    pool.shutdown().unwrap();
    assert_eq!(pool.stats().registered_circuits, 0);
}

#[test]
fn dropped_async_future_is_drained_before_next_transaction() {
    let pool = pool(1);
    let (mut query, (input, output)) =
        circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool));
    input.push(1, 1);
    let _ = query.transaction_async().now_or_never();
    // The first transaction remains pending even if its future was dropped.
    query.commit_progress().unwrap();
    assert_eq!(output.consolidate(), zset! {1 => 1});
    input.push(2, 1);
    query.transaction().unwrap();
    assert_eq!(output.consolidate(), zset! {2 => 1});
}

#[test]
fn foreground_panic_stops_pool_and_wakes_async_callers() {
    let pool = pool(1);
    let (mut bad, _) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()),
        |c| {
            let (stream, input) = c.add_input_zset::<u64>();
            stream.inspect(|_| panic!("injected operator panic"));
            Ok(input)
        },
    )
    .unwrap();
    let (mut good, _) = circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()));
    futures::executor::block_on(async {
        let (bad, good) = futures::join!(bad.transaction_async(), good.transaction_async());
        assert!(bad.is_err());
        assert!(good.is_err());
    });
    pool.shutdown().unwrap();
}

fn storage(path: &std::path::Path) -> CircuitStorageConfig {
    CircuitStorageConfig::for_config(
        StorageConfig {
            path: path.to_string_lossy().into_owned(),
            cache: Default::default(),
        },
        StorageOptions {
            min_storage_bytes: Some(0),
            min_step_storage_bytes: Some(0),
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn shared_mergers_spill_independent_circuits() {
    let temp = tempfile::tempdir().unwrap();
    let pool = pool(2);
    let mut queries = (0..4)
        .map(|id| {
            let path = temp.path().join(format!("circuit-{id}"));
            std::fs::create_dir_all(&path).unwrap();
            circuit(
                CircuitConfig::with_workers(1)
                    .with_runtime_pool(pool.clone())
                    .with_storage(Some(storage(&path))),
            )
        })
        .collect::<Vec<_>>();
    for round in 0..24 {
        for (id, (_, (input, _))) in queries.iter_mut().enumerate() {
            input.push(id as u64 * 1000 + round, 1);
        }
        futures::executor::block_on(async {
            for result in join_all(queries.iter_mut().map(|(h, _)| h.transaction_async())).await {
                result.unwrap();
            }
        });
        for (id, (_, (_, output))) in queries.iter().enumerate() {
            assert_eq!(output.consolidate(), zset! {id as u64 * 1000 + round => 1});
        }
    }
    for (h, _) in &mut queries {
        h.start_compaction().unwrap();
        h.wait_for_compaction(Duration::from_secs(30)).unwrap();
    }
    drop(queries.remove(0));
    for (h, _) in &mut queries {
        h.transaction().unwrap();
    }
    drop(queries);
    assert_eq!(pool.stats().registered_circuits, 0);
    pool.shutdown().unwrap();
}

#[test]
fn transactions_yield_between_steps_but_not_inside_a_step() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let pool = pool(1);
    let events = Arc::new(Mutex::new(Vec::new()));
    let (entered, entry) = crossbeam::channel::bounded(1);
    let (release, released) = crossbeam::channel::bounded(1);
    let once = Arc::new(AtomicBool::new(false));
    let a_events = events.clone();
    let (mut a, input) = Runtime::init_circuit(
        CircuitConfig::with_workers(1)
            .with_runtime_pool(pool.clone())
            .with_splitter_chunk_size_records(1),
        move |c| {
            let (stream, input) = c.add_input_zset::<u64>();
            let indexed = stream.map_index(|x| (0u64, *x));
            let joined = indexed.join(&indexed, |_, left, right| (*left, *right));
            joined.inspect(move |_| {
                a_events.lock().unwrap().push('A');
                if !once.swap(true, Ordering::SeqCst) {
                    entered.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(30)).unwrap();
                }
            });
            Ok(input)
        },
    )
    .unwrap();
    let b_events = events.clone();
    let (mut b, _) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()),
        move |c| {
            let (stream, input) = c.add_input_zset::<u64>();
            stream.inspect(move |_| b_events.lock().unwrap().push('B'));
            Ok(input)
        },
    )
    .unwrap();
    input.append(&mut (0..16).map(|x| Tup2(x, 1)).collect());
    assert!(a.transaction_async().now_or_never().is_none());
    entry.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(b.transaction_async().now_or_never().is_none());
    assert_eq!(*events.lock().unwrap(), vec!['A']);
    release.send(()).unwrap();
    // Administrative commands first drain the transactions whose futures were dropped.
    a.commit_progress().unwrap();
    b.commit_progress().unwrap();
    let events = events.lock().unwrap();
    let b_index = events.iter().position(|event| *event == 'B').unwrap();
    assert!(b_index > 0);
    assert!(
        events[b_index + 1..].contains(&'A'),
        "expected A to resume after B: {events:?}"
    );
}

#[test]
fn distinct_pool_threads_run_concurrently() {
    let pool = pool(2);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let barrier = barrier.clone();
        let (handle, _) = Runtime::init_circuit(
            CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()),
            move |c| {
                let (stream, input) = c.add_input_zset::<u64>();
                stream.inspect(move |_| {
                    barrier.wait();
                });
                Ok(input)
            },
        )
        .unwrap();
        handles.push(handle);
    }
    futures::executor::block_on(async {
        for result in join_all(handles.iter_mut().map(DBSPHandle::transaction_async)).await {
            result.unwrap();
        }
    });
}

#[test]
fn synchronous_transactions_can_run_inside_an_executor() {
    let pool = pool(1);
    for config in [
        CircuitConfig::with_workers(1),
        CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()),
    ] {
        let (mut query, _) = circuit(config);
        futures::executor::block_on(async {
            query.transaction().unwrap();
        });
    }
}

#[test]
fn constructor_panic_and_repeated_registration_release_resources() {
    let pool = pool(1);
    for _ in 0..20 {
        let (handle, _) = circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()));
        drop(handle);
    }
    assert_eq!(pool.stats().registered_circuits, 0);
    let result = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()),
        |c| -> Result<(), anyhow::Error> {
            let (stream, _) = c.add_input_zset::<u64>();
            stream.distinct();
            panic!("injected constructor panic");
        },
    );
    assert!(result.is_err());
    pool.shutdown().unwrap();
    assert_eq!(pool.stats().registered_circuits, 0);
}

#[test]
fn shutdown_releases_queued_registrations() {
    let pool = pool(1);
    let (entered, entry) = crossbeam::channel::bounded(1);
    let (release, released) = crossbeam::channel::bounded(1);
    let first_pool = pool.clone();
    let first = std::thread::spawn(move || {
        Runtime::init_circuit(
            CircuitConfig::with_workers(1).with_runtime_pool(first_pool),
            move |_| {
                entered.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(30)).unwrap();
                Ok(())
            },
        )
    });
    entry.recv_timeout(Duration::from_secs(30)).unwrap();
    let second_pool = pool.clone();
    let (finished, finish) = crossbeam::channel::bounded(1);
    let second = std::thread::spawn(move || {
        let result = Runtime::init_circuit(
            CircuitConfig::with_workers(1).with_runtime_pool(second_pool),
            |_| Ok(()),
        );
        finished.send(result.is_err()).unwrap();
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while pool.stats().registered_circuits < 2 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let shutdown_pool = pool.clone();
    let shutdown = std::thread::spawn(move || shutdown_pool.shutdown().unwrap());
    assert!(finish.recv_timeout(Duration::from_secs(30)).unwrap());
    release.send(()).unwrap();
    drop(first.join().unwrap());
    second.join().unwrap();
    shutdown.join().unwrap();
    assert_eq!(pool.stats().registered_circuits, 0);
}

#[test]
fn dropping_a_peer_from_a_pool_worker_defers_cleanup() {
    let pool = pool(1);
    let (mut peer, (input, _)) =
        circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()));
    input.push(1, 1);
    peer.transaction().unwrap();
    let peer = Arc::new(Mutex::new(Some(peer)));
    let (mut owner, _) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()),
        move |c| {
            let (stream, input) = c.add_input_zset::<u64>();
            stream.inspect(move |_| {
                drop(peer.lock().unwrap().take());
            });
            Ok(input)
        },
    )
    .unwrap();
    owner.transaction().unwrap();
    owner.commit_progress().unwrap();
    assert_eq!(pool.stats().registered_circuits, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn last_owner_can_be_dropped_from_an_async_executor() {
    let (mut query, _) = circuit(CircuitConfig::with_workers(1).with_runtime_pool(pool(1)));
    query.transaction_async().await.unwrap();
    drop(query);
}

#[test]
fn concurrent_shutdown_callers_wait_for_workers() {
    let pool = pool(1);
    let (entered, entry) = crossbeam::channel::bounded(1);
    let (release, released) = crossbeam::channel::bounded(1);
    let (mut query, _) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_runtime_pool(pool.clone()),
        move |c| {
            let (stream, input) = c.add_input_zset::<u64>();
            stream.inspect(move |_| {
                entered.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(30)).unwrap();
            });
            Ok(input)
        },
    )
    .unwrap();
    let cancellation = query.runtime().cancellation_token();
    let transaction = std::thread::spawn(move || query.transaction());
    entry.recv_timeout(Duration::from_secs(30)).unwrap();
    let first_pool = pool.clone();
    let first = std::thread::spawn(move || first_pool.shutdown().unwrap());
    futures::executor::block_on(cancellation.cancelled());
    let (started, start) = crossbeam::channel::bounded(1);
    let (finished, finish) = crossbeam::channel::bounded(1);
    let second = std::thread::spawn(move || {
        started.send(()).unwrap();
        pool.shutdown().unwrap();
        finished.send(()).unwrap();
    });
    start.recv_timeout(Duration::from_secs(30)).unwrap();
    let returned_early = finish.recv_timeout(Duration::from_millis(100)).is_ok();
    release.send(()).unwrap();
    first.join().unwrap();
    second.join().unwrap();
    assert!(transaction.join().unwrap().is_err());
    assert!(
        !returned_early,
        "shutdown returned before the worker exited"
    );
}
