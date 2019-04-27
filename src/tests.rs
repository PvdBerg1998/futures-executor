use crate::*;
use futures::{prelude::*, task::SpawnExt};
use std::{
    panic,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc
    },
    time::Duration
};

const BENCH_AMOUNT: usize = 1000;

#[test]
fn run_futures() {
    let pool = ThreadPool::default();
    let mut handle = pool.as_handle();
    let counter = Arc::new(AtomicUsize::new(0));

    for _i in 0..10 {
        let counter = counter.clone();
        let fut = future::lazy(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        });
        handle.spawn(fut).unwrap();
    }

    pool.wait();
    assert_eq!(counter.load(Ordering::Relaxed), 10);
}

#[test]
fn dont_leak_memory() {
    let pool = ThreadPool::default();
    let mut handle = pool.as_handle();

    let shared = Arc::new(());
    for _ in 0..10 {
        let shared = shared.clone();
        let fut = future::lazy(move |_| {
            let _shared = shared;
        });
        handle.spawn(fut).unwrap();
    }

    pool.wait();
    assert_eq!(Arc::strong_count(&shared), 1);
}

#[test]
fn blocking() {
    let pool = ThreadPool::default();

    let finished = Arc::new(AtomicBool::new(false));
    let fut = {
        let finished = finished.clone();
        future::lazy(move |_| {
            finished.store(true, Ordering::SeqCst);
        })
    };

    pool.block_on(fut.boxed().into());
    assert!(finished.load(Ordering::SeqCst));
}

#[test]
#[should_panic(expected = "Propagate me")]
fn panic_propagated_to_blocker() {
    let pool = ThreadPool::default();
    let mut handle = pool.as_handle();

    let blocking_fut = future::poll_fn(move |_| {
        // Cause a panic on a different thread
        handle
            .spawn(future::lazy(move |_| panic!("Propagate me")))
            .unwrap();
        // Make sure the panicking future actually gets executed
        Poll::Pending
    });

    pool.block_on(blocking_fut.boxed().into());
}

#[test]
fn waiting() {
    let pool = ThreadPool::default();
    let mut handle = pool.as_handle();

    handle.spawn(future::empty()).unwrap();

    // This should block indefinitely, or until we shutdown manually
    let waiter = thread::spawn(move || {
        // Wait some time to make sure the future got spawned
        thread::sleep(Duration::from_secs(1));
        pool.wait();
    });

    // Wait some time to make sure we're blocking
    thread::sleep(Duration::from_secs(1));

    // Kill the pool from the outside
    handle.shutdown().unwrap();

    // This should finish immediately now
    waiter.join().unwrap();
}

#[test]
#[should_panic(expected = "Propagate me")]
fn panic_propagated_to_waiter() {
    let pool = ThreadPool::default();
    let mut handle = pool.as_handle();

    let fut = future::poll_fn(move |_| {
        // Cause a panic on a different thread
        handle
            .spawn(future::lazy(move |_| panic!("Propagate me")))
            .unwrap();
        // Make sure the panicking future actually gets executed
        Poll::Pending
    });

    pool.as_handle().spawn(fut).unwrap();
    pool.wait();
}
