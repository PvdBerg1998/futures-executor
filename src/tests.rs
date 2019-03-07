use crate::*;
use futures::prelude::*;
use futures::task::SpawnExt;
use std::panic;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

const BENCH_AMOUNT: usize = 1000;

#[test]
fn run_futures() {
    let mut pool = ArcThreadPool::default();
    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..10 {
        let counter = counter.clone();
        let fut = future::lazy(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        });
        pool.spawn(fut).unwrap();
    }

    pool.wait();
    assert_eq!(counter.load(Ordering::Relaxed), 10);
}

#[test]
fn dont_leak_memory() {
    let mut pool = ArcThreadPool::default();

    let shared = Arc::new(());
    for _ in 0..10 {
        let shared = shared.clone();
        let fut = future::lazy(move |_| {
            let _shared = shared;
        });
        pool.spawn(fut).unwrap();
    }

    pool.wait();
    assert_eq!(Arc::strong_count(&shared), 1);
}

#[test]
fn panicking_in_poll() {
    let mut pool = ArcThreadPool::new(1);
    let caught_panic = Arc::new(AtomicBool::new(false));

    // FIXME: This pollutes the test output, but changing the global panic handler
    // would result in no output when another test panics on a different tester thread
    let fut1 = future::lazy(move |_| {
        panic!();
    });

    let fut2 = {
        let caught_panic = caught_panic.clone();
        future::lazy(move |_| {
            caught_panic.store(true, Ordering::Relaxed);
        })
    };

    pool.spawn(fut1).unwrap();
    pool.spawn(fut2).unwrap();
    pool.wait();

    assert!(caught_panic.load(Ordering::Relaxed));
}
