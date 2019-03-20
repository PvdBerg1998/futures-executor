use crate::*;
use futures::{prelude::*, task::SpawnExt};
use std::{
    panic,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc
    }
};

const BENCH_AMOUNT: usize = 1000;

#[test]
fn run_futures() {
    let mut pool = ThreadPool::default();
    let counter = Arc::new(AtomicUsize::new(0));

    for _i in 0..10 {
        let counter = counter.clone();
        let fut = future::lazy(move |_| {
            //println!("Add #{}", i);
            counter.fetch_add(1, Ordering::Relaxed);
        });
        pool.spawn(fut).unwrap();
    }

    pool.wait();
    assert_eq!(counter.load(Ordering::Relaxed), 10);
}

#[test]
fn dont_leak_memory() {
    let mut pool = ThreadPool::default();

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

/*
#[test]
fn panicking_in_poll() {
    let mut pool = ThreadPool::new(1);
    let caught_panic = Arc::new(AtomicBool::new(false));

    // FIXME: This pollutes the test output, but changing the global panic handler
    // would result in no output when another test panics on a different tester thread
    let fut1 = future::lazy(move |_| {
        panic!();
    });

    let fut2 = {
        let caught_panic = caught_panic.clone();
        future::lazy(move |_| {
            caught_panic.store(true, Ordering::SeqCst);
        })
    };

    pool.spawn(fut1).unwrap();
    pool.spawn(fut2).unwrap();
    pool.wait();

    assert!(caught_panic.load(Ordering::SeqCst));
}
*/

#[test]
fn blocking() {
    let mut pool = ThreadPool::default();
    let finished = Arc::new(AtomicBool::new(false));

    let fut = {
        let finished = finished.clone();
        future::lazy(move |_| {
            finished.store(true, Ordering::SeqCst);
        })
    };
    pool.block_on(fut).unwrap();
    assert_eq!(pool.shutdown_now(), 0);
    assert!(finished.load(Ordering::SeqCst));
}
