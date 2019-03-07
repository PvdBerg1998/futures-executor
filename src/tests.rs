use crate::*;
use futures::prelude::*;
use futures::task::SpawnExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use test::Bencher;

const BENCH_AMOUNT: usize = 1000;

#[test]
fn run_futures() {
    let mut pool = ArcThreadPool::new();
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
    let mut pool = ArcThreadPool::new();

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

#[bench]
fn counter_futures(b: &mut Bencher) {
    b.iter(|| {
        let mut pool = ArcThreadPool::new();
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..4 {
            let counter = counter.clone();
            let fut = future::lazy(move |_| {
                for _ in 0..BENCH_AMOUNT / 4 {
                    test::black_box(counter.fetch_add(1, Ordering::Relaxed));
                }
            });
            pool.spawn(fut).unwrap();
        }

        pool.wait();
        assert_eq!(counter.load(Ordering::Relaxed), BENCH_AMOUNT);
    })
}

#[bench]
fn counter_naive_threaded(b: &mut Bencher) {
    b.iter(|| {
        let counter = Arc::new(AtomicUsize::new(0));

        (0..4)
            .map(|_| {
                let counter = counter.clone();
                thread::spawn(move || {
                    for _ in 0..BENCH_AMOUNT / 4 {
                        test::black_box(counter.fetch_add(1, Ordering::Relaxed));
                    }
                })
            })
            .for_each(|handle| {
                let _ = handle.join();
            });

        assert_eq!(counter.load(Ordering::Relaxed), BENCH_AMOUNT);
    });
}
