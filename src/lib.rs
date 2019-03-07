#![feature(futures_api)]
#![feature(manually_drop_take)]
#![allow(dead_code)]

#[cfg(test)]
mod tests;
mod waker;

use crossbeam_channel::{self, Receiver, Sender};
use futures::prelude::*;
use futures::task::Poll;
use futures::task::Spawn;
use futures::task::SpawnError;
use futures::task::Waker;
use num_cpus;
use slotmap::{DefaultKey as Key, SecondaryMap, SlotMap};
use std::mem::ManuallyDrop;
use std::panic;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

type DynFuture = futures::future::FutureObj<'static, ()>;

#[derive(Clone)]
pub struct ArcThreadPool {
    inner: Arc<ThreadPoolInner>,
}

impl ArcThreadPool {
    pub fn new(threads: usize) -> Self {
        let workers = (0..threads)
            .map(|_| {
                let (tx, rx) = crossbeam_channel::unbounded::<Message>();
                let worker = Worker {
                    tx,
                    rx,
                    running_futures: Arc::new(AtomicUsize::new(0)),
                };
                let handle = {
                    let worker = worker.clone();
                    thread::spawn(move || worker.work())
                };
                ManuallyDrop::new(WorkerWithHandle { worker, handle })
            })
            .collect::<Vec<ManuallyDrop<WorkerWithHandle>>>()
            .into_boxed_slice();

        ArcThreadPool {
            inner: Arc::new(ThreadPoolInner { workers }),
        }
    }

    pub fn wait(self) {
        for worker in self.inner.workers.iter() {
            let _ = worker.worker.tx.send(Message::HaltOnEmpty);
        }
        drop(self);
    }
}

impl Spawn for ArcThreadPool {
    fn spawn_obj(&mut self, future: DynFuture) -> Result<(), SpawnError> {
        // Find best candidate to give future to
        let (least_full_worker, _index, _running_futures) = self
            .inner
            .workers
            .iter()
            .enumerate()
            .map(|(index, worker)| {
                (
                    worker,
                    index,
                    worker.worker.running_futures.load(Ordering::Acquire),
                )
            })
            .min_by_key(|(_, _, running_futures)| *running_futures)
            .unwrap();
        // Increase future counter
        least_full_worker
            .worker
            .running_futures
            .fetch_add(1, Ordering::Release);
        // Send future to worker
        least_full_worker
            .worker
            .tx
            .send(Message::PushFuture(future))
            .map_err(|_| SpawnError::shutdown())
    }
}

impl Default for ArcThreadPool {
    fn default() -> Self {
        ArcThreadPool::new(num_cpus::get())
    }
}

struct WorkerWithHandle {
    worker: Worker,
    handle: thread::JoinHandle<()>,
}

#[derive(Clone)]
struct Worker {
    tx: Sender<Message>,
    rx: Receiver<Message>,
    running_futures: Arc<AtomicUsize>,
}

impl Worker {
    fn work(&self) {
        let mut future_map: SlotMap<Key, DynFuture> = SlotMap::new();
        let mut waker_map: SecondaryMap<Key, Waker> = SecondaryMap::new();
        let mut halt_on_empty = false;

        for message in self.rx.iter() {
            match message {
                Message::PushFuture(future) => {
                    let key = future_map.insert(future);
                    let waker = waker::new_waker(self.tx.clone(), key);
                    waker_map.insert(key, waker);

                    let became_empty = self.poll_future(&mut future_map, &mut waker_map, key);
                    if halt_on_empty && became_empty {
                        return;
                    }
                }
                Message::WakeFuture(key) => {
                    let became_empty = self.poll_future(&mut future_map, &mut waker_map, key);
                    if halt_on_empty && became_empty {
                        return;
                    }
                }
                Message::HaltOnEmpty => halt_on_empty = true,
                Message::Halt => return,
            }
        }
    }

    // Returns true if futures map became empty
    fn poll_future(
        &self,
        future_map: &mut SlotMap<Key, DynFuture>,
        waker_map: &mut SecondaryMap<Key, Waker>,
        key: Key,
    ) -> bool {
        let future = match future_map.get_mut(key) {
            Some(future) => future,
            None => return false,
        };
        let waker = match waker_map.get(key) {
            Some(waker) => waker,
            None => return false,
        };

        // Don't let a panicking poll kill the threadpool
        let res = panic::catch_unwind(panic::AssertUnwindSafe(|| future.poll_unpin(waker)));
        match res {
            Ok(Poll::Ready(())) => {
                future_map.remove(key);
                waker_map.remove(key);
                self.running_futures.fetch_sub(1, Ordering::Acquire) == 0
            }
            Ok(Poll::Pending) => false,
            Err(_) => false,
        }
    }
}

struct ThreadPoolInner {
    workers: Box<[ManuallyDrop<WorkerWithHandle>]>,
}

impl Drop for ThreadPoolInner {
    fn drop(&mut self) {
        for worker in self.workers.iter() {
            let _ = worker.worker.tx.send(Message::Halt);
        }
        for worker in self.workers.iter_mut() {
            let worker = unsafe { ManuallyDrop::take(worker) };
            let _ = worker.handle.join();
        }
    }
}

enum Message {
    PushFuture(DynFuture),
    WakeFuture(Key),
    HaltOnEmpty,
    Halt,
}
