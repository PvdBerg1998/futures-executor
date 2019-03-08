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

pub struct ThreadPool {
    workers: Box<[ManuallyDrop<WorkerWithHandle>]>,
}

impl ThreadPool {
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
                ManuallyDrop::new(WorkerWithHandle {
                    inner: worker,
                    handle,
                })
            })
            .collect::<Vec<ManuallyDrop<WorkerWithHandle>>>()
            .into_boxed_slice();

        ThreadPool { workers }
    }

    pub fn wait(self) {
        for worker in self.workers.iter() {
            let _ = worker.inner.tx.send(Message::HaltOnEmpty);
        }
        drop(self);
    }

    pub fn shutdown_now(self) {
        for worker in self.workers.iter() {
            let _ = worker.inner.tx.send(Message::Halt);
        }
        drop(self);
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for worker in self.workers.iter_mut() {
            let worker = unsafe { ManuallyDrop::take(worker) };
            let _ = worker.handle.join();
        }
    }
}

impl Spawn for ThreadPool {
    fn spawn_obj(&mut self, future: DynFuture) -> Result<(), SpawnError> {
        // Find best candidate to give future to
        let (least_full_worker, _index, _running_futures) = self
            .workers
            .iter()
            .enumerate()
            .map(|(index, worker)| {
                (
                    worker,
                    index,
                    worker.inner.running_futures.load(Ordering::Acquire),
                )
            })
            .min_by_key(|(_, _, running_futures)| *running_futures)
            .unwrap();
        // Increase future counter
        least_full_worker
            .inner
            .running_futures
            .fetch_add(1, Ordering::AcqRel);
        // Send future to worker
        least_full_worker
            .inner
            .tx
            .send(Message::PushFuture(future))
            .map_err(|_| SpawnError::shutdown())
    }
}

impl Default for ThreadPool {
    fn default() -> Self {
        ThreadPool::new(num_cpus::get())
    }
}

struct WorkerWithHandle {
    inner: Worker,
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
                Message::HaltOnEmpty => {
                    halt_on_empty = true;
                    if self.running_futures.load(Ordering::Acquire) == 0 {
                        return;
                    }
                }
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
            Ok(Poll::Ready(())) | Err(_) => {
                future_map.remove(key);
                waker_map.remove(key);
                // When the previous value was 1, it's now 0, so the map became empty
                self.running_futures.fetch_sub(1, Ordering::AcqRel) == 1
            }
            Ok(Poll::Pending) => false,
        }
    }
}

enum Message {
    PushFuture(DynFuture),
    WakeFuture(Key),
    HaltOnEmpty,
    Halt,
}
