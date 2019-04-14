#![feature(futures_api)]
#![feature(manually_drop_take)]
#![allow(dead_code)]

#[cfg(test)]
mod tests;
mod waker;

use crossbeam_channel::{self, Receiver, Sender};
use futures::{prelude::*, task::*};
use num_cpus;
use slotmap::{DefaultKey as Key, SecondaryMap, SlotMap};
use std::{
    mem::ManuallyDrop,
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, AtomicBool, Ordering},
        Arc, Weak
    },
    thread
};

type DynFuture = futures::future::FutureObj<'static, ()>;

pub trait SpawnWithContext<T> {
    fn spawn_obj_with_context<F: FnOnce(&T) -> DynFuture>(
        &self,
        generator: F
    ) -> Result<(), SpawnError>;
    fn spawn_obj(&self, future: DynFuture) -> Result<(), SpawnError>;
}

/*
    OWNED THREADPOOL IMPL
*/

// Not cloneable, there should be only 1 strong owner of ThreadPoolInner
// Abusing arc for it's Weak mechanism
pub struct ThreadPool<T> {
    inner: Arc<ThreadPoolInner<T>>
}

impl<T: Send + 'static> ThreadPool<T> {
    pub fn new(threads: usize, context: T) -> Self {
        let poisoned = Arc::new(AtomicBool::new(false));
        let workers = (0..threads)
            .map(|_| {
                let (tx, rx) = crossbeam_channel::unbounded::<Message>();
                let worker = Worker {
                    tx,
                    rx,
                    running_futures: Arc::new(AtomicUsize::new(0)),
                    poisoned: poisoned.clone()
                };
                let handle = {
                    let worker = worker.clone();
                    thread::spawn(move || worker.work())
                };
                ManuallyDrop::new(WorkerThread { worker, handle })
            })
            .collect::<Vec<ManuallyDrop<WorkerThread>>>()
            .into_boxed_slice();

        ThreadPool {
            inner: Arc::new(ThreadPoolInner {
                context,
                workers,
                poisoned,
                wait_for_empty: false
            })
        }
    }

    pub fn as_handle(&self) -> ThreadPoolHandle<T> {
        ThreadPoolHandle {
            inner: Arc::downgrade(&self.inner)
        }
    }

    fn unwrap_inner(self) -> ThreadPoolInner<T> {
        let mut inner_arc = self.inner;
        loop {
            match Arc::try_unwrap(inner_arc) {
                Ok(inner) => return inner,
                Err(arc) => inner_arc = arc
            }
            thread::yield_now();
        }
    }

    pub fn wait(self) {
        let mut inner = self.unwrap_inner();
        inner.wait_for_empty = true;
        drop(inner);
    }

    pub fn shutdown(self) {
        let mut inner = self.unwrap_inner();
        inner.wait_for_empty = false;
        drop(inner);
    }
}

impl Default for ThreadPool<()> {
    fn default() -> Self {
        ThreadPool::new(num_cpus::get(), ())
    }
}

impl<T> Deref for ThreadPool<T> {
    type Target = ThreadPoolInner<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/*
    THREADPOOL INNER IMPL
*/

pub struct ThreadPoolInner<T> {
    context: T,
    workers: Box<[ManuallyDrop<WorkerThread>]>,
    // @TODO propagate panics in some way
    poisoned: Arc<AtomicBool>,
    wait_for_empty: bool
}

impl<T> SpawnWithContext<T> for ThreadPoolInner<T> {
    fn spawn_obj_with_context<F: FnOnce(&T) -> DynFuture>(
        &self,
        generator: F
    ) -> Result<(), SpawnError> {
        self.spawn_obj(generator(&self.context))
    }

    fn spawn_obj(&self, future: DynFuture) -> Result<(), SpawnError> {
        // Find best candidate to give future to
        let (least_full_worker, _index, _running_futures) = self
            .workers
            .iter()
            .enumerate()
            .map(|(index, worker)| {
                (
                    worker,
                    index,
                    worker.worker.running_futures.load(Ordering::Acquire)
                )
            })
            .min_by_key(|(_, _, running_futures)| *running_futures)
            .unwrap();
        // Increase future counter
        least_full_worker
            .worker
            .running_futures
            .fetch_add(1, Ordering::AcqRel);
        // Send future to worker
        least_full_worker
            .worker
            .tx
            .send(Message::PushFuture(future))
            .map_err(|_| SpawnError::shutdown())
    }
}

impl<T> Spawn for ThreadPoolInner<T> {
    fn spawn_obj(&mut self, future: DynFuture) -> Result<(), SpawnError> {
        <Self as SpawnWithContext<T>>::spawn_obj(&self, future)
    }
}

impl<T> Drop for ThreadPoolInner<T> {
    fn drop(&mut self) {
        for worker in self.workers.iter() {
            if self.wait_for_empty {
                let _ = worker.worker.tx.send(Message::HaltOnEmpty);
            } else {
                let _ = worker.worker.tx.send(Message::Halt);
            }
        }
        for worker in self.workers.iter_mut() {
            let worker = unsafe { ManuallyDrop::take(worker) };
            let _ = worker.handle.join();
        }
    }
}

/*
    THREADPOOL CLONEABLE HANDLE IMPL
*/

#[derive(Clone)]
pub struct ThreadPoolHandle<T> {
    inner: Weak<ThreadPoolInner<T>>
}

impl<T> SpawnWithContext<T> for ThreadPoolHandle<T> {
    fn spawn_obj_with_context<F: FnOnce(&T) -> DynFuture>(
        &self,
        generator: F
    ) -> Result<(), SpawnError> {
        if let Some(inner) = self.inner.upgrade() {
            inner.spawn_obj_with_context(generator)
        } else {
            Err(SpawnError::shutdown())
        }
    }

    fn spawn_obj(&self, future: DynFuture) -> Result<(), SpawnError> {
        if let Some(inner) = self.inner.upgrade() {
            inner.spawn_obj(future)
        } else {
            Err(SpawnError::shutdown())
        }
    }
}

impl<T> Spawn for ThreadPoolHandle<T> {
    fn spawn_obj(&mut self, future: DynFuture) -> Result<(), SpawnError> {
        <Self as SpawnWithContext<T>>::spawn_obj(&self, future)
    }
}

/*
    WORKER IMPL
*/

struct WorkerThread {
    worker: Worker,
    handle: thread::JoinHandle<()>
}

#[derive(Clone)]
struct Worker {
    tx: Sender<Message>,
    rx: Receiver<Message>,
    running_futures: Arc<AtomicUsize>,
    poisoned: Arc<AtomicBool>
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
                Message::Halt => return
            }
        }
    }

    // Returns true if futures map became empty
    fn poll_future(
        &self,
        future_map: &mut SlotMap<Key, DynFuture>,
        waker_map: &mut SecondaryMap<Key, Waker>,
        key: Key
    ) -> bool {
        let future = match future_map.get_mut(key) {
            Some(future) => future,
            None => return false
        };
        let waker = match waker_map.get(key) {
            Some(waker) => waker,
            None => return false
        };

        // @FIXME Pass context instead of waker
        let mut ctx = Context::from_waker(waker);
        let res = future.poll_unpin(&mut ctx);

        let became_empty = match &res {
            Poll::Ready(()) => {
                future_map.remove(key);
                waker_map.remove(key);
                // When the previous value was 1, it's now 0, so the map became empty
                self.running_futures.fetch_sub(1, Ordering::AcqRel) == 1
            }
            Poll::Pending => false
        };

        became_empty
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            self.poisoned.store(true, Ordering::SeqCst);
        }
    }
}

enum Message {
    PushFuture(DynFuture),
    WakeFuture(Key),
    HaltOnEmpty,
    Halt
}
