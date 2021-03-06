#![feature(weak_counts)]
#![allow(dead_code)]

#[cfg(test)]
mod tests;
mod waker;

use crossbeam_channel::{self, Receiver, Sender};
use futures::{executor::block_on, prelude::*, task::*};
use num_cpus;
use pin_utils::pin_mut;
use slotmap::{DefaultKey as Key, SecondaryMap, SlotMap};
use std::{
    any::Any,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, Weak
    },
    thread
};

type FutureObj = futures::future::FutureObj<'static, ()>;

/*
    OWNED THREADPOOL IMPL
*/

pub struct ThreadPool {
    inner: Arc<ThreadPoolInner>
}

impl ThreadPool {
    /// Creates a new, standalone threadpool
    pub fn new(threads: usize) -> Self {
        let global_running_futures = Arc::new(AtomicUsize::new(0));
        let notifier = Arc::new(Notifier::new());

        let workers = (0..threads)
            .map(|_| {
                let (tx, rx) = crossbeam_channel::unbounded::<Message>();
                let worker = Worker {
                    tx,
                    rx,
                    running_futures: Arc::new(AtomicUsize::new(0)),
                    global_running_futures: global_running_futures.clone(),
                    notifier: notifier.clone()
                };
                let handle = {
                    let worker = worker.clone();
                    let notifier = notifier.clone();
                    thread::spawn(move || {
                        if let Err(e) = catch_unwind(AssertUnwindSafe(|| worker.work())) {
                            notifier.propagate_panic(e);
                        }
                    })
                };
                WorkerThread {
                    worker,
                    handle: Mutex::new(Some(handle))
                }
            })
            .collect::<Vec<WorkerThread>>()
            .into_boxed_slice();

        ThreadPool {
            inner: Arc::new(ThreadPoolInner {
                workers,
                global_running_futures,
                notifier
            })
        }
    }

    /// Creates a new cloneable handle to the threadpool
    pub fn as_handle(&self) -> ThreadPoolHandle {
        ThreadPoolHandle {
            inner: Arc::downgrade(&self.inner)
        }
    }

    pub fn is_idle(&self) -> bool {
        self.inner.global_running_futures.load(Ordering::Acquire) == 0
    }

    pub fn wait(self) {
        block_on(future::poll_fn(|ctx: &mut Context| {
            self.inner.notifier.register_waker(ctx);
            if self.is_idle()
                || self.inner.notifier.is_shutting_down()
                || self.inner.notifier.did_panic()
            {
                self.inner.shutdown();
                self.inner.notifier.try_resume_unwind();
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }));
    }

    pub fn block_on<F: Future<Output = ()>>(self, future: F) {
        pin_mut!(future);
        block_on(future::poll_fn(|ctx: &mut Context| {
            self.inner.notifier.register_waker(ctx);
            if self.inner.notifier.is_shutting_down() || self.inner.notifier.did_panic() {
                self.inner.shutdown();
                self.inner.notifier.try_resume_unwind();
                Poll::Ready(())
            } else {
                if future.as_mut().poll(ctx).is_ready() {
                    self.inner.shutdown();
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        }));
    }
}

impl Default for ThreadPool {
    fn default() -> Self {
        ThreadPool::new(num_cpus::get())
    }
}

/*
    THREADPOOL INNER IMPL
*/

struct ThreadPoolInner {
    workers: Box<[WorkerThread]>,
    global_running_futures: Arc<AtomicUsize>,
    notifier: Arc<Notifier>
}

impl ThreadPoolInner {
    fn shutdown(&self) {
        // Send halt message to worker threads
        for worker in self.workers.iter() {
            let _ = worker.worker.tx.send(Message::Halt);
        }

        // Wait for them to finish
        for worker in self.workers.iter() {
            if let Some(handle) = worker.handle.lock().unwrap().take() {
                let _ = handle.join();
            }
        }
    }

    fn spawn_obj(&self, future: FutureObj) -> Result<(), SpawnError> {
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

        // Increase future counters
        least_full_worker
            .worker
            .running_futures
            .fetch_add(1, Ordering::Release);
        self.global_running_futures.fetch_add(1, Ordering::Release);

        // Send future to worker
        least_full_worker
            .worker
            .tx
            .send(Message::PushFuture(future))
            .map_err(|_| SpawnError::shutdown())
    }
}

/*
    THREADPOOL CLONEABLE HANDLE IMPL
*/

pub struct ThreadPoolHandle {
    inner: Weak<ThreadPoolInner>
}

impl Clone for ThreadPoolHandle {
    fn clone(&self) -> Self {
        ThreadPoolHandle {
            inner: self.inner.clone()
        }
    }
}

impl ThreadPoolHandle {
    pub fn shutdown(&self) -> Result<(), ()> {
        if let Some(inner) = self.inner.upgrade() {
            // NB. We cant join the worker threads here,
            // since this can be called from inside a future,
            // which will deadlock that thread.

            // Send halt message to worker threads.
            // This is necessary since there may not be any waiter to do this for us
            for worker in inner.workers.iter() {
                let _ = worker.worker.tx.send(Message::Halt);
            }

            // Notify any waiters that they should join the threads
            inner.notifier.notify_shutdown();
            Ok(())
        } else {
            Err(())
        }
    }
}

impl Spawn for ThreadPoolHandle {
    fn spawn_obj(&mut self, future: FutureObj) -> Result<(), SpawnError> {
        if let Some(inner) = self.inner.upgrade() {
            inner.spawn_obj(future)
        } else {
            Err(SpawnError::shutdown())
        }
    }

    fn status(&self) -> Result<(), SpawnError> {
        if self.inner.strong_count() > 0 {
            Ok(())
        } else {
            Err(SpawnError::shutdown())
        }
    }
}

/*
    WORKER IMPL
*/

struct WorkerThread {
    worker: Worker,
    handle: Mutex<Option<thread::JoinHandle<()>>>
}

#[derive(Clone)]
struct Worker {
    tx: Sender<Message>,
    rx: Receiver<Message>,
    running_futures: Arc<AtomicUsize>,
    global_running_futures: Arc<AtomicUsize>,
    notifier: Arc<Notifier>
}

impl Worker {
    fn work(&self) {
        let mut future_map: SlotMap<Key, FutureObj> = SlotMap::new();
        let mut waker_map: SecondaryMap<Key, Waker> = SecondaryMap::new();

        for message in self.rx.iter() {
            match message {
                Message::PushFuture(future) => {
                    let key = future_map.insert(future);
                    let waker = waker::new_waker(self.tx.clone(), key);
                    waker_map.insert(key, waker);
                    self.poll_future(&mut future_map, &mut waker_map, key);
                }
                Message::WakeFuture(key) => {
                    self.poll_future(&mut future_map, &mut waker_map, key);
                }
                Message::Halt => {
                    debug_assert_eq!(
                        future_map.len(),
                        self.running_futures.load(Ordering::Acquire)
                    );
                    self.running_futures.store(0, Ordering::Release);
                    self.global_running_futures
                        .fetch_sub(future_map.len(), Ordering::Release);
                    return;
                }
            }
        }
    }

    // Returns true if futures map became empty
    fn poll_future(
        &self,
        future_map: &mut SlotMap<Key, FutureObj>,
        waker_map: &mut SecondaryMap<Key, Waker>,
        key: Key
    ) {
        let future = match future_map.get_mut(key) {
            Some(future) => future,
            None => return
        };
        let waker = match waker_map.get(key) {
            Some(waker) => waker,
            None => return
        };

        // @FIXME Pass context instead of waker
        let mut ctx = Context::from_waker(waker);
        let res = future.poll_unpin(&mut ctx);
        if let Poll::Ready(()) = res {
            future_map.remove(key);
            waker_map.remove(key);
            self.running_futures.fetch_sub(1, Ordering::Release);
            // If there are no more futures, notify wakers so they can shutdown (or not)
            if self.global_running_futures.fetch_sub(1, Ordering::AcqRel) - 1 == 0 {
                self.notifier.notify();
            }
        }
    }
}

enum Message {
    PushFuture(FutureObj),
    WakeFuture(Key),
    Halt
}

/*
    NOTIFIER IMPL
*/

struct Notifier {
    poisoned: AtomicBool,
    shutting_down: AtomicBool,
    waker: AtomicWaker,
    error: Mutex<Option<Box<dyn Any + Send + 'static>>>
}

impl Notifier {
    fn new() -> Self {
        Notifier {
            poisoned: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            waker: AtomicWaker::new(),
            error: Mutex::new(None)
        }
    }

    fn register_waker(&self, ctx: &mut Context) {
        self.waker.register(&ctx.waker());
    }

    fn notify(&self) {
        self.waker.wake();
    }

    fn notify_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.notify();
    }

    fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    fn did_panic(&self) -> bool {
        self.poisoned.load(Ordering::SeqCst)
    }

    fn try_resume_unwind(&self) {
        if self.did_panic() {
            resume_unwind(self.error.lock().unwrap().take().unwrap())
        }
    }

    fn propagate_panic(&self, error: Box<dyn Any + Send + 'static>) {
        self.error.lock().unwrap().replace(error);
        self.poisoned.store(true, Ordering::SeqCst);
        self.notify();
    }
}
