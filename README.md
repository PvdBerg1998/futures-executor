**NB. This crate is a proof of concept and is not supported.**

A futures 0.3 executor, built to avoid leaking shared data in futures.

The threadpool in futures (https://rust-lang-nursery.github.io/futures-api-docs/0.3.0-alpha.13/futures/executor/struct.ThreadPool.html) uses `Arc` to share futures in `Waker`. This can lead to a non-0 strong count after the pool is dropped.
