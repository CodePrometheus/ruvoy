//! Synchronisation primitives, swapped for loom's models under `cfg(loom)`.
//!
//! Concurrent code imports from here rather than from `std` so that
//! `RUSTFLAGS="--cfg loom" cargo test` can explore its interleavings.

#[cfg(loom)]
pub(crate) use loom::{
    sync::{Arc, Mutex, MutexGuard, atomic},
    thread,
};

#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Mutex, MutexGuard, atomic};
