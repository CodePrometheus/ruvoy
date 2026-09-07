//! A Ruby application runtime embedded in Envoy.
//!
//! Producer threads exchange owned Rust data with one long-lived runtime
//! thread, and only that thread initializes or calls CRuby. Two runtimes serve
//! the same [`Request`] and [`Response`] vocabulary: [`fiber`] runs one fiber
//! per request and streams the response as the application produces it, while
//! [`sync`] serves one request at a time and exists as its diagnostic control.

// Only this crate has a Rust API to document; the Envoy modules are loaded
// through generated ABI symbols instead.
#![warn(missing_docs)]

mod budget;
mod concurrency;
mod error;
pub mod host;
mod request;
mod response;
mod stream;

// A loom build models the concurrent primitives on their own; the runtimes
// drive a Ruby VM, which loom cannot schedule.
#[cfg(not(loom))]
pub mod fiber;
#[cfg(not(loom))]
mod rack;
#[cfg(not(loom))]
pub mod sync;
#[cfg(not(loom))]
mod vm;
#[cfg(not(loom))]
mod wake;

pub use crate::{
    budget::{Budget, BudgetExceeded, Lease},
    error::BridgeError,
    request::{Request, RequestDiagnostics, RequestMetadata},
    response::{Response, ResponseHead},
    stream::{ResponseStream, StreamHandle, StreamItem, StreamWaker},
};

#[cfg(not(loom))]
pub use crate::vm::RuntimeInfo;

/// How long a caller waits for a buffered response before giving up.
#[cfg(not(loom))]
pub(crate) const DEFAULT_RESPONSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
