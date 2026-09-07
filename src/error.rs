use std::{error::Error as StdError, fmt};

/// Everything that can go wrong between a producer thread and the Ruby runtime.
///
/// Variants are cloneable so one startup failure can be reported to every
/// request that arrives afterwards.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BridgeError {
    /// The wakeup socket pair failed.
    Io(String),
    /// The runtime thread could not be spawned.
    Spawn(String),
    /// The Ruby VM or the application failed to start.
    Startup(String),
    /// Ruby raised while the request was being served.
    Ruby(String),
    /// The application returned something that is not a Rack response.
    InvalidResponse(String),
    /// The admission limit rejected the request before it reached Ruby.
    Overloaded,
    /// The runtime has shut down and accepts no further work.
    RuntimeStopped,
    /// The runtime did not answer within the response timeout.
    ResponseTimeout,
    /// The runtime thread unwound.
    RuntimePanicked(String),
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(f, "bridge I/O error: {message}"),
            Self::Spawn(message) => write!(f, "failed to spawn Ruby runtime thread: {message}"),
            Self::Startup(message) => write!(f, "Ruby runtime startup failed: {message}"),
            Self::Ruby(message) => write!(f, "Ruby call failed: {message}"),
            Self::InvalidResponse(message) => write!(f, "invalid Rack-like response: {message}"),
            Self::Overloaded => write!(f, "Ruby runtime admission limit reached"),
            Self::RuntimeStopped => write!(f, "Ruby runtime has stopped"),
            Self::ResponseTimeout => write!(f, "timed out waiting for Ruby runtime"),
            Self::RuntimePanicked(message) => write!(f, "Ruby runtime panicked: {message}"),
        }
    }
}

impl StdError for BridgeError {}

#[cfg(not(loom))]
pub(crate) fn ruby_error(context: &str, error: magnus::Error) -> BridgeError {
    BridgeError::Ruby(format!("{context}: {error}"))
}

#[cfg(not(loom))]
pub(crate) fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}
