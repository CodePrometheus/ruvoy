use crate::{BridgeError, error::ruby_error};
use magnus::Ruby;
use std::thread;

/// Identity of the embedded Ruby VM and the thread that owns it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeInfo {
    /// `RUBY_DESCRIPTION` of the embedded interpreter.
    pub ruby_description: String,
    /// Debug form of the Rust thread that owns the VM.
    pub rust_thread_id: String,
    /// `object_id` of the Ruby thread that runs applications.
    pub ruby_thread_object_id: u64,
}

pub(crate) fn runtime_info(ruby: &Ruby) -> Result<RuntimeInfo, BridgeError> {
    let ruby_description = ruby
        .eval::<String>("RUBY_DESCRIPTION")
        .map_err(|error| ruby_error("reading RUBY_DESCRIPTION", error))?;
    let ruby_thread_object_id = ruby
        .eval::<i64>("Thread.current.object_id")
        .map_err(|error| ruby_error("reading Ruby thread object id", error))
        .and_then(|thread_id| {
            u64::try_from(thread_id).map_err(|_| {
                BridgeError::InvalidResponse(format!(
                    "Ruby thread object id {thread_id} is negative"
                ))
            })
        })?;

    Ok(RuntimeInfo {
        ruby_description,
        rust_thread_id: format!("{:?}", thread::current().id()),
        ruby_thread_object_id,
    })
}
