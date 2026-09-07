use ruvoy::{
    BridgeError,
    sync::{RubyRuntime, RuntimeClient},
};
use std::sync::Mutex;

const SYNC_RACK_APP_SOURCE: &str = include_str!("../ruby/rack_app.rb");

pub(crate) struct SyncRackConfig {
    client: RuntimeClient,
    diagnostics_enabled: bool,
    runtime_thread_id: String,
    runtime: Mutex<Option<RubyRuntime>>,
}

impl SyncRackConfig {
    pub(crate) fn start() -> Result<Self, BridgeError> {
        let diagnostics_enabled = flag_env("RUVOY_DIAGNOSTICS")?;
        let runtime = RubyRuntime::start(SYNC_RACK_APP_SOURCE)?;
        let client = runtime.client();
        let runtime_thread_id = runtime.info().rust_thread_id.clone();

        Ok(Self {
            client,
            diagnostics_enabled,
            runtime_thread_id,
            runtime: Mutex::new(Some(runtime)),
        })
    }

    pub(crate) fn client(&self) -> RuntimeClient {
        self.client.clone()
    }

    pub(crate) fn diagnostics_enabled(&self) -> bool {
        self.diagnostics_enabled
    }

    pub(crate) fn runtime_thread_id(&self) -> &str {
        &self.runtime_thread_id
    }
}

/// Thread identity headers describe Ruvoy internals, so they stay off unless an
/// operator turns them on for a test or an investigation.
fn flag_env(name: &str) -> Result<bool, BridgeError> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(false);
    };
    match value.to_str() {
        Some("1") => Ok(true),
        Some("0") => Ok(false),
        _ => Err(BridgeError::Startup(format!("{name} must be 0 or 1"))),
    }
}

impl Drop for SyncRackConfig {
    fn drop(&mut self) {
        let runtime_slot = match self.runtime.get_mut() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };

        // Config destruction can race Envoy's own teardown, so this path stays
        // on stderr rather than calling back into a logger that may be gone.
        if let Some(runtime) = runtime_slot.take() {
            match runtime.shutdown() {
                Ok(()) => eprintln!("[ruvoy] Ruby runtime stopped"),
                Err(error) => eprintln!("[ruvoy] Ruby runtime shutdown failed: {error}"),
            }
        }
    }
}
