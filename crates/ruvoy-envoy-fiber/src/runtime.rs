use ruvoy::{
    BridgeError,
    fiber::{DEFAULT_MAX_INFLIGHT_REQUESTS, FiberRuntime, FiberRuntimeClient},
};
use std::{
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const DEFAULT_MAX_INFLIGHT_BODY_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_SHUTDOWN_TIMEOUT_MS: usize = 10_000;

static PROCESS_RUNTIME: OnceLock<Result<ProcessRuntime, BridgeError>> = OnceLock::new();
static PROCESS_SHUTDOWN_HOOK: OnceLock<Result<(), BridgeError>> = OnceLock::new();

pub(crate) struct FiberRackConfig {
    client: FiberRuntimeClient,
    body_budget: Arc<BodyBudget>,
    diagnostics_enabled: bool,
    runtime_thread_id: String,
}

impl FiberRackConfig {
    pub(crate) fn start(filter_config: &[u8]) -> Result<Self, BridgeError> {
        let requested = ProcessRuntimeConfig::from_filter_config(filter_config)?;
        let runtime = process_runtime(requested)?;
        let (client, runtime_thread_id) = runtime.ready()?;

        Ok(Self {
            client: client.clone(),
            body_budget: Arc::clone(&runtime.body_budget),
            diagnostics_enabled: runtime.config.diagnostics_enabled,
            runtime_thread_id: runtime_thread_id.to_owned(),
        })
    }

    pub(crate) fn diagnostics_enabled(&self) -> bool {
        self.diagnostics_enabled
    }

    pub(crate) fn client(&self) -> FiberRuntimeClient {
        self.client.clone()
    }

    pub(crate) fn runtime_thread_id(&self) -> &str {
        &self.runtime_thread_id
    }

    pub(crate) fn body_budget(&self) -> Arc<BodyBudget> {
        Arc::clone(&self.body_budget)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProcessRuntimeConfig {
    rackup: PathBuf,
    max_inflight_requests: usize,
    max_inflight_body_bytes: usize,
    shutdown_timeout: Duration,
    diagnostics_enabled: bool,
}

impl ProcessRuntimeConfig {
    fn from_filter_config(filter_config: &[u8]) -> Result<Self, BridgeError> {
        let rackup = std::str::from_utf8(filter_config)
            .map_err(|_| BridgeError::Startup("rackup path must contain valid UTF-8".to_owned()))?;
        if rackup.is_empty() {
            return Err(BridgeError::Startup(
                "rackup path must not be empty".to_owned(),
            ));
        }
        let rackup = canonical_rackup(rackup)?;
        let max_inflight_requests =
            positive_env_usize("RUVOY_MAX_INFLIGHT_REQUESTS", DEFAULT_MAX_INFLIGHT_REQUESTS)?;
        let max_inflight_body_bytes = positive_env_usize(
            "RUVOY_MAX_INFLIGHT_BODY_BYTES",
            DEFAULT_MAX_INFLIGHT_BODY_BYTES,
        )?;
        let shutdown_timeout_ms =
            positive_env_usize("RUVOY_SHUTDOWN_TIMEOUT_MS", DEFAULT_SHUTDOWN_TIMEOUT_MS)?;

        Ok(Self {
            rackup,
            max_inflight_requests,
            max_inflight_body_bytes,
            shutdown_timeout: Duration::from_millis(shutdown_timeout_ms.try_into().map_err(
                |_| BridgeError::Startup("RUVOY_SHUTDOWN_TIMEOUT_MS is too large".to_owned()),
            )?),
            diagnostics_enabled: flag_env("RUVOY_DIAGNOSTICS")?,
        })
    }
}

struct ProcessRuntime {
    config: ProcessRuntimeConfig,
    client: Option<FiberRuntimeClient>,
    body_budget: Arc<BodyBudget>,
    runtime_thread_id: Option<String>,
    startup_error: Option<BridgeError>,
    runtime: Mutex<Option<FiberRuntime>>,
}

impl ProcessRuntime {
    fn start(config: ProcessRuntimeConfig) -> Result<Self, BridgeError> {
        let startup = FiberRuntime::start_rackup_with_limit_retained(
            &config.rackup,
            config.max_inflight_requests,
        )?;
        let (runtime, startup_error) = startup.into_parts();
        let (client, runtime_thread_id) = if startup_error.is_none() {
            (
                Some(runtime.client()),
                Some(runtime.info().rust_thread_id.clone()),
            )
        } else {
            (None, None)
        };
        let body_budget = Arc::new(BodyBudget::new(config.max_inflight_body_bytes));
        if startup_error.is_none() {
            eprintln!(
                "[ruvoy] Fiber runtime started: rackup={} max_inflight_requests={} \
                 max_inflight_body_bytes={} shutdown_timeout_ms={} diagnostics={}",
                config.rackup.display(),
                config.max_inflight_requests,
                config.max_inflight_body_bytes,
                config.shutdown_timeout.as_millis(),
                u8::from(config.diagnostics_enabled),
            );
        }

        Ok(Self {
            config,
            client,
            body_budget,
            runtime_thread_id,
            startup_error,
            runtime: Mutex::new(Some(runtime)),
        })
    }

    fn ready(&self) -> Result<(&FiberRuntimeClient, &str), BridgeError> {
        if let Some(error) = &self.startup_error {
            return Err(error.clone());
        }
        Ok((
            self.client
                .as_ref()
                .expect("successful process runtime has a client"),
            self.runtime_thread_id
                .as_deref()
                .expect("successful process runtime has thread information"),
        ))
    }

    fn ensure_compatible(&self, requested: &ProcessRuntimeConfig) -> Result<(), BridgeError> {
        if &self.config == requested {
            return Ok(());
        }

        Err(BridgeError::Startup(format!(
            "Ruvoy is already initialized for rackup {} with process-wide limits; \
             requested rackup {} or limits do not match",
            self.config.rackup.display(),
            requested.rackup.display(),
        )))
    }

    unsafe fn shutdown(&self) -> Result<(), BridgeError> {
        let runtime = match self.runtime.lock() {
            Ok(mut slot) => slot.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        match runtime {
            Some(runtime) => unsafe { runtime.shutdown_with_timeout(self.config.shutdown_timeout) },
            None => Ok(()),
        }
    }
}

fn process_runtime(
    requested: ProcessRuntimeConfig,
) -> Result<&'static ProcessRuntime, BridgeError> {
    register_process_shutdown_hook()?;
    let runtime = PROCESS_RUNTIME
        .get_or_init(|| ProcessRuntime::start(requested.clone()))
        .as_ref()
        .map_err(Clone::clone)?;
    runtime.ensure_compatible(&requested)?;
    runtime.ready()?;
    Ok(runtime)
}

fn register_process_shutdown_hook() -> Result<(), BridgeError> {
    PROCESS_SHUTDOWN_HOOK
        .get_or_init(|| {
            let result = unsafe { libc::atexit(shutdown_process_runtime) };
            if result == 0 {
                Ok(())
            } else {
                Err(BridgeError::Startup(
                    "failed to register the Ruvoy process shutdown hook".to_owned(),
                ))
            }
        })
        .clone()
}

extern "C" fn shutdown_process_runtime() {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        let Some(Ok(runtime)) = PROCESS_RUNTIME.get() else {
            return;
        };
        match unsafe { runtime.shutdown() } {
            Ok(()) => eprintln!("[ruvoy] Fiber runtime stopped"),
            Err(error) => eprintln!("[ruvoy] Fiber runtime shutdown failed: {error}"),
        }
    }));
}

/// A relative rackup resolves against the Envoy working directory, so failures
/// must report that directory instead of leaving the operator guessing.
fn canonical_rackup(path: impl AsRef<Path>) -> Result<PathBuf, BridgeError> {
    let path = path.as_ref();
    let rackup = path.canonicalize().map_err(|error| {
        let working_directory = std::env::current_dir()
            .map(|directory| directory.display().to_string())
            .unwrap_or_else(|_| "<unknown>".to_owned());
        BridgeError::Startup(format!(
            "failed to resolve rackup {} from working directory {working_directory}: {error}",
            path.display()
        ))
    })?;
    if !rackup.is_file() {
        return Err(BridgeError::Startup(format!(
            "rackup is not a file: {}",
            rackup.display()
        )));
    }
    Ok(rackup)
}

/// Thread identity and stage-timing headers describe Ruvoy internals, so they
/// stay off unless an operator turns them on for a test or an investigation.
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

fn positive_env_usize(name: &str, default: usize) -> Result<usize, BridgeError> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    let value = value
        .into_string()
        .map_err(|_| BridgeError::Startup(format!("{name} must contain valid UTF-8")))?;
    let value = value
        .parse::<usize>()
        .map_err(|_| BridgeError::Startup(format!("{name} must be a positive integer")))?;
    if value == 0 {
        return Err(BridgeError::Startup(format!(
            "{name} must be a positive integer"
        )));
    }
    Ok(value)
}

pub(crate) struct BodyBudget {
    reserved: AtomicUsize,
    maximum: usize,
}

impl BodyBudget {
    fn new(maximum: usize) -> Self {
        Self {
            reserved: AtomicUsize::new(0),
            maximum,
        }
    }

    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<BodyLease, BodyBudgetExceeded> {
        self.try_add(bytes)?;
        Ok(BodyLease {
            budget: Arc::clone(self),
            reserved: bytes,
        })
    }

    fn try_add(&self, bytes: usize) -> Result<(), BodyBudgetExceeded> {
        let mut reserved = self.reserved.load(Ordering::Relaxed);
        loop {
            if bytes > self.maximum.saturating_sub(reserved) {
                return Err(BodyBudgetExceeded);
            }
            match self.reserved.compare_exchange_weak(
                reserved,
                reserved + bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => reserved = observed,
            }
        }
    }
}

pub(crate) struct BodyLease {
    budget: Arc<BodyBudget>,
    reserved: usize,
}

impl BodyLease {
    pub(crate) fn ensure_reserved(&mut self, required: usize) -> Result<(), BodyBudgetExceeded> {
        if required <= self.reserved {
            return Ok(());
        }
        let additional = required - self.reserved;
        self.budget.try_add(additional)?;
        self.reserved = required;
        Ok(())
    }
}

impl Drop for BodyLease {
    fn drop(&mut self) {
        let previous = self
            .budget
            .reserved
            .fetch_sub(self.reserved, Ordering::AcqRel);
        debug_assert!(previous >= self.reserved);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BodyBudgetExceeded;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_budget_rejects_overcommit_and_releases_capacity() {
        let budget = Arc::new(BodyBudget::new(10));
        let lease = budget.try_reserve(8).expect("first body should fit");
        assert!(budget.try_reserve(3).is_err());
        drop(lease);
        budget
            .try_reserve(10)
            .expect("dropped body should release its capacity");
    }

    #[test]
    fn chunked_body_lease_grows_without_double_reserving() {
        let budget = Arc::new(BodyBudget::new(10));
        let mut lease = budget.try_reserve(0).expect("empty body should fit");
        lease.ensure_reserved(4).expect("first chunk should fit");
        lease
            .ensure_reserved(4)
            .expect("same size must not reserve twice");
        assert!(lease.ensure_reserved(11).is_err());
        lease
            .ensure_reserved(10)
            .expect("remaining budget should fit");
    }

    #[test]
    fn process_runtime_config_requires_exact_reuse() {
        let active = ProcessRuntimeConfig {
            rackup: PathBuf::from("/srv/app/config.ru"),
            max_inflight_requests: 128,
            max_inflight_body_bytes: 1024,
            shutdown_timeout: Duration::from_secs(5),
            diagnostics_enabled: false,
        };
        assert_eq!(active, active.clone());

        let mut different = active.clone();
        different.rackup = PathBuf::from("/srv/other/config.ru");
        assert_ne!(active, different);
    }
}
