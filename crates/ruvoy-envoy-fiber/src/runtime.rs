use crate::metrics::Metrics;
use envoy_proxy_dynamic_modules_rust_sdk::envoy_log_info;
use ruvoy::{
    BridgeError, UpstreamSettings,
    fiber::{DEFAULT_MAX_INFLIGHT_REQUESTS, FiberRuntime, FiberRuntimeClient},
};
use serde::Deserialize;
use std::{
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

const DEFAULT_SHUTDOWN_TIMEOUT_MS: usize = 10_000;

/// Envoy's own default route timeout.
const DEFAULT_UPSTREAM_TIMEOUT_MS: u64 = 15_000;

/// The same bound as every other body Ruvoy holds in memory.
const DEFAULT_MAX_UPSTREAM_RESPONSE_BYTES: usize = 1024 * 1024;

static PROCESS_RUNTIME: OnceLock<Result<ProcessRuntime, BridgeError>> = OnceLock::new();
static PROCESS_SHUTDOWN_HOOK: OnceLock<Result<(), BridgeError>> = OnceLock::new();

pub(crate) struct FiberRackConfig {
    client: FiberRuntimeClient,
    diagnostics_enabled: bool,
    runtime_thread_id: String,
    metrics: Option<Metrics>,
    context: Option<Arc<ContextConfig>>,
    upstream: Option<Arc<UpstreamSettings>>,
}

impl FiberRackConfig {
    pub(crate) fn start(
        filter_config: &[u8],
        metrics: Option<Metrics>,
    ) -> Result<Self, BridgeError> {
        let FilterConfig {
            rackup,
            context,
            upstream,
        } = FilterConfig::parse(filter_config)?;
        let upstream = upstream.map(UpstreamConfig::into_settings).transpose()?;
        let requested = ProcessRuntimeConfig::new(&rackup)?;
        let runtime = process_runtime(requested)?;
        let ready = runtime.ready()?;

        Ok(Self {
            client: ready.client.clone(),
            diagnostics_enabled: runtime.config.diagnostics_enabled,
            runtime_thread_id: ready.runtime_thread_id.clone(),
            metrics,
            context: context.map(Arc::new),
            upstream: upstream.map(Arc::new),
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

    pub(crate) fn metrics(&self) -> Option<Metrics> {
        self.metrics
    }

    pub(crate) fn context(&self) -> Option<Arc<ContextConfig>> {
        self.context.clone()
    }

    pub(crate) fn upstream(&self) -> Option<Arc<UpstreamSettings>> {
        self.upstream.clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProcessRuntimeConfig {
    rackup: PathBuf,
    max_inflight_requests: usize,
    shutdown_timeout: Duration,
    diagnostics_enabled: bool,
}

/// The filter configuration as a control plane serializes it.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct FilterConfig {
    rackup: String,
    /// Exposes `ruvoy.context` when present.
    #[serde(default)]
    context: Option<ContextConfig>,
    /// Exposes `ruvoy.upstream` when present.
    #[serde(default)]
    upstream: Option<UpstreamConfig>,
}

/// What `ruvoy.context` carries beyond its fixed fields.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContextConfig {
    /// Namespaces copied from both dynamic and route metadata.
    #[serde(default)]
    pub(crate) metadata_namespaces: Vec<String>,
}

/// Which clusters `ruvoy.upstream` may call, and on what terms.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct UpstreamConfig {
    clusters: Vec<String>,
    #[serde(default = "UpstreamConfig::default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "UpstreamConfig::default_max_response_bytes")]
    max_response_bytes: usize,
}

impl FilterConfig {
    /// Reads the filter configuration.
    ///
    /// Control planes deliver it as JSON — either the rackup path on its own or
    /// an object naming it — while a static Envoy configuration may pass the
    /// path unquoted.
    fn parse(filter_config: &[u8]) -> Result<Self, BridgeError> {
        let text = std::str::from_utf8(filter_config)
            .map_err(|_| {
                BridgeError::Startup("filter configuration must contain valid UTF-8".to_owned())
            })?
            .trim();
        if text.is_empty() {
            return Err(BridgeError::Startup(
                "filter configuration must name a rackup file".to_owned(),
            ));
        }
        let rackup_only = |rackup: String| Self {
            rackup,
            context: None,
            upstream: None,
        };
        if !text.starts_with(['{', '"']) {
            return Ok(rackup_only(text.to_owned()));
        }
        if let Ok(rackup) = serde_json::from_str::<String>(text) {
            return Ok(rackup_only(rackup));
        }
        serde_json::from_str::<Self>(text)
            .map_err(|error| BridgeError::Startup(format!("invalid filter configuration: {error}")))
    }
}

impl UpstreamConfig {
    fn default_timeout_ms() -> u64 {
        DEFAULT_UPSTREAM_TIMEOUT_MS
    }

    fn default_max_response_bytes() -> usize {
        DEFAULT_MAX_UPSTREAM_RESPONSE_BYTES
    }

    /// Refuses settings under which no call could ever succeed.
    fn into_settings(self) -> Result<UpstreamSettings, BridgeError> {
        let invalid = |message: &str| {
            Err(BridgeError::Startup(format!(
                "invalid filter configuration: {message}"
            )))
        };
        if self.clusters.is_empty() {
            return invalid("upstream.clusters must name at least one cluster");
        }
        if self.clusters.iter().any(String::is_empty) {
            return invalid("upstream.clusters must not contain an empty name");
        }
        if self.timeout_ms == 0 {
            return invalid("upstream.timeout_ms must be positive");
        }
        if self.max_response_bytes == 0 {
            return invalid("upstream.max_response_bytes must be positive");
        }
        Ok(UpstreamSettings {
            clusters: self.clusters,
            timeout: Duration::from_millis(self.timeout_ms),
            max_response_bytes: self.max_response_bytes,
        })
    }
}

impl ProcessRuntimeConfig {
    fn new(rackup: &str) -> Result<Self, BridgeError> {
        let rackup = canonical_rackup(rackup)?;
        let max_inflight_requests =
            positive_env_usize("RUVOY_MAX_INFLIGHT_REQUESTS", DEFAULT_MAX_INFLIGHT_REQUESTS)?;
        let shutdown_timeout_ms =
            positive_env_usize("RUVOY_SHUTDOWN_TIMEOUT_MS", DEFAULT_SHUTDOWN_TIMEOUT_MS)?;

        Ok(Self {
            rackup,
            max_inflight_requests,
            shutdown_timeout: Duration::from_millis(shutdown_timeout_ms.try_into().map_err(
                |_| BridgeError::Startup("RUVOY_SHUTDOWN_TIMEOUT_MS is too large".to_owned()),
            )?),
            diagnostics_enabled: flag_env("RUVOY_DIAGNOSTICS")?,
        })
    }
}

struct ProcessRuntime {
    config: ProcessRuntimeConfig,
    runtime: Mutex<Option<FiberRuntime>>,
    /// `Err` once the application failed to load. The VM stays alive either way
    /// so its cleanup remains ordered, but it can never serve.
    ready: Result<Ready, BridgeError>,
}

/// What a runtime that loaded its application can hand to a filter.
struct Ready {
    client: FiberRuntimeClient,
    runtime_thread_id: String,
}

impl ProcessRuntime {
    fn start(config: ProcessRuntimeConfig) -> Result<Self, BridgeError> {
        let startup = FiberRuntime::start_rackup_with_limit_retained(
            &config.rackup,
            config.max_inflight_requests,
        )?;
        let (runtime, startup_error) = startup.into_parts();
        let ready = match startup_error {
            Some(error) => Err(error),
            None => Ok(Ready {
                client: runtime.client(),
                runtime_thread_id: runtime.info().rust_thread_id.clone(),
            }),
        };
        if ready.is_ok() {
            envoy_log_info!(
                "[ruvoy] Fiber runtime started: rackup={} max_inflight_requests={} \
                 shutdown_timeout_ms={} diagnostics={}",
                config.rackup.display(),
                config.max_inflight_requests,
                config.shutdown_timeout.as_millis(),
                u8::from(config.diagnostics_enabled),
            );
        }

        Ok(Self {
            config,
            runtime: Mutex::new(Some(runtime)),
            ready,
        })
    }

    fn ready(&self) -> Result<&Ready, BridgeError> {
        self.ready.as_ref().map_err(terminal_startup_failure)
    }

    fn ensure_compatible(&self, requested: &ProcessRuntimeConfig) -> Result<(), BridgeError> {
        if &self.config == requested {
            return Ok(());
        }

        Err(BridgeError::Startup(format!(
            "Ruvoy is already running with a different rackup; this process cannot serve a \
             second configuration. The Ruby VM is created once per process, so restart Envoy \
             to change it. active rackup={} requested rackup={}",
            self.config.rackup.display(),
            requested.rackup.display(),
        )))
    }

    /// # Safety
    ///
    /// Must run only at process exit, after all Ruby execution has stopped.
    unsafe fn shutdown(&self) -> Result<(), BridgeError> {
        let runtime = match self.runtime.lock() {
            Ok(mut slot) => slot.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        match runtime {
            // SAFETY: the caller guarantees the process is exiting, which is
            // when this function's own contract says it may run.
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
        .map_err(terminal_startup_failure)?;
    runtime.ensure_compatible(&requested)?;
    runtime.ready()?;
    Ok(runtime)
}

fn register_process_shutdown_hook() -> Result<(), BridgeError> {
    PROCESS_SHUTDOWN_HOOK
        .get_or_init(|| {
            // SAFETY: the handler takes no arguments, unwinds nothing, and only
            // touches statics that outlive it.
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
        // SAFETY: `atexit` runs after Envoy has stopped serving, so no Ruby
        // execution and no client clone can still be live.
        // Envoy may already have torn its logger down by the time `atexit`
        // handlers run, so this path stays on stderr.
        match unsafe { runtime.shutdown() } {
            Ok(()) => eprintln!("[ruvoy] Fiber runtime stopped"),
            Err(error) => eprintln!("[ruvoy] Fiber runtime shutdown failed: {error}"),
        }
    }));
}

/// Reports a failure that no later configuration can undo.
///
/// The Ruby VM is created once per process, so the first failure is final: an
/// operator who fixes the configuration and pushes it again would otherwise see
/// the original error and conclude the update never arrived.
fn terminal_startup_failure(error: &BridgeError) -> BridgeError {
    BridgeError::Startup(format!(
        "{error}. The Ruby VM is created once per process, so Envoy must be restarted \
         after fixing this."
    ))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn rackup(encoding: &str) -> Result<String, BridgeError> {
        FilterConfig::parse(encoding.as_bytes()).map(|config| config.rackup)
    }

    #[test]
    fn filter_config_accepts_json_and_plain_paths() {
        let expected = "/srv/app/config.ru";
        for encoding in [
            r#"/srv/app/config.ru"#,
            r#""/srv/app/config.ru""#,
            r#"{"rackup": "/srv/app/config.ru"}"#,
            "  /srv/app/config.ru  ",
        ] {
            assert_eq!(
                rackup(encoding).as_deref(),
                Ok(expected),
                "failed to read {encoding}"
            );
        }
    }

    #[test]
    fn filter_config_rejects_empty_and_malformed_json() {
        assert!(rackup("").is_err());
        assert!(rackup("   ").is_err());
        assert!(rackup(r#"{"rackup": 7}"#).is_err());
        assert!(rackup(r#"{"rack_up": "/a"}"#).is_err());
    }

    #[test]
    fn extensions_stay_off_unless_configured() {
        for encoding in [r#"/a"#, r#"{"rackup": "/a"}"#] {
            let config = FilterConfig::parse(encoding.as_bytes()).expect("a valid configuration");
            assert_eq!(config.context, None, "{encoding}");
            assert_eq!(config.upstream, None, "{encoding}");
        }
    }

    #[test]
    fn extension_settings_take_their_documented_defaults() {
        let config = FilterConfig::parse(
            br#"{"rackup": "/a", "context": {}, "upstream": {"clusters": ["users"]}}"#,
        )
        .expect("a valid configuration");

        assert_eq!(config.context, Some(ContextConfig::default()));
        assert_eq!(
            config.upstream.map(UpstreamConfig::into_settings),
            Some(Ok(UpstreamSettings {
                clusters: vec!["users".to_owned()],
                timeout: Duration::from_secs(15),
                max_response_bytes: 1024 * 1024,
            }))
        );
    }

    #[test]
    fn upstream_settings_no_call_could_use_are_refused() {
        for encoding in [
            r#"{"rackup": "/a", "upstream": {"clusters": []}}"#,
            r#"{"rackup": "/a", "upstream": {"clusters": [""]}}"#,
            r#"{"rackup": "/a", "upstream": {"clusters": ["users"], "timeout_ms": 0}}"#,
            r#"{"rackup": "/a", "upstream": {"clusters": ["users"], "max_response_bytes": 0}}"#,
        ] {
            let config = FilterConfig::parse(encoding.as_bytes()).expect("well-formed JSON");
            assert!(
                config
                    .upstream
                    .map(UpstreamConfig::into_settings)
                    .is_some_and(|settings| settings.is_err()),
                "{encoding} should be refused"
            );
        }
    }

    #[test]
    fn misspelled_extension_fields_are_rejected() {
        for encoding in [
            r#"{"rackup": "/a", "context": {"metadata_namespace": []}}"#,
            r#"{"rackup": "/a", "upstream": {"cluster": ["users"]}}"#,
            r#"{"rackup": "/a", "upstreams": {"clusters": ["users"]}}"#,
        ] {
            assert!(
                FilterConfig::parse(encoding.as_bytes()).is_err(),
                "{encoding}"
            );
        }
    }

    #[test]
    fn a_startup_failure_says_it_survives_the_next_configuration() {
        let message =
            terminal_startup_failure(&BridgeError::Startup("no such file".to_owned())).to_string();
        assert!(message.contains("no such file"), "{message}");
        assert!(message.contains("restarted"), "{message}");
    }

    #[test]
    fn process_runtime_config_requires_exact_reuse() {
        let active = ProcessRuntimeConfig {
            rackup: PathBuf::from("/srv/app/config.ru"),
            max_inflight_requests: 128,
            shutdown_timeout: Duration::from_secs(5),
            diagnostics_enabled: false,
        };
        assert_eq!(active, active.clone());

        let mut different = active.clone();
        different.rackup = PathBuf::from("/srv/other/config.ru");
        assert_ne!(active, different);
    }
}
