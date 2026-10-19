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

const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 10_000;

/// Envoy's own default route timeout.
const DEFAULT_UPSTREAM_TIMEOUT_MS: u64 = 15_000;

/// The same bound as every other body Ruvoy holds in memory.
const DEFAULT_MAX_UPSTREAM_RESPONSE_BYTES: usize = 1024 * 1024;

/// What a streamed response may hold waiting for the application, when the
/// filter configuration does not say.
const DEFAULT_STREAM_BUFFER_BYTES: usize = 1024 * 1024;

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
    /// Joins the runtime the bootstrap extension started; a filter cannot
    /// start one.
    pub(crate) fn start(
        filter_config: &[u8],
        metrics: Option<Metrics>,
    ) -> Result<Self, BridgeError> {
        let FilterConfig {
            context, upstream, ..
        } = FilterConfig::parse(filter_config)?;
        let upstream = upstream.map(UpstreamConfig::into_settings).transpose()?;
        let ready = ready()?;

        Ok(Self {
            client: ready.client.clone(),
            diagnostics_enabled: ready.diagnostics_enabled,
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

/// The bootstrap extension's configuration: the application and the terms the
/// runtime serves it on.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeSettings {
    rackup: String,
    #[serde(default = "RuntimeSettings::default_max_inflight_requests")]
    max_inflight_requests: usize,
    #[serde(default = "RuntimeSettings::default_shutdown_timeout_ms")]
    shutdown_timeout_ms: u64,
    #[serde(default)]
    diagnostics: bool,
}

impl RuntimeSettings {
    pub(crate) fn parse(config: &[u8]) -> Result<Self, BridgeError> {
        let text = std::str::from_utf8(config)
            .map_err(|_| {
                BridgeError::Startup(
                    "bootstrap extension configuration must contain valid UTF-8".to_owned(),
                )
            })?
            .trim();
        if text.is_empty() {
            return Err(BridgeError::Startup(
                "bootstrap extension configuration must name a rackup file".to_owned(),
            ));
        }
        let settings = serde_json::from_str::<Self>(text).map_err(|error| {
            BridgeError::Startup(format!(
                "invalid bootstrap extension configuration: {error}"
            ))
        })?;
        if settings.rackup.is_empty() {
            return Err(BridgeError::Startup("rackup must not be empty".to_owned()));
        }
        if settings.max_inflight_requests == 0 {
            return Err(BridgeError::Startup(
                "max_inflight_requests must be a positive integer".to_owned(),
            ));
        }
        if settings.shutdown_timeout_ms == 0 {
            return Err(BridgeError::Startup(
                "shutdown_timeout_ms must be a positive integer".to_owned(),
            ));
        }
        Ok(settings)
    }

    fn default_max_inflight_requests() -> usize {
        DEFAULT_MAX_INFLIGHT_REQUESTS
    }

    fn default_shutdown_timeout_ms() -> u64 {
        DEFAULT_SHUTDOWN_TIMEOUT_MS
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
#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct FilterConfig {
    /// Accepted only to say where it belongs now.
    #[serde(default)]
    rackup: Option<serde::de::IgnoredAny>,
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
    #[serde(default = "UpstreamConfig::default_stream_buffer_bytes")]
    stream_buffer_bytes: usize,
}

impl FilterConfig {
    /// Reads the filter configuration: a JSON object, or nothing.
    ///
    /// The rackup lives on the bootstrap extension, so a configuration that
    /// still names one, as every configuration did before, is told where it
    /// went.
    fn parse(filter_config: &[u8]) -> Result<Self, BridgeError> {
        let text = std::str::from_utf8(filter_config)
            .map_err(|_| {
                BridgeError::Startup("filter configuration must contain valid UTF-8".to_owned())
            })?
            .trim();
        if text.is_empty() {
            return Ok(Self::default());
        }
        let moved = || {
            BridgeError::Startup(
                "the rackup is configured on the ruvoy bootstrap extension, not on the filter; \
                 the filter configuration is a JSON object with optional context and upstream"
                    .to_owned(),
            )
        };
        if !text.starts_with('{') {
            return Err(moved());
        }
        let config = serde_json::from_str::<Self>(text).map_err(|error| {
            BridgeError::Startup(format!("invalid filter configuration: {error}"))
        })?;
        if config.rackup.is_some() {
            return Err(moved());
        }
        Ok(config)
    }
}

impl UpstreamConfig {
    fn default_timeout_ms() -> u64 {
        DEFAULT_UPSTREAM_TIMEOUT_MS
    }

    fn default_max_response_bytes() -> usize {
        DEFAULT_MAX_UPSTREAM_RESPONSE_BYTES
    }

    fn default_stream_buffer_bytes() -> usize {
        DEFAULT_STREAM_BUFFER_BYTES
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
        if self.stream_buffer_bytes == 0 {
            return invalid("upstream.stream_buffer_bytes must be positive");
        }
        Ok(UpstreamSettings {
            clusters: self.clusters,
            timeout: Duration::from_millis(self.timeout_ms),
            max_response_bytes: self.max_response_bytes,
            stream_buffer_bytes: self.stream_buffer_bytes,
        })
    }
}

impl ProcessRuntimeConfig {
    fn new(settings: &RuntimeSettings) -> Result<Self, BridgeError> {
        Ok(Self {
            rackup: canonical_rackup(&settings.rackup)?,
            max_inflight_requests: settings.max_inflight_requests,
            shutdown_timeout: Duration::from_millis(settings.shutdown_timeout_ms),
            diagnostics_enabled: settings.diagnostics,
        })
    }

    /// What the application sees as `Ruvoy::Lifecycle.settings`.
    fn ruby_settings_json(&self) -> String {
        serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "rackup": self.rackup.display().to_string(),
            "max_inflight_requests": self.max_inflight_requests,
            "shutdown_timeout_ms": self.shutdown_timeout.as_millis() as u64,
            "diagnostics": self.diagnostics_enabled,
        })
        .to_string()
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
pub(crate) struct Ready {
    pub(crate) client: FiberRuntimeClient,
    pub(crate) runtime_thread_id: String,
    pub(crate) diagnostics_enabled: bool,
}

impl ProcessRuntime {
    fn start(config: ProcessRuntimeConfig) -> Result<Self, BridgeError> {
        let startup = FiberRuntime::start_rackup_with_limit_retained(
            &config.rackup,
            config.max_inflight_requests,
            Some(config.ruby_settings_json()),
        )?;
        let (runtime, startup_error) = startup.into_parts();
        let ready = match startup_error {
            Some(error) => Err(error),
            None => Ok(Ready {
                client: runtime.client(),
                runtime_thread_id: runtime.info().rust_thread_id.clone(),
                diagnostics_enabled: config.diagnostics_enabled,
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

    /// Stops the runtime; false when it was stopped already.
    ///
    /// # Safety
    ///
    /// Must run only from Envoy's shutdown stage or at process exit. Workers
    /// may still be serving then; they reach Ruby only through the runtime
    /// client, which refuses them once the runtime has stopped.
    unsafe fn shutdown(&self) -> Result<bool, BridgeError> {
        let runtime = match self.runtime.lock() {
            Ok(mut slot) => slot.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        match runtime {
            // SAFETY: Envoy is exiting, so nothing will ask Ruby for more; the
            // workers' clients are refused from here on.
            Some(runtime) => unsafe {
                runtime
                    .shutdown_with_timeout(self.config.shutdown_timeout)
                    .map(|()| true)
            },
            None => Ok(false),
        }
    }
}

/// Starts the runtime for this process, or joins the one already started
/// with the same settings. Only the bootstrap extension calls this.
pub(crate) fn start(settings: RuntimeSettings) -> Result<&'static Ready, BridgeError> {
    let requested = ProcessRuntimeConfig::new(&settings)?;
    register_process_shutdown_hook()?;
    let runtime = PROCESS_RUNTIME
        .get_or_init(|| ProcessRuntime::start(requested.clone()))
        .as_ref()
        .map_err(terminal_startup_failure)?;
    runtime.ensure_compatible(&requested)?;
    runtime.ready()
}

/// The runtime the bootstrap extension started, for a filter configuration.
pub(crate) fn ready() -> Result<&'static Ready, BridgeError> {
    match PROCESS_RUNTIME.get() {
        None => Err(BridgeError::Startup(
            "the Ruby runtime is not configured: add the ruvoy bootstrap extension under \
             bootstrap_extensions, which names the rackup"
                .to_owned(),
        )),
        Some(Err(error)) => Err(terminal_startup_failure(error)),
        Some(Ok(runtime)) => runtime.ready(),
    }
}

/// Stops the runtime if it is still running; true when this call stopped it.
pub(crate) fn shutdown() -> Result<bool, BridgeError> {
    let Some(Ok(runtime)) = PROCESS_RUNTIME.get() else {
        return Ok(false);
    };
    // SAFETY: called only from Envoy's shutdown stage and at process exit.
    unsafe { runtime.shutdown() }
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

/// The last resort for a process that exits without Envoy's shutdown stage.
extern "C" fn shutdown_process_runtime() {
    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        // Envoy may already have torn its logger down by the time `atexit`
        // handlers run, so this path stays on stderr.
        match shutdown() {
            Ok(true) => eprintln!("[ruvoy] Fiber runtime stopped"),
            Ok(false) => {}
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
#[cfg(test)]
mod tests {
    use super::*;

    fn settings(encoding: &str) -> Result<RuntimeSettings, BridgeError> {
        RuntimeSettings::parse(encoding.as_bytes())
    }

    #[test]
    fn runtime_settings_take_their_documented_defaults() {
        let parsed =
            settings(r#"{"rackup": "/srv/app/config.ru"}"#).expect("a valid configuration");
        assert_eq!(parsed.rackup, "/srv/app/config.ru");
        assert_eq!(parsed.max_inflight_requests, DEFAULT_MAX_INFLIGHT_REQUESTS);
        assert_eq!(parsed.shutdown_timeout_ms, DEFAULT_SHUTDOWN_TIMEOUT_MS);
        assert!(!parsed.diagnostics);
    }

    #[test]
    fn runtime_settings_reject_what_could_not_serve() {
        for encoding in [
            "",
            "   ",
            "/srv/app/config.ru",
            r#"{"rackup": ""}"#,
            r#"{"rackup": 7}"#,
            r#"{"rack_up": "/a"}"#,
            r#"{"rackup": "/a", "max_inflight_requests": 0}"#,
            r#"{"rackup": "/a", "shutdown_timeout_ms": 0}"#,
            r#"{"rackup": "/a", "diagnostics": "yes"}"#,
        ] {
            assert!(
                settings(encoding).is_err(),
                "{encoding:?} should be refused"
            );
        }
    }

    #[test]
    fn filter_config_is_optional() {
        for encoding in ["", "   ", "{}"] {
            let config = FilterConfig::parse(encoding.as_bytes()).expect("a valid configuration");
            assert_eq!(config.context, None, "{encoding:?}");
            assert_eq!(config.upstream, None, "{encoding:?}");
        }
    }

    #[test]
    fn a_filter_config_that_still_names_the_rackup_is_told_where_it_went() {
        for encoding in [
            "/srv/app/config.ru",
            r#""/srv/app/config.ru""#,
            r#"{"rackup": "/srv/app/config.ru"}"#,
        ] {
            let error = FilterConfig::parse(encoding.as_bytes())
                .expect_err("the rackup no longer belongs here")
                .to_string();
            assert!(error.contains("bootstrap extension"), "{encoding}: {error}");
        }
    }

    #[test]
    fn extension_settings_take_their_documented_defaults() {
        let config =
            FilterConfig::parse(br#"{"context": {}, "upstream": {"clusters": ["users"]}}"#)
                .expect("a valid configuration");

        assert_eq!(config.context, Some(ContextConfig::default()));
        assert_eq!(
            config.upstream.map(UpstreamConfig::into_settings),
            Some(Ok(UpstreamSettings {
                clusters: vec!["users".to_owned()],
                timeout: Duration::from_secs(15),
                max_response_bytes: 1024 * 1024,
                stream_buffer_bytes: 1024 * 1024,
            }))
        );
    }

    #[test]
    fn upstream_settings_no_call_could_use_are_refused() {
        for encoding in [
            r#"{"upstream": {"clusters": []}}"#,
            r#"{"upstream": {"clusters": [""]}}"#,
            r#"{"upstream": {"clusters": ["users"], "timeout_ms": 0}}"#,
            r#"{"upstream": {"clusters": ["users"], "max_response_bytes": 0}}"#,
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
            r#"{"context": {"metadata_namespace": []}}"#,
            r#"{"upstream": {"cluster": ["users"]}}"#,
            r#"{"upstreams": {"clusters": ["users"]}}"#,
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

    #[test]
    fn the_application_sees_the_settings_it_runs_under() {
        let config = ProcessRuntimeConfig {
            rackup: PathBuf::from("/srv/app/config.ru"),
            max_inflight_requests: 128,
            shutdown_timeout: Duration::from_secs(5),
            diagnostics_enabled: true,
        };
        let json: serde_json::Value =
            serde_json::from_str(&config.ruby_settings_json()).expect("valid JSON");
        assert_eq!(json["rackup"], "/srv/app/config.ru");
        assert_eq!(json["max_inflight_requests"], 128);
        assert_eq!(json["shutdown_timeout_ms"], 5000);
        assert_eq!(json["diagnostics"], true);
        assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
    }
}
