//! The bootstrap extension that owns the Ruby runtime.
//!
//! It starts the application before Envoy accepts traffic, publishes how
//! loaded the runtime is from the main thread once a second, answers the admin
//! interface, and stops the runtime when Envoy exits. The HTTP filter is an entry point into
//! the runtime started here, never its owner.

use crate::runtime::{self, RuntimeSettings};
use envoy_proxy_dynamic_modules_rust_sdk::*;
use ruvoy::{BridgeError, fiber::FiberRuntimeClient};
use std::time::Duration;

/// What `extension_name` must say for this module to serve the configuration.
pub(crate) const EXTENSION_NAME: &str = "ruvoy";
/// How often the gauges are read. A sample costs a few atomic loads and a
/// file read, and keeps the request path free of any publishing.
const PUBLISH_INTERVAL: Duration = Duration::from_secs(1);
/// How long the main thread waits for Ruby to answer an admin request.
const ADMIN_TIMEOUT: Duration = Duration::from_secs(2);
/// Envoy matches an admin path exactly, so each endpoint is registered.
const ADMIN_ENDPOINTS: [(&str, &str); 4] = [
    (
        "/ruvoy",
        "The Ruby runtime: version, application, requests in flight",
    ),
    (
        "/ruvoy/gc",
        "GC.stat; POST collects, and ?compact=1 compacts too",
    ),
    ("/ruvoy/fibers", "Live fibers with their backtraces"),
    ("/ruvoy/config", "The runtime's settings"),
];

pub(crate) fn new_bootstrap_extension_config_fn(
    envoy_config: &mut dyn EnvoyBootstrapExtensionConfig,
    name: &str,
    config: &[u8],
) -> Option<Box<dyn BootstrapExtensionConfig>> {
    if name != EXTENSION_NAME {
        return None;
    }
    let settings = match RuntimeSettings::parse(config) {
        Ok(settings) => settings,
        Err(error) => {
            envoy_log_error!("[ruvoy] invalid bootstrap extension configuration: {error}");
            return None;
        }
    };
    // A refused configuration stops Envoy, as a missing application should:
    // nothing it could serve would be the application the operator named.
    let ready = match runtime::start(settings) {
        Ok(ready) => ready,
        Err(error) => {
            envoy_log_error!("[ruvoy] the application did not start: {error}");
            return None;
        }
    };
    let publisher = envoy_config.new_timer();
    publisher.enable(PUBLISH_INTERVAL);
    for (path, help) in ADMIN_ENDPOINTS {
        if !envoy_config.register_admin_handler(path, help, false, false) {
            envoy_log_warn!("[ruvoy] could not register the {path} admin endpoint");
        }
    }
    let extension = RuvoyBootstrap {
        client: ready.client.clone(),
        gauges: Gauges::define(envoy_config),
        _publisher: publisher,
    };
    extension.publish(envoy_config);
    envoy_config.signal_init_complete();
    Some(Box::new(extension))
}

/// The gauges only the main thread writes.
#[derive(Clone, Copy)]
struct Gauges {
    inflight_requests: EnvoyGaugeId,
    reactor_idle_ms: EnvoyGaugeId,
    resident_bytes: EnvoyGaugeId,
}

impl Gauges {
    /// Serving matters more than observing it, so a proxy that refuses a
    /// definition leaves the module running without these.
    fn define(config: &mut dyn EnvoyBootstrapExtensionConfig) -> Option<Self> {
        let gauges = (|| {
            Some(Self {
                inflight_requests: config.define_gauge("inflight_requests").ok()?,
                reactor_idle_ms: config.define_gauge("reactor_idle_ms").ok()?,
                resident_bytes: config.define_gauge("resident_bytes").ok()?,
            })
        })();
        if gauges.is_none() {
            envoy_log_warn!("[ruvoy] runtime gauges unavailable; serving without them");
        }
        gauges
    }
}

pub(crate) struct RuvoyBootstrap {
    client: FiberRuntimeClient,
    gauges: Option<Gauges>,
    _publisher: Box<dyn EnvoyBootstrapExtensionTimer>,
}

impl RuvoyBootstrap {
    /// Writes every gauge from what is true right now, so an idle proxy
    /// reports an idle runtime and a stuck reactor shows its growing age.
    fn publish(&self, config: &dyn EnvoyBootstrapExtensionConfig) {
        let Some(gauges) = self.gauges else {
            return;
        };
        let _ = config.set_gauge(
            gauges.inflight_requests,
            self.client.inflight_requests() as u64,
        );
        let _ = config.set_gauge(
            gauges.reactor_idle_ms,
            self.client.reactor_idle_for().as_millis() as u64,
        );
        if let Some(bytes) = resident_bytes() {
            let _ = config.set_gauge(gauges.resident_bytes, bytes);
        }
    }
}

impl BootstrapExtensionConfig for RuvoyBootstrap {
    fn new_bootstrap_extension(
        &self,
        _envoy_extension: &mut dyn EnvoyBootstrapExtension,
    ) -> Box<dyn BootstrapExtension> {
        Box::new(RuvoyExtension {
            client: self.client.clone(),
        })
    }

    fn on_timer_fired(
        &self,
        config: &mut dyn EnvoyBootstrapExtensionConfig,
        timer: &dyn EnvoyBootstrapExtensionTimer,
    ) {
        self.publish(config);
        timer.enable(PUBLISH_INTERVAL);
    }

    fn on_admin_request(
        &self,
        _config: &mut dyn EnvoyBootstrapExtensionConfig,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> (u32, String) {
        match self.client.admin(method, path, body, ADMIN_TIMEOUT) {
            Ok(answer) => answer,
            Err(BridgeError::ResponseTimeout) => (
                504,
                format!(
                    "Ruby did not answer within {} ms\n",
                    ADMIN_TIMEOUT.as_millis()
                ),
            ),
            Err(error) => (500, format!("{error}\n")),
        }
    }
}

struct RuvoyExtension {
    client: FiberRuntimeClient,
}

impl BootstrapExtension for RuvoyExtension {
    fn on_drain_started(&mut self, _envoy_extension: &mut dyn EnvoyBootstrapExtension) {
        let _ = self.client.drain();
    }

    fn on_shutdown(
        &mut self,
        _envoy_extension: &mut dyn EnvoyBootstrapExtension,
        completion: CompletionCallback,
    ) {
        match runtime::shutdown() {
            Ok(true) => envoy_log_info!("[ruvoy] Fiber runtime stopped"),
            Ok(false) => {}
            Err(error) => envoy_log_error!("[ruvoy] Fiber runtime shutdown failed: {error}"),
        }
        completion.done();
    }
}

/// Resident memory of this process.
///
/// An embedded VM grows in ways the proxy cannot see, so the orchestrator gets
/// the number and decides what to do with it. Platforms without a cheap way to
/// read it simply report nothing rather than a number that is not comparable.
#[cfg(target_os = "linux")]
fn resident_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    // SAFETY: `sysconf` reads a static system parameter and takes no pointers.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(page_size).ok().map(|size| pages * size)
}

#[cfg(not(target_os = "linux"))]
fn resident_bytes() -> Option<u64> {
    None
}
