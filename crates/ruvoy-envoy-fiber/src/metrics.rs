//! Module state published through Envoy's own statistics.
//!
//! Reporting into Envoy's stats rather than a channel of our own means the
//! numbers reach whatever sink the proxy is already configured with.
//!
//! Counters record events as they happen; the saturation gauges are republished
//! from the budgets themselves so a missed update cannot leave them drifting.

use envoy_proxy_dynamic_modules_rust_sdk::{
    EnvoyCounterId, EnvoyCounterVecId, EnvoyGaugeId, EnvoyHistogramId, EnvoyHttpFilter,
    EnvoyHttpFilterConfig, envoy_log_warn,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

/// Requests between resident-memory samples.
///
/// Reading it costs a file read, which is too much per request at the rates
/// this runtime reaches, and memory does not move fast enough to need it.
const RESIDENT_SAMPLE_INTERVAL: u64 = 512;

static SAMPLES: AtomicU64 = AtomicU64::new(0);

/// Why a request never reached the application.
pub(crate) const REJECTED_ADMISSION: &str = "admission";
pub(crate) const REJECTED_BODY_BUDGET: &str = "body_budget";
pub(crate) const REJECTED_BODY_TOO_LARGE: &str = "body_too_large";
pub(crate) const REJECTED_INVALID_REQUEST: &str = "invalid_request";
pub(crate) const REJECTED_INTERNAL: &str = "internal";

/// How a request that reached the application ended.
pub(crate) const OUTCOME_COMPLETED: &str = "completed";
pub(crate) const OUTCOME_FAILED: &str = "failed";

#[derive(Clone, Copy, Debug)]
pub(crate) struct Metrics {
    requests: EnvoyCounterId,
    rejected: EnvoyCounterVecId,
    responses: EnvoyCounterVecId,
    inflight_requests: EnvoyGaugeId,
    inflight_body_bytes: EnvoyGaugeId,
    resident_bytes: EnvoyGaugeId,
    reactor_idle_ms: EnvoyGaugeId,
    duration_ms: EnvoyHistogramId,
}

impl Metrics {
    /// Defines every metric, or reports that this configuration has none.
    ///
    /// Serving matters more than observing it, so a proxy that refuses a
    /// definition leaves the module running without metrics.
    pub(crate) fn define<EC: EnvoyHttpFilterConfig>(config: &mut EC) -> Option<Self> {
        let metrics = (|| {
            Some(Self {
                requests: config.define_counter("requests_total").ok()?,
                rejected: config
                    .define_counter_vec("rejected_total", &["reason"])
                    .ok()?,
                responses: config
                    .define_counter_vec("responses_total", &["outcome"])
                    .ok()?,
                inflight_requests: config.define_gauge("inflight_requests").ok()?,
                inflight_body_bytes: config.define_gauge("inflight_body_bytes").ok()?,
                resident_bytes: config.define_gauge("resident_bytes").ok()?,
                reactor_idle_ms: config.define_gauge("reactor_idle_ms").ok()?,
                duration_ms: config.define_histogram("duration_ms").ok()?,
            })
        })();
        if metrics.is_none() {
            envoy_log_warn!("[ruvoy] statistics unavailable; serving without them");
        }
        metrics
    }

    /// A request was handed to the application.
    pub(crate) fn submitted<EHF: EnvoyHttpFilter>(self, envoy_filter: &EHF) {
        let _ = envoy_filter.increment_counter(self.requests, 1);
    }

    /// A request was refused before the application saw it.
    pub(crate) fn rejected<EHF: EnvoyHttpFilter>(self, envoy_filter: &EHF, reason: &str) {
        let _ = envoy_filter.increment_counter_vec(self.rejected, &[reason], 1);
    }

    /// A request that reached the application finished.
    pub(crate) fn responded<EHF: EnvoyHttpFilter>(
        self,
        envoy_filter: &EHF,
        outcome: &str,
        duration: Duration,
    ) {
        let _ = envoy_filter.increment_counter_vec(self.responses, &[outcome], 1);
        let _ = envoy_filter.record_histogram_value(self.duration_ms, duration.as_millis() as u64);
    }

    /// Republishes how loaded and how responsive the runtime currently is.
    pub(crate) fn saturation<EHF: EnvoyHttpFilter>(
        self,
        envoy_filter: &EHF,
        requests: usize,
        body_bytes: usize,
        reactor_idle: Duration,
    ) {
        let _ = envoy_filter.set_gauge(self.inflight_requests, requests as u64);
        let _ = envoy_filter.set_gauge(self.inflight_body_bytes, body_bytes as u64);
        let _ = envoy_filter.set_gauge(self.reactor_idle_ms, reactor_idle.as_millis() as u64);

        if SAMPLES
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(RESIDENT_SAMPLE_INTERVAL)
            && let Some(bytes) = resident_bytes()
        {
            let _ = envoy_filter.set_gauge(self.resident_bytes, bytes);
        }
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
