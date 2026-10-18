//! Module state published through Envoy's own statistics.
//!
//! Reporting into Envoy's stats rather than a channel of our own means the
//! numbers reach whatever sink the proxy is already configured with.
//!
//! Counters record events as they happen; the saturation gauges are republished
//! from the runtime itself so a missed update cannot leave them drifting.

use envoy_proxy_dynamic_modules_rust_sdk::{
    EnvoyCounterId, EnvoyCounterVecId, EnvoyGaugeId, EnvoyHistogramId, EnvoyHistogramVecId,
    EnvoyHttpFilter, EnvoyHttpFilterConfig, envoy_log_warn,
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
pub(crate) const REJECTED_INVALID_REQUEST: &str = "invalid_request";
pub(crate) const REJECTED_INTERNAL: &str = "internal";

/// How a request that reached the application ended.
pub(crate) const OUTCOME_COMPLETED: &str = "completed";
pub(crate) const OUTCOME_FAILED: &str = "failed";
pub(crate) const OUTCOME_CANCELLED: &str = "cancelled";

/// How a call the application made to a cluster ended, as the calling fiber
/// saw it. HTTP statuses, Envoy's own timeouts among them, are in the
/// cluster's statistics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CallOutcome {
    /// A response arrived; for an upgraded stream, the session ended.
    Completed,
    /// Envoy turned the call away before sending it.
    Refused,
    /// The application gave up after the upstream went quiet.
    TimedOut,
    /// The upstream sent faster than the application read.
    Overflow,
    /// The response was over the size limit.
    TooLarge,
    /// The upstream reset the call or broke the protocol.
    Reset,
    /// The application or its request finished before the call did.
    Abandoned,
}

impl CallOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Refused => "refused",
            Self::TimedOut => "timed_out",
            Self::Overflow => "overflow",
            Self::TooLarge => "too_large",
            Self::Reset => "reset",
            Self::Abandoned => "abandoned",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Metrics {
    requests: EnvoyCounterId,
    rejected: EnvoyCounterVecId,
    responses: EnvoyCounterVecId,
    inflight_requests: EnvoyGaugeId,
    resident_bytes: EnvoyGaugeId,
    reactor_idle_ms: EnvoyGaugeId,
    duration_ms: EnvoyHistogramId,
    upstream_calls: EnvoyCounterVecId,
    upstream_duration_ms: EnvoyHistogramVecId,
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
                resident_bytes: config.define_gauge("resident_bytes").ok()?,
                reactor_idle_ms: config.define_gauge("reactor_idle_ms").ok()?,
                duration_ms: config.define_histogram("duration_ms").ok()?,
                upstream_calls: config
                    .define_counter_vec("upstream_calls_total", &["cluster", "outcome"])
                    .ok()?,
                upstream_duration_ms: config
                    .define_histogram_vec("upstream_duration_ms", &["cluster"])
                    .ok()?,
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

    /// A call to `cluster` ended.
    pub(crate) fn upstream_call<EHF: EnvoyHttpFilter>(
        self,
        envoy_filter: &EHF,
        cluster: &str,
        outcome: CallOutcome,
    ) {
        let _ =
            envoy_filter.increment_counter_vec(self.upstream_calls, &[cluster, outcome.label()], 1);
    }

    /// `cluster` answered after `waited`: the whole response for a call, the
    /// head for a stream.
    pub(crate) fn upstream_answered<EHF: EnvoyHttpFilter>(
        self,
        envoy_filter: &EHF,
        cluster: &str,
        waited: Duration,
    ) {
        let _ = envoy_filter.record_histogram_value_vec(
            self.upstream_duration_ms,
            &[cluster],
            waited.as_millis() as u64,
        );
    }

    /// Republishes how loaded and how responsive the runtime currently is.
    ///
    /// Only a worker thread may write statistics, so these are sampled as
    /// requests begin and end. An idle proxy therefore reports what it last saw
    /// rather than what is true right now.
    pub(crate) fn saturation<EHF: EnvoyHttpFilter>(
        self,
        envoy_filter: &EHF,
        requests: usize,
        reactor_idle: Duration,
    ) {
        let _ = envoy_filter.set_gauge(self.inflight_requests, requests as u64);
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
