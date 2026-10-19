//! Module state published through Envoy's own statistics.
//!
//! Reporting into Envoy's stats rather than a channel of our own means the
//! numbers reach whatever sink the proxy is already configured with.
//!
//! Counters and histograms record events as they happen, on the worker that saw
//! them. How loaded the runtime is lives on the bootstrap extension, which
//! publishes it from the main thread.

use envoy_proxy_dynamic_modules_rust_sdk::{
    EnvoyCounterId, EnvoyCounterVecId, EnvoyHistogramId, EnvoyHistogramVecId, EnvoyHttpFilter,
    EnvoyHttpFilterConfig, envoy_log_warn,
};
use std::time::Duration;

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
}
