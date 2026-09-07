//! Envoy dynamic module serving Rack applications on the fiber runtime.

use envoy_proxy_dynamic_modules_rust_sdk::*;

mod metrics;
mod runtime;
mod worker;

declare_init_functions!(init, new_http_filter_config_fn);

fn init() -> bool {
    // The Ruby VM outlives any single filter configuration, so refuse to load
    // at all rather than risk being unmapped once the last one goes away.
    ruvoy::host::pin_in_memory()
}

fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    envoy_filter_config: &mut EC,
    name: &str,
    config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    if name != "fiber_rack" {
        return None;
    }

    match runtime::FiberRackConfig::start(config, metrics::Metrics::define(envoy_filter_config)) {
        Ok(config) => Some(Box::new(config)),
        Err(error) => {
            envoy_log_error!("[ruvoy] failed to start Fiber runtime: {error}");
            None
        }
    }
}
