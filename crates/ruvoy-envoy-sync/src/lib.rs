//! Envoy dynamic module serving Rack applications on the serial runtime.

use envoy_proxy_dynamic_modules_rust_sdk::*;

mod runtime;
mod worker;

declare_init_functions!(init, new_http_filter_config_fn);

fn init() -> bool {
    // The Ruby VM outlives any single filter configuration, so refuse to load
    // at all rather than risk being unmapped once the last one goes away.
    ruvoy::host::pin_in_memory()
}

fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    _envoy_filter_config: &mut EC,
    name: &str,
    _config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    if name != "sync_rack" {
        return None;
    }

    match runtime::SyncRackConfig::start() {
        Ok(config) => Some(Box::new(config)),
        Err(error) => {
            envoy_log_error!("[ruvoy] failed to start Ruby runtime: {error}");
            None
        }
    }
}
