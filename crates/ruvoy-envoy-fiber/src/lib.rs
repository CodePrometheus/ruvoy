use envoy_proxy_dynamic_modules_rust_sdk::*;

mod runtime;
mod worker;

declare_init_functions!(init, new_http_filter_config_fn);

fn init() -> bool {
    true
}

fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    _envoy_filter_config: &mut EC,
    name: &str,
    _config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    if name != "fiber_rack" {
        return None;
    }

    match runtime::FiberRackConfig::start() {
        Ok(config) => Some(Box::new(config)),
        Err(error) => {
            eprintln!("[ruvoy] failed to start Fiber runtime: {error}");
            None
        }
    }
}
