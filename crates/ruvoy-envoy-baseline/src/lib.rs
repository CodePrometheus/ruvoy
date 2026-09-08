//! Envoy dynamic module that answers without entering Ruby.
//!
//! It measures what the surrounding machinery costs, so the Rack modules can be
//! read against a ceiling rather than against nothing.

use abi::*;
use envoy_proxy_dynamic_modules_rust_sdk::*;
use std::thread::JoinHandle;
use std::time::Duration;

const SCHEDULER_EVENT_ID: u64 = 1;
const BENCHMARK_EVENT_ID: u64 = 2;
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

declare_init_functions!(init, new_http_filter_config_fn);

fn init() -> bool {
    true
}

fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    _envoy_filter_config: &mut EC,
    name: &str,
    _config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    (name == "baseline").then(|| Box::new(BaselineConfig) as Box<dyn HttpFilterConfig<EHF>>)
}

struct BaselineConfig;

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for BaselineConfig {
    fn new_http_filter(&self, _envoy_filter: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        Box::new(BaselineFilter {
            worker: None,
            benchmark: None,
            waiting: false,
            responded: false,
        })
    }
}

struct BenchmarkRequest {
    wait_ms: u64,
    response_bytes: usize,
    expected_request_bytes: Option<usize>,
    request_body: Vec<u8>,
}

struct BaselineFilter {
    worker: Option<JoinHandle<()>>,
    benchmark: Option<BenchmarkRequest>,
    waiting: bool,
    responded: bool,
}

impl BaselineFilter {
    fn finish_benchmark<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) {
        if self.waiting || self.responded {
            return;
        }

        let body_size_mismatch = self.benchmark.as_ref().is_some_and(|request| {
            request
                .expected_request_bytes
                .is_some_and(|expected| request.request_body.len() != expected)
        });
        if body_size_mismatch {
            self.send_bad_request(envoy_filter, b"unexpected request body size");
            return;
        }

        let wait_ms = self
            .benchmark
            .as_ref()
            .map(|request| request.wait_ms)
            .unwrap_or_default();
        if wait_ms == 0 {
            self.send_benchmark_response(envoy_filter);
            return;
        }

        let scheduler = envoy_filter.new_scheduler();
        self.worker = Some(std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(wait_ms));
            scheduler.commit(BENCHMARK_EVENT_ID);
        }));
        self.waiting = true;
    }

    fn send_benchmark_response<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) {
        let Some(request) = self.benchmark.take() else {
            return;
        };
        let body = vec![b'B'; request.response_bytes];
        let content_length = body.len().to_string();
        let request_bytes = request.request_body.len().to_string();
        envoy_filter.send_response(
            200,
            &[
                ("content-type", b"application/octet-stream"),
                ("content-length", content_length.as_bytes()),
                ("x-request-bytes", request_bytes.as_bytes()),
            ],
            Some(&body),
            Some("ruvoy_baseline_benchmark"),
        );
        self.waiting = false;
        self.responded = true;
    }

    fn send_bad_request<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF, message: &[u8]) {
        envoy_filter.send_response(
            400,
            &[("content-type", b"text/plain")],
            Some(message),
            Some("ruvoy_baseline_bad_benchmark_request"),
        );
        self.responded = true;
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for BaselineFilter {
    fn on_request_headers(
        &mut self,
        envoy_filter: &mut EHF,
        end_of_stream: bool,
    ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
        let path = envoy_filter
            .get_request_header_value(":path")
            .map(|value| String::from_utf8_lossy(value.as_slice()).into_owned())
            .unwrap_or_else(|| "/".to_owned());

        if path.split('?').next() == Some("/benchmark") {
            match parse_benchmark_request(&path) {
                Ok(request) => {
                    self.benchmark = Some(request);
                    if end_of_stream {
                        self.finish_benchmark(envoy_filter);
                    }
                }
                Err(message) => self.send_bad_request(envoy_filter, message.as_bytes()),
            }
            return envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
        }

        if path == "/scheduler" {
            let scheduler = envoy_filter.new_scheduler();
            self.worker = Some(std::thread::spawn(move || {
                scheduler.commit(SCHEDULER_EVENT_ID);
            }));
        } else {
            envoy_filter.send_response(
                200,
                &[("content-type", b"text/plain"), ("x-ruvoy-mode", b"direct")],
                Some(b"rust-baseline"),
                Some("ruvoy_baseline_direct"),
            );
        }

        envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
    }

    fn on_request_body(
        &mut self,
        envoy_filter: &mut EHF,
        end_of_stream: bool,
    ) -> envoy_dynamic_module_type_on_http_filter_request_body_status {
        if self.responded || self.benchmark.is_none() {
            return envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer;
        }

        let received = envoy_filter
            .get_received_request_body()
            .map(|buffers| {
                buffers
                    .into_iter()
                    .flat_map(|buffer| buffer.as_slice().to_vec())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let request = self
            .benchmark
            .as_mut()
            .expect("benchmark request is present");
        if request.request_body.len().saturating_add(received.len()) > MAX_BODY_BYTES {
            self.send_bad_request(envoy_filter, b"request body exceeds the 2 MiB limit");
            return envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer;
        }
        request.request_body.extend_from_slice(&received);

        if end_of_stream {
            self.finish_benchmark(envoy_filter);
            envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer
        } else {
            envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationAndBuffer
        }
    }

    fn on_request_trailers(
        &mut self,
        envoy_filter: &mut EHF,
    ) -> envoy_dynamic_module_type_on_http_filter_request_trailers_status {
        self.finish_benchmark(envoy_filter);
        envoy_dynamic_module_type_on_http_filter_request_trailers_status::StopIteration
    }

    fn on_scheduled(&mut self, envoy_filter: &mut EHF, event_id: u64) {
        if event_id == BENCHMARK_EVENT_ID && self.waiting {
            self.send_benchmark_response(envoy_filter);
        } else if event_id == SCHEDULER_EVENT_ID {
            envoy_filter.send_response(
                200,
                &[
                    ("content-type", b"text/plain"),
                    ("x-ruvoy-mode", b"scheduler"),
                ],
                Some(b"rust-scheduler"),
                Some("ruvoy_baseline_scheduler"),
            );
        } else {
            envoy_filter.send_response(
                500,
                &[("content-type", b"text/plain")],
                Some(b"unexpected scheduler event"),
                Some("ruvoy_baseline_bad_event"),
            );
        }
    }
}

fn parse_benchmark_request(path: &str) -> Result<BenchmarkRequest, String> {
    let query = path.split_once('?').map(|(_, query)| query).unwrap_or("");
    let mut wait_ms = 0_u64;
    let mut response_bytes = 0_usize;
    let mut expected_request_bytes = None;

    for parameter in query.split('&').filter(|parameter| !parameter.is_empty()) {
        let (name, value) = parameter
            .split_once('=')
            .ok_or_else(|| format!("invalid benchmark parameter: {parameter}"))?;
        match name {
            "wait_ms" => {
                wait_ms = value
                    .parse()
                    .map_err(|_| format!("invalid wait_ms: {value}"))?;
            }
            "response_bytes" => {
                response_bytes = value
                    .parse()
                    .map_err(|_| format!("invalid response_bytes: {value}"))?;
            }
            "expected_request_bytes" => {
                expected_request_bytes = Some(
                    value
                        .parse()
                        .map_err(|_| format!("invalid expected_request_bytes: {value}"))?,
                );
            }
            _ => return Err(format!("unknown benchmark parameter: {name}")),
        }
    }

    if wait_ms > 1_000 {
        return Err("wait_ms exceeds the 1000 ms limit".to_owned());
    }
    if response_bytes > MAX_BODY_BYTES {
        return Err("response_bytes exceeds the 2 MiB limit".to_owned());
    }
    if expected_request_bytes.is_some_and(|value| value > MAX_BODY_BYTES) {
        return Err("expected_request_bytes exceeds the 2 MiB limit".to_owned());
    }

    Ok(BenchmarkRequest {
        wait_ms,
        response_bytes,
        expected_request_bytes,
        request_body: Vec::new(),
    })
}

impl Drop for BaselineFilter {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_request_accepts_expected_request_bytes() {
        let request = parse_benchmark_request(
            "/benchmark?wait_ms=5&response_bytes=10&expected_request_bytes=1024",
        )
        .expect("benchmark parameters should parse");

        assert_eq!(request.wait_ms, 5);
        assert_eq!(request.response_bytes, 10);
        assert_eq!(request.expected_request_bytes, Some(1024));
    }

    #[test]
    fn benchmark_request_rejects_oversized_expected_request_bytes() {
        let error = match parse_benchmark_request(&format!(
            "/benchmark?expected_request_bytes={}",
            MAX_BODY_BYTES + 1
        )) {
            Ok(_) => panic!("oversized expected request body must fail"),
            Err(error) => error,
        };

        assert_eq!(error, "expected_request_bytes exceeds the 2 MiB limit");
    }
}
