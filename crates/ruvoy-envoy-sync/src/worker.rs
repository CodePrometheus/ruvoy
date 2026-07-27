use crate::runtime::SyncRackConfig;
use abi::*;
use envoy_proxy_dynamic_modules_rust_sdk::*;
use ruvoy_poc::{BridgeError, Request, Response, RuntimeClient};
use std::sync::{Arc, Mutex};

const RESPONSE_EVENT_ID: u64 = 1;
const MAX_REQUEST_BODY_BYTES: usize = 2 * 1024 * 1024;

type ResultSlot = Arc<Mutex<Option<Result<Response, BridgeError>>>>;

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for SyncRackConfig {
    fn new_http_filter(&self, _envoy_filter: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        Box::new(SyncRackFilter {
            client: self.client(),
            runtime_thread_id: self.runtime_thread_id().to_owned(),
            worker_thread_id: format!("{:?}", std::thread::current().id()),
            request: None,
            result: Arc::new(Mutex::new(None)),
            state: FilterState::Collecting,
        })
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FilterState {
    Collecting,
    Waiting,
    Responded,
}

struct SyncRackFilter {
    client: RuntimeClient,
    runtime_thread_id: String,
    worker_thread_id: String,
    request: Option<Request>,
    result: ResultSlot,
    state: FilterState,
}

impl SyncRackFilter {
    fn copy_headers<EHF: EnvoyHttpFilter>(envoy_filter: &EHF) -> Request {
        let headers = envoy_filter
            .get_request_headers()
            .into_iter()
            .map(|(name, value)| {
                (
                    String::from_utf8_lossy(name.as_slice()).into_owned(),
                    value.as_slice().to_vec(),
                )
            })
            .collect::<Vec<_>>();

        let method = envoy_filter
            .get_request_header_value(":method")
            .map(|value| String::from_utf8_lossy(value.as_slice()).into_owned())
            .unwrap_or_else(|| "GET".to_owned());
        let path = envoy_filter
            .get_request_header_value(":path")
            .map(|value| String::from_utf8_lossy(value.as_slice()).into_owned())
            .unwrap_or_else(|| "/".to_owned());
        let force_gc = path.split('?').next() == Some("/gc");

        Request {
            method,
            path,
            body: Vec::new(),
            headers,
            force_gc,
            diagnostics: None,
        }
    }

    fn copy_received_body<EHF: EnvoyHttpFilter>(
        &mut self,
        envoy_filter: &mut EHF,
    ) -> Result<(), ()> {
        let received = envoy_filter
            .get_received_request_body()
            .map(|buffers| {
                buffers
                    .into_iter()
                    .flat_map(|buffer| buffer.as_slice().to_vec())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let request = self.request.as_mut().ok_or(())?;
        let new_size = request.body.len().saturating_add(received.len());
        if new_size > MAX_REQUEST_BODY_BYTES {
            return Err(());
        }
        request.body.extend_from_slice(&received);
        Ok(())
    }

    fn submit<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) {
        let Some(request) = self.request.take() else {
            self.send_bridge_error(
                envoy_filter,
                500,
                b"missing owned request",
                "ruvoy_sync_missing_request",
            );
            return;
        };

        let scheduler = envoy_filter.new_scheduler();
        let result_slot = Arc::clone(&self.result);
        match self.client.submit(request, move |result| {
            match result_slot.lock() {
                Ok(mut slot) => *slot = Some(result),
                Err(poisoned) => *poisoned.into_inner() = Some(result),
            }
            scheduler.commit(RESPONSE_EVENT_ID);
        }) {
            Ok(()) => self.state = FilterState::Waiting,
            Err(error) => {
                let body = error.to_string();
                self.send_bridge_error(
                    envoy_filter,
                    503,
                    body.as_bytes(),
                    "ruvoy_sync_submit_failed",
                );
            }
        }
    }

    fn send_bridge_error<EHF: EnvoyHttpFilter>(
        &mut self,
        envoy_filter: &mut EHF,
        status: u32,
        body: &[u8],
        details: &str,
    ) {
        self.state = FilterState::Responded;
        envoy_filter.send_response(
            status,
            &[("content-type", b"text/plain"), ("x-ruvoy-error", b"true")],
            Some(body),
            Some(details),
        );
    }

    fn send_rack_response<EHF: EnvoyHttpFilter>(
        &mut self,
        envoy_filter: &mut EHF,
        response: Response,
    ) {
        let mut headers = Vec::with_capacity(response.headers.len() + 2);
        headers.extend(
            response
                .headers
                .iter()
                .filter(|(name, _)| !name.starts_with(':'))
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        );
        headers.push((
            "x-ruvoy-runtime-rust-thread-id",
            self.runtime_thread_id.as_bytes(),
        ));
        headers.push((
            "x-ruvoy-worker-rust-thread-id",
            self.worker_thread_id.as_bytes(),
        ));

        self.state = FilterState::Responded;
        envoy_filter.send_response(
            u32::from(response.status),
            &headers,
            Some(&response.body),
            Some("ruvoy_sync_rack_response"),
        );
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for SyncRackFilter {
    fn on_request_headers(
        &mut self,
        envoy_filter: &mut EHF,
        end_of_stream: bool,
    ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
        self.request = Some(Self::copy_headers(envoy_filter));
        if end_of_stream {
            self.submit(envoy_filter);
        }
        envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
    }

    fn on_request_body(
        &mut self,
        envoy_filter: &mut EHF,
        end_of_stream: bool,
    ) -> envoy_dynamic_module_type_on_http_filter_request_body_status {
        if self.state != FilterState::Collecting {
            return envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer;
        }

        if self.copy_received_body(envoy_filter).is_err() {
            self.send_bridge_error(
                envoy_filter,
                413,
                b"request body exceeds 2 MiB PoC limit",
                "ruvoy_sync_body_too_large",
            );
            return envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer;
        }

        if end_of_stream {
            self.submit(envoy_filter);
            envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer
        } else {
            envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationAndBuffer
        }
    }

    fn on_request_trailers(
        &mut self,
        envoy_filter: &mut EHF,
    ) -> envoy_dynamic_module_type_on_http_filter_request_trailers_status {
        if self.state == FilterState::Collecting {
            self.submit(envoy_filter);
        }
        envoy_dynamic_module_type_on_http_filter_request_trailers_status::StopIteration
    }

    fn on_scheduled(&mut self, envoy_filter: &mut EHF, event_id: u64) {
        if event_id != RESPONSE_EVENT_ID || self.state != FilterState::Waiting {
            return;
        }

        let result = match self.result.lock() {
            Ok(mut slot) => slot.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        match result {
            Some(Ok(response)) => self.send_rack_response(envoy_filter, response),
            Some(Err(error)) => {
                let body = error.to_string();
                self.send_bridge_error(envoy_filter, 500, body.as_bytes(), "ruvoy_sync_ruby_error");
            }
            None => self.send_bridge_error(
                envoy_filter,
                500,
                b"scheduler event arrived without a result",
                "ruvoy_sync_missing_result",
            ),
        }
    }
}
