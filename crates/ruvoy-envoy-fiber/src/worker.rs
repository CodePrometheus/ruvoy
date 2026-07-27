use crate::runtime::{BodyBudget, BodyLease, FiberRackConfig};
use abi::*;
use envoy_proxy_dynamic_modules_rust_sdk::*;
use ruvoy_poc::{BridgeError, Request, RequestDiagnostics, Response, fiber::FiberRuntimeClient};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const RESPONSE_EVENT_ID: u64 = 1;
const MAX_REQUEST_BODY_BYTES: usize = 2 * 1024 * 1024;

struct TimedResult {
    completed_at: Instant,
    result: Result<Response, BridgeError>,
}

type ResultSlot = Arc<Mutex<Option<TimedResult>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BodyCopyError {
    Allocation,
    MissingRequest,
    Overloaded,
    TooLarge,
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for FiberRackConfig {
    fn new_http_filter(&self, _envoy_filter: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        Box::new(FiberRackFilter {
            client: self.client(),
            body_budget: self.body_budget(),
            body_lease: None,
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

struct FiberRackFilter {
    client: FiberRuntimeClient,
    body_budget: Arc<BodyBudget>,
    body_lease: Option<BodyLease>,
    runtime_thread_id: String,
    worker_thread_id: String,
    request: Option<Request>,
    result: ResultSlot,
    state: FilterState,
}

impl FiberRackFilter {
    fn copy_headers<EHF: EnvoyHttpFilter>(
        &self,
        envoy_filter: &EHF,
        received_at: Instant,
    ) -> Result<(Request, BodyLease), BodyCopyError> {
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
        let body_capacity = declared_body_capacity(
            envoy_filter
                .get_request_header_value("content-length")
                .as_ref()
                .map(|value| value.as_slice()),
        )?;
        let mut body = Vec::new();
        let body_lease = self
            .body_budget
            .try_reserve(body_capacity)
            .map_err(|_| BodyCopyError::Overloaded)?;
        body.try_reserve_exact(body_capacity)
            .map_err(|_| BodyCopyError::Allocation)?;
        let diagnostics_enabled = envoy_filter
            .get_request_header_value("x-ruvoy-stage-timing")
            .is_some_and(|value| value.as_slice() == b"1");

        Ok((
            Request {
                method,
                path,
                body,
                headers,
                force_gc,
                diagnostics: diagnostics_enabled
                    .then(|| RequestDiagnostics::new(received_at, body_capacity)),
            },
            body_lease,
        ))
    }

    fn copy_received_body<EHF: EnvoyHttpFilter>(
        &mut self,
        envoy_filter: &mut EHF,
    ) -> Result<(), BodyCopyError> {
        let buffers = envoy_filter.get_received_request_body().unwrap_or_default();
        let received_size = buffers.iter().try_fold(0usize, |size, buffer| {
            size.checked_add(buffer.as_slice().len())
                .ok_or(BodyCopyError::TooLarge)
        })?;
        let current_size = self
            .request
            .as_ref()
            .ok_or(BodyCopyError::MissingRequest)?
            .body
            .len();
        let required_size = current_size
            .checked_add(received_size)
            .ok_or(BodyCopyError::TooLarge)?;
        if required_size > MAX_REQUEST_BODY_BYTES {
            return Err(BodyCopyError::TooLarge);
        }
        self.body_lease
            .as_mut()
            .ok_or(BodyCopyError::MissingRequest)?
            .ensure_reserved(required_size)
            .map_err(|_| BodyCopyError::Overloaded)?;
        let request = self.request.as_mut().ok_or(BodyCopyError::MissingRequest)?;
        let copy_started_at = Instant::now();
        let capacity_before = request.body.capacity();
        let result = append_body_slices(
            &mut request.body,
            buffers.iter().map(|buffer| buffer.as_slice()),
        );
        if let Some(diagnostics) = request.diagnostics.as_mut() {
            diagnostics.body_copy_time += copy_started_at.elapsed();
            diagnostics.body_callbacks += 1;
            diagnostics.body_reallocations += u64::from(request.body.capacity() != capacity_before);
        }
        result
    }

    fn submit<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) {
        let Some(mut request) = self.request.take() else {
            self.send_bridge_error(
                envoy_filter,
                500,
                b"missing owned request",
                "ruvoy_fiber_missing_request",
            );
            return;
        };
        if let Some(diagnostics) = request.diagnostics.as_mut() {
            diagnostics.submitted_at = Some(Instant::now());
        }

        let scheduler = envoy_filter.new_scheduler();
        let result_slot = Arc::clone(&self.result);
        let body_lease = self.body_lease.take();
        match self.client.submit(request, move |result| {
            let result = TimedResult {
                completed_at: Instant::now(),
                result,
            };
            match result_slot.lock() {
                Ok(mut slot) => *slot = Some(result),
                Err(poisoned) => *poisoned.into_inner() = Some(result),
            }
            scheduler.commit(RESPONSE_EVENT_ID);
            drop(body_lease);
        }) {
            Ok(()) => self.state = FilterState::Waiting,
            Err(error) => {
                let body = error.to_string();
                self.send_bridge_error(
                    envoy_filter,
                    503,
                    body.as_bytes(),
                    "ruvoy_fiber_submit_failed",
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
        self.request = None;
        self.body_lease = None;
        self.state = FilterState::Responded;
        envoy_filter.send_response(
            status,
            &[("content-type", b"text/plain"), ("x-ruvoy-error", b"true")],
            Some(body),
            Some(details),
        );
    }

    fn send_body_error<EHF: EnvoyHttpFilter>(
        &mut self,
        envoy_filter: &mut EHF,
        error: BodyCopyError,
    ) {
        match error {
            BodyCopyError::TooLarge => self.send_bridge_error(
                envoy_filter,
                413,
                b"request body exceeds 2 MiB PoC limit",
                "ruvoy_fiber_body_too_large",
            ),
            BodyCopyError::Allocation => self.send_bridge_error(
                envoy_filter,
                503,
                b"request body allocation failed",
                "ruvoy_fiber_body_allocation_failed",
            ),
            BodyCopyError::Overloaded => self.send_bridge_error(
                envoy_filter,
                503,
                b"request body admission limit reached",
                "ruvoy_fiber_body_overloaded",
            ),
            BodyCopyError::MissingRequest => self.send_bridge_error(
                envoy_filter,
                500,
                b"missing owned request",
                "ruvoy_fiber_missing_request",
            ),
        }
    }

    fn send_rack_response<EHF: EnvoyHttpFilter>(
        &mut self,
        envoy_filter: &mut EHF,
        response: Response,
        scheduler_return: Option<Duration>,
    ) {
        let scheduler_return_value =
            scheduler_return.map(|duration| duration.as_nanos().to_string());
        let mut headers = Vec::with_capacity(
            response.headers.len() + 2 + usize::from(scheduler_return_value.is_some()),
        );
        headers.extend(
            response
                .headers
                .iter()
                .filter(|(name, _)| !name.starts_with(':'))
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        );
        if let Some(value) = scheduler_return_value.as_deref() {
            headers.push(("x-ruvoy-stage-scheduler-return-ns", value.as_bytes()));
        }
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
            Some("ruvoy_fiber_rack_response"),
        );
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for FiberRackFilter {
    fn on_request_headers(
        &mut self,
        envoy_filter: &mut EHF,
        end_of_stream: bool,
    ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
        match self.copy_headers(envoy_filter, Instant::now()) {
            Ok((request, body_lease)) => {
                self.request = Some(request);
                self.body_lease = Some(body_lease);
            }
            Err(error) => {
                self.send_body_error(envoy_filter, error);
                return envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
            }
        }
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

        if let Err(error) = self.copy_received_body(envoy_filter) {
            self.send_body_error(envoy_filter, error);
            return envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer;
        }

        if end_of_stream {
            self.submit(envoy_filter);
        }
        envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer
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
            Some(TimedResult {
                completed_at,
                result: Ok(response),
            }) => {
                let scheduler_return = (response.header("x-ruvoy-stage-timing") == Some("1"))
                    .then(|| completed_at.elapsed());
                self.send_rack_response(envoy_filter, response, scheduler_return);
            }
            Some(TimedResult {
                result: Err(error), ..
            }) => {
                let body = error.to_string();
                self.send_bridge_error(
                    envoy_filter,
                    500,
                    body.as_bytes(),
                    "ruvoy_fiber_ruby_error",
                );
            }
            None => self.send_bridge_error(
                envoy_filter,
                500,
                b"scheduler event arrived without a result",
                "ruvoy_fiber_missing_result",
            ),
        }
    }
}

fn declared_body_capacity(value: Option<&[u8]>) -> Result<usize, BodyCopyError> {
    let Some(value) = value else {
        return Ok(0);
    };
    let Ok(value) = std::str::from_utf8(value) else {
        return Ok(0);
    };
    let Ok(value) = value.parse::<usize>() else {
        return Ok(0);
    };
    if value > MAX_REQUEST_BODY_BYTES {
        return Err(BodyCopyError::TooLarge);
    }
    Ok(value)
}

fn append_body_slices<'a, I>(body: &mut Vec<u8>, slices: I) -> Result<(), BodyCopyError>
where
    I: IntoIterator<Item = &'a [u8]>,
    I::IntoIter: Clone,
{
    let slices = slices.into_iter();
    let received_size = slices.clone().try_fold(0usize, |size, slice| {
        size.checked_add(slice.len()).ok_or(BodyCopyError::TooLarge)
    })?;
    let new_size = body
        .len()
        .checked_add(received_size)
        .ok_or(BodyCopyError::TooLarge)?;
    if new_size > MAX_REQUEST_BODY_BYTES {
        return Err(BodyCopyError::TooLarge);
    }
    body.try_reserve_exact(received_size)
        .map_err(|_| BodyCopyError::Allocation)?;
    for slice in slices {
        body.extend_from_slice(slice);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_length_is_bounded_and_optional() {
        assert_eq!(declared_body_capacity(None), Ok(0));
        assert_eq!(declared_body_capacity(Some(b"1048576")), Ok(1_048_576));
        assert_eq!(declared_body_capacity(Some(b"chunked")), Ok(0));
        assert_eq!(
            declared_body_capacity(Some(b"2097153")),
            Err(BodyCopyError::TooLarge)
        );
    }

    #[test]
    fn body_slices_are_appended_directly_into_the_owned_request() {
        let mut body = Vec::with_capacity(11);
        append_body_slices(&mut body, [b"hello ".as_slice(), b"world".as_slice()])
            .expect("body should fit");
        assert_eq!(body, b"hello world");
        assert_eq!(body.capacity(), 11);
    }

    #[test]
    fn body_limit_applies_across_callbacks() {
        let mut body = vec![0; MAX_REQUEST_BODY_BYTES - 1];
        assert_eq!(
            append_body_slices(&mut body, [b"ab".as_slice()]),
            Err(BodyCopyError::TooLarge)
        );
        assert_eq!(body.len(), MAX_REQUEST_BODY_BYTES - 1);
    }
}
