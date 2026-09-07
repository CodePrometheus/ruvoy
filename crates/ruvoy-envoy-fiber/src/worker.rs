use crate::{
    metrics::{
        Metrics, OUTCOME_COMPLETED, OUTCOME_FAILED, REJECTED_ADMISSION, REJECTED_INTERNAL,
        REJECTED_INVALID_REQUEST,
    },
    runtime::FiberRackConfig,
};
use abi::*;
use envoy_proxy_dynamic_modules_rust_sdk::*;
use ruvoy::{
    Request, RequestDiagnostics, RequestMetadata, ResponseHead, ResponseStream, StreamHandle,
    StreamItem, StreamWaker, fiber::FiberRuntimeClient,
};
use std::{sync::Arc, time::Instant};

const RESPONSE_EVENT_ID: u64 = 1;
const REQUEST_EVENT_ID: u64 = 2;

/// Caps what one body may hold in memory between Envoy and Ruby, in either
/// direction. Beyond this the producing side is asked to wait, so a body of any
/// size costs the same and the runtime's admission limit bounds the total.
const MAX_BUFFERED_BODY_BYTES: usize = 1024 * 1024;

/// What a request was missing, in the words sent back to the client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InvalidRequest(&'static str);

/// Which of Envoy's request buffers a copy reads from.
#[derive(Clone, Copy)]
enum BodySource {
    /// The frame handed to the callback that is running.
    Received,
    /// What earlier callbacks left for Envoy to hold on to.
    Retained,
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for FiberRackConfig {
    fn new_http_filter(&self, _envoy_filter: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        Box::new(FiberRackFilter {
            client: self.client(),
            diagnostics_enabled: self.diagnostics_enabled(),
            runtime_thread_id: self.runtime_thread_id().to_owned(),
            worker_thread_id: format!("{:?}", std::thread::current().id()),
            request: None,
            request_body: None,
            request_end_pending: false,
            diagnostics: None,
            stream: None,
            state: FilterState::Collecting,
            metrics: self.metrics(),
            submitted_at: None,
        })
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FilterState {
    Collecting,
    Waiting,
    /// Headers are on the wire; only body chunks may follow.
    Streaming,
    Responded,
}

struct FiberRackFilter {
    client: FiberRuntimeClient,
    diagnostics_enabled: bool,
    runtime_thread_id: String,
    worker_thread_id: String,
    request: Option<Request>,
    request_body: Option<StreamHandle>,
    /// Set once Envoy has delivered the last request byte, and cleared once
    /// that end has been passed on to the runtime behind the bytes before it.
    request_end_pending: bool,
    diagnostics: Option<RequestDiagnostics>,
    stream: Option<Arc<ResponseStream>>,
    state: FilterState,
    metrics: Option<Metrics>,
    submitted_at: Option<Instant>,
}

impl Drop for FiberRackFilter {
    fn drop(&mut self) {
        // The downstream is gone, so tell Ruby to stop enumerating the body.
        if let Some(stream) = self.stream.take() {
            stream.cancel();
        }
    }
}

impl FiberRackFilter {
    fn copy_headers<EHF: EnvoyHttpFilter>(
        &self,
        envoy_filter: &EHF,
        received_at: Instant,
    ) -> Result<Request, InvalidRequest> {
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
            .ok_or(InvalidRequest("missing :method"))?;
        let path = envoy_filter
            .get_request_header_value(":path")
            .map(|value| String::from_utf8_lossy(value.as_slice()).into_owned())
            .ok_or(InvalidRequest("missing :path"))?;
        let metadata = request_metadata(envoy_filter)?;
        let stage_timing_requested = self.diagnostics_enabled
            && envoy_filter
                .get_request_header_value("x-ruvoy-stage-timing")
                .is_some_and(|value| value.as_slice() == b"1");

        Ok(Request {
            method,
            path,
            body: Vec::new(),
            body_stream: None,
            headers,
            metadata,
            force_gc: false,
            diagnostics: stage_timing_requested.then(|| RequestDiagnostics::new(received_at)),
        })
    }

    /// Moves as much of the request body as the runtime has room for.
    ///
    /// Whatever does not fit stays where Envoy put it. Returning
    /// `StopIterationAndWatermark` makes that a watermark buffer, so filling it
    /// stops Envoy reading from the client and draining it starts the client
    /// again: a slow application slows the upload down instead of being charged
    /// for a body it has not read.
    fn forward_request_body<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) {
        let Some(handle) = self.request_body.clone() else {
            return;
        };
        let copy_started_at = Instant::now();

        // A filter ahead of us may resume by re-delivering what it buffered, in
        // which case both accessors name the same bytes.
        let received_is_retained = envoy_filter.received_buffered_request_body();
        let retained = envoy_filter.get_buffered_request_body_size();
        let received = if received_is_retained {
            0
        } else {
            envoy_filter.get_received_request_body_size()
        };

        let room = (retained + received).min(handle.stream.spare_capacity());
        // Retained bytes arrived before the frame in hand, so a short copy from
        // them must not let the newer frame overtake.
        let mut copied = copy_body(envoy_filter, BodySource::Retained, room, &handle.stream);
        if copied < room && !received_is_retained {
            copied += copy_body(
                envoy_filter,
                BodySource::Received,
                room - copied,
                &handle.stream,
            );
        }

        // The end may only follow the last byte, which Envoy still holds
        // whenever the runtime had no room for all of it.
        let ended = self.request_end_pending
            && envoy_filter.get_buffered_request_body_size() == 0
            && envoy_filter.get_received_request_body_size() == 0;
        if ended {
            handle.stream.push_end();
            self.request_end_pending = false;
        }
        if copied > 0 || ended {
            self.client.wake_body(Arc::clone(&handle.stream));
        }
        if let Some(diagnostics) = self.diagnostics.as_mut() {
            diagnostics.body_copy_time += copy_started_at.elapsed();
            diagnostics.body_callbacks += 1;
        }
    }

    fn submit<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) {
        let Some(mut request) = self.request.take() else {
            if let Some(metrics) = self.metrics {
                metrics.rejected(envoy_filter, REJECTED_INTERNAL);
            }
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
        self.diagnostics = request.diagnostics.clone();

        let scheduler = envoy_filter.new_scheduler();
        let request_scheduler = envoy_filter.new_scheduler();
        let request_stream = Arc::new(ResponseStream::new(MAX_BUFFERED_BODY_BYTES));
        let request_handle = StreamHandle::new(
            Arc::clone(&request_stream),
            Arc::new(move || request_scheduler.commit(REQUEST_EVENT_ID)),
        );
        self.request_body = Some(request_handle.clone());
        request.body_stream = Some(request_handle);
        let stream = Arc::new(ResponseStream::new(MAX_BUFFERED_BODY_BYTES));
        // The waker only signals; the worker thread does every Envoy call when
        // the scheduled event arrives.
        let waker: StreamWaker = Arc::new(move || {
            scheduler.commit(RESPONSE_EVENT_ID);
        });
        self.stream = Some(Arc::clone(&stream));

        match self
            .client
            .submit(request, StreamHandle::new(stream, waker))
        {
            Ok(()) => {
                self.state = FilterState::Waiting;
                self.submitted_at = Some(Instant::now());
                self.report_admission(envoy_filter, None);
            }
            Err(error) => {
                self.stream = None;
                let body = error.to_string();
                self.report_admission(envoy_filter, Some(REJECTED_ADMISSION));
                self.send_bridge_error(
                    envoy_filter,
                    503,
                    body.as_bytes(),
                    "ruvoy_fiber_submit_failed",
                );
            }
        }
    }

    /// Records the admission outcome and republishes how loaded the runtime is.
    fn report_admission<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF, rejected: Option<&str>) {
        let Some(metrics) = self.metrics else {
            return;
        };
        match rejected {
            Some(reason) => metrics.rejected(envoy_filter, reason),
            None => metrics.submitted(envoy_filter),
        }
        self.publish_saturation(envoy_filter, metrics);
    }

    fn publish_saturation<EHF: EnvoyHttpFilter>(&self, envoy_filter: &EHF, metrics: Metrics) {
        metrics.saturation(
            envoy_filter,
            self.client.inflight_requests(),
            self.client.reactor_idle_for(),
        );
    }

    /// Records how a request that reached the application ended.
    fn report_outcome<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &EHF, outcome: &str) {
        let (Some(metrics), Some(submitted_at)) = (self.metrics, self.submitted_at.take()) else {
            return;
        };
        metrics.responded(envoy_filter, outcome, submitted_at.elapsed());
        self.publish_saturation(envoy_filter, metrics);
    }

    fn send_bridge_error<EHF: EnvoyHttpFilter>(
        &mut self,
        envoy_filter: &mut EHF,
        status: u32,
        body: &[u8],
        details: &str,
    ) {
        // Nothing may touch this filter once the reply goes out, so everything
        // it owns is released first.
        self.request = None;
        self.request_body = None;
        self.request_end_pending = false;
        self.state = FilterState::Responded;
        envoy_filter.send_response(
            status,
            &[("content-type", b"text/plain"), ("x-ruvoy-error", b"true")],
            Some(body),
            Some(details),
        );
    }

    fn send_invalid_request<EHF: EnvoyHttpFilter>(
        &mut self,
        envoy_filter: &mut EHF,
        InvalidRequest(message): InvalidRequest,
    ) {
        if let Some(metrics) = self.metrics {
            metrics.rejected(envoy_filter, REJECTED_INVALID_REQUEST);
        }
        self.send_bridge_error(
            envoy_filter,
            400,
            message.as_bytes(),
            "ruvoy_fiber_invalid_request",
        );
    }

    fn send_response_head<EHF: EnvoyHttpFilter>(
        &mut self,
        envoy_filter: &mut EHF,
        head: ResponseHead,
    ) {
        let status = head.status.to_string();
        let mut headers = Vec::with_capacity(head.headers.len() + 3);
        headers.push((":status", status.as_bytes()));
        headers.extend(
            head.headers
                .iter()
                .filter(|(name, _)| !name.starts_with(':') && !name.starts_with("rack."))
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        );
        if self.diagnostics_enabled {
            headers.push((
                "x-ruvoy-runtime-rust-thread-id",
                self.runtime_thread_id.as_bytes(),
            ));
            headers.push((
                "x-ruvoy-worker-rust-thread-id",
                self.worker_thread_id.as_bytes(),
            ));
        }

        self.state = FilterState::Streaming;
        envoy_filter.send_response_headers(&headers, false);
    }

    /// Moves whatever the Ruby side has produced so far to the downstream.
    ///
    /// Called on the worker thread for every scheduled event, so a slow client
    /// simply leaves items queued and the producer sees the backpressure.
    fn drain_stream<EHF: EnvoyHttpFilter>(&mut self, envoy_filter: &mut EHF) {
        let Some(stream) = self.stream.clone() else {
            return;
        };

        while let Some(item) = stream.take_next() {
            match item {
                StreamItem::Head(head) => {
                    if self.state == FilterState::Waiting {
                        self.send_response_head(envoy_filter, head);
                    }
                }
                StreamItem::Chunk(chunk) => {
                    if self.state != FilterState::Streaming {
                        continue;
                    }
                    envoy_filter.send_response_data(&chunk, false);
                }
                StreamItem::End => {
                    // Ending the stream destroys this filter, so every field is
                    // settled and every metric recorded before that call.
                    let streaming = self.state == FilterState::Streaming;
                    self.state = FilterState::Responded;
                    self.stream = None;
                    self.report_outcome(envoy_filter, OUTCOME_COMPLETED);
                    if streaming {
                        envoy_filter.send_response_data(&[], true);
                    }
                    return;
                }
                StreamItem::Failed(error) => {
                    let body = error.to_string();
                    let local_reply_possible = self.state == FilterState::Waiting;
                    self.stream = None;
                    self.report_outcome(envoy_filter, OUTCOME_FAILED);
                    if local_reply_possible {
                        // Nothing is on the wire yet, so a clean local reply is
                        // still possible.
                        self.send_bridge_error(
                            envoy_filter,
                            500,
                            body.as_bytes(),
                            "ruvoy_fiber_ruby_error",
                        );
                    } else {
                        // Headers are already out; the only honest signal left
                        // is to end the stream short rather than fake success.
                        self.state = FilterState::Responded;
                        envoy_filter.send_response_data(&[], true);
                    }
                    return;
                }
            }
        }
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for FiberRackFilter {
    fn on_request_headers(
        &mut self,
        envoy_filter: &mut EHF,
        end_of_stream: bool,
    ) -> envoy_dynamic_module_type_on_http_filter_request_headers_status {
        match self.copy_headers(envoy_filter, Instant::now()) {
            Ok(request) => self.request = Some(request),
            Err(error) => {
                self.send_invalid_request(envoy_filter, error);
                return envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration;
            }
        }
        // The application starts before the body has arrived, which is what
        // lets it overlap with the upload.
        self.submit(envoy_filter);
        self.request_end_pending = end_of_stream;
        if end_of_stream {
            self.forward_request_body(envoy_filter);
        }
        envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
    }

    fn on_request_body(
        &mut self,
        envoy_filter: &mut EHF,
        end_of_stream: bool,
    ) -> envoy_dynamic_module_type_on_http_filter_request_body_status {
        self.request_end_pending |= end_of_stream;
        self.forward_request_body(envoy_filter);
        envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationAndWatermark
    }

    fn on_request_trailers(
        &mut self,
        envoy_filter: &mut EHF,
    ) -> envoy_dynamic_module_type_on_http_filter_request_trailers_status {
        self.request_end_pending = true;
        self.forward_request_body(envoy_filter);
        envoy_dynamic_module_type_on_http_filter_request_trailers_status::StopIteration
    }

    fn on_scheduled(&mut self, envoy_filter: &mut EHF, event_id: u64) {
        match event_id {
            // The runtime took bytes out of the request queue, so there is room
            // for more of what Envoy has been holding back.
            REQUEST_EVENT_ID => self.forward_request_body(envoy_filter),
            RESPONSE_EVENT_ID
                if matches!(self.state, FilterState::Waiting | FilterState::Streaming) =>
            {
                self.drain_stream(envoy_filter);
            }
            _ => {}
        }
    }

    fn on_downstream_above_write_buffer_high_watermark(&mut self, _envoy_filter: &mut EHF) {
        // Stop pulling from the queue so the Ruby producer blocks instead of
        // letting a slow client turn into unbounded memory.
        if let Some(stream) = self.stream.as_ref() {
            stream.set_paused(true);
        }
    }

    fn on_downstream_below_write_buffer_low_watermark(&mut self, envoy_filter: &mut EHF) {
        if self.stream.is_none() {
            return;
        }
        if let Some(stream) = self.stream.as_ref() {
            stream.set_paused(false);
        }
        // Writing from inside a watermark callback re-enters Envoy while it is
        // still adjusting the buffer it is reporting on. Resume on the next
        // dispatcher turn instead, where sending is a normal operation.
        envoy_filter.new_scheduler().commit(RESPONSE_EVENT_ID);
    }
}

fn request_metadata<EHF: EnvoyHttpFilter>(
    envoy_filter: &EHF,
) -> Result<RequestMetadata, InvalidRequest> {
    let scheme = attribute_string(
        envoy_filter,
        envoy_dynamic_module_type_attribute_id::RequestScheme,
    )
    .ok_or(InvalidRequest("missing request scheme"))?;
    let authority = attribute_string(
        envoy_filter,
        envoy_dynamic_module_type_attribute_id::RequestHost,
    )
    .or_else(|| {
        envoy_filter
            .get_request_header_value(":authority")
            .map(|value| String::from_utf8_lossy(value.as_slice()).into_owned())
    })
    .ok_or(InvalidRequest("missing request authority"))?;
    let protocol = attribute_string(
        envoy_filter,
        envoy_dynamic_module_type_attribute_id::RequestProtocol,
    )
    .ok_or(InvalidRequest("missing request protocol"))?;

    let destination_address = attribute_string(
        envoy_filter,
        envoy_dynamic_module_type_attribute_id::DestinationAddress,
    );
    let server_name = authority_host(&authority)
        .or_else(|| destination_address.as_deref().and_then(authority_host))
        .ok_or(InvalidRequest("invalid request authority"))?
        .to_owned();
    let server_port = envoy_filter
        .get_attribute_int(envoy_dynamic_module_type_attribute_id::DestinationPort)
        .and_then(|port| u16::try_from(port).ok())
        .or_else(|| authority_port(&authority))
        .or_else(|| default_port(&scheme))
        .ok_or(InvalidRequest("missing destination port"))?;
    let remote_addr = attribute_string(
        envoy_filter,
        envoy_dynamic_module_type_attribute_id::SourceAddress,
    )
    .and_then(|address| authority_host(&address).map(str::to_owned));

    Ok(RequestMetadata {
        authority,
        scheme,
        server_name,
        server_port,
        protocol,
        remote_addr,
    })
}

/// Moves up to `limit` bytes out of one of Envoy's request buffers.
fn copy_body<EHF: EnvoyHttpFilter>(
    envoy_filter: &mut EHF,
    source: BodySource,
    limit: usize,
    stream: &ResponseStream,
) -> usize {
    if limit == 0 {
        return 0;
    }
    let buffers = match source {
        BodySource::Received => envoy_filter.get_received_request_body(),
        BodySource::Retained => envoy_filter.get_buffered_request_body(),
    }
    .unwrap_or_default();
    let mut copied = 0;
    for buffer in &buffers {
        let slice = buffer.as_slice();
        let take = slice.len().min(limit - copied);
        if take == 0 {
            break;
        }
        stream.push_chunk(slice[..take].to_vec());
        copied += take;
    }
    drop(buffers);
    if copied > 0 {
        match source {
            BodySource::Received => envoy_filter.drain_received_request_body(copied),
            BodySource::Retained => envoy_filter.drain_buffered_request_body(copied),
        };
    }
    copied
}

fn attribute_string<EHF: EnvoyHttpFilter>(
    envoy_filter: &EHF,
    attribute: envoy_dynamic_module_type_attribute_id,
) -> Option<String> {
    envoy_filter
        .get_attribute_string(attribute)
        .map(|value| String::from_utf8_lossy(value.as_slice()).into_owned())
        .filter(|value| !value.is_empty())
}

fn authority_host(authority: &str) -> Option<&str> {
    if let Some(rest) = authority.strip_prefix('[') {
        let closing = rest.find(']')?;
        return Some(&authority[..closing + 2]);
    }

    match authority.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') && port.parse::<u16>().is_ok() => {
            (!host.is_empty()).then_some(host)
        }
        _ => (!authority.is_empty()).then_some(authority),
    }
}

fn authority_port(authority: &str) -> Option<u16> {
    if let Some(rest) = authority.strip_prefix('[') {
        let closing = rest.find(']')?;
        return rest[closing + 1..].strip_prefix(':')?.parse().ok();
    }

    let (host, port) = authority.rsplit_once(':')?;
    (!host.contains(':')).then(|| port.parse().ok()).flatten()
}

fn default_port(scheme: &str) -> Option<u16> {
    match scheme {
        "http" | "ws" => Some(80),
        "https" | "wss" => Some(443),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_is_split_without_corrupting_ipv6_hosts() {
        assert_eq!(authority_host("example.com:8443"), Some("example.com"));
        assert_eq!(authority_port("example.com:8443"), Some(8443));
        assert_eq!(authority_host("[::1]:18083"), Some("[::1]"));
        assert_eq!(authority_port("[::1]:18083"), Some(18083));
        assert_eq!(authority_host("example.com"), Some("example.com"));
        assert_eq!(authority_port("example.com"), None);
    }
}
