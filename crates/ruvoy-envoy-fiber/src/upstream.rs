//! Sends the application's upstream calls and streams through Envoy and hands
//! the answers back to the fibers waiting on them.

use crate::metrics::CallOutcome;
use abi::*;
use envoy_proxy_dynamic_modules_rust_sdk::*;
use ruvoy::{
    BridgeError, ResponseHead, ResponseStream, StreamChannel, StreamItem, UpstreamRequest,
    UpstreamStreamRequest,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

/// The trace context a call carries on from the request that made it, unless
/// the application set the header itself.
pub(crate) const TRACE_HEADERS: [&str; 9] = [
    "traceparent",
    "tracestate",
    "x-request-id",
    "b3",
    "x-b3-traceid",
    "x-b3-spanid",
    "x-b3-parentspanid",
    "x-b3-sampled",
    "x-b3-flags",
];

/// A call a filter has sent and is waiting on.
pub(crate) struct PendingCall {
    pub(crate) response: Arc<ResponseStream>,
    pub(crate) cluster: String,
    sent_at: Instant,
}

impl PendingCall {
    /// How long the call has been out.
    pub(crate) fn waited(&self) -> Duration {
        self.sent_at.elapsed()
    }
}

/// Calls a filter has sent and is waiting on, by the id Envoy gave them.
pub(crate) type InFlight = HashMap<u64, PendingCall>;

/// The headers a call goes out with: the application's, then whatever of the
/// request's trace context it did not set.
fn with_trace<'a>(
    headers: &'a [(String, Vec<u8>)],
    trace: &'a [(&'static str, Vec<u8>)],
) -> Vec<(&'a str, &'a [u8])> {
    let mut fields = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_slice()))
        .collect::<Vec<_>>();
    for (name, value) in trace {
        if !headers.iter().any(|(own, _)| own == name) {
            fields.push((name, value.as_slice()));
        }
    }
    fields
}

/// Sends one call. A call Envoy refuses fails on the spot, and its stream and
/// cluster come back for the caller to wake the fiber waiting on it.
pub(crate) fn send<EHF: EnvoyHttpFilter>(
    envoy_filter: &mut EHF,
    request: UpstreamRequest,
    trace: &[(&'static str, Vec<u8>)],
    in_flight: &mut InFlight,
) -> Option<(Arc<ResponseStream>, String)> {
    let UpstreamRequest {
        cluster,
        headers,
        body,
        timeout,
        response,
    } = request;
    let headers = with_trace(&headers, trace);
    let body = (!body.is_empty()).then_some(body.as_slice());
    // Envoy reads zero as no deadline at all.
    let timeout_ms = u64::try_from(timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);

    let sent_at = Instant::now();
    let (result, callout_id) = envoy_filter.send_http_callout(&cluster, &headers, body, timeout_ms);
    let Some(refusal) = refusal(result, &cluster) else {
        in_flight.insert(
            callout_id,
            PendingCall {
                response,
                cluster,
                sent_at,
            },
        );
        return None;
    };
    response.push_failure(BridgeError::Upstream(refusal));
    Some((response, cluster))
}

/// Why Envoy turned a call or stream away, or nothing when it took it.
fn refusal(
    result: envoy_dynamic_module_type_http_callout_init_result,
    cluster: &str,
) -> Option<String> {
    match result {
        envoy_dynamic_module_type_http_callout_init_result::Success => None,
        envoy_dynamic_module_type_http_callout_init_result::ClusterNotFound => {
            Some(format!("cluster {cluster} does not exist"))
        }
        envoy_dynamic_module_type_http_callout_init_result::CannotCreateRequest => Some(format!(
            "cluster {cluster} turned the call away at once, for example because it has no \
             healthy host or a circuit breaker is open"
        )),
        envoy_dynamic_module_type_http_callout_init_result::MissingRequiredHeaders => {
            Some("the call is missing :method, :path or host".to_owned())
        }
        envoy_dynamic_module_type_http_callout_init_result::DuplicateCalloutId => {
            Some("Envoy handed out a call id that was already in use".to_owned())
        }
    }
}

/// Hands a finished call's outcome to the stream its fiber waits on, and says
/// what that outcome was.
///
/// Envoy owns the headers and body only for the duration of the callback, so
/// both are copied here.
pub(crate) fn deliver(
    response: &ResponseStream,
    result: envoy_dynamic_module_type_http_callout_result,
    headers: Option<&[(EnvoyBuffer<'_>, EnvoyBuffer<'_>)]>,
    body: Option<&[EnvoyBuffer<'_>]>,
    max_response_bytes: usize,
) -> CallOutcome {
    let outcome = match result {
        envoy_dynamic_module_type_http_callout_result::Success => answer(
            headers.unwrap_or_default(),
            body.unwrap_or_default(),
            max_response_bytes,
        ),
        envoy_dynamic_module_type_http_callout_result::Reset => Err(BridgeError::Upstream(
            "the upstream stream was reset".to_owned(),
        )),
        envoy_dynamic_module_type_http_callout_result::ExceedResponseBufferLimit => Err(
            BridgeError::UpstreamTooLarge("the response exceeded Envoy's buffer limit".to_owned()),
        ),
    };
    match outcome {
        Ok((head, body)) => {
            response.push_head(head);
            if !body.is_empty() {
                response.push_chunk(body);
            }
            response.push_end();
            CallOutcome::Completed
        }
        Err(error) => {
            let outcome = failed(&error);
            response.push_failure(error);
            outcome
        }
    }
}

/// The outcome a failure the worker found stands for.
fn failed(error: &BridgeError) -> CallOutcome {
    match error {
        BridgeError::UpstreamOverflow(_) => CallOutcome::Overflow,
        BridgeError::UpstreamTooLarge(_) => CallOutcome::TooLarge,
        _ => CallOutcome::Reset,
    }
}

fn answer(
    headers: &[(EnvoyBuffer<'_>, EnvoyBuffer<'_>)],
    body: &[EnvoyBuffer<'_>],
    max_response_bytes: usize,
) -> Result<(ResponseHead, Vec<u8>), BridgeError> {
    let size = body
        .iter()
        .map(|chunk| chunk.as_slice().len())
        .sum::<usize>();
    if size > max_response_bytes {
        return Err(BridgeError::UpstreamTooLarge(format!(
            "the {size} byte response body is over the {max_response_bytes} byte limit"
        )));
    }
    let head = head_of(headers)
        .ok_or_else(|| BridgeError::Upstream("the response carried no valid :status".to_owned()))?;
    Ok((head, concatenate(body)))
}

/// The status and fields of a response head; nothing without a valid `:status`.
fn head_of(headers: &[(EnvoyBuffer<'_>, EnvoyBuffer<'_>)]) -> Option<ResponseHead> {
    let mut status = None;
    let mut fields = Vec::with_capacity(headers.len());
    for (name, value) in headers {
        let name = String::from_utf8_lossy(name.as_slice());
        let value = String::from_utf8_lossy(value.as_slice()).into_owned();
        if name == ":status" {
            status = value.parse::<u16>().ok();
        } else if !name.starts_with(':') {
            fields.push((name.into_owned(), value));
        }
    }
    Some(ResponseHead {
        status: status?,
        headers: fields,
        ruby_thread_object_id: 0,
    })
}

fn concatenate(chunks: &[EnvoyBuffer<'_>]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(chunks.iter().map(|chunk| chunk.as_slice().len()).sum());
    for chunk in chunks {
        bytes.extend_from_slice(chunk.as_slice());
    }
    bytes
}

/// Streams a filter has started, by the handle Envoy gave them.
pub(crate) type OpenStreams = HashMap<u64, OpenStream>;

/// A stream a filter has started that Envoy has not yet reported finished.
pub(crate) struct OpenStream {
    pub(crate) channel: Arc<StreamChannel>,
    pub(crate) cluster: String,
    started_at: Instant,
    /// How the stream ended, once it has.
    outcome: Option<CallOutcome>,
    /// The request body has gone out to its end.
    request_ended: bool,
    /// The response has ended or failed; whatever else Envoy reports is dropped.
    response_ended: bool,
    /// The reset has been sent, so it is not sent again.
    reset_sent: bool,
    /// The upstream agreed to switch protocols, so the exchange is now a byte
    /// stream.
    upgraded: bool,
}

/// One thing the worker must ask of Envoy for a stream.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum StreamAction {
    Reset,
    Send(Vec<u8>, bool),
}

/// What became of a stream the worker tried to start.
pub(crate) enum Started {
    Open(u64, OpenStream),
    /// Envoy turned it away; the fiber reading it must be woken.
    Refused(Arc<StreamChannel>, String),
    /// The fiber closed it before it started.
    Skipped(String),
}

impl OpenStream {
    fn new(channel: Arc<StreamChannel>, cluster: String, request_ended: bool) -> Self {
        Self {
            channel,
            cluster,
            started_at: Instant::now(),
            outcome: None,
            request_ended,
            response_ended: false,
            reset_sent: false,
            upgraded: false,
        }
    }

    /// What to ask of Envoy next: a reset the fiber or the worker wants, or the
    /// next piece of the request body. Taken one at a time, because Envoy may
    /// report the stream's end from inside the call that carries it out.
    pub(crate) fn next_action(&mut self) -> Option<StreamAction> {
        if self.channel.wants_reset() {
            if self.reset_sent {
                return None;
            }
            self.reset_sent = true;
            return Some(StreamAction::Reset);
        }
        if self.request_ended {
            return None;
        }
        match self.channel.request.take_next()? {
            StreamItem::Chunk(chunk) => Some(StreamAction::Send(chunk, false)),
            StreamItem::End => {
                self.request_ended = true;
                Some(StreamAction::Send(Vec::new(), true))
            }
            _ => None,
        }
    }

    /// The response head has arrived; how long it took when it is usable.
    pub(crate) fn headers_arrived(
        &mut self,
        headers: &[(EnvoyBuffer<'_>, EnvoyBuffer<'_>)],
        end_stream: bool,
    ) -> Option<Duration> {
        if self.response_ended {
            return None;
        }
        let Some(head) = head_of(headers) else {
            self.fail(BridgeError::Upstream(
                "the response carried no valid :status".to_owned(),
            ));
            return None;
        };
        self.upgraded = head.status == 101;
        self.channel.response.push_head(head);
        if end_stream {
            self.end();
        }
        Some(self.started_at.elapsed())
    }

    /// How the stream ended.
    pub(crate) fn outcome(&self) -> CallOutcome {
        self.outcome.unwrap_or(CallOutcome::Abandoned)
    }

    /// The fiber stopped the stream: it gave up, or the upstream went quiet
    /// for too long. Ending an upgraded session is how it completes.
    fn stopped_by_fiber(&self) -> CallOutcome {
        if self.channel.timed_out() {
            CallOutcome::TimedOut
        } else if self.upgraded {
            CallOutcome::Completed
        } else {
            CallOutcome::Abandoned
        }
    }

    /// A piece of the body has arrived. True when it did not fit: the stream
    /// has failed and the worker must reset it.
    pub(crate) fn data_arrived(&mut self, data: &[EnvoyBuffer<'_>], end_stream: bool) -> bool {
        if self.response_ended {
            return false;
        }
        let bytes = concatenate(data);
        if !bytes.is_empty() {
            match self.channel.response.push_chunk_if_room(bytes) {
                Some(true) => {}
                Some(false) => {
                    self.fail(BridgeError::UpstreamOverflow(
                        "the upstream sent faster than the application read, past the stream's \
                         buffer"
                            .to_owned(),
                    ));
                    return true;
                }
                // The fiber closed the stream, and its reset is already wanted.
                None => {
                    self.response_ended = true;
                    self.outcome.get_or_insert(self.stopped_by_fiber());
                    return false;
                }
            }
        }
        if end_stream {
            self.end();
        }
        false
    }

    /// The response has ended with trailers.
    pub(crate) fn trailers_arrived(&mut self, trailers: &[(EnvoyBuffer<'_>, EnvoyBuffer<'_>)]) {
        if self.response_ended {
            return;
        }
        self.channel.set_trailers(
            trailers
                .iter()
                .map(|(name, value)| {
                    (
                        String::from_utf8_lossy(name.as_slice()).into_owned(),
                        String::from_utf8_lossy(value.as_slice()).into_owned(),
                    )
                })
                .collect(),
        );
        self.end();
    }

    /// Envoy reports the stream finished.
    pub(crate) fn completed(&mut self) {
        if !self.response_ended {
            self.end();
        }
        self.gone();
    }

    /// Envoy reports the stream reset.
    pub(crate) fn reset_by_envoy(
        &mut self,
        reason: envoy_dynamic_module_type_http_stream_reset_reason,
    ) {
        if self.upgraded && !self.reset_sent {
            // A byte stream ends when the upstream closes it, which is how Envoy
            // reports it, and the protocol on top judges whether that was early.
            self.completed();
            return;
        }
        if self.reset_sent && self.outcome.is_none() {
            self.outcome = Some(self.stopped_by_fiber());
        }
        if reason == envoy_dynamic_module_type_http_stream_reset_reason::Overflow {
            self.outcome.get_or_insert(CallOutcome::Refused);
        }
        if !self.response_ended {
            self.fail(BridgeError::Upstream(reset_reason(reason).to_owned()));
        }
        self.gone();
    }

    /// The request is over before the stream; whoever reads it hears so.
    pub(crate) fn abandon(&mut self) {
        // An upgraded session whose client went away has simply ended.
        self.outcome.get_or_insert(if self.upgraded {
            CallOutcome::Completed
        } else {
            CallOutcome::Abandoned
        });
        if !self.response_ended {
            self.fail(BridgeError::Upstream(
                "the request finished before the stream did".to_owned(),
            ));
        }
        self.gone();
    }

    /// The stream is out of Envoy's hands, so no more of the body can go up.
    fn gone(&self) {
        self.channel.request.cancel();
    }

    fn end(&mut self) {
        self.response_ended = true;
        self.outcome.get_or_insert(CallOutcome::Completed);
        self.channel.response.push_end();
    }

    fn fail(&mut self, error: BridgeError) {
        self.response_ended = true;
        self.outcome.get_or_insert(failed(&error));
        self.channel.fail(error);
    }
}

/// Starts one stream. Nothing of the filter's own state is held while Envoy
/// works, since Envoy may call back into the filter before this returns.
pub(crate) fn start<EHF: EnvoyHttpFilter>(
    envoy_filter: &mut EHF,
    request: UpstreamStreamRequest,
    trace: &[(&'static str, Vec<u8>)],
) -> Started {
    let UpstreamStreamRequest {
        cluster,
        headers,
        timeout,
        channel,
    } = request;
    if channel.wants_reset() {
        return Started::Skipped(cluster);
    }
    // A body the fiber finished before the stream started goes out with the
    // headers, so a stream without one is never sent as chunked.
    let first = channel.request.take_next();
    let (body, request_ended) = match &first {
        Some(StreamItem::End) => (None, true),
        Some(StreamItem::Chunk(chunk)) => (Some(chunk.as_slice()), false),
        _ => (None, false),
    };
    let headers = with_trace(&headers, trace);
    // Envoy reads zero as no deadline at all, which a sub-millisecond one must
    // not become.
    let timeout_ms = if timeout.is_zero() {
        0
    } else {
        u64::try_from(timeout.as_millis())
            .unwrap_or(u64::MAX)
            .max(1)
    };
    let (result, handle) =
        envoy_filter.start_http_stream(&cluster, &headers, body, request_ended, timeout_ms);
    if let Some(refusal) = refusal(result, &cluster) {
        channel.fail(BridgeError::Upstream(refusal));
        return Started::Refused(channel, cluster);
    }
    Started::Open(handle, OpenStream::new(channel, cluster, request_ended))
}

fn reset_reason(reason: envoy_dynamic_module_type_http_stream_reset_reason) -> &'static str {
    match reason {
        envoy_dynamic_module_type_http_stream_reset_reason::ConnectionFailure => {
            "the upstream could not be reached"
        }
        envoy_dynamic_module_type_http_stream_reset_reason::ConnectionTermination => {
            "the upstream connection closed"
        }
        envoy_dynamic_module_type_http_stream_reset_reason::LocalReset => {
            "the stream was reset locally"
        }
        envoy_dynamic_module_type_http_stream_reset_reason::LocalRefusedStreamReset => {
            "the stream was refused locally"
        }
        envoy_dynamic_module_type_http_stream_reset_reason::Overflow => {
            "Envoy turned the stream away, for example because a circuit breaker is open"
        }
        envoy_dynamic_module_type_http_stream_reset_reason::RemoteReset => {
            "the upstream reset the stream"
        }
        envoy_dynamic_module_type_http_stream_reset_reason::RemoteRefusedStreamReset => {
            "the upstream refused the stream"
        }
        envoy_dynamic_module_type_http_stream_reset_reason::ProtocolError => {
            "the upstream broke the HTTP protocol"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    type Outcome = envoy_dynamic_module_type_http_callout_result;

    fn request(body: &[u8], timeout: Duration) -> UpstreamRequest {
        UpstreamRequest {
            cluster: "users".to_owned(),
            headers: vec![
                (":method".to_owned(), b"GET".to_vec()),
                (":path".to_owned(), b"/users/42".to_vec()),
                ("host".to_owned(), b"users".to_vec()),
            ],
            body: body.to_vec(),
            timeout,
            response: Arc::new(ResponseStream::new(64)),
        }
    }

    fn items(stream: &ResponseStream) -> Vec<StreamItem> {
        std::iter::from_fn(|| stream.take_next()).collect()
    }

    #[test]
    fn a_sent_call_waits_under_the_id_envoy_gave_it() {
        let mut envoy_filter = MockEnvoyHttpFilter::default();
        envoy_filter
            .expect_send_http_callout()
            .returning(|cluster, headers, body, timeout| {
                assert_eq!(cluster, "users");
                assert_eq!(headers[1], (":path", b"/users/42".as_slice()));
                assert_eq!(body, None, "an empty body is sent as none");
                assert_eq!(
                    timeout, 1,
                    "a sub-millisecond deadline must not become none"
                );
                (
                    envoy_dynamic_module_type_http_callout_init_result::Success,
                    7,
                )
            });
        let mut in_flight = InFlight::default();

        let refused = send(
            &mut envoy_filter,
            request(b"", Duration::from_micros(10)),
            &[],
            &mut in_flight,
        );

        assert!(refused.is_none());
        assert!(in_flight.contains_key(&7));
    }

    #[test]
    fn a_call_envoy_refuses_fails_on_the_spot() {
        let mut envoy_filter = MockEnvoyHttpFilter::default();
        envoy_filter
            .expect_send_http_callout()
            .returning(|_, _, body, _| {
                assert_eq!(body, Some(b"payload".as_slice()));
                (
                    envoy_dynamic_module_type_http_callout_init_result::ClusterNotFound,
                    0,
                )
            });
        let mut in_flight = InFlight::default();

        let (refused, cluster) = send(
            &mut envoy_filter,
            request(b"payload", Duration::from_secs(1)),
            &[],
            &mut in_flight,
        )
        .expect("a refused call comes back to be woken");

        assert_eq!(cluster, "users");
        assert!(in_flight.is_empty());
        match items(&refused).as_slice() {
            [StreamItem::Failed(BridgeError::Upstream(reason))] => {
                assert!(reason.contains("does not exist"), "{reason}");
            }
            other => panic!("expected one failure, got {other:?}"),
        }
    }

    #[test]
    fn the_request_trace_goes_along_unless_the_application_set_it() {
        let mut envoy_filter = MockEnvoyHttpFilter::default();
        envoy_filter
            .expect_send_http_callout()
            .returning(|_, headers, _, _| {
                assert!(headers.contains(&("traceparent", b"00-mine-01".as_slice())));
                assert!(!headers.contains(&("traceparent", b"00-request-01".as_slice())));
                assert!(headers.contains(&("x-request-id", b"r-1".as_slice())));
                (
                    envoy_dynamic_module_type_http_callout_init_result::Success,
                    7,
                )
            });
        let mut own = request(b"", Duration::from_secs(1));
        own.headers
            .push(("traceparent".to_owned(), b"00-mine-01".to_vec()));
        let trace = [
            ("traceparent", b"00-request-01".to_vec()),
            ("x-request-id", b"r-1".to_vec()),
        ];

        assert!(send(&mut envoy_filter, own, &trace, &mut InFlight::default()).is_none());
    }

    #[test]
    fn a_response_arrives_as_its_head_body_and_end() {
        let stream = ResponseStream::new(64);
        let headers = [
            (EnvoyBuffer::new(b":status"), EnvoyBuffer::new(b"201")),
            (EnvoyBuffer::new(b"set-cookie"), EnvoyBuffer::new(b"a=1")),
            (EnvoyBuffer::new(b"set-cookie"), EnvoyBuffer::new(b"b=2")),
        ];
        let body = [EnvoyBuffer::new(b"hel"), EnvoyBuffer::new(b"lo")];

        let outcome = deliver(
            &stream,
            envoy_dynamic_module_type_http_callout_result::Success,
            Some(&headers),
            Some(&body),
            64,
        );

        assert_eq!(outcome, CallOutcome::Completed);
        match items(&stream).as_slice() {
            [
                StreamItem::Head(head),
                StreamItem::Chunk(chunk),
                StreamItem::End,
            ] => {
                assert_eq!(head.status, 201);
                assert_eq!(
                    head.headers,
                    vec![
                        ("set-cookie".to_owned(), "a=1".to_owned()),
                        ("set-cookie".to_owned(), "b=2".to_owned()),
                    ]
                );
                assert_eq!(chunk, b"hello");
            }
            other => panic!("expected head, body and end, got {other:?}"),
        }
    }

    /// Why delivering a five byte body under `limit` failed, and the outcome
    /// it was counted as.
    fn failure(
        result: Outcome,
        headers: &[(EnvoyBuffer<'_>, EnvoyBuffer<'_>)],
        limit: usize,
    ) -> (String, CallOutcome) {
        let stream = ResponseStream::new(64);
        let body = [EnvoyBuffer::new(b"12345")];
        let outcome = deliver(&stream, result, Some(headers), Some(&body), limit);
        match items(&stream).as_slice() {
            [
                StreamItem::Failed(
                    BridgeError::Upstream(reason) | BridgeError::UpstreamTooLarge(reason),
                ),
            ] => (reason.clone(), outcome),
            other => panic!("expected one failure, got {other:?}"),
        }
    }

    #[test]
    fn every_unusable_outcome_arrives_as_a_failure() {
        let status = [(EnvoyBuffer::new(b":status"), EnvoyBuffer::new(b"200"))];
        let no_status = [(EnvoyBuffer::new(b"x-a"), EnvoyBuffer::new(b"1"))];

        let cases = [
            (
                failure(Outcome::Success, &status, 4),
                "limit",
                CallOutcome::TooLarge,
            ),
            (
                failure(Outcome::Success, &no_status, 64),
                ":status",
                CallOutcome::Reset,
            ),
            (
                failure(Outcome::Reset, &[], 64),
                "reset",
                CallOutcome::Reset,
            ),
            (
                failure(Outcome::ExceedResponseBufferLimit, &[], 64),
                "buffer limit",
                CallOutcome::TooLarge,
            ),
        ];
        for ((reason, outcome), expected, expected_outcome) in cases {
            assert!(reason.contains(expected), "{reason}");
            assert_eq!(outcome, expected_outcome, "{reason}");
        }
    }

    fn stream_request(channel: &Arc<StreamChannel>) -> UpstreamStreamRequest {
        UpstreamStreamRequest {
            cluster: "users".to_owned(),
            headers: vec![
                (":method".to_owned(), b"GET".to_vec()),
                (":path".to_owned(), b"/feed".to_vec()),
                ("host".to_owned(), b"users".to_vec()),
            ],
            timeout: Duration::ZERO,
            channel: Arc::clone(channel),
        }
    }

    fn status(code: &'static [u8]) -> [(EnvoyBuffer<'static>, EnvoyBuffer<'static>); 1] {
        [(EnvoyBuffer::new(b":status"), EnvoyBuffer::new(code))]
    }

    #[test]
    fn a_stream_without_a_body_starts_ended_and_without_a_deadline() {
        let channel = Arc::new(StreamChannel::new(64));
        channel.request.push_end();
        let mut envoy_filter = MockEnvoyHttpFilter::default();
        envoy_filter.expect_start_http_stream().returning(
            |cluster, _, body, end_stream, timeout_ms| {
                assert_eq!(cluster, "users");
                assert!(body.is_none());
                assert!(
                    end_stream,
                    "a stream without a body must not go out as chunked"
                );
                assert_eq!(timeout_ms, 0, "Envoy reads zero as no deadline");
                (
                    envoy_dynamic_module_type_http_callout_init_result::Success,
                    9,
                )
            },
        );

        let Started::Open(handle, mut stream) =
            start(&mut envoy_filter, stream_request(&channel), &[])
        else {
            panic!("the stream should open");
        };
        assert_eq!(handle, 9);
        assert_eq!(stream.next_action(), None, "nothing is left to send");
    }

    #[test]
    fn a_sub_millisecond_deadline_is_kept() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut envoy_filter = MockEnvoyHttpFilter::default();
        envoy_filter
            .expect_start_http_stream()
            .returning(|_, _, _, _, timeout_ms| {
                assert_eq!(
                    timeout_ms, 1,
                    "a sub-millisecond deadline must not become none"
                );
                (
                    envoy_dynamic_module_type_http_callout_init_result::Success,
                    9,
                )
            });
        let request = UpstreamStreamRequest {
            timeout: Duration::from_micros(400),
            ..stream_request(&channel)
        };

        assert!(matches!(
            start(&mut envoy_filter, request, &[]),
            Started::Open(9, _)
        ));
    }

    #[test]
    fn a_stream_envoy_refuses_fails_on_the_spot() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut envoy_filter = MockEnvoyHttpFilter::default();
        envoy_filter
            .expect_start_http_stream()
            .returning(|_, _, _, _, _| {
                (
                    envoy_dynamic_module_type_http_callout_init_result::ClusterNotFound,
                    0,
                )
            });

        assert!(matches!(
            start(&mut envoy_filter, stream_request(&channel), &[]),
            Started::Refused(_, _)
        ));
        match items(&channel.response).as_slice() {
            [StreamItem::Failed(BridgeError::Upstream(reason))] => {
                assert!(reason.contains("does not exist"), "{reason}");
            }
            other => panic!("expected one failure, got {other:?}"),
        }
    }

    #[test]
    fn a_stream_the_fiber_closed_before_it_started_never_starts() {
        let channel = Arc::new(StreamChannel::new(64));
        channel.close();
        // No expectation is set, so a call to Envoy would fail the test.
        let mut envoy_filter = MockEnvoyHttpFilter::default();

        assert!(matches!(
            start(&mut envoy_filter, stream_request(&channel), &[]),
            Started::Skipped(_)
        ));
    }

    #[test]
    fn the_body_goes_out_a_piece_at_a_time_and_then_ends() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), false);

        assert_eq!(channel.request.push_chunk_within_budget(b"ab"), Some(2));
        assert_eq!(
            stream.next_action(),
            Some(StreamAction::Send(b"ab".to_vec(), false))
        );
        assert_eq!(
            stream.next_action(),
            None,
            "nothing more until the fiber writes"
        );
        channel.request.push_end();
        assert_eq!(
            stream.next_action(),
            Some(StreamAction::Send(Vec::new(), true))
        );
        assert_eq!(stream.next_action(), None);
    }

    #[test]
    fn a_wanted_reset_is_asked_for_once() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), false);
        channel.close();

        assert_eq!(stream.next_action(), Some(StreamAction::Reset));
        assert_eq!(stream.next_action(), None);
    }

    #[test]
    fn the_response_arrives_as_its_head_its_chunks_and_its_end() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), true);
        let headers = [
            (EnvoyBuffer::new(b":status"), EnvoyBuffer::new(b"200")),
            (
                EnvoyBuffer::new(b"content-type"),
                EnvoyBuffer::new(b"text/event-stream"),
            ),
        ];

        assert!(stream.headers_arrived(&headers, false).is_some());
        assert!(!stream.data_arrived(&[EnvoyBuffer::new(b"data: 1\n\n")], false));
        assert!(!stream.data_arrived(&[EnvoyBuffer::new(b"data: 2\n\n")], true));
        stream.completed();
        assert_eq!(stream.outcome(), CallOutcome::Completed);

        match items(&channel.response).as_slice() {
            [
                StreamItem::Head(head),
                StreamItem::Chunk(first),
                StreamItem::Chunk(second),
                StreamItem::End,
            ] => {
                assert_eq!(head.status, 200);
                assert_eq!(
                    head.headers,
                    vec![("content-type".to_owned(), "text/event-stream".to_owned())]
                );
                assert_eq!(first, b"data: 1\n\n");
                assert_eq!(second, b"data: 2\n\n");
            }
            other => panic!("expected head, two chunks and one end, got {other:?}"),
        }
    }

    #[test]
    fn a_body_still_going_up_when_the_stream_ends_is_stopped() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), false);
        assert_eq!(channel.request.push_chunk_within_budget(b"part"), Some(4));

        stream.headers_arrived(&status(b"200"), true);
        stream.completed();

        assert!(channel.request.is_cancelled());
        assert_eq!(channel.request.push_chunk_within_budget(b"rest"), None);
        assert!(matches!(
            items(&channel.response).as_slice(),
            [StreamItem::Head(_), StreamItem::End]
        ));
    }

    #[test]
    fn a_delivery_past_the_buffer_fails_the_stream_and_wants_a_reset() {
        let channel = Arc::new(StreamChannel::new(4));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), true);
        stream.headers_arrived(&status(b"200"), false);

        assert!(!stream.data_arrived(&[EnvoyBuffer::new(b"abc")], false));
        assert!(
            stream.data_arrived(&[EnvoyBuffer::new(b"de")], false),
            "3 + 2 is over a budget of 4"
        );
        assert!(channel.wants_reset());
        assert!(
            !stream.data_arrived(&[EnvoyBuffer::new(b"f")], false),
            "a failed stream drops the rest"
        );
        let items = items(&channel.response);
        assert!(
            matches!(
                items.last(),
                Some(StreamItem::Failed(BridgeError::UpstreamOverflow(_)))
            ),
            "{items:?}"
        );
        assert_eq!(stream.next_action(), Some(StreamAction::Reset));
        stream.reset_by_envoy(envoy_dynamic_module_type_http_stream_reset_reason::LocalReset);
        assert_eq!(stream.outcome(), CallOutcome::Overflow);
    }

    #[test]
    fn a_reset_from_envoy_arrives_as_a_failure_naming_its_reason() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), true);
        stream
            .reset_by_envoy(envoy_dynamic_module_type_http_stream_reset_reason::ConnectionFailure);
        assert_eq!(stream.outcome(), CallOutcome::Reset);

        match items(&channel.response).as_slice() {
            [StreamItem::Failed(BridgeError::Upstream(reason))] => {
                assert!(reason.contains("could not be reached"), "{reason}");
            }
            other => panic!("expected one failure, got {other:?}"),
        }
    }

    #[test]
    fn an_upgraded_stream_the_upstream_closes_ends_after_what_it_sent() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), false);
        stream.headers_arrived(&status(b"101"), false);
        assert!(!stream.data_arrived(&[EnvoyBuffer::new(b"bye")], false));
        stream.reset_by_envoy(envoy_dynamic_module_type_http_stream_reset_reason::LocalReset);

        assert!(matches!(
            items(&channel.response).as_slice(),
            [StreamItem::Head(_), StreamItem::Chunk(bytes), StreamItem::End] if bytes == b"bye"
        ));
        assert!(channel.request.is_cancelled(), "nothing more can go up");
        assert_eq!(stream.outcome(), CallOutcome::Completed);
    }

    #[test]
    fn an_upgraded_stream_the_worker_reset_still_fails() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), false);
        stream.headers_arrived(&status(b"101"), false);
        channel.close();
        assert_eq!(stream.next_action(), Some(StreamAction::Reset));
        stream.reset_by_envoy(envoy_dynamic_module_type_http_stream_reset_reason::LocalReset);

        assert!(channel.response.is_cancelled());
        assert_eq!(
            stream.outcome(),
            CallOutcome::Completed,
            "ending a session is how it completes"
        );
    }

    #[test]
    fn trailers_end_the_response_and_are_kept() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), true);
        stream.headers_arrived(&status(b"200"), false);
        stream.trailers_arrived(&[(EnvoyBuffer::new(b"grpc-status"), EnvoyBuffer::new(b"0"))]);

        assert_eq!(
            channel.trailers(),
            Some(vec![("grpc-status".to_owned(), "0".to_owned())])
        );
        assert!(matches!(
            items(&channel.response).last(),
            Some(StreamItem::End)
        ));
    }

    #[test]
    fn a_request_that_finishes_first_fails_its_streams() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), true);
        stream.abandon();

        match items(&channel.response).as_slice() {
            [StreamItem::Failed(BridgeError::Upstream(reason))] => {
                assert!(reason.contains("finished before"), "{reason}");
            }
            other => panic!("expected one failure, got {other:?}"),
        }
        assert_eq!(stream.outcome(), CallOutcome::Abandoned);
    }

    /// The outcome of a stream that got its head, after the fiber `stops` it
    /// and Envoy reports the reset that follows.
    fn stopped(stops: impl FnOnce(&StreamChannel), status_code: &'static [u8]) -> CallOutcome {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), true);
        stream.headers_arrived(&status(status_code), false);
        stops(&channel);
        assert_eq!(stream.next_action(), Some(StreamAction::Reset));
        stream.reset_by_envoy(envoy_dynamic_module_type_http_stream_reset_reason::LocalReset);
        stream.outcome()
    }

    #[test]
    fn a_stream_the_fiber_stops_is_abandoned_or_timed_out() {
        assert_eq!(
            stopped(StreamChannel::close, b"200"),
            CallOutcome::Abandoned
        );
        assert_eq!(
            stopped(StreamChannel::time_out, b"200"),
            CallOutcome::TimedOut
        );
        assert_eq!(
            stopped(StreamChannel::time_out, b"101"),
            CallOutcome::TimedOut
        );
    }

    #[test]
    fn a_stream_envoy_turns_away_after_starting_it_is_refused() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), true);
        stream.reset_by_envoy(envoy_dynamic_module_type_http_stream_reset_reason::Overflow);

        assert_eq!(stream.outcome(), CallOutcome::Refused);
    }

    #[test]
    fn an_upgraded_session_whose_request_finished_has_completed() {
        let channel = Arc::new(StreamChannel::new(64));
        let mut stream = OpenStream::new(Arc::clone(&channel), "users".to_owned(), false);
        stream.headers_arrived(&status(b"101"), false);
        stream.abandon();

        assert_eq!(stream.outcome(), CallOutcome::Completed);
    }
}
