//! Sends the application's upstream calls through Envoy and hands the answers
//! back to the fibers waiting on them.

use abi::*;
use envoy_proxy_dynamic_modules_rust_sdk::*;
use ruvoy::{BridgeError, ResponseHead, ResponseStream, UpstreamRequest};
use std::{collections::HashMap, sync::Arc};

/// Calls a filter has sent and is waiting on, by the id Envoy gave them.
pub(crate) type InFlight = HashMap<u64, Arc<ResponseStream>>;

/// Sends one call. A call Envoy refuses fails on the spot, and its stream comes
/// back for the caller to wake the fiber waiting on it.
pub(crate) fn send<EHF: EnvoyHttpFilter>(
    envoy_filter: &mut EHF,
    request: UpstreamRequest,
    in_flight: &mut InFlight,
) -> Option<Arc<ResponseStream>> {
    let UpstreamRequest {
        cluster,
        headers,
        body,
        timeout,
        response,
    } = request;
    let headers = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_slice()))
        .collect::<Vec<_>>();
    let body = (!body.is_empty()).then_some(body.as_slice());
    // Envoy reads zero as no deadline at all.
    let timeout_ms = u64::try_from(timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);

    let (result, callout_id) = envoy_filter.send_http_callout(&cluster, &headers, body, timeout_ms);
    let refusal = match result {
        envoy_dynamic_module_type_http_callout_init_result::Success => {
            in_flight.insert(callout_id, response);
            return None;
        }
        envoy_dynamic_module_type_http_callout_init_result::ClusterNotFound => {
            format!("cluster {cluster} does not exist")
        }
        envoy_dynamic_module_type_http_callout_init_result::CannotCreateRequest => {
            format!(
                "cluster {cluster} turned the call away at once, for example because it has no \
                 healthy host or a circuit breaker is open"
            )
        }
        envoy_dynamic_module_type_http_callout_init_result::MissingRequiredHeaders => {
            "the call is missing :method, :path or host".to_owned()
        }
        envoy_dynamic_module_type_http_callout_init_result::DuplicateCalloutId => {
            "Envoy handed out a call id that was already in use".to_owned()
        }
    };
    response.push_failure(BridgeError::Upstream(refusal));
    Some(response)
}

/// Hands a finished call's outcome to the stream its fiber waits on.
///
/// Envoy owns the headers and body only for the duration of the callback, so
/// both are copied here.
pub(crate) fn deliver(
    response: &ResponseStream,
    result: envoy_dynamic_module_type_http_callout_result,
    headers: Option<&[(EnvoyBuffer<'_>, EnvoyBuffer<'_>)]>,
    body: Option<&[EnvoyBuffer<'_>]>,
    max_response_bytes: usize,
) {
    let outcome = match result {
        envoy_dynamic_module_type_http_callout_result::Success => answer(
            headers.unwrap_or_default(),
            body.unwrap_or_default(),
            max_response_bytes,
        ),
        envoy_dynamic_module_type_http_callout_result::Reset => {
            Err("the upstream stream was reset".to_owned())
        }
        envoy_dynamic_module_type_http_callout_result::ExceedResponseBufferLimit => {
            Err("the response exceeded Envoy's buffer limit".to_owned())
        }
    };
    match outcome {
        Ok((head, body)) => {
            response.push_head(head);
            if !body.is_empty() {
                response.push_chunk(body);
            }
            response.push_end();
        }
        Err(reason) => {
            response.push_failure(BridgeError::Upstream(reason));
        }
    }
}

fn answer(
    headers: &[(EnvoyBuffer<'_>, EnvoyBuffer<'_>)],
    body: &[EnvoyBuffer<'_>],
    max_response_bytes: usize,
) -> Result<(ResponseHead, Vec<u8>), String> {
    let size = body
        .iter()
        .map(|chunk| chunk.as_slice().len())
        .sum::<usize>();
    if size > max_response_bytes {
        return Err(format!(
            "the {size} byte response body is over the {max_response_bytes} byte limit"
        ));
    }

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
    let status = status.ok_or_else(|| "the response carried no valid :status".to_owned())?;

    let mut bytes = Vec::with_capacity(size);
    for chunk in body {
        bytes.extend_from_slice(chunk.as_slice());
    }
    Ok((
        ResponseHead {
            status,
            headers: fields,
            ruby_thread_object_id: 0,
        },
        bytes,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruvoy::StreamItem;
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

        let refused = send(
            &mut envoy_filter,
            request(b"payload", Duration::from_secs(1)),
            &mut in_flight,
        )
        .expect("a refused call comes back to be woken");

        assert!(in_flight.is_empty());
        match items(&refused).as_slice() {
            [StreamItem::Failed(BridgeError::Upstream(reason))] => {
                assert!(reason.contains("does not exist"), "{reason}");
            }
            other => panic!("expected one failure, got {other:?}"),
        }
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

        deliver(
            &stream,
            envoy_dynamic_module_type_http_callout_result::Success,
            Some(&headers),
            Some(&body),
            64,
        );

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

    /// Why delivering a five byte body under `limit` failed.
    fn failure(
        result: Outcome,
        headers: &[(EnvoyBuffer<'_>, EnvoyBuffer<'_>)],
        limit: usize,
    ) -> String {
        let stream = ResponseStream::new(64);
        let body = [EnvoyBuffer::new(b"12345")];
        deliver(&stream, result, Some(headers), Some(&body), limit);
        match items(&stream).as_slice() {
            [StreamItem::Failed(BridgeError::Upstream(reason))] => reason.clone(),
            other => panic!("expected one failure, got {other:?}"),
        }
    }

    #[test]
    fn every_unusable_outcome_arrives_as_a_failure() {
        let status = [(EnvoyBuffer::new(b":status"), EnvoyBuffer::new(b"200"))];
        let no_status = [(EnvoyBuffer::new(b"x-a"), EnvoyBuffer::new(b"1"))];

        assert!(failure(Outcome::Success, &status, 4).contains("limit"));
        assert!(failure(Outcome::Success, &no_status, 64).contains(":status"));
        assert!(failure(Outcome::Reset, &[], 64).contains("reset"));
        assert!(failure(Outcome::ExceedResponseBufferLimit, &[], 64).contains("buffer limit"));
    }
}
