//! Translation between owned Rust requests and the Rack protocol.

use crate::{
    BridgeError, Request, RequestDiagnostics, Response, ResponseHead, StreamHandle,
    chunks::ChunkSource,
    error::ruby_error,
    extensions::{ContextObject, UpstreamObject},
};
use magnus::{
    IntoValue, RArray, RClass, RHash, RString, Ruby, Value, gc, prelude::*, r_hash::ForEach,
    value::Opaque,
};
use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{Duration, Instant},
};

/// Collects a Rack body into one buffer; used by the serial runtime.
pub(crate) const BODY_READER_SOURCE: &str = include_str!("../ruby/rack_body_reader.rb");

/// Feeds a Rack body into a stream sink; used by the fiber runtime.
pub(crate) const STREAMING_BODY_READER_SOURCE: &str =
    include_str!("../ruby/rack_streaming_body_reader.rb");

/// Whether the runtime may re-enter `app.call` before an earlier call returns.
///
/// The fiber runtime does, whenever a call suspends on scheduler-aware I/O, so
/// `rack.multithread` must not promise the application exclusive access.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Concurrency {
    Serial,
    Interleaved,
}

impl Concurrency {
    fn multithread(self) -> bool {
        matches!(self, Self::Interleaved)
    }
}

/// The Ruby values a runtime needs on hand to serve one request.
pub(crate) struct CallContext {
    pub(crate) app: Value,
    pub(crate) string_io_class: RClass,
    /// Wraps a body that is still arriving. Absent for the serial runtime,
    /// which only ever has the whole body already.
    pub(crate) request_body_class: Option<Value>,
    pub(crate) rack_errors: Value,
    pub(crate) body_reader: Value,
    pub(crate) ruby_thread_object_id: u64,
    pub(crate) concurrency: Concurrency,
    /// Whether the runtime defines the Ruby side of `ruvoy.context` and
    /// `ruvoy.upstream`; the serial runtime, a diagnostic control, does not.
    pub(crate) extensions: bool,
}

/// Runs the application and collects its body into one response.
pub(crate) fn call(
    ruby: &Ruby,
    context: &CallContext,
    request: Request,
) -> Result<Response, BridgeError> {
    let &CallContext {
        app,
        body_reader,
        ruby_thread_object_id,
        ..
    } = context;
    let build = build_env(ruby, context, request)?;

    let rack_call_started_at = Instant::now();
    let rack_response = app
        .funcall::<_, _, RArray>("call", (build.env,))
        .map_err(|error| ruby_error("calling app.call(env)", error))?;
    let rack_call_time = rack_call_started_at.elapsed();

    let response_copy_started_at = Instant::now();
    let ResponseParts {
        status,
        mut headers,
        body: response_body,
    } = response_parts(&rack_response)?;
    let response_body = body_reader
        .funcall::<_, _, RString>("call", (response_body,))
        .map_err(|error| ruby_error("consuming response body", error))?;
    // SAFETY: the bytes are copied before control returns to Ruby, so the
    // string cannot be moved or collected while the slice is alive.
    let body = unsafe { response_body.as_slice() }.to_vec();
    headers.extend(stage_timing_headers(
        &build,
        rack_call_time,
        response_copy_started_at.elapsed(),
    ));

    Ok(Response {
        status,
        headers,
        body,
        ruby_thread_object_id,
    })
}

/// Runs the application and streams its body into `handle` chunk by chunk.
///
/// The head is published before the body is enumerated, so Envoy can start
/// writing to the client while Ruby is still producing.
pub(crate) fn call_streaming(
    ruby: &Ruby,
    context: &CallContext,
    request: Request,
    handle: &StreamHandle,
    sink: Value,
) -> Result<(), BridgeError> {
    let build = build_env(ruby, context, request)?;
    let result = respond(context, &build, handle, sink);
    if let Some(input) = build.streaming_input {
        // Releases a body kept in a temporary file now, not when the collector gets to it.
        let _ = input.funcall::<_, _, Value>("close", ());
    }
    result
}

fn respond(
    context: &CallContext,
    build: &EnvBuild,
    handle: &StreamHandle,
    sink: Value,
) -> Result<(), BridgeError> {
    let &CallContext {
        app,
        body_reader,
        ruby_thread_object_id,
        ..
    } = context;

    let rack_call_started_at = Instant::now();
    let rack_response = app
        .funcall::<_, _, RArray>("call", (build.env,))
        .map_err(|error| ruby_error("calling app.call(env)", error))?;
    let rack_call_time = rack_call_started_at.elapsed();

    let ResponseParts {
        mut status,
        mut headers,
        body: response_body,
    } = response_parts(&rack_response)?;
    if let Some(protocol) = accepted_protocol(&mut headers, build.protocols.as_deref())? {
        status = 101;
        headers.push(("upgrade".to_owned(), protocol));
        headers.push(("connection".to_owned(), "upgrade".to_owned()));
    }
    headers.extend(stage_timing_headers(build, rack_call_time, Duration::ZERO));
    if !handle.stream.push_head(ResponseHead {
        status,
        headers,
        ruby_thread_object_id,
    }) {
        return Ok(());
    }
    handle.wake();

    body_reader
        .funcall::<_, _, Value>("call", (response_body, sink, build.input))
        .map_err(|error| ruby_error("streaming response body", error))?;

    handle.stream.push_end();
    handle.wake();
    Ok(())
}

struct EnvBuild {
    env: RHash,
    input: Value,
    /// The protocols the client offered to upgrade to, when it offered any.
    protocols: Option<Vec<String>>,
    /// The input over a body still arriving, which the runtime closes once the
    /// request is done.
    streaming_input: Option<Value>,
    diagnostics: Option<RequestDiagnostics>,
    runtime_queue_time: Option<Duration>,
    rack_input_time: Duration,
}

fn build_env(
    ruby: &Ruby,
    context: &CallContext,
    request: Request,
) -> Result<EnvBuild, BridgeError> {
    let &CallContext {
        string_io_class,
        rack_errors,
        concurrency,
        extensions,
        ..
    } = context;
    let runtime_started_at = Instant::now();

    let Request {
        method,
        path,
        body,
        body_stream,
        headers,
        metadata,
        context: envoy_context,
        upstreams,
        force_gc,
        diagnostics,
    } = request;
    let runtime_queue_time = diagnostics
        .as_ref()
        .and_then(|diagnostics| diagnostics.submitted_at)
        .map(|submitted_at| runtime_started_at.duration_since(submitted_at));
    let (path_info, query_string) = path.split_once('?').unwrap_or((&path, ""));

    let env = ruby.hash_new();
    set(ruby, env, "REQUEST_METHOD", method)?;
    set(ruby, env, "SCRIPT_NAME", "")?;
    set(ruby, env, "PATH_INFO", path_info)?;
    set(ruby, env, "QUERY_STRING", query_string)?;
    set(ruby, env, "SERVER_NAME", metadata.server_name)?;
    set(ruby, env, "SERVER_PORT", metadata.server_port.to_string())?;
    set(ruby, env, "SERVER_PROTOCOL", metadata.protocol)?;
    set(ruby, env, "HTTP_HOST", metadata.authority)?;
    if let Some(remote_addr) = metadata.remote_addr {
        set(ruby, env, "REMOTE_ADDR", remote_addr)?;
    }
    set(ruby, env, "rack.url_scheme", metadata.scheme)?;
    set(ruby, env, "rack.errors", rack_errors)?;
    set(ruby, env, "rack.multithread", concurrency.multithread())?;
    set(ruby, env, "rack.multiprocess", false)?;
    set(ruby, env, "rack.run_once", false)?;
    set(ruby, env, "ruvoy.force_gc", force_gc)?;
    if let Some(envoy_context) = envoy_context.filter(|_| extensions) {
        set(
            ruby,
            env,
            "ruvoy.context",
            ruby.obj_wrap(ContextObject(envoy_context)),
        )?;
    }
    if let Some(upstreams) = upstreams.filter(|_| extensions) {
        set(
            ruby,
            env,
            "ruvoy.upstream",
            ruby.obj_wrap(UpstreamObject(upstreams)),
        )?;
    }

    let protocols = upgrade_protocols(&headers).filter(|_| extensions);
    if let Some(protocols) = &protocols {
        set(
            ruby,
            env,
            "rack.protocol",
            ruby.ary_from_vec(protocols.clone()),
        )?;
    }

    let mut header_key = String::new();
    for (name, value) in headers {
        if let Some(key) = env_header_key(&name, &mut header_key) {
            set(ruby, env, key, ruby.str_from_slice(&value))?;
        }
    }

    let rack_input_started_at = Instant::now();
    let (rack_input, closed_after) = match body_stream {
        Some(handle) => {
            let input = streaming_input(ruby, context, handle)?;
            (input, Some(input))
        }
        None => (collected_input(ruby, string_io_class, &body)?, None),
    };
    set(ruby, env, "rack.input", rack_input)?;

    Ok(EnvBuild {
        env,
        input: rack_input,
        protocols,
        streaming_input: closed_after,
        diagnostics,
        runtime_queue_time,
        rack_input_time: rack_input_started_at.elapsed(),
    })
}

/// Wraps a body that has already been collected.
fn collected_input(
    ruby: &Ruby,
    string_io_class: RClass,
    body: &[u8],
) -> Result<Value, BridgeError> {
    let input = string_io_class
        .funcall::<_, _, Value>("new", (ruby.str_from_slice(body),))
        .map_err(|error| ruby_error("creating rack.input StringIO", error))?;
    input
        .funcall::<_, _, Value>("binmode", ())
        .map_err(|error| ruby_error("setting rack.input binary mode", error))?;
    Ok(input)
}

/// Wraps a body the client is still sending.
fn streaming_input(
    ruby: &Ruby,
    context: &CallContext,
    handle: StreamHandle,
) -> Result<Value, BridgeError> {
    let Some(class) = context.request_body_class else {
        return Err(BridgeError::InvalidResponse(
            "this runtime cannot serve a streaming request body".to_owned(),
        ));
    };
    let chunks = ruby.obj_wrap(ChunkSource::new(handle));
    class
        .funcall::<_, _, Value>("new", (chunks,))
        .map_err(|error| ruby_error("creating rack.input", error))
}

/// Environment keys as interned strings, built once.
///
/// A key string handed to `Hash#[]=` is deduplicated against the interned
/// table anyway, so a fresh one spends an allocation per key per request
/// while the interned string spends none.
struct EnvKeys(HashMap<&'static str, Opaque<RString>>);

/// The keys every request carries, plus the header keys common enough to be
/// worth interning up front. A key outside this list still works; it is the
/// fresh string the other path avoids.
const INTERNED_KEYS: &[&str] = &[
    "CONTENT_LENGTH",
    "CONTENT_TYPE",
    "HTTP_ACCEPT",
    "HTTP_ACCEPT_ENCODING",
    "HTTP_ACCEPT_LANGUAGE",
    "HTTP_AUTHORIZATION",
    "HTTP_CACHE_CONTROL",
    "HTTP_CONNECTION",
    "HTTP_COOKIE",
    "HTTP_HOST",
    "HTTP_IF_MODIFIED_SINCE",
    "HTTP_IF_NONE_MATCH",
    "HTTP_ORIGIN",
    "HTTP_RANGE",
    "HTTP_REFERER",
    "HTTP_USER_AGENT",
    "HTTP_X_FORWARDED_FOR",
    "HTTP_X_FORWARDED_PROTO",
    "HTTP_X_REQUEST_ID",
    "PATH_INFO",
    "QUERY_STRING",
    "REMOTE_ADDR",
    "REQUEST_METHOD",
    "SCRIPT_NAME",
    "SERVER_NAME",
    "SERVER_PORT",
    "SERVER_PROTOCOL",
    "rack.errors",
    "rack.input",
    "rack.multiprocess",
    "rack.multithread",
    "rack.run_once",
    "rack.url_scheme",
    "ruvoy.context",
    "ruvoy.force_gc",
    "ruvoy.upstream",
];

static ENV_KEYS: OnceLock<EnvKeys> = OnceLock::new();

/// Interned strings outlive every request, so the table is built once and
/// shared by both runtimes.
fn env_keys(ruby: &Ruby) -> &'static EnvKeys {
    ENV_KEYS.get_or_init(|| {
        EnvKeys(
            INTERNED_KEYS
                .iter()
                .map(|&key| {
                    let interned = ruby.str_new(key).to_interned_str();
                    // The interned table holds its strings weakly, and this one
                    // is reachable only from Rust.
                    gc::register_mark_object(interned);
                    (key, Opaque::from(interned))
                })
                .collect(),
        )
    })
}

fn set(ruby: &Ruby, env: RHash, key: &str, value: impl IntoValue) -> Result<(), BridgeError> {
    let written = match env_keys(ruby).0.get(key) {
        Some(&interned) => env.aset(ruby.get_inner(interned), value),
        None => env.aset(key, value),
    };
    written.map_err(|error| ruby_error(&format!("setting {key}"), error))
}

struct ResponseParts {
    status: u16,
    headers: Vec<(String, String)>,
    body: Value,
}

fn response_parts(rack_response: &RArray) -> Result<ResponseParts, BridgeError> {
    if rack_response.len() != 3 {
        return Err(BridgeError::InvalidResponse(format!(
            "expected 3 entries, got {}",
            rack_response.len()
        )));
    }
    let status = rack_response
        .entry::<i64>(0)
        .map_err(|error| ruby_error("converting response status", error))
        .and_then(|status| {
            u16::try_from(status).map_err(|_| {
                BridgeError::InvalidResponse(format!("status {status} is outside the u16 range"))
            })
        })?;
    let headers = copy_headers(
        rack_response
            .entry::<RHash>(1)
            .map_err(|error| ruby_error("converting response headers", error))?,
    )?;
    let body = rack_response
        .entry::<Value>(2)
        .map_err(|error| ruby_error("reading response body", error))?;

    Ok(ResponseParts {
        status,
        headers,
        body,
    })
}

/// The protocols a client asks to upgrade to. Envoy presents an HTTP/2
/// extended CONNECT the same way, as an `upgrade` header.
fn upgrade_protocols(headers: &[(String, Vec<u8>)]) -> Option<Vec<String>> {
    let tokens = |wanted: &'static str| {
        headers
            .iter()
            .filter(move |(name, _)| name.eq_ignore_ascii_case(wanted))
            .flat_map(|(_, value)| {
                String::from_utf8_lossy(value)
                    .split(',')
                    .map(|token| token.trim().to_owned())
                    .filter(|token| !token.is_empty())
                    .collect::<Vec<_>>()
            })
    };
    if !tokens("connection").any(|token| token.eq_ignore_ascii_case("upgrade")) {
        return None;
    }
    let protocols = tokens("upgrade").collect::<Vec<_>>();
    (!protocols.is_empty()).then_some(protocols)
}

/// Takes the `rack.protocol` response header: the protocol the application
/// accepted, which must be one the client offered. Upgrade headers the
/// application set itself are dropped in favour of the ones that go with it.
fn accepted_protocol(
    headers: &mut Vec<(String, String)>,
    offered: Option<&[String]>,
) -> Result<Option<String>, BridgeError> {
    let Some(index) = headers.iter().position(|(name, _)| name == "rack.protocol") else {
        return Ok(None);
    };
    let (_, protocol) = headers.remove(index);
    if !offered.is_some_and(|offered| offered.iter().any(|candidate| candidate == &protocol)) {
        return Err(BridgeError::InvalidResponse(format!(
            "rack.protocol {protocol:?} is not one the client offered"
        )));
    }
    headers.retain(|(name, _)| {
        !name.eq_ignore_ascii_case("upgrade") && !name.eq_ignore_ascii_case("connection")
    });
    Ok(Some(protocol))
}

fn copy_headers(headers: RHash) -> Result<Vec<(String, String)>, BridgeError> {
    let mut result = Vec::with_capacity(headers.len());
    headers
        .foreach(|name: String, value: Value| {
            if let Some(values) = RArray::from_value(value) {
                for value in values.to_vec::<String>()? {
                    result.push((name.clone(), value));
                }
            } else {
                result.push((name, String::try_convert(value)?));
            }
            Ok(ForEach::Continue)
        })
        .map_err(|error| ruby_error("copying response headers", error))?;

    Ok(result)
}

/// Maps an HTTP header name onto its Rack environment key.
///
/// Pseudo-headers have no Rack representation and are dropped.
fn env_header_key<'a>(name: &str, key: &'a mut String) -> Option<&'a str> {
    if name.starts_with(':') {
        return None;
    }
    key.clear();
    if name.eq_ignore_ascii_case("content-type") {
        key.push_str("CONTENT_TYPE");
    } else if name.eq_ignore_ascii_case("content-length") {
        key.push_str("CONTENT_LENGTH");
    } else {
        key.push_str("HTTP_");
        key.extend(name.chars().map(|character| match character {
            '-' => '_',
            _ => character.to_ascii_uppercase(),
        }));
    }
    Some(key)
}

fn stage_timing_headers(
    build: &EnvBuild,
    rack_call_time: Duration,
    response_copy_time: Duration,
) -> Vec<(String, String)> {
    let Some(diagnostics) = build.diagnostics.as_ref() else {
        return Vec::new();
    };
    let ingress_time = diagnostics
        .submitted_at
        .map(|submitted_at| submitted_at.duration_since(diagnostics.received_at))
        .unwrap_or_default();
    let nanos = |duration: Duration| duration.as_nanos().to_string();

    vec![
        ("x-ruvoy-stage-timing".to_owned(), "1".to_owned()),
        ("x-ruvoy-stage-ingress-ns".to_owned(), nanos(ingress_time)),
        (
            "x-ruvoy-stage-body-copy-ns".to_owned(),
            nanos(diagnostics.body_copy_time),
        ),
        (
            "x-ruvoy-stage-body-callbacks".to_owned(),
            diagnostics.body_callbacks.to_string(),
        ),
        (
            "x-ruvoy-stage-runtime-queue-ns".to_owned(),
            nanos(build.runtime_queue_time.unwrap_or_default()),
        ),
        (
            "x-ruvoy-stage-rack-input-ns".to_owned(),
            nanos(build.rack_input_time),
        ),
        (
            "x-ruvoy-stage-rack-call-ns".to_owned(),
            nanos(rack_call_time),
        ),
        (
            "x-ruvoy-stage-response-copy-ns".to_owned(),
            nanos(response_copy_time),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::{INTERNED_KEYS, accepted_protocol, env_header_key, upgrade_protocols};

    fn request_headers(pairs: &[(&str, &str)]) -> Vec<(String, Vec<u8>)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.as_bytes().to_vec()))
            .collect()
    }

    #[test]
    fn an_upgrade_offers_its_protocols_only_with_connection_upgrade() {
        let offered = request_headers(&[
            ("connection", "keep-alive, Upgrade"),
            ("upgrade", "websocket"),
        ]);
        assert_eq!(
            upgrade_protocols(&offered),
            Some(vec!["websocket".to_owned()])
        );

        let without_connection = request_headers(&[("upgrade", "websocket")]);
        assert_eq!(upgrade_protocols(&without_connection), None);
        assert_eq!(
            upgrade_protocols(&request_headers(&[("connection", "upgrade")])),
            None
        );
    }

    #[test]
    fn an_accepted_protocol_must_be_one_the_client_offered() {
        let offered = vec!["websocket".to_owned()];
        let mut headers = vec![
            ("rack.protocol".to_owned(), "websocket".to_owned()),
            ("Upgrade".to_owned(), "h2c".to_owned()),
            ("sec-websocket-accept".to_owned(), "x".to_owned()),
        ];
        assert_eq!(
            accepted_protocol(&mut headers, Some(&offered)).unwrap(),
            Some("websocket".to_owned())
        );
        assert_eq!(
            headers,
            vec![("sec-websocket-accept".to_owned(), "x".to_owned())]
        );

        let mut unoffered = vec![("rack.protocol".to_owned(), "h2c".to_owned())];
        assert!(accepted_protocol(&mut unoffered, Some(&offered)).is_err());
        let mut no_upgrade = vec![("rack.protocol".to_owned(), "websocket".to_owned())];
        assert!(accepted_protocol(&mut no_upgrade, None).is_err());

        let mut plain = vec![("content-type".to_owned(), "text/plain".to_owned())];
        assert_eq!(accepted_protocol(&mut plain, Some(&offered)).unwrap(), None);
    }

    #[test]
    fn header_names_map_onto_rack_keys() {
        let mut key = String::new();
        assert_eq!(env_header_key(":method", &mut key), None);
        assert_eq!(
            env_header_key("Content-Type", &mut key),
            Some("CONTENT_TYPE")
        );
        assert_eq!(
            env_header_key("content-length", &mut key),
            Some("CONTENT_LENGTH")
        );
        assert_eq!(
            env_header_key("X-Ruvoy-Test", &mut key),
            Some("HTTP_X_RUVOY_TEST")
        );
    }

    #[test]
    fn a_reused_buffer_carries_nothing_from_the_header_before_it() {
        let mut key = String::new();
        env_header_key("X-A-Very-Long-Header-Name", &mut key);

        assert_eq!(env_header_key("X-Short", &mut key), Some("HTTP_X_SHORT"));
    }

    #[test]
    fn interned_keys_are_unique() {
        let mut keys = INTERNED_KEYS.to_vec();
        keys.sort_unstable();
        keys.dedup();

        assert_eq!(keys.len(), INTERNED_KEYS.len());
    }
}
