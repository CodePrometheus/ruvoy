//! Translation between owned Rust requests and the Rack protocol.

use crate::{
    BridgeError, Request, RequestDiagnostics, Response, ResponseHead, StreamHandle, StreamItem,
    error::ruby_error,
};
use magnus::{IntoValue, RArray, RClass, RHash, RString, Ruby, Value, prelude::*, r_hash::ForEach};
use std::time::{Duration, Instant};

/// Collects a Rack body into one buffer; used by the serial runtime.
pub(crate) const BODY_READER_SOURCE: &str = include_str!("../ruby/rack_body_reader.rb");

/// Feeds a Rack body into a stream sink; used by the fiber runtime.
pub(crate) const STREAMING_BODY_READER_SOURCE: &str =
    include_str!("../ruby/rack_streaming_body_reader.rb");

/// The Ruby-facing half of a request body still arriving from the client.
///
/// Chunks are handed over as owned bytes; `park` returns the `Async::Variable`
/// the reactor resolves once more of them land.
#[magnus::wrap(class = "Ruvoy::RequestChunks", free_immediately)]
pub(crate) struct RequestChunks {
    handle: StreamHandle,
}

impl RequestChunks {
    /// Takes the next chunk, or `nil` when none is buffered right now.
    pub(crate) fn next_chunk(ruby: &Ruby, chunks: &Self) -> Option<RString> {
        let stream = &chunks.handle.stream;
        let item = stream.take_next()?;
        // Envoy stopped sending once the queue filled up; taking from it is what
        // lets the worker ask for the rest.
        chunks.handle.wake();
        match item {
            StreamItem::Chunk(bytes) => Some(ruby.str_from_slice(&bytes)),
            _ => None,
        }
    }

    /// True when a read would not have to wait: bytes are buffered, or the
    /// client will send nothing more.
    pub(crate) fn ready(&self) -> bool {
        let stream = &self.handle.stream;
        stream.buffered_bytes() > 0 || stream.is_finished()
    }

    /// True once the client will send nothing more.
    pub(crate) fn finished(&self) -> bool {
        self.handle.stream.is_finished()
    }

    /// Parks the calling fiber on `waiter` until more of the body arrives.
    pub(crate) fn park(&self, waiter: Value) {
        self.handle.stream.park(waiter.into());
    }
}

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

    let ResponseParts {
        status,
        mut headers,
        body: response_body,
    } = response_parts(&rack_response)?;
    headers.extend(stage_timing_headers(&build, rack_call_time, Duration::ZERO));
    if !handle.stream.push_head(ResponseHead {
        status,
        headers,
        ruby_thread_object_id,
    }) {
        return Ok(());
    }
    handle.wake();

    body_reader
        .funcall::<_, _, Value>("call", (response_body, sink))
        .map_err(|error| ruby_error("streaming response body", error))?;

    handle.stream.push_end();
    handle.wake();
    Ok(())
}

struct EnvBuild {
    env: RHash,
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
        force_gc,
        diagnostics,
    } = request;
    let runtime_queue_time = diagnostics
        .as_ref()
        .and_then(|diagnostics| diagnostics.submitted_at)
        .map(|submitted_at| runtime_started_at.duration_since(submitted_at));
    let (path_info, query_string) = path.split_once('?').unwrap_or((&path, ""));

    let env = ruby.hash_new();
    set(env, "REQUEST_METHOD", method)?;
    set(env, "SCRIPT_NAME", "")?;
    set(env, "PATH_INFO", path_info)?;
    set(env, "QUERY_STRING", query_string)?;
    set(env, "SERVER_NAME", metadata.server_name)?;
    set(env, "SERVER_PORT", metadata.server_port.to_string())?;
    set(env, "SERVER_PROTOCOL", metadata.protocol)?;
    set(env, "HTTP_HOST", metadata.authority)?;
    if let Some(remote_addr) = metadata.remote_addr {
        set(env, "REMOTE_ADDR", remote_addr)?;
    }
    set(env, "rack.url_scheme", metadata.scheme)?;
    set(env, "rack.errors", rack_errors)?;
    set(env, "rack.multithread", concurrency.multithread())?;
    set(env, "rack.multiprocess", false)?;
    set(env, "rack.run_once", false)?;
    set(env, "ruvoy.force_gc", force_gc)?;

    for (name, value) in headers {
        if let Some(key) = env_header_name(&name) {
            set(env, &key, ruby.str_from_slice(&value))?;
        }
    }

    let rack_input_started_at = Instant::now();
    let rack_input = match body_stream {
        Some(handle) => streaming_input(ruby, context, handle)?,
        None => collected_input(ruby, string_io_class, &body)?,
    };
    set(env, "rack.input", rack_input)?;

    Ok(EnvBuild {
        env,
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
    let chunks = ruby.obj_wrap(RequestChunks { handle });
    class
        .funcall::<_, _, Value>("new", (chunks,))
        .map_err(|error| ruby_error("creating rack.input", error))
}

fn set(env: RHash, key: &str, value: impl IntoValue) -> Result<(), BridgeError> {
    env.aset(key, value)
        .map_err(|error| ruby_error(&format!("setting {key}"), error))
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
fn env_header_name(name: &str) -> Option<String> {
    let lowercase = name.to_ascii_lowercase();
    match lowercase.as_str() {
        name if name.starts_with(':') => None,
        "content-type" => Some("CONTENT_TYPE".to_owned()),
        "content-length" => Some("CONTENT_LENGTH".to_owned()),
        _ => Some(format!(
            "HTTP_{}",
            lowercase.replace('-', "_").to_ascii_uppercase()
        )),
    }
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
    use super::env_header_name;

    #[test]
    fn header_names_map_onto_rack_keys() {
        assert_eq!(env_header_name(":method"), None);
        assert_eq!(
            env_header_name("Content-Type").as_deref(),
            Some("CONTENT_TYPE")
        );
        assert_eq!(
            env_header_name("X-Ruvoy-Test").as_deref(),
            Some("HTTP_X_RUVOY_TEST")
        );
    }
}
