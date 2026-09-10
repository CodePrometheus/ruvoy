//! The Ruby side of what a filter can expose beyond Rack: `ruvoy.context` and
//! `ruvoy.upstream`.

use crate::{
    BridgeError, Context, MetadataValue, Namespace, ResponseHead, ResponseStream, StreamItem,
    UpstreamRequest, Upstreams,
    context::{Connection, Tls},
    error::ruby_error,
};
use magnus::{
    Error, ExceptionClass, IntoValue, RArray, RHash, RModule, RString, Ruby, Value, method,
    prelude::*, typed_data::Obj,
};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
    time::Duration,
};

/// Waits on an upstream call from inside the request's fiber.
const UPSTREAM_SOURCE: &str = include_str!("../ruby/upstream.rb");

/// Defines `Ruvoy::Context`, `Ruvoy::Upstream`, `Ruvoy::UpstreamCall` and
/// `Ruvoy::UpstreamError`.
pub(crate) fn define(ruby: &Ruby, module: RModule) -> Result<(), BridgeError> {
    let context = module
        .define_class("Context", ruby.class_object())
        .map_err(|error| ruby_error("defining Context", error))?;
    context
        .define_method("route_name", method!(ContextObject::route_name, 0))
        .and_then(|()| context.define_method("connection", method!(ContextObject::connection, 0)))
        .and_then(|()| context.define_method("tls", method!(ContextObject::tls, 0)))
        .and_then(|()| {
            context.define_method(
                "dynamic_metadata",
                method!(ContextObject::dynamic_metadata, 0),
            )
        })
        .and_then(|()| {
            context.define_method("route_metadata", method!(ContextObject::route_metadata, 0))
        })
        .and_then(|()| context.define_method("to_h", method!(ContextObject::to_h, 0)))
        .map_err(|error| ruby_error("defining Context methods", error))?;

    let upstream = module
        .define_class("Upstream", ruby.class_object())
        .map_err(|error| ruby_error("defining Upstream", error))?;
    upstream
        .define_private_method("dispatch", method!(UpstreamObject::dispatch, 4))
        .map_err(|error| ruby_error("defining Upstream methods", error))?;
    let call = module
        .define_class("UpstreamCall", ruby.class_object())
        .map_err(|error| ruby_error("defining UpstreamCall", error))?;
    call.define_method("response", method!(UpstreamCall::response, 0))
        .and_then(|()| call.define_method("park", method!(UpstreamCall::park, 1)))
        .and_then(|()| call.define_method("unpark", method!(UpstreamCall::unpark, 0)))
        .map_err(|error| ruby_error("defining UpstreamCall methods", error))?;
    module
        .define_error("UpstreamError", ruby.exception_standard_error())
        .map_err(|error| ruby_error("defining UpstreamError", error))?;
    ruby.eval::<Value>(UPSTREAM_SOURCE)
        .map_err(|error| ruby_error("loading Upstream#call", error))?;
    Ok(())
}

/// `env["ruvoy.context"]`. Every reader builds a fresh value.
#[magnus::wrap(class = "Ruvoy::Context", free_immediately, size)]
pub(crate) struct ContextObject(pub(crate) Context);

impl ContextObject {
    fn route_name(&self) -> Option<String> {
        self.0.route_name.clone()
    }

    fn connection(ruby: &Ruby, rb_self: &Self) -> Result<RHash, Error> {
        connection_hash(ruby, &rb_self.0.connection)
    }

    fn tls(ruby: &Ruby, rb_self: &Self) -> Result<Option<RHash>, Error> {
        rb_self
            .0
            .tls
            .as_ref()
            .map(|tls| tls_hash(ruby, tls))
            .transpose()
    }

    fn dynamic_metadata(ruby: &Ruby, rb_self: &Self) -> Result<RHash, Error> {
        metadata_hash(ruby, &rb_self.0.dynamic_metadata)
    }

    fn route_metadata(ruby: &Ruby, rb_self: &Self) -> Result<RHash, Error> {
        metadata_hash(ruby, &rb_self.0.route_metadata)
    }

    fn to_h(ruby: &Ruby, rb_self: &Self) -> Result<RHash, Error> {
        let hash = ruby.hash_new();
        hash.aset("route_name", rb_self.route_name())?;
        hash.aset("connection", Self::connection(ruby, rb_self)?)?;
        hash.aset("tls", Self::tls(ruby, rb_self)?)?;
        hash.aset("dynamic_metadata", Self::dynamic_metadata(ruby, rb_self)?)?;
        hash.aset("route_metadata", Self::route_metadata(ruby, rb_self)?)?;
        Ok(hash)
    }
}

fn connection_hash(ruby: &Ruby, connection: &Connection) -> Result<RHash, Error> {
    let hash = ruby.hash_new();
    hash.aset("id", connection.id)?;
    hash.aset("source_address", connection.source_address.as_deref())?;
    hash.aset("source_port", connection.source_port)?;
    hash.aset(
        "destination_address",
        connection.destination_address.as_deref(),
    )?;
    hash.aset("destination_port", connection.destination_port)?;
    Ok(hash)
}

fn tls_hash(ruby: &Ruby, tls: &Tls) -> Result<RHash, Error> {
    let hash = ruby.hash_new();
    hash.aset("version", tls.version.as_str())?;
    hash.aset("server_name", tls.server_name.as_deref())?;
    let peer = match &tls.peer_certificate {
        Some(certificate) => {
            let peer = ruby.hash_new();
            peer.aset("subject", certificate.subject.as_deref())?;
            peer.aset("uri_san", certificate.uri_san.as_deref())?;
            peer.aset("dns_san", certificate.dns_san.as_deref())?;
            peer.aset("sha256", certificate.sha256.as_deref())?;
            Some(peer)
        }
        None => None,
    };
    hash.aset("peer_certificate", peer)?;
    Ok(hash)
}

fn metadata_hash(ruby: &Ruby, namespaces: &[Namespace]) -> Result<RHash, Error> {
    let hash = ruby.hash_new();
    for namespace in namespaces {
        let fields = ruby.hash_new();
        for (key, value) in &namespace.fields {
            fields.aset(key.as_str(), metadata_value(ruby, value)?)?;
        }
        hash.aset(namespace.name.as_str(), fields)?;
    }
    Ok(hash)
}

fn metadata_value(ruby: &Ruby, value: &MetadataValue) -> Result<Value, Error> {
    Ok(match value {
        MetadataValue::String(text) => ruby.str_new(text).as_value(),
        MetadataValue::Number(number) => number.into_value_with(ruby),
        MetadataValue::Bool(flag) => flag.into_value_with(ruby),
        MetadataValue::List(values) => {
            let list = ruby.ary_new_capa(values.len());
            for value in values {
                list.push(metadata_value(ruby, value)?)?;
            }
            list.as_value()
        }
    })
}

/// `env["ruvoy.upstream"]`: calls into the clusters the filter allows.
#[magnus::wrap(class = "Ruvoy::Upstream", free_immediately, size)]
pub(crate) struct UpstreamObject(pub(crate) Upstreams);

impl UpstreamObject {
    /// Queues a call for the request's worker and returns what to wait on.
    fn dispatch(
        ruby: &Ruby,
        rb_self: &Self,
        cluster: String,
        headers: RArray,
        body: Option<RString>,
        timeout: Option<f64>,
    ) -> Result<Obj<UpstreamCall>, Error> {
        let settings = rb_self.0.settings();
        if !settings.allows(&cluster) {
            return Err(Error::new(
                ruby.exception_arg_error(),
                format!("cluster {cluster:?} is not one this filter lets the application call"),
            ));
        }
        let timeout = match timeout {
            None => settings.timeout,
            Some(seconds) => Duration::try_from_secs_f64(seconds)
                .ok()
                .filter(|timeout| !timeout.is_zero())
                .ok_or_else(|| {
                    Error::new(
                        ruby.exception_arg_error(),
                        format!("timeout must be a positive number of seconds, got {seconds}"),
                    )
                })?,
        };
        let headers = header_pairs(headers)?;
        // SAFETY: the bytes are copied before control returns to Ruby, so the
        // string cannot be moved or collected while the slice is alive.
        let body = body.map_or_else(Vec::new, |body| unsafe { body.as_slice() }.to_vec());
        let response = Arc::new(ResponseStream::new(settings.max_response_bytes));
        rb_self
            .0
            .queue(UpstreamRequest {
                cluster,
                headers,
                body,
                timeout,
                response: Arc::clone(&response),
            })
            .map_err(|error| upstream_error(ruby, &error))?;
        Ok(ruby.obj_wrap(UpstreamCall {
            response,
            head: RefCell::new(None),
            body: RefCell::new(Vec::new()),
            taken: Cell::new(false),
        }))
    }
}

/// Copies `[[name, value], ...]` as `Upstream#call` shaped it.
fn header_pairs(headers: RArray) -> Result<Vec<(String, Vec<u8>)>, Error> {
    (0..headers.len())
        .map(|index| {
            let pair = headers.entry::<RArray>(isize::try_from(index).unwrap_or(isize::MAX))?;
            Ok((
                pair.entry::<String>(0)?,
                pair.entry::<String>(1)?.into_bytes(),
            ))
        })
        .collect()
}

/// One call in flight, as the fiber that made it sees it.
#[magnus::wrap(class = "Ruvoy::UpstreamCall", free_immediately, size)]
pub(crate) struct UpstreamCall {
    response: Arc<ResponseStream>,
    head: RefCell<Option<ResponseHead>>,
    body: RefCell<Vec<u8>>,
    taken: Cell<bool>,
}

impl UpstreamCall {
    /// `[status, headers, body]` once the whole response is in, nil before
    /// that; raises `Ruvoy::UpstreamError` when the call failed.
    fn response(ruby: &Ruby, rb_self: &Self) -> Result<Option<RArray>, Error> {
        if rb_self.taken.get() {
            return Err(Error::new(
                ruby.exception_runtime_error(),
                "the response was already taken",
            ));
        }
        while let Some(item) = rb_self.response.take_next() {
            match item {
                StreamItem::Head(head) => *rb_self.head.borrow_mut() = Some(head),
                StreamItem::Chunk(chunk) => rb_self.body.borrow_mut().extend_from_slice(&chunk),
                StreamItem::End => {
                    rb_self.taken.set(true);
                    let head = rb_self.head.borrow_mut().take().ok_or_else(|| {
                        upstream_error(
                            ruby,
                            &BridgeError::Upstream("the response ended before its head".to_owned()),
                        )
                    })?;
                    return rack_response(ruby, head, &rb_self.body.take()).map(Some);
                }
                StreamItem::Failed(error) => {
                    rb_self.taken.set(true);
                    return Err(upstream_error(ruby, &error));
                }
            }
        }
        Ok(None)
    }

    /// Parks the calling fiber on `waiter` until the answer lands; false when
    /// it already has, and the fiber must not wait.
    fn park(&self, waiter: Value) -> bool {
        self.response.park(waiter.into())
    }

    /// Lets go of the waiter of a fiber that stopped waiting without being
    /// woken.
    fn unpark(&self) {
        self.response.unpark();
    }
}

/// Repeated header names become an Array of values, as in a Rack 3 response.
fn rack_response(ruby: &Ruby, head: ResponseHead, body: &[u8]) -> Result<RArray, Error> {
    let headers = ruby.hash_new();
    for (name, value) in head.headers {
        match headers.get(name.as_str()) {
            None => headers.aset(name, value)?,
            Some(existing) => match RArray::from_value(existing) {
                Some(values) => values.push(value)?,
                None => {
                    let values = ruby.ary_new_capa(2);
                    values.push(existing)?;
                    values.push(value)?;
                    headers.aset(name, values)?;
                }
            },
        }
    }
    let response = ruby.ary_new_capa(3);
    response.push(head.status)?;
    response.push(headers)?;
    response.push(ruby.str_from_slice(body))?;
    Ok(response)
}

fn upstream_error(ruby: &Ruby, error: &BridgeError) -> Error {
    ruby.class_object()
        .const_get::<_, RModule>("Ruvoy")
        .and_then(|module| module.const_get::<_, ExceptionClass>("UpstreamError"))
        .map_or_else(
            |lookup| lookup,
            |class| Error::new(class, error.to_string()),
        )
}
