//! CRuby-owned request runtime for Ruvoy.
//!
//! Producer threads exchange owned Rust data with one long-lived runtime
//! thread. Only that runtime thread initializes and calls CRuby.

pub mod fiber;

use magnus::{
    RArray, RClass, RHash, RString, Ruby, Value, prelude::*, r_hash::ForEach, value::BoxValue,
};
use std::{
    collections::VecDeque,
    error::Error as StdError,
    fmt,
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

// The synchronous runtime stays buffered: it exists as a diagnostic control for
// the Fiber runtime, and giving it a second response path would make the two
// harder to compare.
const RACK_BODY_READER_SOURCE: &str = r#"
lambda do |body|
  raise TypeError, "streaming Rack response bodies are not supported" unless body.respond_to?(:each)

  output = +"".b
  begin
    body.each { |chunk| output << chunk }
  ensure
    body.close if body.respond_to?(:close)
  end
  output
end
"#;

// Pushing each chunk into the sink keeps the whole response out of memory and
// lets the first bytes reach the client while the application is still
// producing. Waiting for capacity happens in Ruby because only Ruby can yield
// the Fiber; blocking in Rust would stall every other request on the runtime
// thread. A cancelled sink stops the enumeration instead of walking a body
// nobody will read.
pub(crate) const RACK_STREAMING_BODY_READER_SOURCE: &str = r#"
lambda do |body, sink|
  raise TypeError, "streaming Rack response bodies are not supported" unless body.respond_to?(:each)

  begin
    body.each do |chunk|
      until sink.writable?
        break if sink.cancelled?
        sleep 0.001
      end
      break unless sink.write(chunk)
    end
  ensure
    body.close if body.respond_to?(:close)
  end
  nil
end
"#;

pub const DEFAULT_APP_SOURCE: &str = r#"
Class.new do
  def call(env)
    raise "intentional boom" if env.fetch("PATH_INFO") == "/raise"

    @calls = (@calls || 0) + 1
    GC.start if env["ruvoy.force_gc"]

    input = env.fetch("rack.input")

    body = [
      env.fetch("REQUEST_METHOD"),
      env.fetch("PATH_INFO"),
      input.read
    ].join(" ")

    headers = {
      "content-type" => "text/plain",
      "x-ruby-call-count" => @calls.to_s
    }
    if env["HTTP_X_RUVOY_TEST"]
      headers["x-request-header"] = env["HTTP_X_RUVOY_TEST"]
    end

    [
      200,
      headers,
      [body]
    ]
  end
end.new
"#;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub metadata: RequestMetadata,
    pub force_gc: bool,
    pub diagnostics: Option<RequestDiagnostics>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestMetadata {
    pub authority: String,
    pub scheme: String,
    pub server_name: String,
    pub server_port: u16,
    pub protocol: String,
    pub remote_addr: Option<String>,
}

impl Default for RequestMetadata {
    fn default() -> Self {
        Self {
            authority: "localhost".to_owned(),
            scheme: "http".to_owned(),
            server_name: "localhost".to_owned(),
            server_port: 80,
            protocol: "HTTP/1.1".to_owned(),
            remote_addr: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestDiagnostics {
    pub received_at: Instant,
    pub body_copy_time: Duration,
    pub body_callbacks: u64,
    pub body_reallocations: u64,
    pub declared_capacity: usize,
    pub submitted_at: Option<Instant>,
}

impl RequestDiagnostics {
    pub fn new(received_at: Instant, declared_capacity: usize) -> Self {
        Self {
            received_at,
            body_copy_time: Duration::ZERO,
            body_callbacks: 0,
            body_reallocations: 0,
            declared_capacity,
            submitted_at: None,
        }
    }
}

impl Request {
    pub fn new(
        method: impl Into<String>,
        path: impl Into<String>,
        body: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            method: method.into(),
            path: path.into(),
            body: body.into(),
            headers: Vec::new(),
            metadata: RequestMetadata::default(),
            force_gc: false,
            diagnostics: None,
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn with_forced_gc(mut self) -> Self {
        self.force_gc = true;
        self
    }

    pub fn with_metadata(mut self, metadata: RequestMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub ruby_thread_object_id: u64,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// The response head, delivered before the body has been produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponseHead {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub ruby_thread_object_id: u64,
}

/// What the Envoy worker still has to send downstream.
#[derive(Debug)]
pub enum StreamItem {
    Head(ResponseHead),
    Chunk(Vec<u8>),
    End,
    Failed(BridgeError),
}

/// The hand-off between the Ruby owner thread and one Envoy worker stream.
///
/// The queue is bounded by buffered bytes rather than chunk count, because a
/// Rack body is free to yield either many small strings or a few large ones,
/// and only the byte total bounds memory.
pub struct ResponseStream {
    inner: Mutex<ResponseStreamInner>,
    max_buffered_bytes: usize,
}

#[derive(Default)]
struct ResponseStreamInner {
    items: VecDeque<StreamItem>,
    buffered_bytes: usize,
    cancelled: bool,
    /// Set while the downstream write buffer is over its high watermark.
    paused: bool,
}

impl ResponseStream {
    pub fn new(max_buffered_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(ResponseStreamInner::default()),
            max_buffered_bytes: max_buffered_bytes.max(1),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ResponseStreamInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Marks the stream dead. Called when the Envoy filter goes away, so the
    /// Ruby side can stop enumerating a body nobody will read.
    pub fn cancel(&self) {
        let mut inner = self.lock();
        inner.cancelled = true;
        inner.items.clear();
        inner.buffered_bytes = 0;
    }

    pub fn is_cancelled(&self) -> bool {
        self.lock().cancelled
    }

    pub fn set_paused(&self, paused: bool) {
        self.lock().paused = paused;
    }

    /// True while the producer must wait: either the buffer is full or the
    /// downstream asked us to stop writing.
    pub fn is_saturated(&self) -> bool {
        let inner = self.lock();
        inner.paused || inner.buffered_bytes >= self.max_buffered_bytes
    }

    fn push(&self, item: StreamItem) -> bool {
        let mut inner = self.lock();
        if inner.cancelled {
            return false;
        }
        if let StreamItem::Chunk(chunk) = &item {
            inner.buffered_bytes += chunk.len();
        }
        inner.items.push_back(item);
        true
    }

    pub fn push_head(&self, head: ResponseHead) -> bool {
        self.push(StreamItem::Head(head))
    }

    pub fn push_chunk(&self, chunk: Vec<u8>) -> bool {
        self.push(StreamItem::Chunk(chunk))
    }

    pub fn push_end(&self) -> bool {
        self.push(StreamItem::End)
    }

    pub fn push_failure(&self, error: BridgeError) -> bool {
        self.push(StreamItem::Failed(error))
    }

    /// Takes the next item unless the downstream is paused, in which case the
    /// worker leaves it queued so the producer keeps feeling the backpressure.
    pub fn take_next(&self) -> Option<StreamItem> {
        let mut inner = self.lock();
        if inner.paused {
            return None;
        }
        let item = inner.items.pop_front()?;
        if let StreamItem::Chunk(chunk) = &item {
            inner.buffered_bytes = inner.buffered_bytes.saturating_sub(chunk.len());
        }
        Some(item)
    }
}

/// Wakes the Envoy worker that owns the downstream stream.
///
/// The Ruby thread must never touch Envoy state directly, so producing a chunk
/// only signals the worker; the worker then drains the queue on its own thread.
pub type StreamWaker = Arc<dyn Fn() + Send + Sync + 'static>;

/// One streaming response: the shared queue plus the way to wake its worker.
#[derive(Clone)]
pub struct StreamHandle {
    pub stream: Arc<ResponseStream>,
    pub waker: StreamWaker,
}

impl StreamHandle {
    pub fn new(stream: Arc<ResponseStream>, waker: StreamWaker) -> Self {
        Self { stream, waker }
    }

    pub fn wake(&self) {
        (self.waker)();
    }
}

/// The Fiber runtime re-enters `app.call` while an earlier call is suspended on
/// scheduler-aware I/O, so `rack.multithread` must not promise exclusive access.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RackConcurrency {
    Serial,
    Interleaved,
}

impl RackConcurrency {
    fn multithread(self) -> bool {
        matches!(self, Self::Interleaved)
    }
}

pub(crate) struct RackCallContext {
    pub(crate) app: Value,
    pub(crate) string_io_class: RClass,
    pub(crate) rack_errors: Value,
    pub(crate) body_reader: Value,
    pub(crate) ruby_thread_object_id: u64,
    pub(crate) concurrency: RackConcurrency,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeInfo {
    pub ruby_description: String,
    pub rust_thread_id: String,
    pub ruby_thread_object_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BridgeError {
    Io(String),
    Spawn(String),
    Startup(String),
    Ruby(String),
    InvalidResponse(String),
    Overloaded,
    RuntimeStopped,
    ResponseTimeout,
    RuntimePanicked(String),
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(f, "bridge I/O error: {message}"),
            Self::Spawn(message) => write!(f, "failed to spawn Ruby runtime thread: {message}"),
            Self::Startup(message) => write!(f, "Ruby runtime startup failed: {message}"),
            Self::Ruby(message) => write!(f, "Ruby call failed: {message}"),
            Self::InvalidResponse(message) => write!(f, "invalid Rack-like response: {message}"),
            Self::Overloaded => write!(f, "Ruby runtime admission limit reached"),
            Self::RuntimeStopped => write!(f, "Ruby runtime has stopped"),
            Self::ResponseTimeout => write!(f, "timed out waiting for Ruby runtime"),
            Self::RuntimePanicked(message) => write!(f, "Ruby runtime panicked: {message}"),
        }
    }
}

impl StdError for BridgeError {}

enum Command {
    Call(Box<BufferedCall>),
    ForceGc {
        reply: SyncSender<Result<(), BridgeError>>,
    },
    Shutdown {
        reply: SyncSender<()>,
    },
}

pub(crate) type Completion = Box<dyn FnOnce(Result<Response, BridgeError>) + Send + 'static>;

struct BufferedCall {
    request: Request,
    completion: Completion,
}

#[derive(Clone)]
pub struct RuntimeClient {
    command_tx: Sender<Command>,
    wake_writer: Arc<Mutex<UnixStream>>,
    response_timeout: Duration,
}

impl RuntimeClient {
    pub fn call(&self, request: Request) -> Result<Response, BridgeError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.submit(request, move |result| {
            let _ = reply_tx.send(result);
        })?;
        recv_response(reply_rx, self.response_timeout)?
    }

    pub fn submit<F>(&self, request: Request, completion: F) -> Result<(), BridgeError>
    where
        F: FnOnce(Result<Response, BridgeError>) + Send + 'static,
    {
        self.send_and_wake(Command::Call(Box::new(BufferedCall {
            request,
            completion: Box::new(completion),
        })))
    }

    pub fn force_gc(&self) -> Result<(), BridgeError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.send_and_wake(Command::ForceGc { reply: reply_tx })?;
        recv_response(reply_rx, self.response_timeout)?
    }

    fn request_shutdown(&self) -> Result<(), BridgeError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.send_and_wake(Command::Shutdown { reply: reply_tx })?;
        reply_rx
            .recv_timeout(self.response_timeout)
            .map_err(map_recv_timeout)
    }

    fn send_and_wake(&self, command: Command) -> Result<(), BridgeError> {
        self.command_tx
            .send(command)
            .map_err(|_| BridgeError::RuntimeStopped)?;
        wake_runtime(&self.wake_writer)
    }
}

pub struct RubyRuntime {
    client: Option<RuntimeClient>,
    info: RuntimeInfo,
    join: Option<JoinHandle<Result<(), BridgeError>>>,
}

impl RubyRuntime {
    pub fn start(app_source: impl Into<String>) -> Result<Self, BridgeError> {
        let (command_tx, command_rx) = mpsc::channel();
        let (wake_reader, wake_writer) =
            UnixStream::pair().map_err(|error| BridgeError::Io(error.to_string()))?;
        wake_reader
            .set_nonblocking(true)
            .map_err(|error| BridgeError::Io(error.to_string()))?;
        wake_writer
            .set_nonblocking(true)
            .map_err(|error| BridgeError::Io(error.to_string()))?;

        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let panic_ready_tx = ready_tx.clone();
        let app_source = app_source.into();
        let join = thread::Builder::new()
            .name("ruvoy-ruby-runtime".to_owned())
            .spawn(move || {
                match panic::catch_unwind(AssertUnwindSafe(|| {
                    runtime_main(app_source, command_rx, wake_reader, ready_tx)
                })) {
                    Ok(result) => result,
                    Err(payload) => {
                        let error = BridgeError::RuntimePanicked(panic_message(payload));
                        let _ = panic_ready_tx.send(Err(error.clone()));
                        Err(error)
                    }
                }
            })
            .map_err(|error| BridgeError::Spawn(error.to_string()))?;

        let info = match ready_rx.recv() {
            Ok(Ok(info)) => info,
            Ok(Err(error)) => {
                let _ = join.join();
                return Err(error);
            }
            Err(_) => {
                return match join.join() {
                    Ok(Err(error)) => Err(error),
                    Ok(Ok(())) => Err(BridgeError::Startup(
                        "runtime exited before reporting readiness".to_owned(),
                    )),
                    Err(payload) => Err(BridgeError::RuntimePanicked(panic_message(payload))),
                };
            }
        };

        Ok(Self {
            client: Some(RuntimeClient {
                command_tx,
                wake_writer: Arc::new(Mutex::new(wake_writer)),
                response_timeout: DEFAULT_RESPONSE_TIMEOUT,
            }),
            info,
            join: Some(join),
        })
    }

    pub fn start_default() -> Result<Self, BridgeError> {
        Self::start(DEFAULT_APP_SOURCE)
    }

    pub fn client(&self) -> RuntimeClient {
        self.client
            .as_ref()
            .expect("runtime client is present until shutdown")
            .clone()
    }

    pub fn info(&self) -> &RuntimeInfo {
        &self.info
    }

    pub fn shutdown(mut self) -> Result<(), BridgeError> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> Result<(), BridgeError> {
        let shutdown_result = match self.client.take() {
            Some(client) => client.request_shutdown(),
            None => Ok(()),
        };
        let join_result = match self.join.take() {
            Some(join) => join
                .join()
                .map_err(|payload| BridgeError::RuntimePanicked(panic_message(payload)))?,
            None => Ok(()),
        };
        shutdown_result?;
        join_result
    }
}

impl Drop for RubyRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

fn runtime_main(
    app_source: String,
    command_rx: Receiver<Command>,
    mut wake_reader: UnixStream,
    ready_tx: SyncSender<Result<RuntimeInfo, BridgeError>>,
) -> Result<(), BridgeError> {
    let cleanup = unsafe { magnus::embed::init() };
    let ruby = &*cleanup;

    let string_io_class = match ruby
        .require("stringio")
        .and_then(|_| ruby.class_object().const_get::<_, RClass>("StringIO"))
    {
        Ok(class) => class,
        Err(error) => {
            let error = ruby_error("loading StringIO", error);
            let _ = ready_tx.send(Err(error.clone()));
            return Err(error);
        }
    };
    let rack_errors = match ruby.eval::<Value>("STDERR") {
        Ok(value) => BoxValue::new(value),
        Err(error) => {
            let error = ruby_error("loading rack.errors", error);
            let _ = ready_tx.send(Err(error.clone()));
            return Err(error);
        }
    };
    let body_reader = match ruby.eval::<Value>(RACK_BODY_READER_SOURCE) {
        Ok(value) => BoxValue::new(value),
        Err(error) => {
            let error = ruby_error("loading Rack body reader", error);
            let _ = ready_tx.send(Err(error.clone()));
            return Err(error);
        }
    };

    let app = match ruby.eval::<Value>(&app_source) {
        Ok(value) => BoxValue::new(value),
        Err(error) => {
            let error = ruby_error("evaluating app source", error);
            let _ = ready_tx.send(Err(error.clone()));
            return Err(error);
        }
    };

    let info = match runtime_info(ruby) {
        Ok(info) => info,
        Err(error) => {
            let _ = ready_tx.send(Err(error.clone()));
            return Err(error);
        }
    };
    let context = RackCallContext {
        app: *app,
        string_io_class,
        rack_errors: *rack_errors,
        body_reader: *body_reader,
        ruby_thread_object_id: info.ruby_thread_object_id,
        concurrency: RackConcurrency::Serial,
    };
    ready_tx
        .send(Ok(info))
        .map_err(|_| BridgeError::Startup("starter dropped readiness channel".to_owned()))?;

    loop {
        ruby.thread_wait_fd(&wake_reader)
            .map_err(|error| ruby_error("waiting for bridge wakeup", error))?;
        drain_wake_bytes(&mut wake_reader)?;

        let mut should_shutdown = false;
        loop {
            match command_rx.try_recv() {
                Ok(Command::Call(call)) => {
                    (call.completion)(call_app(ruby, &context, call.request));
                }
                Ok(Command::ForceGc { reply }) => {
                    ruby.gc_start();
                    let _ = reply.send(Ok(()));
                }
                Ok(Command::Shutdown { reply }) => {
                    let _ = reply.send(());
                    should_shutdown = true;
                    break;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    should_shutdown = true;
                    break;
                }
            }
        }

        if should_shutdown {
            break;
        }
    }

    Ok(())
}

fn runtime_info(ruby: &Ruby) -> Result<RuntimeInfo, BridgeError> {
    let ruby_description = ruby
        .eval::<String>("RUBY_DESCRIPTION")
        .map_err(|error| ruby_error("reading RUBY_DESCRIPTION", error))?;
    let ruby_thread_object_id = ruby
        .eval::<i64>("Thread.current.object_id")
        .map_err(|error| ruby_error("reading Ruby thread object id", error))
        .and_then(to_u64_thread_id)?;

    Ok(RuntimeInfo {
        ruby_description,
        rust_thread_id: format!("{:?}", thread::current().id()),
        ruby_thread_object_id,
    })
}

struct RackEnvBuild {
    env: RHash,
    diagnostics: Option<RequestDiagnostics>,
    runtime_queue_time: Option<Duration>,
    rack_input_time: Duration,
}

fn build_rack_env(
    ruby: &Ruby,
    context: &RackCallContext,
    request: Request,
) -> Result<RackEnvBuild, BridgeError> {
    let &RackCallContext {
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
    env.aset("REQUEST_METHOD", method)
        .map_err(|error| ruby_error("setting REQUEST_METHOD", error))?;
    env.aset("SCRIPT_NAME", "")
        .map_err(|error| ruby_error("setting SCRIPT_NAME", error))?;
    env.aset("PATH_INFO", path_info)
        .map_err(|error| ruby_error("setting PATH_INFO", error))?;
    env.aset("QUERY_STRING", query_string)
        .map_err(|error| ruby_error("setting QUERY_STRING", error))?;
    env.aset("SERVER_NAME", metadata.server_name)
        .map_err(|error| ruby_error("setting SERVER_NAME", error))?;
    env.aset("SERVER_PORT", metadata.server_port.to_string())
        .map_err(|error| ruby_error("setting SERVER_PORT", error))?;
    env.aset("SERVER_PROTOCOL", metadata.protocol)
        .map_err(|error| ruby_error("setting SERVER_PROTOCOL", error))?;
    env.aset("HTTP_HOST", metadata.authority)
        .map_err(|error| ruby_error("setting HTTP_HOST", error))?;
    if let Some(remote_addr) = metadata.remote_addr {
        env.aset("REMOTE_ADDR", remote_addr)
            .map_err(|error| ruby_error("setting REMOTE_ADDR", error))?;
    }
    env.aset("rack.url_scheme", metadata.scheme)
        .map_err(|error| ruby_error("setting rack.url_scheme", error))?;
    env.aset("rack.errors", rack_errors)
        .map_err(|error| ruby_error("setting rack.errors", error))?;
    env.aset("rack.multithread", concurrency.multithread())
        .map_err(|error| ruby_error("setting rack.multithread", error))?;
    env.aset("rack.multiprocess", false)
        .map_err(|error| ruby_error("setting rack.multiprocess", error))?;
    env.aset("rack.run_once", false)
        .map_err(|error| ruby_error("setting rack.run_once", error))?;

    for (name, value) in headers {
        if let Some(key) = rack_env_header_name(&name) {
            env.aset(key, ruby.str_from_slice(&value))
                .map_err(|error| ruby_error("copying request header into Rack env", error))?;
        }
    }

    let rack_input_started_at = Instant::now();
    let rack_input = string_io_class
        .funcall::<_, _, Value>("new", (ruby.str_from_slice(&body),))
        .map_err(|error| ruby_error("creating rack.input StringIO", error))?;
    rack_input
        .funcall::<_, _, Value>("binmode", ())
        .map_err(|error| ruby_error("setting rack.input binary mode", error))?;
    env.aset("rack.input", rack_input)
        .map_err(|error| ruby_error("setting rack.input", error))?;
    env.aset("ruvoy.force_gc", force_gc)
        .map_err(|error| ruby_error("setting ruvoy.force_gc", error))?;

    Ok(RackEnvBuild {
        env,
        diagnostics,
        runtime_queue_time,
        rack_input_time: rack_input_started_at.elapsed(),
    })
}

fn stage_timing_headers(
    build: &RackEnvBuild,
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

    vec![
        ("x-ruvoy-stage-timing".to_owned(), "1".to_owned()),
        (
            "x-ruvoy-stage-ingress-ns".to_owned(),
            ingress_time.as_nanos().to_string(),
        ),
        (
            "x-ruvoy-stage-body-copy-ns".to_owned(),
            diagnostics.body_copy_time.as_nanos().to_string(),
        ),
        (
            "x-ruvoy-stage-body-callbacks".to_owned(),
            diagnostics.body_callbacks.to_string(),
        ),
        (
            "x-ruvoy-stage-body-reallocations".to_owned(),
            diagnostics.body_reallocations.to_string(),
        ),
        (
            "x-ruvoy-stage-declared-capacity".to_owned(),
            diagnostics.declared_capacity.to_string(),
        ),
        (
            "x-ruvoy-stage-runtime-queue-ns".to_owned(),
            build
                .runtime_queue_time
                .unwrap_or_default()
                .as_nanos()
                .to_string(),
        ),
        (
            "x-ruvoy-stage-rack-input-ns".to_owned(),
            build.rack_input_time.as_nanos().to_string(),
        ),
        (
            "x-ruvoy-stage-rack-call-ns".to_owned(),
            rack_call_time.as_nanos().to_string(),
        ),
        (
            "x-ruvoy-stage-response-copy-ns".to_owned(),
            response_copy_time.as_nanos().to_string(),
        ),
    ]
}

struct RackResponseParts {
    status: u16,
    headers: Vec<(String, String)>,
    body: Value,
}

fn rack_response_parts(rack_response: &RArray) -> Result<RackResponseParts, BridgeError> {
    if rack_response.len() != 3 {
        return Err(BridgeError::InvalidResponse(format!(
            "expected 3 entries, got {}",
            rack_response.len()
        )));
    }
    let status = rack_response
        .entry::<i64>(0)
        .map_err(|error| ruby_error("converting response status", error))
        .and_then(to_u16_status)?;
    let headers = copy_response_headers(
        rack_response
            .entry::<RHash>(1)
            .map_err(|error| ruby_error("converting response headers", error))?,
    )?;
    let body = rack_response
        .entry::<Value>(2)
        .map_err(|error| ruby_error("reading response body", error))?;

    Ok(RackResponseParts {
        status,
        headers,
        body,
    })
}

fn call_app(
    ruby: &Ruby,
    context: &RackCallContext,
    request: Request,
) -> Result<Response, BridgeError> {
    let &RackCallContext {
        app,
        body_reader,
        ruby_thread_object_id,
        ..
    } = context;
    let build = build_rack_env(ruby, context, request)?;

    let rack_call_started_at = Instant::now();
    let rack_response = app
        .funcall::<_, _, RArray>("call", (build.env,))
        .map_err(|error| ruby_error("calling app.call(env)", error))?;
    let rack_call_time = rack_call_started_at.elapsed();

    let response_copy_started_at = Instant::now();
    let RackResponseParts {
        status,
        mut headers,
        body: response_body,
    } = rack_response_parts(&rack_response)?;
    let response_body = body_reader
        .funcall::<_, _, RString>("call", (response_body,))
        .map_err(|error| ruby_error("consuming response body", error))?;
    // SAFETY: no Ruby calls occur while the borrowed bytes are copied.
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
pub(crate) fn call_app_streaming(
    ruby: &Ruby,
    context: &RackCallContext,
    request: Request,
    handle: &StreamHandle,
    sink: Value,
) -> Result<(), BridgeError> {
    let &RackCallContext {
        app,
        body_reader,
        ruby_thread_object_id,
        ..
    } = context;
    let build = build_rack_env(ruby, context, request)?;

    let rack_call_started_at = Instant::now();
    let rack_response = app
        .funcall::<_, _, RArray>("call", (build.env,))
        .map_err(|error| ruby_error("calling app.call(env)", error))?;
    let rack_call_time = rack_call_started_at.elapsed();

    let RackResponseParts {
        status,
        mut headers,
        body: response_body,
    } = rack_response_parts(&rack_response)?;
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

fn copy_response_headers(headers: RHash) -> Result<Vec<(String, String)>, BridgeError> {
    let mut result = Vec::with_capacity(headers.len());
    headers
        .foreach(|name: String, value: Value| {
            if let Some(values) = RArray::from_value(value) {
                for value in values.to_vec::<String>()? {
                    result.push((name.clone(), value));
                }
            } else {
                let value = String::try_convert(value)?;
                result.push((name, value));
            }
            Ok(ForEach::Continue)
        })
        .map_err(|error| ruby_error("copying response headers", error))?;

    Ok(result)
}

fn rack_env_header_name(name: &str) -> Option<String> {
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

fn wake_runtime(wake_writer: &Mutex<UnixStream>) -> Result<(), BridgeError> {
    let mut writer = wake_writer
        .lock()
        .map_err(|_| BridgeError::Io("wake writer mutex was poisoned".to_owned()))?;
    loop {
        match writer.write(&[1]) {
            Ok(1) => return Ok(()),
            Ok(_) => {
                return Err(BridgeError::Io(
                    "wake socket accepted zero bytes".to_owned(),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) => return Err(BridgeError::Io(error.to_string())),
        }
    }
}

fn drain_wake_bytes(wake_reader: &mut UnixStream) -> Result<(), BridgeError> {
    let mut buffer = [0_u8; 256];
    loop {
        match wake_reader.read(&mut buffer) {
            Ok(0) => {
                return Err(BridgeError::Io(
                    "wake socket closed before shutdown".to_owned(),
                ));
            }
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) => return Err(BridgeError::Io(error.to_string())),
        }
    }
}

fn recv_response<T>(
    receiver: Receiver<Result<T, BridgeError>>,
    timeout: Duration,
) -> Result<Result<T, BridgeError>, BridgeError> {
    receiver.recv_timeout(timeout).map_err(map_recv_timeout)
}

fn map_recv_timeout(error: RecvTimeoutError) -> BridgeError {
    match error {
        RecvTimeoutError::Timeout => BridgeError::ResponseTimeout,
        RecvTimeoutError::Disconnected => BridgeError::RuntimeStopped,
    }
}

fn ruby_error(context: &str, error: magnus::Error) -> BridgeError {
    BridgeError::Ruby(format!("{context}: {error}"))
}

fn to_u16_status(status: i64) -> Result<u16, BridgeError> {
    u16::try_from(status).map_err(|_| {
        BridgeError::InvalidResponse(format!("status {status} is outside the u16 range"))
    })
}

fn to_u64_thread_id(thread_id: i64) -> Result<u64, BridgeError> {
    u64::try_from(thread_id).map_err(|_| {
        BridgeError::InvalidResponse(format!("Ruby thread object id {thread_id} is negative"))
    })
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn cruby_is_owned_by_one_runtime_thread() {
        let runtime = RubyRuntime::start_default().expect("Ruby runtime should start");
        let client = runtime.client();
        let runtime_thread_id = runtime.info().ruby_thread_object_id;

        assert!(Ruby::get().is_err());

        let first = client
            .call(
                Request::new("POST", "/hello", b"world".to_vec())
                    .with_header("x-ruvoy-test", b"from-rust".to_vec()),
            )
            .expect("basic app call should succeed");
        assert_eq!(first.status, 200);
        assert_eq!(first.body, b"POST /hello world");
        assert_eq!(first.header("x-ruby-call-count"), Some("1"));
        assert_eq!(first.header("x-request-header"), Some("from-rust"));
        assert_eq!(first.ruby_thread_object_id, runtime_thread_id);

        let error = client
            .call(Request::new("GET", "/raise", Vec::new()))
            .expect_err("Ruby exception should cross the bridge as owned data");
        assert!(
            error.to_string().contains("intentional boom"),
            "unexpected error: {error}"
        );

        let (async_tx, async_rx) = mpsc::sync_channel(1);
        client
            .submit(
                Request::new("GET", "/async", b"callback".to_vec()),
                move |result| {
                    let _ = async_tx.send((format!("{:?}", thread::current().id()), result));
                },
            )
            .expect("asynchronous submission should succeed");
        let (completion_thread_id, async_result) = async_rx
            .recv_timeout(DEFAULT_RESPONSE_TIMEOUT)
            .expect("asynchronous completion should arrive");
        assert_eq!(completion_thread_id, runtime.info().rust_thread_id);
        assert_eq!(
            async_result
                .expect("asynchronous app call should succeed")
                .body,
            b"GET /async callback"
        );

        for _ in 0..25 {
            client.force_gc().expect("forced GC should succeed");
        }
        let after_gc = client
            .call(Request::new("GET", "/after-gc", b"alive".to_vec()).with_forced_gc())
            .expect("GC-rooted app should survive repeated collections");
        assert_eq!(after_gc.body, b"GET /after-gc alive");
        assert_eq!(after_gc.ruby_thread_object_id, runtime_thread_id);

        let workers = 8;
        let calls_per_worker = 50;
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let client = client.clone();
            handles.push(thread::spawn(move || {
                assert!(Ruby::get().is_err());
                let mut ruby_thread_ids = Vec::with_capacity(calls_per_worker);
                for call in 0..calls_per_worker {
                    let path = format!("/workers/{worker}/{call}");
                    let request = Request::new("PUT", path.clone(), b"payload".to_vec());
                    let response = client
                        .call(request)
                        .expect("concurrent call should succeed");
                    assert_eq!(response.status, 200);
                    assert_eq!(response.body, format!("PUT {path} payload").into_bytes(),);
                    ruby_thread_ids.push(response.ruby_thread_object_id);
                }
                ruby_thread_ids
            }));
        }

        let observed_thread_ids = handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("producer thread should not panic"))
            .collect::<HashSet<_>>();
        assert_eq!(observed_thread_ids, HashSet::from([runtime_thread_id]));

        let stale_client = client.clone();
        runtime
            .shutdown()
            .expect("runtime should shut down cleanly");
        assert_eq!(
            stale_client
                .call(Request::new("GET", "/after-shutdown", Vec::new()))
                .expect_err("calls after shutdown must fail"),
            BridgeError::RuntimeStopped
        );
    }
}
