//! The fiber runtime: one fiber per request, responses streamed as produced.
//!
//! Scheduler-aware Ruby I/O suspends its fiber instead of the thread, so a call
//! waiting on I/O leaves the runtime free to serve others.

use crate::{
    BridgeError, Budget, DEFAULT_RESPONSE_TIMEOUT, Lease, Request, Response, ResponseHead,
    ResponseStream, RuntimeInfo, StreamHandle, StreamItem, StreamWaker,
    error::{panic_message, ruby_error},
    rack::{self, CallContext, Concurrency},
    response::Completion,
    vm::runtime_info,
    wake,
};
use magnus::{RArray, RClass, RModule, Ruby, Value, kwargs, method, prelude::*, value::BoxValue};
use std::{
    cell::RefCell,
    fmt,
    os::{fd::AsRawFd, unix::net::UnixStream},
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, Sender, SyncSender, TryRecvError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// In-flight requests admitted before the runtime starts rejecting work.
pub const DEFAULT_MAX_INFLIGHT_REQUESTS: usize = 1024;

/// Drives the reactor that runs one fiber per request.
const FIBER_RUNNER_SOURCE: &str = include_str!("../ruby/fiber_runner.rb");

enum FiberApp {
    Source(String),
    Rackup(PathBuf),
}

enum FiberCommand {
    Call(Box<FiberCall>),
    Shutdown { reply: SyncSender<()> },
}

struct FiberCall {
    request: Request,
    handle: StreamHandle,
    /// Held for the lifetime of the response so admission capacity is released
    /// only once the body has been fully produced, not when the head is sent.
    permit: Lease,
}

/// How long the reactor loop has gone without running.
///
/// The loop waits with a deadline, so it reports in regularly whether or not
/// there is work. A growing age therefore means some fiber is holding the
/// thread instead of yielding, which stalls every other request.
#[derive(Debug)]
struct ReactorHeartbeat {
    started_at: Instant,
    last_pass_ms: AtomicU64,
}

impl ReactorHeartbeat {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            last_pass_ms: AtomicU64::new(0),
        }
    }

    fn record_pass(&self) {
        self.last_pass_ms
            .store(self.elapsed_ms(), Ordering::Relaxed);
    }

    fn idle_for(&self) -> Duration {
        Duration::from_millis(
            self.elapsed_ms()
                .saturating_sub(self.last_pass_ms.load(Ordering::Relaxed)),
        )
    }

    fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }
}

/// The Ruby-facing half of a streaming response.
#[magnus::wrap(class = "Ruvoy::StreamSink", free_immediately)]
struct StreamSink {
    handle: StreamHandle,
}

impl StreamSink {
    fn write(&self, chunk: magnus::RString) -> bool {
        // SAFETY: the bytes are copied before any further Ruby call can run.
        // SAFETY: the bytes are copied before control returns to Ruby, so the
        // string cannot be moved or collected while the slice is alive.
        let bytes = unsafe { chunk.as_slice() }.to_vec();
        if !self.handle.stream.push_chunk(bytes) {
            return false;
        }
        self.handle.wake();
        true
    }

    fn writable(&self) -> bool {
        !self.handle.stream.is_saturated()
    }

    fn cancelled(&self) -> bool {
        self.handle.stream.is_cancelled()
    }
}

#[magnus::wrap(class = "Ruvoy::FiberEnvelope", free_immediately)]
struct FiberEnvelope {
    /// Kept past the call itself so a fiber can still be asked whether the
    /// client it was serving has gone away.
    stream: Arc<ResponseStream>,
    call: RefCell<Option<FiberCall>>,
}

impl FiberEnvelope {
    fn cancelled(&self) -> bool {
        self.stream.is_cancelled()
    }
}

#[magnus::wrap(class = "Ruvoy::FiberBridge", free_immediately)]
struct FiberBridge {
    command_rx: RefCell<Receiver<FiberCommand>>,
    wake_reader: RefCell<UnixStream>,
    shutdown_reply: RefCell<Option<SyncSender<()>>>,
    ruby_thread_object_id: u64,
    heartbeat: Arc<ReactorHeartbeat>,
}

impl FiberBridge {
    fn fd(&self) -> i32 {
        self.wake_reader.borrow().as_raw_fd()
    }

    fn drain(ruby: &Ruby, bridge: &Self) -> Result<RArray, magnus::Error> {
        // Called once per pass of the reactor loop, whether or not work arrived.
        bridge.heartbeat.record_pass();
        wake::drain(&mut bridge.wake_reader.borrow_mut())
            .map_err(|error| fiber_bridge_error(ruby, error))?;

        let envelopes = ruby.ary_new();
        let mut shutting_down = false;
        loop {
            match bridge.command_rx.borrow().try_recv() {
                Ok(FiberCommand::Call(call)) => {
                    let envelope = ruby.obj_wrap(FiberEnvelope {
                        stream: Arc::clone(&call.handle.stream),
                        call: RefCell::new(Some(*call)),
                    });
                    envelopes.push(envelope)?;
                }
                Ok(FiberCommand::Shutdown { reply }) => {
                    *bridge.shutdown_reply.borrow_mut() = Some(reply);
                    shutting_down = true;
                    break;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    shutting_down = true;
                    break;
                }
            }
        }

        let batch = ruby.ary_new();
        batch.push(envelopes)?;
        batch.push(shutting_down)?;
        Ok(batch)
    }

    fn execute(
        ruby: &Ruby,
        bridge: &Self,
        envelope: &FiberEnvelope,
        app: Value,
        rack_errors: Value,
        body_reader: Value,
    ) -> Result<(), magnus::Error> {
        let call =
            envelope.call.borrow_mut().take().ok_or_else(|| {
                magnus::Error::new(ruby.exception_runtime_error(), "request reused")
            })?;
        let context = CallContext {
            app,
            string_io_class: ruby.class_object().const_get::<_, RClass>("StringIO")?,
            rack_errors,
            body_reader,
            ruby_thread_object_id: bridge.ruby_thread_object_id,
            concurrency: Concurrency::Interleaved,
        };
        let sink = ruby.obj_wrap(StreamSink {
            handle: call.handle.clone(),
        });
        let result =
            rack::call_streaming(ruby, &context, call.request, &call.handle, sink.as_value());
        if let Err(error) = result {
            // The worker decides what a failure means: a local reply if nothing
            // has been sent yet, otherwise resetting the half-written stream.
            call.handle.stream.push_failure(error);
            call.handle.wake();
        }
        drop(call.permit);
        Ok(())
    }

    fn ack_shutdown(&self) -> bool {
        self.shutdown_reply
            .borrow_mut()
            .take()
            .is_some_and(|reply| reply.send(()).is_ok())
    }
}

struct CollectingResponse {
    head: Option<ResponseHead>,
    body: Vec<u8>,
    completion: Option<Completion>,
}

impl CollectingResponse {
    fn finish(&mut self, result: Result<Response, BridgeError>) {
        if let Some(completion) = self.completion.take() {
            completion(result);
        }
    }
}

fn fiber_bridge_error(ruby: &Ruby, error: BridgeError) -> magnus::Error {
    magnus::Error::new(ruby.exception_runtime_error(), error.to_string())
}

/// A cloneable handle used to submit work from any thread.
#[derive(Clone)]
pub struct FiberRuntimeClient {
    command_tx: Sender<FiberCommand>,
    wake_writer: Arc<Mutex<UnixStream>>,
    admission: Budget,
    heartbeat: Arc<ReactorHeartbeat>,
}

impl FiberRuntimeClient {
    /// Collects a streamed response into one buffer.
    ///
    /// Used by tests and probes; the Envoy filter streams instead.
    pub fn call(&self, request: Request) -> Result<Response, BridgeError> {
        let (ready_tx, ready_rx) = mpsc::sync_channel::<()>(1);
        let waker: StreamWaker = Arc::new(move || {
            let _ = ready_tx.try_send(());
        });
        let stream = Arc::new(ResponseStream::new(usize::MAX));
        let handle = StreamHandle::new(Arc::clone(&stream), waker);
        self.submit(request, handle)?;

        let deadline = std::time::Instant::now() + DEFAULT_RESPONSE_TIMEOUT;
        let mut head: Option<ResponseHead> = None;
        let mut body = Vec::new();
        loop {
            while let Some(item) = stream.take_next() {
                match item {
                    StreamItem::Head(value) => head = Some(value),
                    StreamItem::Chunk(chunk) => body.extend_from_slice(&chunk),
                    StreamItem::Failed(error) => return Err(error),
                    StreamItem::End => {
                        let head = head.ok_or_else(|| {
                            BridgeError::InvalidResponse("stream ended without a head".to_owned())
                        })?;
                        return Ok(Response {
                            status: head.status,
                            headers: head.headers,
                            body,
                            ruby_thread_object_id: head.ruby_thread_object_id,
                        });
                    }
                }
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(BridgeError::ResponseTimeout);
            }
            let _ = ready_rx.recv_timeout(remaining);
        }
    }

    /// Streams a response but reports it as one buffered result.
    ///
    /// Callers that do not care about incremental delivery (probes, examples)
    /// use this; the Envoy filter consumes the stream directly.
    pub fn submit_collected<F>(&self, request: Request, completion: F) -> Result<(), BridgeError>
    where
        F: FnOnce(Result<Response, BridgeError>) + Send + 'static,
    {
        let stream = Arc::new(ResponseStream::new(usize::MAX));
        let state = Arc::new(Mutex::new(CollectingResponse {
            head: None,
            body: Vec::new(),
            completion: Some(Box::new(completion)),
        }));
        let waker_stream = Arc::clone(&stream);
        let waker: StreamWaker = Arc::new(move || {
            let mut state = match state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            while let Some(item) = waker_stream.take_next() {
                match item {
                    StreamItem::Head(head) => state.head = Some(head),
                    StreamItem::Chunk(chunk) => state.body.extend_from_slice(&chunk),
                    StreamItem::Failed(error) => state.finish(Err(error)),
                    StreamItem::End => {
                        let result = match state.head.take() {
                            Some(head) => Ok(Response {
                                status: head.status,
                                headers: head.headers,
                                body: std::mem::take(&mut state.body),
                                ruby_thread_object_id: head.ruby_thread_object_id,
                            }),
                            None => Err(BridgeError::InvalidResponse(
                                "stream ended without a head".to_owned(),
                            )),
                        };
                        state.finish(result);
                    }
                }
            }
        });

        self.submit(request, StreamHandle::new(stream, waker))
    }

    /// In-flight requests currently admitted.
    #[must_use]
    pub fn inflight_requests(&self) -> usize {
        self.admission.used()
    }

    /// How long the reactor loop has gone without running.
    ///
    /// Anything beyond the loop's own deadline means a fiber is blocking the
    /// thread rather than yielding, so every other request is waiting on it.
    #[must_use]
    pub fn reactor_idle_for(&self) -> Duration {
        self.heartbeat.idle_for()
    }

    /// Queues the request and streams its response into `handle`.
    pub fn submit(&self, request: Request, handle: StreamHandle) -> Result<(), BridgeError> {
        let permit = self
            .admission
            .try_acquire(1)
            .ok_or(BridgeError::Overloaded)?;
        self.send_and_wake(FiberCommand::Call(Box::new(FiberCall {
            request,
            handle,
            permit,
        })))
    }

    fn request_shutdown(&self, timeout: Duration) -> Result<(), BridgeError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.send_and_wake(FiberCommand::Shutdown { reply: reply_tx })?;
        reply_rx
            .recv_timeout(timeout)
            .map_err(wake::map_recv_timeout)
    }

    fn send_and_wake(&self, command: FiberCommand) -> Result<(), BridgeError> {
        self.command_tx
            .send(command)
            .map_err(|_| BridgeError::RuntimeStopped)?;
        wake::signal(&self.wake_writer)
    }
}

impl fmt::Debug for FiberRuntimeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FiberRuntimeClient")
            .field("admission", &self.admission)
            .finish_non_exhaustive()
    }
}

/// Owns the Ruby VM, its reactor thread, and the admission budget.
#[derive(Debug)]
pub struct FiberRuntime {
    client: Option<FiberRuntimeClient>,
    info: Option<RuntimeInfo>,
    join: Option<JoinHandle<Result<(), BridgeError>>>,
}

/// A runtime that started its thread, whether or not the application loaded.
///
/// A failed application still owns a live Ruby VM, so the runtime is returned
/// alongside the error rather than dropped: tearing the VM down mid-process is
/// what the caller must avoid.
#[derive(Debug)]
pub struct FiberRuntimeStartup {
    runtime: FiberRuntime,
    startup_error: Option<BridgeError>,
}

impl FiberRuntimeStartup {
    /// Splits the runtime from the error the application raised, if any.
    #[must_use]
    pub fn into_parts(self) -> (FiberRuntime, Option<BridgeError>) {
        (self.runtime, self.startup_error)
    }

    fn into_result(self) -> Result<FiberRuntime, BridgeError> {
        match self.startup_error {
            Some(error) => {
                std::mem::forget(self.runtime);
                Err(error)
            }
            None => Ok(self.runtime),
        }
    }
}

impl FiberRuntime {
    /// Starts a runtime whose application is the value `app_source` evaluates to.
    pub fn start(app_source: impl Into<String>) -> Result<Self, BridgeError> {
        Self::start_with_limit(app_source, DEFAULT_MAX_INFLIGHT_REQUESTS)
    }

    /// Starts an application source with a bounded number of in-flight requests.
    pub fn start_with_limit(
        app_source: impl Into<String>,
        max_inflight_requests: usize,
    ) -> Result<Self, BridgeError> {
        Self::start_app_retained(FiberApp::Source(app_source.into()), max_inflight_requests)?
            .into_result()
    }

    /// Starts a Fiber runtime with the application built from a rackup file.
    pub fn start_rackup(path: impl AsRef<Path>) -> Result<Self, BridgeError> {
        Self::start_rackup_with_limit(path, DEFAULT_MAX_INFLIGHT_REQUESTS)
    }

    /// Starts a rackup application with a bounded number of in-flight requests.
    pub fn start_rackup_with_limit(
        path: impl AsRef<Path>,
        max_inflight_requests: usize,
    ) -> Result<Self, BridgeError> {
        Self::start_rackup_with_limit_retained(path, max_inflight_requests)?.into_result()
    }

    /// Starts a rackup application, keeping the VM alive even if loading fails.
    pub fn start_rackup_with_limit_retained(
        path: impl AsRef<Path>,
        max_inflight_requests: usize,
    ) -> Result<FiberRuntimeStartup, BridgeError> {
        let path = path.as_ref();
        let rackup = path.canonicalize().map_err(|error| {
            BridgeError::Startup(format!(
                "failed to resolve rackup {}: {error}",
                path.display()
            ))
        })?;
        if !rackup.is_file() {
            return Err(BridgeError::Startup(format!(
                "rackup is not a file: {}",
                rackup.display()
            )));
        }
        Self::start_app_retained(FiberApp::Rackup(rackup), max_inflight_requests)
    }

    fn start_app_retained(
        app: FiberApp,
        max_inflight_requests: usize,
    ) -> Result<FiberRuntimeStartup, BridgeError> {
        if max_inflight_requests == 0 {
            return Err(BridgeError::Startup(
                "max in-flight requests must be positive".to_owned(),
            ));
        }
        let (command_tx, command_rx) = mpsc::channel();
        let (wake_reader, wake_writer) =
            UnixStream::pair().map_err(|error| BridgeError::Io(error.to_string()))?;
        wake_reader
            .set_nonblocking(true)
            .map_err(|error| BridgeError::Io(error.to_string()))?;
        wake_writer
            .set_nonblocking(true)
            .map_err(|error| BridgeError::Io(error.to_string()))?;

        let heartbeat = Arc::new(ReactorHeartbeat::new());
        let runtime_heartbeat = Arc::clone(&heartbeat);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name("ruvoy-ruby-fiber-runtime".to_owned())
            .spawn(move || {
                fiber_runtime_main(app, command_rx, wake_reader, ready_tx, runtime_heartbeat)
            })
            .map_err(|error| BridgeError::Spawn(error.to_string()))?;

        let readiness = match ready_rx.recv() {
            Ok(readiness) => readiness,
            Err(_) => Err(BridgeError::Startup(
                "fiber runtime exited before reporting readiness".to_owned(),
            )),
        };
        let (info, startup_error) = match readiness {
            Ok(info) => (Some(info), None),
            Err(error) => (None, Some(error)),
        };
        let runtime = Self {
            client: Some(FiberRuntimeClient {
                command_tx,
                wake_writer: Arc::new(Mutex::new(wake_writer)),
                admission: Budget::new(max_inflight_requests),
                heartbeat,
            }),
            info,
            join: Some(join),
        };

        Ok(FiberRuntimeStartup {
            runtime,
            startup_error,
        })
    }

    /// Returns a handle for submitting work.
    ///
    /// # Panics
    ///
    /// Panics if called after shutdown.
    pub fn client(&self) -> FiberRuntimeClient {
        self.client
            .as_ref()
            .expect("fiber runtime client is present until shutdown")
            .clone()
    }

    /// Identity of the embedded VM.
    ///
    /// # Panics
    ///
    /// Panics if the application failed to load.
    pub fn info(&self) -> &RuntimeInfo {
        self.info
            .as_ref()
            .expect("fiber runtime info is only available after successful startup")
    }

    /// Performs the final Ruby VM shutdown.
    ///
    /// # Safety
    ///
    /// This must only run at the end of the process, after all possible Ruby
    /// execution and all clones of the runtime client have stopped.
    pub unsafe fn shutdown(mut self) -> Result<(), BridgeError> {
        self.shutdown_inner(DEFAULT_RESPONSE_TIMEOUT)
    }

    /// Performs the final Ruby VM shutdown with a bounded wait.
    ///
    /// # Safety
    ///
    /// This has the same process-lifetime requirements as [`Self::shutdown`].
    pub unsafe fn shutdown_with_timeout(mut self, timeout: Duration) -> Result<(), BridgeError> {
        self.shutdown_inner(timeout)
    }

    fn shutdown_inner(&mut self, timeout: Duration) -> Result<(), BridgeError> {
        let shutdown_result = match self.client.take() {
            Some(client) => client.request_shutdown(timeout),
            None => Ok(()),
        };
        if let Err(error) = shutdown_result {
            self.join.take();
            return Err(error);
        }
        match self.join.take() {
            Some(join) => join
                .join()
                .map_err(|payload| BridgeError::RuntimePanicked(panic_message(payload)))?,
            None => Ok(()),
        }
    }
}

impl Drop for FiberRuntime {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            std::mem::forget(client);
        }
        if let Some(join) = self.join.take() {
            std::mem::forget(join);
        }
    }
}

fn fiber_runtime_main(
    app: FiberApp,
    command_rx: Receiver<FiberCommand>,
    wake_reader: UnixStream,
    ready_tx: SyncSender<Result<RuntimeInfo, BridgeError>>,
    heartbeat: Arc<ReactorHeartbeat>,
) -> Result<(), BridgeError> {
    // SAFETY: this thread owns the process-wide VM for its whole lifetime and
    // never hands a Ruby handle to another thread.
    let cleanup = unsafe { magnus::embed::init() };
    let ruby = &*cleanup;

    let prepared = panic::catch_unwind(AssertUnwindSafe(|| prepare_fiber_runtime(ruby, app)));
    let (app, rack_errors, body_reader, runner, info) = match prepared {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(error)) => {
            let _ = ready_tx.send(Err(error.clone()));
            retain_failed_runtime(command_rx, error.clone());
            return Err(error);
        }
        Err(payload) => {
            let error = BridgeError::RuntimePanicked(panic_message(payload));
            let _ = ready_tx.send(Err(error.clone()));
            retain_failed_runtime(command_rx, error.clone());
            return Err(error);
        }
    };

    let bridge = ruby.obj_wrap(FiberBridge {
        command_rx: RefCell::new(command_rx),
        wake_reader: RefCell::new(wake_reader),
        shutdown_reply: RefCell::new(None),
        ruby_thread_object_id: info.ruby_thread_object_id,
        heartbeat,
    });
    if ready_tx.send(Ok(info)).is_err() {
        std::mem::forget(cleanup);
        return Err(BridgeError::Startup(
            "starter dropped fiber readiness channel".to_owned(),
        ));
    }

    let run_result = panic::catch_unwind(AssertUnwindSafe(|| {
        runner
            .funcall::<_, _, Value>("call", (bridge, *app, *rack_errors, *body_reader))
            .map_err(|error| ruby_error("running Async reactor", error))
    }));
    match run_result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => {
            std::mem::forget(cleanup);
            Err(error)
        }
        Err(payload) => {
            let error = BridgeError::RuntimePanicked(panic_message(payload));
            std::mem::forget(cleanup);
            Err(error)
        }
    }
}

type PreparedFiberRuntime = (
    BoxValue<Value>,
    BoxValue<Value>,
    BoxValue<Value>,
    BoxValue<Value>,
    RuntimeInfo,
);

fn prepare_fiber_runtime(ruby: &Ruby, app: FiberApp) -> Result<PreparedFiberRuntime, BridgeError> {
    for feature in ["bundler/setup", "stringio", "io/wait", "async"] {
        ruby.require(feature)
            .map_err(|error| ruby_error(&format!("loading {feature}"), error))?;
    }

    let ruvoy_module = ruby
        .define_module("Ruvoy")
        .map_err(|error| ruby_error("defining Ruvoy module", error))?;
    let bridge_class = ruvoy_module
        .define_class("FiberBridge", ruby.class_object())
        .map_err(|error| ruby_error("defining FiberBridge", error))?;
    bridge_class
        .define_method("fd", method!(FiberBridge::fd, 0))
        .and_then(|_| bridge_class.define_method("drain", method!(FiberBridge::drain, 0)))
        .and_then(|_| bridge_class.define_method("execute", method!(FiberBridge::execute, 4)))
        .and_then(|_| {
            bridge_class.define_method("ack_shutdown", method!(FiberBridge::ack_shutdown, 0))
        })
        .map_err(|error| ruby_error("defining FiberBridge methods", error))?;
    let envelope_class = ruvoy_module
        .define_class("FiberEnvelope", ruby.class_object())
        .map_err(|error| ruby_error("defining FiberEnvelope", error))?;
    envelope_class
        .define_method("cancelled?", method!(FiberEnvelope::cancelled, 0))
        .map_err(|error| ruby_error("defining FiberEnvelope methods", error))?;
    let sink_class = ruvoy_module
        .define_class("StreamSink", ruby.class_object())
        .map_err(|error| ruby_error("defining StreamSink", error))?;
    sink_class
        .define_method("write", method!(StreamSink::write, 1))
        .and_then(|_| sink_class.define_method("writable?", method!(StreamSink::writable, 0)))
        .and_then(|_| sink_class.define_method("cancelled?", method!(StreamSink::cancelled, 0)))
        .map_err(|error| ruby_error("defining StreamSink methods", error))?;

    let app = BoxValue::new(load_fiber_app(ruby, app)?);
    let rack_errors = BoxValue::new(
        ruby.eval::<Value>("STDERR")
            .map_err(|error| ruby_error("loading rack.errors", error))?,
    );
    let body_reader = BoxValue::new(
        ruby.eval::<Value>(rack::STREAMING_BODY_READER_SOURCE)
            .map_err(|error| ruby_error("loading Rack body reader", error))?,
    );
    let runner = BoxValue::new(
        ruby.eval::<Value>(FIBER_RUNNER_SOURCE)
            .map_err(|error| ruby_error("evaluating fiber runner", error))?,
    );
    let info = runtime_info(ruby)?;

    Ok((app, rack_errors, body_reader, runner, info))
}

fn retain_failed_runtime(command_rx: Receiver<FiberCommand>, startup_error: BridgeError) {
    loop {
        match command_rx.recv() {
            Ok(FiberCommand::Call(call)) => {
                call.handle.stream.push_failure(startup_error.clone());
                call.handle.wake();
            }
            Ok(FiberCommand::Shutdown { reply }) => {
                let _ = reply.send(());
                return;
            }
            Err(_) => loop {
                thread::park();
            },
        }
    }
}

fn load_fiber_app(ruby: &Ruby, app: FiberApp) -> Result<Value, BridgeError> {
    match app {
        FiberApp::Source(source) => ruby
            .eval(&source)
            .map_err(|error| ruby_error("evaluating fiber app source", error)),
        FiberApp::Rackup(path) => {
            ruby.require("rack")
                .map_err(|error| ruby_error("loading Rack", error))?;
            let path = path.to_str().ok_or_else(|| {
                BridgeError::Startup(format!(
                    "rackup path must contain valid UTF-8: {}",
                    path.display()
                ))
            })?;
            let rack = ruby
                .class_object()
                .const_get::<_, RModule>("Rack")
                .map_err(|error| ruby_error("loading Rack module", error))?;
            let builder = rack
                .const_get::<_, RClass>("Builder")
                .map_err(|error| ruby_error("loading Rack::Builder", error))?;
            builder
                .funcall(
                    "parse_file",
                    (path, kwargs!(ruby, "isolation" => ruby.to_symbol("fiber"))),
                )
                .map_err(|error| ruby_error("loading rackup", error))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_runtime_rejects_calls_and_waits_for_final_shutdown() {
        let (command_tx, command_rx) = mpsc::channel();
        let startup_error = BridgeError::Startup("invalid rackup".to_owned());
        let expected_error = startup_error.clone();
        let owner = thread::spawn(move || retain_failed_runtime(command_rx, startup_error));

        let (woken_tx, woken_rx) = mpsc::sync_channel(1);
        let stream = Arc::new(ResponseStream::new(1024));
        let admission = Budget::new(1);
        command_tx
            .send(FiberCommand::Call(Box::new(FiberCall {
                request: Request::new("GET", "/", Vec::new()),
                handle: StreamHandle::new(
                    Arc::clone(&stream),
                    Arc::new(move || {
                        let _ = woken_tx.try_send(());
                    }),
                ),
                permit: admission.try_acquire(1).expect("capacity should be free"),
            })))
            .expect("failed runtime should remain available to reject work");
        woken_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("failed runtime should wake the waiting worker");
        assert!(matches!(
            stream.take_next(),
            Some(StreamItem::Failed(error)) if error == expected_error
        ));

        let (shutdown_tx, shutdown_rx) = mpsc::sync_channel(1);
        command_tx
            .send(FiberCommand::Shutdown { reply: shutdown_tx })
            .expect("failed runtime should accept final shutdown");
        shutdown_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("failed runtime should acknowledge final shutdown");
        owner.join().expect("failed runtime owner should stop");
    }
}
