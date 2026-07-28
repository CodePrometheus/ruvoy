use super::{
    BridgeError, Completion, DEFAULT_RESPONSE_TIMEOUT, RACK_BODY_READER_SOURCE, Request, Response,
    RuntimeInfo, call_app, drain_wake_bytes, map_recv_timeout, panic_message, ruby_error,
    runtime_info, wake_runtime,
};
use magnus::{RArray, RClass, RModule, Ruby, Value, kwargs, method, prelude::*, value::BoxValue};
use std::{
    cell::RefCell,
    os::{fd::AsRawFd, unix::net::UnixStream},
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender, SyncSender, TryRecvError},
    },
    thread::{self, JoinHandle},
};

pub const DEFAULT_MAX_INFLIGHT_REQUESTS: usize = 1024;

const FIBER_RUNNER_SOURCE: &str = r#"
lambda do |bridge, app, rack_errors, body_reader|
  io = IO.for_fd(bridge.fd, autoclose: false)

  begin
    Async do |parent|
      shutting_down = false
      until shutting_down
        io.wait_readable
        envelopes, shutting_down = bridge.drain
        envelopes.each do |envelope|
          parent.async(envelope) do |_task, current_envelope|
            bridge.execute(current_envelope, app, rack_errors, body_reader)
          end
        end
      end
      parent.wait_all
    end
  ensure
    bridge.ack_shutdown
  end
end
"#;

enum FiberApp {
    Source(String),
    Rackup(PathBuf),
}

enum FiberCommand {
    Call {
        request: Request,
        completion: Completion,
    },
    Shutdown {
        reply: SyncSender<()>,
    },
}

struct FiberCall {
    request: Request,
    completion: Completion,
}

#[magnus::wrap(class = "Ruvoy::FiberEnvelope", free_immediately)]
struct FiberEnvelope(RefCell<Option<FiberCall>>);

#[magnus::wrap(class = "Ruvoy::FiberBridge", free_immediately)]
struct FiberBridge {
    command_rx: RefCell<Receiver<FiberCommand>>,
    wake_reader: RefCell<UnixStream>,
    shutdown_reply: RefCell<Option<SyncSender<()>>>,
    ruby_thread_object_id: u64,
}

impl FiberBridge {
    fn fd(&self) -> i32 {
        self.wake_reader.borrow().as_raw_fd()
    }

    fn drain(ruby: &Ruby, bridge: &Self) -> Result<RArray, magnus::Error> {
        drain_wake_bytes(&mut bridge.wake_reader.borrow_mut())
            .map_err(|error| fiber_bridge_error(ruby, error))?;

        let envelopes = ruby.ary_new();
        let mut shutting_down = false;
        loop {
            match bridge.command_rx.borrow().try_recv() {
                Ok(FiberCommand::Call {
                    request,
                    completion,
                }) => {
                    let envelope = ruby.obj_wrap(FiberEnvelope(RefCell::new(Some(FiberCall {
                        request,
                        completion,
                    }))));
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
            envelope.0.borrow_mut().take().ok_or_else(|| {
                magnus::Error::new(ruby.exception_runtime_error(), "request reused")
            })?;
        let string_io_class = ruby.class_object().const_get::<_, RClass>("StringIO")?;
        let result = call_app(
            ruby,
            app,
            string_io_class,
            rack_errors,
            body_reader,
            bridge.ruby_thread_object_id,
            call.request,
        );
        (call.completion)(result);
        Ok(())
    }

    fn ack_shutdown(&self) -> bool {
        self.shutdown_reply
            .borrow_mut()
            .take()
            .is_some_and(|reply| reply.send(()).is_ok())
    }
}

fn fiber_bridge_error(ruby: &Ruby, error: BridgeError) -> magnus::Error {
    magnus::Error::new(ruby.exception_runtime_error(), error.to_string())
}

#[derive(Clone)]
pub struct FiberRuntimeClient {
    command_tx: Sender<FiberCommand>,
    wake_writer: Arc<Mutex<UnixStream>>,
    admission: Arc<RequestAdmission>,
}

impl FiberRuntimeClient {
    pub fn call(&self, request: Request) -> Result<Response, BridgeError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.submit(request, move |result| {
            let _ = reply_tx.send(result);
        })?;
        reply_rx
            .recv_timeout(DEFAULT_RESPONSE_TIMEOUT)
            .map_err(map_recv_timeout)?
    }

    pub fn submit<F>(&self, request: Request, completion: F) -> Result<(), BridgeError>
    where
        F: FnOnce(Result<Response, BridgeError>) + Send + 'static,
    {
        let permit = self.admission.try_acquire()?;
        self.send_and_wake(FiberCommand::Call {
            request,
            completion: Box::new(move |result| {
                completion(result);
                drop(permit);
            }),
        })
    }

    fn request_shutdown(&self) -> Result<(), BridgeError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.send_and_wake(FiberCommand::Shutdown { reply: reply_tx })?;
        reply_rx
            .recv_timeout(DEFAULT_RESPONSE_TIMEOUT)
            .map_err(map_recv_timeout)
    }

    fn send_and_wake(&self, command: FiberCommand) -> Result<(), BridgeError> {
        self.command_tx
            .send(command)
            .map_err(|_| BridgeError::RuntimeStopped)?;
        wake_runtime(&self.wake_writer)
    }
}

pub struct FiberRuntime {
    client: Option<FiberRuntimeClient>,
    info: RuntimeInfo,
    join: Option<JoinHandle<Result<(), BridgeError>>>,
}

impl FiberRuntime {
    pub fn start(app_source: impl Into<String>) -> Result<Self, BridgeError> {
        Self::start_with_limit(app_source, DEFAULT_MAX_INFLIGHT_REQUESTS)
    }

    pub fn start_with_limit(
        app_source: impl Into<String>,
        max_inflight_requests: usize,
    ) -> Result<Self, BridgeError> {
        Self::start_app(FiberApp::Source(app_source.into()), max_inflight_requests)
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
        Self::start_app(FiberApp::Rackup(rackup), max_inflight_requests)
    }

    fn start_app(app: FiberApp, max_inflight_requests: usize) -> Result<Self, BridgeError> {
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

        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let panic_ready_tx = ready_tx.clone();
        let join = thread::Builder::new()
            .name("ruvoy-ruby-fiber-runtime".to_owned())
            .spawn(move || {
                match panic::catch_unwind(AssertUnwindSafe(|| {
                    fiber_runtime_main(app, command_rx, wake_reader, ready_tx)
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
                        "fiber runtime exited before reporting readiness".to_owned(),
                    )),
                    Err(payload) => Err(BridgeError::RuntimePanicked(panic_message(payload))),
                };
            }
        };

        Ok(Self {
            client: Some(FiberRuntimeClient {
                command_tx,
                wake_writer: Arc::new(Mutex::new(wake_writer)),
                admission: Arc::new(RequestAdmission::new(max_inflight_requests)),
            }),
            info,
            join: Some(join),
        })
    }

    pub fn client(&self) -> FiberRuntimeClient {
        self.client
            .as_ref()
            .expect("fiber runtime client is present until shutdown")
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

struct RequestAdmission {
    active: AtomicUsize,
    maximum: usize,
}

impl RequestAdmission {
    fn new(maximum: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            maximum,
        }
    }

    fn try_acquire(self: &Arc<Self>) -> Result<RequestPermit, BridgeError> {
        let mut active = self.active.load(Ordering::Relaxed);
        loop {
            if active >= self.maximum {
                return Err(BridgeError::Overloaded);
            }
            match self.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(RequestPermit {
                        admission: Arc::clone(self),
                    });
                }
                Err(observed) => active = observed,
            }
        }
    }
}

struct RequestPermit {
    admission: Arc<RequestAdmission>,
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        let previous = self.admission.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

impl Drop for FiberRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

fn fiber_runtime_main(
    app: FiberApp,
    command_rx: Receiver<FiberCommand>,
    wake_reader: UnixStream,
    ready_tx: SyncSender<Result<RuntimeInfo, BridgeError>>,
) -> Result<(), BridgeError> {
    let cleanup = unsafe { magnus::embed::init() };
    let ruby = &*cleanup;

    for feature in ["bundler/setup", "stringio", "io/wait", "async"] {
        if let Err(error) = ruby.require(feature) {
            let error = ruby_error(&format!("loading {feature}"), error);
            let _ = ready_tx.send(Err(error.clone()));
            return Err(error);
        }
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
    ruvoy_module
        .define_class("FiberEnvelope", ruby.class_object())
        .map_err(|error| ruby_error("defining FiberEnvelope", error))?;

    let app = match load_fiber_app(ruby, app) {
        Ok(value) => BoxValue::new(value),
        Err(error) => {
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
    let runner = match ruby.eval::<Value>(FIBER_RUNNER_SOURCE) {
        Ok(value) => BoxValue::new(value),
        Err(error) => {
            let error = ruby_error("evaluating fiber runner", error);
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
    let bridge = ruby.obj_wrap(FiberBridge {
        command_rx: RefCell::new(command_rx),
        wake_reader: RefCell::new(wake_reader),
        shutdown_reply: RefCell::new(None),
        ruby_thread_object_id: info.ruby_thread_object_id,
    });
    ready_tx
        .send(Ok(info))
        .map_err(|_| BridgeError::Startup("starter dropped fiber readiness channel".to_owned()))?;

    runner
        .funcall::<_, _, Value>("call", (bridge, *app, *rack_errors, *body_reader))
        .map_err(|error| ruby_error("running Async reactor", error))?;

    Ok(())
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
    fn admission_is_non_blocking_and_releases_capacity() {
        let admission = Arc::new(RequestAdmission::new(1));
        let permit = admission.try_acquire().expect("first request should fit");
        assert!(matches!(
            admission.try_acquire(),
            Err(BridgeError::Overloaded)
        ));
        drop(permit);
        admission
            .try_acquire()
            .expect("capacity should be released after completion");
    }
}
