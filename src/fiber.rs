use super::{
    BridgeError, Completion, DEFAULT_RESPONSE_TIMEOUT, Request, Response, RuntimeInfo, call_app,
    drain_wake_bytes, map_recv_timeout, panic_message, ruby_error, runtime_info, wake_runtime,
};
use magnus::{RArray, RClass, Ruby, Value, method, prelude::*, value::BoxValue};
use std::{
    cell::RefCell,
    os::{fd::AsRawFd, unix::net::UnixStream},
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender, SyncSender, TryRecvError},
    },
    thread::{self, JoinHandle},
};

pub const DEFAULT_MAX_INFLIGHT_REQUESTS: usize = 1024;

const FIBER_RUNNER_SOURCE: &str = r#"
lambda do |bridge, app|
  io = IO.for_fd(bridge.fd, autoclose: false)

  begin
    Async do |parent|
      shutting_down = false
      until shutting_down
        io.wait_readable
        envelopes, shutting_down = bridge.drain
        envelopes.each do |envelope|
          parent.async(envelope) do |_task, current_envelope|
            bridge.execute(current_envelope, app)
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
        _bridge: &Self,
        envelope: &FiberEnvelope,
        app: Value,
    ) -> Result<(), magnus::Error> {
        let call =
            envelope.0.borrow_mut().take().ok_or_else(|| {
                magnus::Error::new(ruby.exception_runtime_error(), "request reused")
            })?;
        let string_io_class = ruby.class_object().const_get::<_, RClass>("StringIO")?;
        let result = call_app(ruby, app, string_io_class, call.request);
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
        let app_source = app_source.into();
        let join = thread::Builder::new()
            .name("ruvoy-ruby-fiber-runtime".to_owned())
            .spawn(move || {
                match panic::catch_unwind(AssertUnwindSafe(|| {
                    fiber_runtime_main(app_source, command_rx, wake_reader, ready_tx)
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
    app_source: String,
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
        .and_then(|_| bridge_class.define_method("execute", method!(FiberBridge::execute, 2)))
        .and_then(|_| {
            bridge_class.define_method("ack_shutdown", method!(FiberBridge::ack_shutdown, 0))
        })
        .map_err(|error| ruby_error("defining FiberBridge methods", error))?;
    ruvoy_module
        .define_class("FiberEnvelope", ruby.class_object())
        .map_err(|error| ruby_error("defining FiberEnvelope", error))?;

    let app = match ruby.eval::<Value>(&app_source) {
        Ok(value) => BoxValue::new(value),
        Err(error) => {
            let error = ruby_error("evaluating fiber app source", error);
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
    let bridge = ruby.obj_wrap(FiberBridge {
        command_rx: RefCell::new(command_rx),
        wake_reader: RefCell::new(wake_reader),
        shutdown_reply: RefCell::new(None),
    });

    let info = match runtime_info(ruby) {
        Ok(info) => info,
        Err(error) => {
            let _ = ready_tx.send(Err(error.clone()));
            return Err(error);
        }
    };
    ready_tx
        .send(Ok(info))
        .map_err(|_| BridgeError::Startup("starter dropped fiber readiness channel".to_owned()))?;

    runner
        .funcall::<_, _, Value>("call", (bridge, *app))
        .map_err(|error| ruby_error("running Async reactor", error))?;

    Ok(())
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
