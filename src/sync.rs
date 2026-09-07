//! The serial runtime: one request at a time on the Ruby owner thread.
//!
//! It exists as a diagnostic control for [`crate::fiber`]. Responses are
//! collected in full rather than streamed, so the two runtimes stay comparable.

use crate::{
    BridgeError, DEFAULT_RESPONSE_TIMEOUT, Request, Response, RuntimeInfo,
    error::{panic_message, ruby_error},
    rack::{self, CallContext, Concurrency},
    response::Completion,
    vm::runtime_info,
    wake,
};
use magnus::{RClass, Ruby, Value, prelude::*, value::BoxValue};
use std::{
    fmt,
    os::unix::net::UnixStream,
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, Sender, SyncSender, TryRecvError},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

/// Minimal Rack-like application used by examples and unit tests.
pub(crate) const DEFAULT_APP_SOURCE: &str = include_str!("../ruby/default_app.rb");

enum Command {
    Call(Box<BufferedCall>),
    ForceGc {
        reply: SyncSender<Result<(), BridgeError>>,
    },
    Shutdown {
        reply: SyncSender<()>,
    },
}

struct BufferedCall {
    request: Request,
    completion: Completion,
}

/// A cloneable handle used to submit work from any thread.
#[derive(Clone)]
pub struct RuntimeClient {
    command_tx: Sender<Command>,
    wake_writer: Arc<Mutex<UnixStream>>,
    response_timeout: Duration,
}

impl RuntimeClient {
    /// Runs the application and waits for the complete response.
    pub fn call(&self, request: Request) -> Result<Response, BridgeError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.submit(request, move |result| {
            let _ = reply_tx.send(result);
        })?;
        wake::recv_reply(&reply_rx, self.response_timeout)?
    }

    /// Queues the request and reports the outcome to `completion`.
    ///
    /// The callback runs on the Ruby owner thread, so it must not block.
    pub fn submit<F>(&self, request: Request, completion: F) -> Result<(), BridgeError>
    where
        F: FnOnce(Result<Response, BridgeError>) + Send + 'static,
    {
        self.send(Command::Call(Box::new(BufferedCall {
            request,
            completion: Box::new(completion),
        })))
    }

    /// Runs a collection on the Ruby owner thread.
    pub fn force_gc(&self) -> Result<(), BridgeError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.send(Command::ForceGc { reply: reply_tx })?;
        wake::recv_reply(&reply_rx, self.response_timeout)?
    }

    fn request_shutdown(&self) -> Result<(), BridgeError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.send(Command::Shutdown { reply: reply_tx })?;
        reply_rx
            .recv_timeout(self.response_timeout)
            .map_err(wake::map_recv_timeout)
    }

    fn send(&self, command: Command) -> Result<(), BridgeError> {
        self.command_tx
            .send(command)
            .map_err(|_| BridgeError::RuntimeStopped)?;
        wake::signal(&self.wake_writer)
    }
}

impl fmt::Debug for RuntimeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeClient")
            .field("response_timeout", &self.response_timeout)
            .finish_non_exhaustive()
    }
}

/// Owns the Ruby VM and the thread it runs on.
#[derive(Debug)]
pub struct RubyRuntime {
    client: Option<RuntimeClient>,
    info: RuntimeInfo,
    join: Option<JoinHandle<Result<(), BridgeError>>>,
}

impl RubyRuntime {
    /// Starts a runtime whose application is the value `app_source` evaluates to.
    pub fn start(app_source: impl Into<String>) -> Result<Self, BridgeError> {
        let (command_tx, command_rx) = mpsc::channel();
        let (wake_reader, wake_writer) = wake::pair()?;

        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let panic_ready_tx = ready_tx.clone();
        let app_source = app_source.into();
        let join = thread::Builder::new()
            .name("ruvoy-ruby-runtime".to_owned())
            .spawn(move || {
                match panic::catch_unwind(AssertUnwindSafe(|| {
                    main(app_source, command_rx, wake_reader, ready_tx)
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

    /// Starts a runtime serving the built-in demonstration application.
    pub fn start_default() -> Result<Self, BridgeError> {
        Self::start(DEFAULT_APP_SOURCE)
    }

    /// Returns a handle for submitting work.
    ///
    /// # Panics
    ///
    /// Panics if called after [`Self::shutdown`].
    #[must_use]
    pub fn client(&self) -> RuntimeClient {
        self.client
            .as_ref()
            .expect("runtime client is present until shutdown")
            .clone()
    }

    /// Identity of the embedded VM.
    #[must_use]
    pub fn info(&self) -> &RuntimeInfo {
        &self.info
    }

    /// Stops the runtime and waits for the Ruby thread to finish.
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

fn main(
    app_source: String,
    command_rx: Receiver<Command>,
    mut wake_reader: UnixStream,
    ready_tx: SyncSender<Result<RuntimeInfo, BridgeError>>,
) -> Result<(), BridgeError> {
    // SAFETY: this thread owns the process-wide VM for its whole lifetime and
    // never hands a Ruby handle to another thread.
    let cleanup = unsafe { magnus::embed::init() };
    let ruby = &*cleanup;

    // `prepared` roots every Ruby value the loop uses, so it must outlive it.
    let prepared = match prepare(ruby, &app_source) {
        Ok(prepared) => prepared,
        Err(error) => {
            let _ = ready_tx.send(Err(error.clone()));
            return Err(error);
        }
    };
    let context = prepared.context();
    ready_tx
        .send(Ok(prepared.info.clone()))
        .map_err(|_| BridgeError::Startup("starter dropped readiness channel".to_owned()))?;

    loop {
        ruby.thread_wait_fd(&wake_reader)
            .map_err(|error| ruby_error("waiting for bridge wakeup", error))?;
        wake::drain(&mut wake_reader)?;

        loop {
            match command_rx.try_recv() {
                Ok(Command::Call(call)) => {
                    (call.completion)(rack::call(ruby, &context, call.request));
                }
                Ok(Command::ForceGc { reply }) => {
                    ruby.gc_start();
                    let _ = reply.send(Ok(()));
                }
                Ok(Command::Shutdown { reply }) => {
                    let _ = reply.send(());
                    return Ok(());
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }
    }
}

/// Everything the request loop needs, holding the Ruby values it roots.
struct Prepared {
    app: BoxValue<Value>,
    rack_errors: BoxValue<Value>,
    body_reader: BoxValue<Value>,
    string_io_class: RClass,
    info: RuntimeInfo,
}

impl Prepared {
    fn context(&self) -> CallContext {
        CallContext {
            app: *self.app,
            string_io_class: self.string_io_class,
            rack_errors: *self.rack_errors,
            body_reader: *self.body_reader,
            ruby_thread_object_id: self.info.ruby_thread_object_id,
            concurrency: Concurrency::Serial,
        }
    }
}

fn prepare(ruby: &Ruby, app_source: &str) -> Result<Prepared, BridgeError> {
    let string_io_class = ruby
        .require("stringio")
        .and_then(|_| ruby.class_object().const_get::<_, RClass>("StringIO"))
        .map_err(|error| ruby_error("loading StringIO", error))?;

    Ok(Prepared {
        app: boxed(ruby, app_source, "evaluating app source")?,
        rack_errors: boxed(ruby, "STDERR", "loading rack.errors")?,
        body_reader: boxed(ruby, rack::BODY_READER_SOURCE, "loading Rack body reader")?,
        string_io_class,
        info: runtime_info(ruby)?,
    })
}

/// Evaluates `source` and keeps the result rooted for the process lifetime.
fn boxed(ruby: &Ruby, source: &str, context: &str) -> Result<BoxValue<Value>, BridgeError> {
    ruby.eval::<Value>(source)
        .map(BoxValue::new)
        .map_err(|error| ruby_error(context, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use magnus::Ruby;
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
