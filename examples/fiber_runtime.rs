//! Exercises the fiber runtime without Envoy in the picture.

use ruvoy::{
    Request, Response,
    fiber::{FiberRuntime, FiberRuntimeClient},
};
use std::{
    collections::HashSet,
    io::Write,
    net::TcpListener,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

const APP_SOURCE: &str = r#"
require "socket"

Class.new do
  def call(env)
    path = env.fetch("PATH_INFO")
    duration = env.fetch("QUERY_STRING", "").split("=", 2).last.to_f

    case path
    when "/async-sleep"
      sleep duration
    when "/async-io"
      socket = TCPSocket.new("127.0.0.1", __PORT__)
      socket.read
      socket.close
    when "/blocking"
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + duration
      while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      end
    end

    input = env.fetch("rack.input")
    input.rewind
    body = [
      env.fetch("REQUEST_METHOD"),
      path,
      input.read
    ].join(" ")

    [
      200,
      {
        "content-type" => "text/plain",
        "x-async-version" => Async::VERSION,
        "x-ruby-thread-object-id" => Thread.current.object_id.to_s,
        "x-ruby-fiber-object-id" => Fiber.current.object_id.to_s
      },
      [body]
    ]
  end
end.new
"#;

fn run_batch(client: &FiberRuntimeClient, count: usize, path: &str) -> (Duration, Vec<Response>) {
    let started = Instant::now();
    let (response_tx, response_rx) = mpsc::channel();
    let mut producers = Vec::with_capacity(count);

    for index in 0..count {
        let client = client.clone();
        let response_tx = response_tx.clone();
        let path = path.to_owned();
        producers.push(thread::spawn(move || {
            client
                .submit_collected(
                    Request::new("GET", path, format!("request-{index}").into_bytes()),
                    move |result| {
                        response_tx
                            .send(result)
                            .expect("result receiver should remain alive");
                    },
                )
                .expect("foreign producer should submit without entering Ruby");
        }));
    }
    drop(response_tx);

    for producer in producers {
        producer.join().expect("foreign producer should not panic");
    }

    let responses = (0..count)
        .map(|_| {
            response_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("fiber response should arrive")
                .expect("fiber app should succeed")
        })
        .collect::<Vec<_>>();
    (started.elapsed(), responses)
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("delayed I/O listener should bind");
    let io_port = listener
        .local_addr()
        .expect("delayed I/O listener should have an address")
        .port();
    let io_server = thread::spawn(move || {
        let mut handlers = Vec::with_capacity(10);
        for _ in 0..10 {
            let (mut stream, _) = listener.accept().expect("I/O client should connect");
            handlers.push(thread::spawn(move || {
                thread::sleep(Duration::from_millis(200));
                stream
                    .write_all(b"io-ready")
                    .expect("I/O server should write");
            }));
        }
        for handler in handlers {
            handler.join().expect("I/O server handler should not panic");
        }
    });

    let app_source = APP_SOURCE.replace("__PORT__", &io_port.to_string());
    let runtime = FiberRuntime::start(app_source).expect("Fiber runtime should start");
    let client = runtime.client();
    let runtime_thread_id = runtime.info().ruby_thread_object_id;
    let runtime_rust_thread_id = runtime.info().rust_thread_id.clone();
    let producer_rust_thread_id = format!("{:?}", thread::current().id());
    assert_ne!(runtime_rust_thread_id, producer_rust_thread_id);

    let (async_elapsed, async_responses) = run_batch(&client, 10, "/async-sleep?seconds=0.2");
    assert!(
        async_elapsed < Duration::from_millis(700),
        "scheduler-aware batch took {async_elapsed:?}; sequential time is 2s"
    );
    assert!(
        async_responses
            .iter()
            .all(|response| response.status == 200)
    );
    let async_version = async_responses
        .first()
        .and_then(|response| response.header("x-async-version"))
        .expect("scheduler-aware responses report the Async version")
        .to_owned();
    assert!(async_responses.iter().all(|response| {
        response.ruby_thread_object_id == runtime_thread_id
            && response.header("x-async-version") == Some(async_version.as_str())
    }));
    let async_fiber_ids = async_responses
        .iter()
        .map(|response| {
            response
                .header("x-ruby-fiber-object-id")
                .expect("fiber id header")
                .to_owned()
        })
        .collect::<HashSet<_>>();
    assert_eq!(async_fiber_ids.len(), 10);

    let (io_elapsed, io_responses) = run_batch(&client, 10, "/async-io");
    assert!(
        io_elapsed < Duration::from_millis(700),
        "scheduler-aware TCP reads took {io_elapsed:?}; sequential time is 2s"
    );
    assert!(io_responses.iter().all(|response| response.status == 200));
    let io_fiber_ids = io_responses
        .iter()
        .map(|response| {
            response
                .header("x-ruby-fiber-object-id")
                .expect("fiber id header")
                .to_owned()
        })
        .collect::<HashSet<_>>();
    assert_eq!(io_fiber_ids.len(), 10);
    io_server.join().expect("I/O server should stop cleanly");

    let (blocking_elapsed, blocking_responses) = run_batch(&client, 5, "/blocking?seconds=0.1");
    assert!(
        blocking_elapsed >= Duration::from_millis(450),
        "scheduler-unaware batch unexpectedly overlapped: {blocking_elapsed:?}"
    );
    assert!(
        blocking_elapsed < Duration::from_secs(2),
        "scheduler-unaware probe took unexpectedly long: {blocking_elapsed:?}"
    );
    assert!(
        blocking_responses
            .iter()
            .all(|response| response.status == 200)
    );

    // SAFETY: the program is about to exit and every client clone has been
    // dropped, so no Ruby execution can still be in flight.
    unsafe {
        runtime
            .shutdown()
            .expect("Fiber runtime should shut down cleanly");
    }

    println!("result=PASS");
    println!("async_version={async_version}");
    println!("foreign_producer_threads=10");
    println!("runtime_rust_thread_id={runtime_rust_thread_id}");
    println!("producer_rust_thread_id={producer_rust_thread_id}");
    println!("scheduler_aware_requests=10");
    println!(
        "scheduler_aware_elapsed_seconds={:.6}",
        async_elapsed.as_secs_f64()
    );
    println!("scheduler_aware_unique_fibers={}", async_fiber_ids.len());
    println!("scheduler_io_requests=10");
    println!(
        "scheduler_io_elapsed_seconds={:.6}",
        io_elapsed.as_secs_f64()
    );
    println!("scheduler_io_unique_fibers={}", io_fiber_ids.len());
    println!("scheduler_unaware_requests=5");
    println!(
        "scheduler_unaware_elapsed_seconds={:.6}",
        blocking_elapsed.as_secs_f64()
    );
    println!("shutdown=PASS");
}
