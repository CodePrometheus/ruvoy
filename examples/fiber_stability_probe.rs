use magnus::Ruby;
use ruvoy::{
    BridgeError, Request, Response,
    fiber::{DEFAULT_MAX_INFLIGHT_REQUESTS, FiberRuntime, FiberRuntimeClient},
};
use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

const APP_SOURCE: &str = r#"
Class.new do
  def call(env)
    @calls = (@calls || 0) + 1
    GC.start if env["ruvoy.force_gc"]

    input = env.fetch("rack.input")
    input.rewind
    request_body = input.read
    expected_bytes = Integer(env.fetch("HTTP_X_RUVOY_EXPECTED_BYTES"), 10)
    raise "unexpected request body size" unless request_body.bytesize == expected_bytes

    [
      200,
      {
        "content-type" => "application/octet-stream",
        "content-length" => "0",
        "x-request-bytes" => request_body.bytesize.to_s,
        "x-ruvoy-probe-id" => env.fetch("HTTP_X_RUVOY_PROBE_ID"),
        "x-ruby-call-count" => @calls.to_s,
        "x-ruby-heap-live-slots" => GC.stat(:heap_live_slots).to_s,
        "x-ruby-thread-object-id" => Thread.current.object_id.to_s,
        "x-ruby-fiber-object-id" => Fiber.current.object_id.to_s
      },
      [""]
    ]
  end
end.new
"#;

type ProbeResult = (usize, Result<Response, BridgeError>);

struct IdleHostThreads {
    stop: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

impl IdleHostThreads {
    fn start(count: usize) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(count);

        for index in 0..count {
            let thread_stop = Arc::clone(&stop);
            match thread::Builder::new()
                .name(format!("ruvoy-probe-host-{index}"))
                .spawn(move || {
                    while !thread_stop.load(Ordering::Acquire) {
                        thread::park_timeout(Duration::from_millis(50));
                    }
                }) {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    stop.store(true, Ordering::Release);
                    for handle in &handles {
                        handle.thread().unpark();
                    }
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(error);
                }
            }
        }

        Ok(Self { stop, handles })
    }
}

impl Drop for IdleHostThreads {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for handle in &self.handles {
            handle.thread().unpark();
        }
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn positive_env_usize(name: &str, default: usize) -> io::Result<usize> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    let value = value.into_string().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must contain valid UTF-8"),
        )
    })?;
    let value = value.parse::<usize>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be a positive integer"),
        )
    })?;
    if value == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be a positive integer"),
        ));
    }
    Ok(value)
}

fn non_negative_env_usize(name: &str, default: usize) -> io::Result<usize> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    let value = value.into_string().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must contain valid UTF-8"),
        )
    })?;
    value.parse::<usize>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be a non-negative integer"),
        )
    })
}

fn probe_request(id: usize, body: &[u8]) -> Request {
    Request::new("POST", "/benchmark", body.to_vec())
        .with_header("host", b"127.0.0.1".to_vec())
        .with_header("accept", b"*/*".to_vec())
        .with_header("user-agent", b"ruvoy-fiber-stability-probe".to_vec())
        .with_header("content-length", body.len().to_string().into_bytes())
        .with_header("x-ruvoy-probe-id", id.to_string().into_bytes())
        .with_header(
            "x-ruvoy-expected-bytes",
            body.len().to_string().into_bytes(),
        )
}

fn submit_request(
    client: &FiberRuntimeClient,
    result_tx: &Sender<ProbeResult>,
    id: usize,
    body: &[u8],
) -> Result<(), BridgeError> {
    let result_tx = result_tx.clone();
    client.submit_collected(probe_request(id, body), move |result| {
        let _ = result_tx.send((id, result));
    })
}

fn verify_response(
    expected_id: usize,
    expected_body_bytes: usize,
    expected_ruby_thread_id: u64,
    response: &Response,
) -> io::Result<()> {
    if response.status != 200 {
        return Err(io::Error::other(format!(
            "request {expected_id} returned status {}",
            response.status
        )));
    }
    if response.ruby_thread_object_id != expected_ruby_thread_id {
        return Err(io::Error::other(format!(
            "request {expected_id} moved from Ruby thread {expected_ruby_thread_id} to {}",
            response.ruby_thread_object_id
        )));
    }
    let expected_id = expected_id.to_string();
    if response.header("x-ruvoy-probe-id") != Some(expected_id.as_str()) {
        return Err(io::Error::other(format!(
            "response probe id mismatch for request {expected_id}"
        )));
    }
    let expected_body_bytes = expected_body_bytes.to_string();
    if response.header("x-request-bytes") != Some(expected_body_bytes.as_str()) {
        return Err(io::Error::other(format!(
            "response body size mismatch for request {expected_id}"
        )));
    }
    if !response.body.is_empty() {
        return Err(io::Error::other(format!(
            "request {expected_id} returned a non-empty body"
        )));
    }
    Ok(())
}

fn receive_result(
    result_rx: &Receiver<ProbeResult>,
    body_bytes: usize,
    ruby_thread_id: u64,
) -> Result<(), Box<dyn Error>> {
    let (id, result) = result_rx.recv_timeout(RESPONSE_TIMEOUT)?;
    let response = result?;
    verify_response(id, body_bytes, ruby_thread_id, &response)?;
    Ok(())
}

fn run_wave(
    client: &FiberRuntimeClient,
    wave: usize,
    requests: usize,
    concurrency: usize,
    body: &[u8],
    ruby_thread_id: u64,
) -> Result<(Duration, Response), Box<dyn Error>> {
    let (result_tx, result_rx) = mpsc::channel();
    let first_id = (wave - 1)
        .checked_mul(requests)
        .ok_or_else(|| io::Error::other("request id overflow"))?;
    let initial = concurrency.min(requests);
    let started_at = Instant::now();

    for offset in 0..initial {
        submit_request(client, &result_tx, first_id + offset, body)?;
    }

    let mut submitted = initial;
    let mut completed = 0;
    while completed < requests {
        receive_result(&result_rx, body.len(), ruby_thread_id)?;
        completed += 1;
        if submitted < requests {
            submit_request(client, &result_tx, first_id + submitted, body)?;
            submitted += 1;
        }
    }

    let gc_id = first_id
        .checked_add(requests)
        .ok_or_else(|| io::Error::other("GC request id overflow"))?;
    let gc_response = client.call(probe_request(gc_id, &[]).with_forced_gc())?;
    verify_response(gc_id, 0, ruby_thread_id, &gc_response)?;

    Ok((started_at.elapsed(), gc_response))
}

fn required_header<'a>(response: &'a Response, name: &str) -> io::Result<&'a str> {
    response
        .header(name)
        .ok_or_else(|| io::Error::other(format!("missing {name} response header")))
}

fn main() -> Result<(), Box<dyn Error>> {
    let waves = positive_env_usize("RUVOY_PROBE_WAVES", 3)?;
    let requests_per_wave = positive_env_usize("RUVOY_PROBE_REQUESTS_PER_WAVE", 1_000)?;
    let concurrency = positive_env_usize("RUVOY_PROBE_CONCURRENCY", 100)?;
    let body_bytes = non_negative_env_usize("RUVOY_PROBE_BODY_BYTES", 0)?;
    let host_thread_count = non_negative_env_usize("RUVOY_PROBE_HOST_THREADS", 4)?;

    if concurrency > DEFAULT_MAX_INFLIGHT_REQUESTS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "RUVOY_PROBE_CONCURRENCY exceeds runtime limit {DEFAULT_MAX_INFLIGHT_REQUESTS}"
            ),
        )
        .into());
    }
    if body_bytes > MAX_BODY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("RUVOY_PROBE_BODY_BYTES exceeds {MAX_BODY_BYTES}"),
        )
        .into());
    }

    let _host_threads = IdleHostThreads::start(host_thread_count)?;
    let runtime = FiberRuntime::start(APP_SOURCE)?;
    let client = runtime.client();
    let runtime_info = runtime.info().clone();
    let main_thread_ruby_access = Ruby::get().is_ok();
    if main_thread_ruby_access {
        return Err(io::Error::other("main thread unexpectedly entered Ruby").into());
    }

    println!("probe=ruvoy-magnus-fiber-stability");
    println!("pid={}", std::process::id());
    println!("ruby_description={}", runtime_info.ruby_description);
    println!("runtime_rust_thread_id={}", runtime_info.rust_thread_id);
    println!(
        "runtime_ruby_thread_object_id={}",
        runtime_info.ruby_thread_object_id
    );
    println!("main_thread_ruby_access=denied");
    println!("host_threads={host_thread_count}");
    println!("waves={waves}");
    println!("requests_per_wave={requests_per_wave}");
    println!("concurrency={concurrency}");
    println!("body_bytes={body_bytes}");

    let body = vec![b'R'; body_bytes];
    let total_started_at = Instant::now();
    for wave in 1..=waves {
        let (elapsed, gc_response) = run_wave(
            &client,
            wave,
            requests_per_wave,
            concurrency,
            &body,
            runtime_info.ruby_thread_object_id,
        )?;
        let heap_live_slots = required_header(&gc_response, "x-ruby-heap-live-slots")?;
        let ruby_call_count = required_header(&gc_response, "x-ruby-call-count")?;
        println!(
            "wave={wave} requests={requests_per_wave} elapsed_seconds={:.6} rps={:.2} \
             heap_live_slots={heap_live_slots} ruby_call_count={ruby_call_count}",
            elapsed.as_secs_f64(),
            requests_per_wave as f64 / elapsed.as_secs_f64()
        );
    }

    unsafe {
        runtime.shutdown()?;
    }
    println!(
        "total_requests={} total_elapsed_seconds={:.6}",
        waves * requests_per_wave,
        total_started_at.elapsed().as_secs_f64()
    );
    println!("shutdown=PASS");
    println!("result=PASS");
    Ok(())
}
