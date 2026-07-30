use ruvoy::{Request, Response, RubyRuntime};
use std::hint::black_box;
use std::time::Instant;

const APP_SOURCE: &str = r#"
Class.new do
  def call(env)
    input = env.fetch("rack.input")
    input.rewind
    request_body = input.read
    body = "".b
    [
      200,
      {
        "content-type" => "application/octet-stream",
        "content-length" => "0",
        "x-request-bytes" => request_body.bytesize.to_s,
        "x-ruby-thread-object-id" => Thread.current.object_id.to_s
      },
      [body]
    ]
  end
end.new
"#;

fn pure_rust_call(request: Request) -> Response {
    let request_bytes = request.body.len();
    black_box(request);
    Response {
        status: 200,
        headers: vec![
            (
                "content-type".to_owned(),
                "application/octet-stream".to_owned(),
            ),
            ("content-length".to_owned(), "0".to_owned()),
            ("x-request-bytes".to_owned(), request_bytes.to_string()),
        ],
        body: Vec::new(),
        ruby_thread_object_id: 0,
    }
}

fn percentile(sorted_microseconds: &[f64], percentile: f64) -> f64 {
    let index = ((sorted_microseconds.len() - 1) as f64 * percentile).round() as usize;
    sorted_microseconds[index]
}

fn summarize(mut samples: Vec<f64>) -> (f64, f64, f64, f64) {
    samples.sort_by(f64::total_cmp);
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    (
        mean,
        percentile(&samples, 0.50),
        percentile(&samples, 0.95),
        percentile(&samples, 0.99),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let iterations = std::env::var("RUVOY_BRIDGE_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(5_000);
    let pure_iterations = iterations * 20;

    for _ in 0..1_000 {
        black_box(pure_rust_call(Request::new(
            "GET",
            "/benchmark",
            Vec::new(),
        )));
    }
    let mut pure_samples = Vec::with_capacity(pure_iterations);
    for _ in 0..pure_iterations {
        let started = Instant::now();
        black_box(pure_rust_call(Request::new(
            "GET",
            "/benchmark",
            Vec::new(),
        )));
        pure_samples.push(started.elapsed().as_secs_f64() * 1_000_000.0);
    }

    let runtime = RubyRuntime::start(APP_SOURCE)?;
    let client = runtime.client();
    for _ in 0..100 {
        black_box(client.call(Request::new("GET", "/benchmark", Vec::new()))?);
    }
    let mut bridge_samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        black_box(client.call(Request::new("GET", "/benchmark", Vec::new()))?);
        bridge_samples.push(started.elapsed().as_secs_f64() * 1_000_000.0);
    }
    runtime.shutdown()?;

    let (pure_mean, pure_p50, pure_p95, pure_p99) = summarize(pure_samples);
    let (bridge_mean, bridge_p50, bridge_p95, bridge_p99) = summarize(bridge_samples);
    println!(
        concat!(
            "{{\"iterations\":{},",
            "\"pure_rust\":{{\"mean_us\":{:.6},\"p50_us\":{:.6},",
            "\"p95_us\":{:.6},\"p99_us\":{:.6}}},",
            "\"ruby_bridge\":{{\"mean_us\":{:.6},\"p50_us\":{:.6},",
            "\"p95_us\":{:.6},\"p99_us\":{:.6}}}}}"
        ),
        iterations,
        pure_mean,
        pure_p50,
        pure_p95,
        pure_p99,
        bridge_mean,
        bridge_p50,
        bridge_p95,
        bridge_p99
    );
    Ok(())
}
