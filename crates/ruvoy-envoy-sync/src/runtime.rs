use ruvoy::{BridgeError, RubyRuntime, RuntimeClient};
use std::sync::Mutex;

const SYNC_RACK_APP_SOURCE: &str = r#"
Class.new do
  def call(env)
    path = env.fetch("PATH_INFO")
    query = env.fetch("QUERY_STRING", "")
    raise "intentional envoy boom" if path == "/raise"

    sleep 0.5 if path == "/slow"
    sleep 1.0 if path == "/slow-shutdown"

    @calls = (@calls || 0) + 1
    GC.start if path == "/gc" || env["ruvoy.force_gc"]

    input = env.fetch("rack.input")
    input.rewind
    request_body = input.read

    benchmark_response_bytes = nil
    if path == "/benchmark"
      parameters = query.split("&").to_h { |entry| entry.split("=", 2) }
      wait_ms = Integer(parameters.fetch("wait_ms", "0"), 10)
      benchmark_response_bytes = Integer(parameters.fetch("response_bytes", "0"), 10)
      raise "invalid benchmark wait_ms" unless (0..1000).cover?(wait_ms)
      raise "invalid benchmark response_bytes" unless (0..2 * 1024 * 1024).cover?(benchmark_response_bytes)
      sleep(wait_ms / 1000.0) if wait_ms.positive?
    end

    response_body =
      case path
      when "/benchmark"
        "B".b * benchmark_response_bytes
      when "/echo"
        request_body
      when "/large-response"
        "R".b * (1024 * 1024)
      else
        [env.fetch("REQUEST_METHOD"), path, request_body].join(" ")
      end

    headers = {
      "content-type" => "application/octet-stream",
      "content-length" => response_body.bytesize.to_s,
      "x-request-bytes" => request_body.bytesize.to_s,
      "x-ruby-call-count" => @calls.to_s,
      "x-ruby-thread-object-id" => Thread.current.object_id.to_s
    }
    if env["HTTP_X_RUVOY_TEST"]
      headers["x-rack-request-header"] = env["HTTP_X_RUVOY_TEST"]
    end

    [200, headers, [response_body]]
  end
end.new
"#;

pub(crate) struct SyncRackConfig {
    client: RuntimeClient,
    diagnostics_enabled: bool,
    runtime_thread_id: String,
    runtime: Mutex<Option<RubyRuntime>>,
}

impl SyncRackConfig {
    pub(crate) fn start() -> Result<Self, BridgeError> {
        let diagnostics_enabled = flag_env("RUVOY_DIAGNOSTICS")?;
        let runtime = RubyRuntime::start(SYNC_RACK_APP_SOURCE)?;
        let client = runtime.client();
        let runtime_thread_id = runtime.info().rust_thread_id.clone();

        Ok(Self {
            client,
            diagnostics_enabled,
            runtime_thread_id,
            runtime: Mutex::new(Some(runtime)),
        })
    }

    pub(crate) fn client(&self) -> RuntimeClient {
        self.client.clone()
    }

    pub(crate) fn diagnostics_enabled(&self) -> bool {
        self.diagnostics_enabled
    }

    pub(crate) fn runtime_thread_id(&self) -> &str {
        &self.runtime_thread_id
    }
}

/// Thread identity headers describe Ruvoy internals, so they stay off unless an
/// operator turns them on for a test or an investigation.
fn flag_env(name: &str) -> Result<bool, BridgeError> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(false);
    };
    match value.to_str() {
        Some("1") => Ok(true),
        Some("0") => Ok(false),
        _ => Err(BridgeError::Startup(format!("{name} must be 0 or 1"))),
    }
}

impl Drop for SyncRackConfig {
    fn drop(&mut self) {
        let runtime_slot = match self.runtime.get_mut() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };

        if let Some(runtime) = runtime_slot.take() {
            match runtime.shutdown() {
                Ok(()) => eprintln!("[ruvoy] Ruby runtime stopped"),
                Err(error) => eprintln!("[ruvoy] Ruby runtime shutdown failed: {error}"),
            }
        }
    }
}
