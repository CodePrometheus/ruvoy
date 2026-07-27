use ruvoy_poc::{
    BridgeError,
    fiber::{DEFAULT_MAX_INFLIGHT_REQUESTS, FiberRuntime, FiberRuntimeClient},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

const DEFAULT_MAX_INFLIGHT_BODY_BYTES: usize = 256 * 1024 * 1024;

const FIBER_RACK_APP_SOURCE: &str = r#"
Class.new do
  def call(env)
    path = env.fetch("PATH_INFO")
    query = env.fetch("QUERY_STRING", "")
    duration = query.split("=", 2).last.to_f
    raise "intentional fiber envoy boom" if path == "/raise"

    case path
    when "/async-sleep", "/slow-shutdown"
      sleep duration
    when "/blocking"
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + duration
      while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      end
    end

    @calls = (@calls || 0) + 1
    GC.start if path == "/gc" || path == "/gc-stats" || env["ruvoy.force_gc"]

    input = env.fetch("rack.input")
    input.rewind
    request_body = input.read

    benchmark_response_bytes = nil
    if path == "/benchmark"
      parameters = query.split("&").to_h { |entry| entry.split("=", 2) }
      wait_ms = Integer(parameters.fetch("wait_ms", "0"), 10)
      benchmark_response_bytes = Integer(parameters.fetch("response_bytes", "0"), 10)
      expected_request_bytes = Integer(parameters.fetch("expected_request_bytes", request_body.bytesize.to_s), 10)
      raise "invalid benchmark wait_ms" unless (0..1000).cover?(wait_ms)
      raise "invalid benchmark response_bytes" unless (0..2 * 1024 * 1024).cover?(benchmark_response_bytes)
      raise "invalid expected request bytes" unless (0..2 * 1024 * 1024).cover?(expected_request_bytes)
      raise "unexpected request body size" unless request_body.bytesize == expected_request_bytes
      sleep(wait_ms / 1000.0) if wait_ms.positive?
    end

    response_body =
      case path
      when "/benchmark"
        "B".b * benchmark_response_bytes
      when "/echo"
        request_body
      when "/large-response"
        "F".b * (1024 * 1024)
      else
        [env.fetch("REQUEST_METHOD"), path, request_body].join(" ")
      end

    headers = {
      "content-type" => "application/octet-stream",
      "content-length" => response_body.bytesize.to_s,
      "x-request-bytes" => request_body.bytesize.to_s,
      "x-async-version" => Async::VERSION,
      "x-ruby-call-count" => @calls.to_s,
      "x-ruby-thread-object-id" => Thread.current.object_id.to_s,
      "x-ruby-fiber-object-id" => Fiber.current.object_id.to_s
    }
    if env["HTTP_X_RUVOY_TEST"]
      headers["x-rack-request-header"] = env["HTTP_X_RUVOY_TEST"]
    end
    if path == "/gc-stats"
      headers["x-ruby-heap-live-slots"] = GC.stat(:heap_live_slots).to_s
      headers["x-ruby-heap-available-slots"] = GC.stat(:heap_available_slots).to_s
    end

    [200, headers, [response_body]]
  end
end.new
"#;

pub(crate) struct FiberRackConfig {
    client: FiberRuntimeClient,
    body_budget: Arc<BodyBudget>,
    runtime_thread_id: String,
    runtime: Mutex<Option<FiberRuntime>>,
}

impl FiberRackConfig {
    pub(crate) fn start() -> Result<Self, BridgeError> {
        let max_inflight_requests =
            positive_env_usize("RUVOY_MAX_INFLIGHT_REQUESTS", DEFAULT_MAX_INFLIGHT_REQUESTS)?;
        let max_inflight_body_bytes = positive_env_usize(
            "RUVOY_MAX_INFLIGHT_BODY_BYTES",
            DEFAULT_MAX_INFLIGHT_BODY_BYTES,
        )?;
        let runtime = FiberRuntime::start_with_limit(FIBER_RACK_APP_SOURCE, max_inflight_requests)?;
        let client = runtime.client();
        let runtime_thread_id = runtime.info().rust_thread_id.clone();

        Ok(Self {
            client,
            body_budget: Arc::new(BodyBudget::new(max_inflight_body_bytes)),
            runtime_thread_id,
            runtime: Mutex::new(Some(runtime)),
        })
    }

    pub(crate) fn client(&self) -> FiberRuntimeClient {
        self.client.clone()
    }

    pub(crate) fn runtime_thread_id(&self) -> &str {
        &self.runtime_thread_id
    }

    pub(crate) fn body_budget(&self) -> Arc<BodyBudget> {
        Arc::clone(&self.body_budget)
    }
}

fn positive_env_usize(name: &str, default: usize) -> Result<usize, BridgeError> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    let value = value
        .into_string()
        .map_err(|_| BridgeError::Startup(format!("{name} must contain valid UTF-8")))?;
    let value = value
        .parse::<usize>()
        .map_err(|_| BridgeError::Startup(format!("{name} must be a positive integer")))?;
    if value == 0 {
        return Err(BridgeError::Startup(format!(
            "{name} must be a positive integer"
        )));
    }
    Ok(value)
}

pub(crate) struct BodyBudget {
    reserved: AtomicUsize,
    maximum: usize,
}

impl BodyBudget {
    fn new(maximum: usize) -> Self {
        Self {
            reserved: AtomicUsize::new(0),
            maximum,
        }
    }

    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<BodyLease, BodyBudgetExceeded> {
        self.try_add(bytes)?;
        Ok(BodyLease {
            budget: Arc::clone(self),
            reserved: bytes,
        })
    }

    fn try_add(&self, bytes: usize) -> Result<(), BodyBudgetExceeded> {
        let mut reserved = self.reserved.load(Ordering::Relaxed);
        loop {
            if bytes > self.maximum.saturating_sub(reserved) {
                return Err(BodyBudgetExceeded);
            }
            match self.reserved.compare_exchange_weak(
                reserved,
                reserved + bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => reserved = observed,
            }
        }
    }
}

pub(crate) struct BodyLease {
    budget: Arc<BodyBudget>,
    reserved: usize,
}

impl BodyLease {
    pub(crate) fn ensure_reserved(&mut self, required: usize) -> Result<(), BodyBudgetExceeded> {
        if required <= self.reserved {
            return Ok(());
        }
        let additional = required - self.reserved;
        self.budget.try_add(additional)?;
        self.reserved = required;
        Ok(())
    }
}

impl Drop for BodyLease {
    fn drop(&mut self) {
        let previous = self
            .budget
            .reserved
            .fetch_sub(self.reserved, Ordering::AcqRel);
        debug_assert!(previous >= self.reserved);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BodyBudgetExceeded;

impl Drop for FiberRackConfig {
    fn drop(&mut self) {
        let runtime_slot = match self.runtime.get_mut() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };

        if let Some(runtime) = runtime_slot.take() {
            match runtime.shutdown() {
                Ok(()) => eprintln!("[ruvoy] Fiber runtime stopped"),
                Err(error) => eprintln!("[ruvoy] Fiber runtime shutdown failed: {error}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_budget_rejects_overcommit_and_releases_capacity() {
        let budget = Arc::new(BodyBudget::new(10));
        let lease = budget.try_reserve(8).expect("first body should fit");
        assert!(budget.try_reserve(3).is_err());
        drop(lease);
        budget
            .try_reserve(10)
            .expect("dropped body should release its capacity");
    }

    #[test]
    fn chunked_body_lease_grows_without_double_reserving() {
        let budget = Arc::new(BodyBudget::new(10));
        let mut lease = budget.try_reserve(0).expect("empty body should fit");
        lease.ensure_reserved(4).expect("first chunk should fit");
        lease
            .ensure_reserved(4)
            .expect("same size must not reserve twice");
        assert!(lease.ensure_reserved(11).is_err());
        lease
            .ensure_reserved(10)
            .expect("remaining budget should fit");
    }
}
