# frozen_string_literal: true

class RackCompatibilityMiddleware
  def initialize(app)
    @app = app
  end

  def call(env)
    status, headers, body = @app.call(env)
    headers["x-rack-middleware"] = "active"
    [status, headers, body]
  end
end

class TrackedRackBody
  class << self
    attr_accessor :closed_count
  end
  self.closed_count = 0

  def initialize(*chunks)
    @chunks = chunks
  end

  def each
    @chunks.each { |chunk| yield chunk }
  end

  def close
    self.class.closed_count += 1
  end
end

class RackCompatibilityApp
  MAX_BODY_BYTES = 2 * 1024 * 1024
  MAX_WAIT_MS = 1_000

  def call(env)
    path = env.fetch("PATH_INFO")
    query = env.fetch("QUERY_STRING", "")
    duration = query.split("=", 2).last.to_f
    raise "intentional fiber envoy boom" if path == "/raise"

    wait_for_test_route(path, duration)
    @calls = (@calls || 0) + 1
    GC.start if path == "/gc" || path == "/gc-stats" || env["ruvoy.force_gc"]

    request_body = read_request_body(env)
    return rack_env_response(env) if path == "/rack-env"
    return enumerable_response if path == "/enumerable"
    return text_response(TrackedRackBody.closed_count.to_s) if path == "/closed"

    response_body = route_body(env, path, query, request_body)
    headers = response_headers(env, request_body, response_body)
    [200, headers, [response_body]]
  end

  private

  def wait_for_test_route(path, duration)
    case path
    when "/async-sleep", "/slow-shutdown"
      sleep duration
    when "/blocking"
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + duration
      nil while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    end
  end

  def read_request_body(env)
    env.fetch("rack.input").read
  end

  def route_body(env, path, query, request_body)
    case path
    when "/benchmark"
      benchmark_body(query, request_body)
    when "/echo"
      request_body
    when "/large-response"
      "F".b * (1024 * 1024)
    else
      [env.fetch("REQUEST_METHOD"), path, request_body].join(" ")
    end
  end

  def benchmark_body(query, request_body)
    parameters = query.split("&").to_h { |entry| entry.split("=", 2) }
    wait_ms = Integer(parameters.fetch("wait_ms", "0"), 10)
    response_bytes = Integer(parameters.fetch("response_bytes", "0"), 10)
    expected_request_bytes = Integer(
      parameters.fetch("expected_request_bytes", request_body.bytesize.to_s),
      10
    )
    raise "invalid benchmark wait_ms" unless (0..MAX_WAIT_MS).cover?(wait_ms)
    raise "invalid benchmark response_bytes" unless (0..MAX_BODY_BYTES).cover?(response_bytes)
    raise "invalid expected request bytes" unless (0..MAX_BODY_BYTES).cover?(expected_request_bytes)
    raise "unexpected request body size" unless request_body.bytesize == expected_request_bytes

    sleep(wait_ms / 1000.0) if wait_ms.positive?
    "B".b * response_bytes
  end

  def response_headers(env, request_body, response_body)
    headers = {
      "content-type" => "application/octet-stream",
      "content-length" => response_body.bytesize.to_s,
      "x-request-bytes" => request_body.bytesize.to_s,
      "x-ruby-call-count" => @calls.to_s,
      "x-ruby-thread-object-id" => Thread.current.object_id.to_s,
      "x-ruby-fiber-object-id" => Fiber.current.object_id.to_s
    }
    headers["x-async-version"] = Async::VERSION if defined?(Async::VERSION)
    headers["x-rack-request-header"] = env["HTTP_X_RUVOY_TEST"] if env["HTTP_X_RUVOY_TEST"]
    if env.fetch("PATH_INFO") == "/gc-stats"
      headers["x-ruby-heap-live-slots"] = GC.stat(:heap_live_slots).to_s
      headers["x-ruby-heap-available-slots"] = GC.stat(:heap_available_slots).to_s
    end
    headers
  end

  def rack_env_response(env)
    body = %w[
      REQUEST_METHOD
      PATH_INFO
      QUERY_STRING
      SERVER_NAME
      SERVER_PORT
      SERVER_PROTOCOL
      REMOTE_ADDR
      HTTP_HOST
      HTTP_X_RUVOY_TEST
      rack.url_scheme
      rack.multithread
      rack.multiprocess
      rack.run_once
    ].map { |key| "#{key}=#{env[key].inspect}" }.join("\n")
    text_response(body)
  end

  def enumerable_response
    [
      200,
      {
        "content-type" => "text/plain",
        "content-length" => "9",
        "set-cookie" => ["first=1", "second=2"]
      },
      TrackedRackBody.new("rack", "-body")
    ]
  end

  def text_response(body)
    [
      200,
      {
        "content-type" => "text/plain",
        "content-length" => body.bytesize.to_s
      },
      [body]
    ]
  end
end
