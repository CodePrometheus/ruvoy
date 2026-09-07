# frozen_string_literal: true

class RackCompatibilityMiddleware
  def initialize(app)
    @app = app
  end

  def call(env)
    status, headers, body = @app.call(env)
    headers["x-rack-middleware"] = "active"
    [ status, headers, body ]
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

# Yields chunks with a gap between them so a streaming server can be observed
# delivering the first bytes before the body is finished. A buffering server
# cannot produce the same timing.
class PacedRackBody
  class << self
    attr_accessor :yielded_count
  end
  self.yielded_count = 0

  def initialize(chunks:, chunk_bytes:, gap_seconds:)
    @chunks = chunks
    @chunk_bytes = chunk_bytes
    @gap_seconds = gap_seconds
  end

  def each
    @chunks.times do |index|
      sleep @gap_seconds if index.positive? && @gap_seconds.positive?
      self.class.yielded_count += 1
      # A single-digit marker keeps every chunk exactly chunk_bytes long.
      yield((index % 10).to_s.b + ("C".b * (@chunk_bytes - 1)))
    end
  end

  def close
    TrackedRackBody.closed_count += 1
  end
end

# Fails partway through the body, when the status and headers are already on
# the wire and cannot be taken back.
class FailingRackBody
  def initialize(chunks:, chunk_bytes:)
    @chunks = chunks
    @chunk_bytes = chunk_bytes
    @failed_at = chunks / 2
  end

  def each
    @chunks.times do |index|
      raise "intentional mid-body failure" if index == @failed_at
      yield "D".b * @chunk_bytes
    end
  end

  def close
    TrackedRackBody.closed_count += 1
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
    return paced_response(query) if path == "/paced"
    return failing_body_response if path == "/raise-mid-body"
    return text_response(PacedRackBody.yielded_count.to_s) if path == "/paced-yielded"
    return text_response(@calls.to_s) if path == "/counted"
    return text_response(TrackedRackBody.closed_count.to_s) if path == "/closed"

    response_body = route_body(env, path, query, request_body)
    headers = response_headers(env, request_body, response_body)
    [ 200, headers, [ response_body ] ]
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
    # rack.input is optional since Rack 3.1, so a server may legitimately omit it.
    env["rack.input"]&.read || ""
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
      [ env.fetch("REQUEST_METHOD"), path, request_body ].join(" ")
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

  def paced_response(query)
    parameters = query.split("&").to_h { |entry| entry.split("=", 2) }
    chunks = Integer(parameters.fetch("chunks", "4"), 10)
    chunk_bytes = Integer(parameters.fetch("chunk_bytes", "16"), 10)
    gap_ms = Integer(parameters.fetch("gap_ms", "200"), 10)
    raise "invalid paced chunks" unless (1..1_000).cover?(chunks)
    raise "invalid paced chunk_bytes" unless (1..1_048_576).cover?(chunk_bytes)
    raise "invalid paced gap_ms" unless (0..5_000).cover?(gap_ms)

    [
      200,
      {
        "content-type" => "application/octet-stream",
        "x-paced-chunks" => chunks.to_s,
        "x-paced-chunk-bytes" => chunk_bytes.to_s
      },
      PacedRackBody.new(chunks: chunks, chunk_bytes: chunk_bytes, gap_seconds: gap_ms / 1000.0)
    ]
  end

  # No content-length: the length is not known once the body may stop early.
  def failing_body_response
    [
      200,
      { "content-type" => "application/octet-stream", "x-failing-chunks" => "8" },
      FailingRackBody.new(chunks: 8, chunk_bytes: 4096)
    ]
  end

  def enumerable_response
    [
      200,
      {
        "content-type" => "text/plain",
        "content-length" => "9",
        "set-cookie" => [ "first=1", "second=2" ]
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
      [ body ]
    ]
  end
end
