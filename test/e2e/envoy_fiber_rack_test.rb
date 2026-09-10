# frozen_string_literal: true

require "socket"
require "timeout"
require_relative "test_helper"

# The fiber Rack bridge against a real Envoy: every way a body can arrive,
# one Ruby thread with a fiber per request, requests that yield overlapping
# while requests that block queue, cancellation, and a clean stop with a
# fiber still running.
class EnvoyFiberRackTest < E2ETestCase
  FIBER_PORT = 19183
  CONTROL_PORT = 19184
  ONE_MIB = 1024 * 1024
  EIGHT_MIB = 8 * ONE_MIB
  SDK_COMMIT = "8eea3285d6bdb89f8ea34632cfe7ce1608a8f374"
  STAGES = %w[ingress-ns body-copy-ns body-callbacks runtime-queue-ns rack-input-ns rack-call-ns response-copy-ns].freeze
  ASYNC_REQUESTS = 10
  WORKER_PROBES = 64
  BLOCKING_REQUESTS = 5

  def test_bridge_serves_through_a_real_envoy
    [ FIBER_PORT, CONTROL_PORT ].each { |port| assert_port_free(port) }
    @concurrency = Integer(ENV.fetch("RUVOY_ENVOY_CONCURRENCY", "1"), 10)
    raise ArgumentError, "RUVOY_ENVOY_CONCURRENCY must be a positive integer" unless @concurrency.positive?

    @log = File.join(result_dir, "poc3-envoy-fiber-rack-#{build_profile}-#{run_id}.log")
    @envoy_log = File.join(scratch_dir, "envoy.log")
    @zeros = "\0".b * ONE_MIB
    preflight
    start_envoy

    info = assert_info
    ruby_thread = info["x-ruby-thread-object-id"]
    runtime_thread = info["x-ruvoy-runtime-rust-thread-id"]
    assert_request_bodies
    stages = assert_stage_timing
    assert_large_response_and_exception
    5.times do |index|
      response = http_get(url("/gc"))
      assert_equal "200", response.code, "Fiber GC request #{index + 1} returned HTTP #{response.code}"
      assert_equal ruby_thread, response["x-ruby-thread-object-id"], "Fiber GC request #{index + 1} changed the Ruby runtime thread"
    end
    async_elapsed, unique_fibers = assert_yielding_requests_overlap(ruby_thread)
    unique_workers = assert_workers_stay_outside_the_runtime(runtime_thread)
    blocking_elapsed = assert_blocking_requests_queue
    control_seconds = assert_control_answers_while_blocked
    assert_cancellation
    assert_clean_stop_with_active_fiber

    text = File.read(@envoy_log)
    assert_includes text, "Dynamic module ABI version v0.1.0 matched", "Envoy did not report a matching Fiber module ABI"
    assert_includes text, "caught SIGINT", "Envoy did not record SIGINT"
    assert_includes text, "[ruvoy] Fiber runtime stopped", "Fiber runtime did not report a clean shutdown"
    refute_match(/panic|fatal|segmentation fault|Fiber runtime shutdown failed/i, text, "Envoy Fiber log contains a fatal runtime error")

    record("result=PASS", "async_version=#{locked_version("async")}", "get_post_headers_1mib=PASS", "chunked_1mib=PASS",
           "beyond_buffer_body=PASS", *stages.map { |name, value| "stage_#{name.tr("-", "_")}=#{value}" },
           "ruby_exception_gc=PASS", "scheduler_aware_requests=#{ASYNC_REQUESTS}",
           "scheduler_aware_elapsed_seconds=#{format("%.6f", async_elapsed)}", "scheduler_aware_unique_fibers=#{unique_fibers}",
           "configured_envoy_workers=#{@concurrency}", "observed_envoy_worker_threads=#{unique_workers}",
           "scheduler_unaware_requests=#{BLOCKING_REQUESTS}",
           "scheduler_unaware_elapsed_seconds=#{format("%.6f", blocking_elapsed)}",
           "runtime_thread_id=#{runtime_thread}", "worker_thread_id=#{info["x-ruvoy-worker-rust-thread-id"]}",
           "ruby_thread_object_id=#{ruby_thread}", "control_latency_while_fiber_blocked_seconds=#{control_seconds}",
           "client_cancel_late_response=PASS", "client_cancel_during_body=PASS", "sigint_with_active_fiber=PASS",
           "fiber_runtime_shutdown=PASS")
    puts "raw result: #{@log}"
  rescue Minitest::Assertion => failure
    record("result=FAIL", "failure=#{failure.message}")
    raise
  ensure
    @envoy&.stop
    record("", "===== envoy log =====", File.read(@envoy_log)) if @envoy_log && File.exist?(@envoy_log)
  end

  private

  def url(path)
    "http://127.0.0.1:#{FIBER_PORT}#{path}"
  end

  def record(*lines)
    File.write(@log, lines.map { |line| "#{line}\n" }.join, mode: "a")
  end

  def preflight
    record("run_id=#{run_id}", "build_profile=#{build_profile}", "envoy_concurrency=#{@concurrency}",
           "envoy_package=#{Envoy::PACKAGE}", "envoy_sdk_commit=#{SDK_COMMIT}", "ruby_version=#{RUBY_VERSION}",
           "async_version=#{locked_version("async")}", "rustc=#{IO.popen(%w[rustc --version], &:read).strip}",
           "host=#{IO.popen(%w[uname -srm], &:read).strip}")
    build_module("fiber", log: @log)
    check_worker_boundary(log: @log)
    system("uvx", "--from", Envoy::PACKAGE, "envoy", "--version", out: [ @log, "a" ], err: [ :child, :out ])
  end

  def start_envoy
    config = File.join(scratch_dir, "envoy.yaml")
    template = File.read(File.join(root, "config", "envoy-fiber-rack.yaml"))
    File.write(config, template.sub("value: bench/config.ru", "value: #{fixture_rackup}"))
    assert_includes File.read(config), "value: #{fixture_rackup}", "failed to configure the Rack fixture"
    @envoy = Envoy.start(config: config, modules: module_dir, log: @envoy_log, concurrency: @concurrency,
                         log_level: "info", env: bundle_env.merge("RUVOY_DIAGNOSTICS" => "1"))
    ready = @envoy.serving?("http://127.0.0.1:#{CONTROL_PORT}/", timeout: 15) && @envoy.serving?(url("/info"), timeout: 15)
    assert ready, "Fiber listener did not become ready:\n#{tail(@envoy_log)}"
  end

  def assert_info
    response = http_get(url("/info"))
    assert_equal "200", response.code, "GET /info returned HTTP #{response.code}"
    assert_equal locked_version("async"), response["x-async-version"], "the response did not report the locked Async"
    %w[x-ruvoy-runtime-rust-thread-id x-ruvoy-worker-rust-thread-id x-ruby-thread-object-id].each do |header|
      refute_nil response[header], "missing #{header}"
    end
    refute_equal response["x-ruvoy-runtime-rust-thread-id"], response["x-ruvoy-worker-rust-thread-id"],
                 "Fiber runtime and Envoy worker used the same Rust thread"
    response
  end

  def post(path, body, headers = {}, &configure)
    uri = URI(url(path))
    request = Net::HTTP::Post.new(uri.request_uri, { "content-type" => "application/octet-stream" }.merge(headers))
    request.body = body
    Net::HTTP.start(uri.host, uri.port, read_timeout: 30) do |http|
      configure&.call(http, request)
      http.request(request)
    end
  end

  def assert_echo(response, expected, context)
    assert_equal "200", response.code, "#{context} returned HTTP #{response.code}"
    assert response.body.to_s.b == expected, "#{context} was not echoed exactly"
  end

  def assert_request_bodies
    small = post("/echo", "fiber-body", "x-ruvoy-test" => "fiber-header")
    assert_echo small, "fiber-body".b, "Fiber POST /echo"
    assert_equal "fiber-header", small["x-rack-request-header"], "Fiber request header was not copied into the Rack env"

    timed = post("/echo", @zeros, "x-ruvoy-stage-timing" => "1")
    assert_echo timed, @zeros, "Fiber 1 MiB POST /echo"
    STAGES.each do |stage|
      assert_match(/\A\d+\z/, timed["x-ruvoy-stage-#{stage}"].to_s, "invalid or missing Fiber stage timing header #{stage}")
    end

    chunked = post("/echo", nil, "x-ruvoy-stage-timing" => "1", "transfer-encoding" => "chunked") do |_, request|
      request.body = nil
      request.body_stream = StringIO.new(@zeros)
    end
    assert_echo chunked, @zeros, "Fiber chunked 1 MiB POST /echo"

    # A request that says it has a body and then has none still has to reach
    # the application with a readable, empty `rack.input`.
    assert_echo post("/echo", ""), "".b, "Fiber empty POST /echo"

    # Asking permission before sending a large body starts the application on
    # headers that are not yet followed by anything.
    continued = post("/echo", @zeros, "expect" => "100-continue") { |http, _| http.continue_timeout = 5 }
    assert_echo continued, @zeros, "Fiber 1 MiB POST /echo with 100-continue"

    # HTTP/2 frames the body differently from chunked HTTP/1.1 and is what a
    # real deployment usually carries.
    assert_echo http2_post("/echo", @zeros), @zeros, "Fiber 1 MiB POST /echo over HTTP/2"

    # Trailers arrive after the last body byte and reach a different callback
    # than the body itself, so a body that ends in one must still be complete.
    assert_equal ONE_MIB, trailered_echo(@zeros), "Fiber trailered request did not echo the whole body"

    # Several times the runtime's own buffer, so it can only be served by
    # taking the body in pieces while the rest waits in Envoy.
    eight = "\0".b * EIGHT_MIB
    assert_echo post("/echo", eight), eight, "Fiber 8 MiB POST /echo"
  end

  Http2Response = Struct.new(:code, :body)

  def http2_post(path, body)
    input = File.join(scratch_dir, "http2-request.bin")
    output = File.join(scratch_dir, "http2-response.bin")
    File.binwrite(input, body)
    code = IO.popen([ "curl", "--http2-prior-knowledge", "--silent", "--show-error", "--max-time", "30",
                      "--output", output, "--write-out", "%{http_code}", "--request", "POST",
                      "--data-binary", "@#{input}", url(path) ], &:read)
    Http2Response.new(code, File.exist?(output) ? File.binread(output) : "")
  end

  def trailered_echo(body)
    socket = TCPSocket.new("127.0.0.1", FIBER_PORT)
    socket.timeout = 15
    socket.write("POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n" \
                 "Transfer-Encoding: chunked\r\nTrailer: x-ruvoy-trailer\r\n\r\n")
    body.bytes.each_slice(65_536) do |slice|
      chunk = slice.pack("C*")
      socket.write(format("%x\r\n", chunk.bytesize), chunk, "\r\n")
    end
    socket.write("0\r\nx-ruvoy-trailer: done\r\n\r\n")
    raw = socket.read.b
    head, payload = raw.split("\r\n\r\n".b, 2)
    assert head.start_with?("HTTP/1.1 200"), "Fiber trailered request did not return 200"
    return payload.bytesize unless head.downcase.include?("chunked")

    payload.scan(/^([0-9a-f]+)\r\n/i).sum { |(size)| size.to_i(16) }
  ensure
    socket&.close
  end

  def assert_stage_timing
    response = post("/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=#{ONE_MIB}", @zeros,
                    "x-ruvoy-stage-timing" => "1")
    assert_equal "200", response.code, "Fiber timed 1 MiB POST /benchmark returned HTTP #{response.code}"
    assert_empty response.body.to_s, "timed Fiber benchmark unexpectedly returned a response body"
    assert_equal ONE_MIB.to_s, response["x-request-bytes"], "timed Fiber benchmark did not receive the complete request body"
    (STAGES + %w[scheduler-return-ns]).to_h { |stage| [ stage, response["x-ruvoy-stage-#{stage}"] ] }
  end

  def assert_large_response_and_exception
    large = http_get(url("/large-response"))
    assert_equal "200", large.code, "Fiber GET /large-response returned HTTP #{large.code}"
    assert large.body.b == "F".b * ONE_MIB, "Fiber 1 MiB response body was not returned exactly"

    error = http_get(url("/raise"))
    assert_equal "500", error.code, "Fiber GET /raise returned HTTP #{error.code}"
    assert_includes error.body, "intentional fiber envoy boom", "Fiber Ruby exception did not cross the owned bridge"
  end

  def concurrently(count, &request)
    Array.new(count) { |index| Thread.new { request.call(index) } }.map(&:value)
  end

  def assert_yielding_requests_overlap(ruby_thread)
    responses = nil
    seconds = elapsed { responses = concurrently(ASYNC_REQUESTS) { http_get(url("/async-sleep?seconds=0.2")) } }
    assert_operator seconds, :<, 0.8, "#{ASYNC_REQUESTS} scheduler-aware 200 ms requests took #{seconds}s"
    responses.each_with_index do |response, index|
      assert_equal "200", response.code, "Async sleep request #{index + 1} did not return 200"
      assert_equal ruby_thread, response["x-ruby-thread-object-id"], "Async sleep request #{index + 1} ran on a different Ruby thread"
    end
    fibers = responses.map { |response| response["x-ruby-fiber-object-id"] }.uniq.size
    assert_equal ASYNC_REQUESTS, fibers, "expected #{ASYNC_REQUESTS} request Fibers, observed #{fibers}"
    [ seconds, fibers ]
  end

  def assert_workers_stay_outside_the_runtime(runtime_thread)
    responses = concurrently(WORKER_PROBES) { http_get(url("/info"), "connection" => "close") }
    workers = responses.each_with_index.map do |response, index|
      assert_equal "200", response.code, "worker distribution request #{index + 1} did not return 200"
      assert_equal runtime_thread, response["x-ruvoy-runtime-rust-thread-id"], "worker distribution request #{index + 1} changed the runtime thread"
      worker = response["x-ruvoy-worker-rust-thread-id"]
      refute_nil worker, "worker distribution request #{index + 1} reported no worker thread"
      refute_equal runtime_thread, worker, "worker distribution request #{index + 1} ran on the runtime thread"
      worker
    end
    unique = workers.uniq.size
    assert_operator unique, :>=, 2, "multi-worker probe observed only #{unique} Envoy worker" if @concurrency > 1
    unique
  end

  def assert_blocking_requests_queue
    seconds = elapsed do
      concurrently(BLOCKING_REQUESTS) { http_get(url("/blocking?seconds=0.1")) }.each do |response|
        assert_equal "200", response.code, "a scheduler-unaware blocking request failed"
      end
    end
    assert_operator seconds, :>=, 0.45, "#{BLOCKING_REQUESTS} scheduler-unaware 100 ms requests overlapped in #{seconds}s"
    assert_operator seconds, :<, 2.0, "#{BLOCKING_REQUESTS} scheduler-unaware 100 ms requests took #{seconds}s"
    seconds
  end

  def assert_control_answers_while_blocked
    blocked = Thread.new { http_get(url("/blocking?seconds=0.5")) }
    sleep 0.05
    control = nil
    seconds = elapsed { control = http_get("http://127.0.0.1:#{CONTROL_PORT}/") }
    assert_equal "200", blocked.value.code, "long blocking Fiber request failed"
    assert_equal "fiber-control-ok", control.body, "Fiber control listener returned the wrong body"
    assert_operator seconds, :<, 0.2, "control listener took #{seconds}s while the Fiber reactor was blocked"
    seconds
  end

  def assert_cancellation
    assert_raises(Timeout::Error, "Fiber cancel probe did not time out") do
      Timeout.timeout(0.02) { http_get(url("/async-sleep?seconds=0.5")) }
    end
    sleep 0.6
    assert_raises(Timeout::Error, "Fiber body cancel probe did not time out") do
      Timeout.timeout(0.05) do
        post("/echo", nil, "content-length" => ONE_MIB.to_s) do |_, request|
          request.body = nil
          request.body_stream = ThrottledUpload.new(@zeros, 64 * 1024)
        end
      end
    end
    after = http_get(url("/info"))
    assert_equal "200", after.code, "request after cancelled Fiber response returned HTTP #{after.code}"
  end

  def assert_clean_stop_with_active_fiber
    active = Thread.new do
      http_get(url("/slow-shutdown?seconds=1.0"))
    rescue StandardError
      nil
    end
    sleep 0.1
    status = @envoy.stop(grace: 15)
    refute_nil status, "Envoy hung during SIGINT with an active Fiber request"
    assert_predicate status, :success?, "Envoy Fiber process exited non-zero after SIGINT"
    active.join
  end
end
