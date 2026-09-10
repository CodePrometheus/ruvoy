# frozen_string_literal: true

require "digest"
require "timeout"
require_relative "test_helper"

# The synchronous Rack bridge against a real Envoy: bodies in both directions,
# one Ruby thread for every request, an exception crossing the bridge, a
# control listener that answers while Ruby blocks, and a clean stop with a
# request still in flight.
class EnvoySyncRackTest < E2ETestCase
  SYNC_PORT = 19181
  CONTROL_PORT = 19182
  ONE_MIB = 1024 * 1024
  GC_REQUESTS = 10
  CONCURRENT_REQUESTS = 64
  SDK_COMMIT = "8eea3285d6bdb89f8ea34632cfe7ce1608a8f374"

  def test_bridge_serves_through_a_real_envoy
    [ SYNC_PORT, CONTROL_PORT ].each { |port| assert_port_free(port) }
    @log = File.join(result_dir, "poc2-envoy-sync-rack-#{run_id}.log")
    @envoy_log = File.join(scratch_dir, "envoy.log")
    preflight
    @envoy = Envoy.start(config: File.join(root, "config", "envoy-sync-rack.yaml"), modules: module_dir,
                         log: @envoy_log, log_level: "info", env: { "RUVOY_DIAGNOSTICS" => "1" })
    assert @envoy.serving?(control_url, timeout: 15) && @envoy.serving?(url("/info"), timeout: 15),
           "Envoy did not become ready:\n#{tail(@envoy_log)}"

    info = assert_info
    ruby_thread = info["x-ruby-thread-object-id"]
    large_request, large_response = assert_bodies
    GC_REQUESTS.times do |index|
      response = http_get(url("/gc"))
      assert_equal "200", response.code, "GC request #{index + 1} returned HTTP #{response.code}"
      assert_equal ruby_thread, response["x-ruby-thread-object-id"], "GC request #{index + 1} changed the Ruby runtime thread"
    end
    assert_concurrency(ruby_thread)
    assert_exception
    control_seconds = assert_control_answers_while_ruby_blocks
    assert_late_response_survives_cancellation
    assert_clean_stop_with_active_request

    text = File.read(@envoy_log)
    assert_includes text, "Dynamic module ABI version v0.1.0 matched", "Envoy did not report a matching dynamic-module ABI"
    assert_includes text, "caught SIGINT", "Envoy did not record SIGINT"
    assert_includes text, "[ruvoy] Ruby runtime stopped", "Ruby runtime did not report a clean shutdown"
    refute_match(/panic|fatal|segmentation fault|Ruby runtime shutdown failed/i, text, "Envoy log contains a fatal runtime error")

    record("result=PASS", "get_post_headers=PASS", "empty_small_1mib_request=PASS", "one_mib_response=PASS",
           "forced_gc_requests=#{GC_REQUESTS}", "concurrent_requests=#{CONCURRENT_REQUESTS}", "ruby_exception=PASS",
           "runtime_thread_id=#{info["x-ruvoy-runtime-rust-thread-id"]}",
           "worker_thread_id=#{info["x-ruvoy-worker-rust-thread-id"]}",
           "ruby_thread_object_id=#{ruby_thread}",
           "control_latency_while_ruby_blocked_seconds=#{control_seconds}",
           "client_cancel_late_response=PASS", "sigint_with_active_request=PASS", "ruby_runtime_shutdown=PASS",
           "large_request_sha256=#{Digest::SHA256.hexdigest(large_request)}",
           "large_response_sha256=#{Digest::SHA256.hexdigest(large_response)}")
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
    "http://127.0.0.1:#{SYNC_PORT}#{path}"
  end

  def control_url
    "http://127.0.0.1:#{CONTROL_PORT}/"
  end

  def record(*lines)
    File.write(@log, lines.map { |line| "#{line}\n" }.join, mode: "a")
  end

  def preflight
    record("run_id=#{run_id}", "envoy_package=#{Envoy::PACKAGE}", "envoy_sdk_commit=#{SDK_COMMIT}",
           "ruby_version=#{RUBY_VERSION}", "build_profile=#{build_profile}",
           "rustc=#{IO.popen(%w[rustc --version], &:read).strip}", "host=#{IO.popen(%w[uname -srm], &:read).strip}")
    build_module("sync", log: @log)
    check_worker_boundary(log: @log)
    system("uvx", "--from", Envoy::PACKAGE, "envoy", "--version", out: [ @log, "a" ], err: [ :child, :out ])
  end

  def assert_info
    response = http_get(url("/info"))
    assert_equal "200", response.code, "GET /info returned HTTP #{response.code}"
    assert_equal "GET /info ", response.body, "GET /info returned an unexpected body"
    %w[x-ruvoy-runtime-rust-thread-id x-ruvoy-worker-rust-thread-id x-ruby-thread-object-id].each do |header|
      refute_nil response[header], "missing #{header}"
    end
    refute_equal response["x-ruvoy-runtime-rust-thread-id"], response["x-ruvoy-worker-rust-thread-id"],
                 "Ruby runtime and Envoy worker used the same Rust thread"
    response
  end

  def assert_bodies
    small = http_post(url("/echo"), "hello-rack", "x-ruvoy-test" => "copied-header")
    assert_equal "200", small.code, "small POST /echo returned HTTP #{small.code}"
    assert_equal "hello-rack", small.body, "small POST body was not echoed exactly"
    assert_equal "copied-header", small["x-rack-request-header"], "owned request header was not copied into the Rack env"

    empty = http_post(url("/echo"), "")
    assert_equal "200", empty.code, "empty POST /echo returned HTTP #{empty.code}"
    assert_empty empty.body.to_s, "empty POST did not return an empty body"

    zeros = "\0".b * ONE_MIB
    large = http_post(url("/echo"), zeros, "content-type" => "application/octet-stream")
    assert_equal "200", large.code, "1 MiB POST /echo returned HTTP #{large.code}"
    assert large.body.b == zeros, "1 MiB request body was not echoed exactly"

    response = http_get(url("/large-response"))
    assert_equal "200", response.code, "GET /large-response returned HTTP #{response.code}"
    assert response.body.b == "R".b * ONE_MIB, "1 MiB response body was not returned exactly"
    [ large.body.b, response.body.b ]
  end

  def assert_concurrency(ruby_thread)
    responses = Array.new(CONCURRENT_REQUESTS) do |index|
      Thread.new do
        uri = URI(url("/echo"))
        Net::HTTP.start(uri.host, uri.port, read_timeout: 10) { |http| http.post(uri.request_uri, "body-#{index + 1}") }
      end
    end.map(&:value)
    responses.each_with_index do |response, index|
      assert_equal "200", response.code, "concurrent request #{index + 1} did not return 200"
      assert_equal "body-#{index + 1}", response.body, "concurrent request #{index + 1} returned the wrong body"
      assert_equal ruby_thread, response["x-ruby-thread-object-id"], "concurrent request #{index + 1} ran on a different Ruby thread"
    end
  end

  def assert_exception
    response = http_get(url("/raise"))
    assert_equal "500", response.code, "GET /raise returned HTTP #{response.code}"
    assert_equal "true", response["x-ruvoy-error"], "Ruby exception response did not carry x-ruvoy-error"
    assert_includes response.body, "intentional envoy boom", "Ruby exception text did not cross the owned bridge"
  end

  def assert_control_answers_while_ruby_blocks
    slow = Thread.new { http_get(url("/slow")) }
    sleep 0.05
    control = nil
    seconds = elapsed { control = http_get(control_url) }
    assert_equal "200", slow.value.code, "slow Rack request did not return 200"
    assert_equal "control-ok", control.body, "control listener returned the wrong body"
    assert_operator seconds, :<, 0.2, "control listener took #{seconds}s while Ruby was blocked"
    seconds
  end

  def assert_late_response_survives_cancellation
    assert_raises(Timeout::Error, "cancel probe did not time out") do
      Timeout.timeout(0.02) { http_get(url("/slow")) }
    end
    sleep 0.65
    response = http_get(url("/info"))
    assert_equal "200", response.code, "request after cancelled late response returned HTTP #{response.code}"
  end

  def assert_clean_stop_with_active_request
    active = Thread.new do
      http_get(url("/slow-shutdown"))
    rescue StandardError
      nil
    end
    sleep 0.1
    status = @envoy.stop(grace: 15)
    refute_nil status, "Envoy hung during SIGINT with an active Ruby request"
    assert_predicate status, :success?, "Envoy exited non-zero after SIGINT"
    active.join
  end
end
