# frozen_string_literal: true

require "stringio"
require "timeout"
require_relative "test_helper"

# Request admission and request-body streaming under the fiber runtime: the
# admission limit answers 503 while the control listener stays responsive, and
# a body larger than the hand-off queue waits in Envoy instead of being
# refused or truncated.
class FiberBackpressureTest < E2ETestCase
  FIBER_PORT = 19183
  CONTROL_PORT = 19184
  ADMIN_PORT = 19185
  ONE_MIB = 1024 * 1024
  EIGHT_MIB = 8 * ONE_MIB
  ABANDON_BYTES_PER_SECOND = 200 * 1024
  PAUSED_READING = "http.ruvoy_fiber_rack.downstream_flow_control_paused_reading_total"

  class << self
    def results
      @results ||= File.join(result_dir, "poc4-fiber-backpressure-#{run_id}").tap { |dir| FileUtils.mkdir_p(dir) }
    end

    def config
      @config ||= File.join(scratch_dir, "envoy.yaml").tap do |path|
        template = File.read(File.join(root, "config", "envoy-fiber-rack.yaml"))
        File.write(path, template.sub("value: bench/config.ru", "value: #{fixture_rackup}"))
        raise "failed to configure the Rack fixture" unless File.read(path).include?("value: #{fixture_rackup}")
      end
    end

    def prepared
      @prepared ||= begin
        log = File.join(results, "preflight.log")
        File.write(log, [ "run_id=#{run_id}", "envoy_package=#{Envoy::PACKAGE}", "ruby_version=#{RUBY_VERSION}",
                          "rustc=#{IO.popen(%w[rustc --version], &:read).strip}", "" ].join("\n"))
        build_module("build-fiber-module.sh", log: log)
        run_script("check-worker-boundary.sh", log: log)
        true
      end
    end
  end

  def setup
    self.class.prepared
    [ FIBER_PORT, CONTROL_PORT, ADMIN_PORT ].each { |port| assert_port_free(port) }
  end

  def teardown
    @envoy&.stop
  end

  def test_the_admission_limit_refuses_and_then_releases
    start_envoy("request-limit", 2)
    holders = Array.new(2) { Thread.new { http_get(url("/async-sleep?seconds=0.6")) } }
    sleep 0.15

    overload = http_get(url("/info"))
    assert_equal "503", overload.code, "request admission overload returned HTTP #{overload.code}"
    assert_equal "true", overload["x-ruvoy-error"], "request admission overload response omitted x-ruvoy-error"
    assert_includes overload.body, "Ruby runtime admission limit reached", "request admission overload response returned the wrong reason"

    control_seconds = assert_control_answers("during request overload")
    holders.each_with_index do |holder, index|
      assert_equal "200", holder.value.code, "request admission holder #{index + 1} did not return 200"
    end
    released = http_get(url("/info"))
    assert_equal "200", released.code, "request after admission release returned HTTP #{released.code}"
    stop_envoy
    summarize("request_limit=2", "request_overload_status=503",
              "request_control_latency_seconds=#{control_seconds}", "request_capacity_release=PASS")
  end

  # One request may hold 1 MiB, so eight of them exercise the wait in Envoy.
  def test_bodies_larger_than_the_queue_stream_instead_of_being_refused
    start_envoy("body-streaming", 16)
    eight_mib = "\0".b * EIGHT_MIB

    streamed = http_post(url("/echo"), eight_mib, "content-type" => "application/octet-stream")
    assert_equal "200", streamed.code, "8 MiB POST /echo returned HTTP #{streamed.code}"
    assert streamed.body.b == eight_mib, "a body eight times the hand-off queue was not echoed exactly"

    chunked = chunked_post(url("/echo"), eight_mib)
    assert_equal "200", chunked.code, "chunked 8 MiB POST /echo returned HTTP #{chunked.code}"
    assert chunked.body.b == eight_mib, "a chunked body eight times the hand-off queue was not echoed exactly"

    # Uploads whose application sleeps before reading them: what the runtime
    # has no room for waits in Envoy until Envoy stops reading from the client.
    paused_before = admin_stat(ADMIN_PORT, PAUSED_READING).to_i
    unread = Array.new(2) do
      Thread.new { http_post(url("/async-sleep?seconds=0.4"), eight_mib, "content-type" => "application/octet-stream") }
    end
    sleep 0.2
    control_seconds = assert_control_answers("while bodies were queued")
    unread.each_with_index do |upload, index|
      response = upload.value
      assert_equal "200", response.code, "unread-body upload #{index + 1} returned HTTP #{response.code}"
      assert_equal EIGHT_MIB.to_s, response["x-request-bytes"], "unread-body upload #{index + 1} was truncated"
    end

    # A client that leaves mid-upload strands a body split between Envoy and
    # the runtime; both have to be reclaimed and the request accounted for.
    abandoned_before = module_stat("responses_total.*cancelled")
    abandon_upload(eight_mib)
    sleep 1
    abandoned_after = module_stat("responses_total.*cancelled")
    assert_operator abandoned_after, :>, abandoned_before,
                    "an upload abandoned mid-body was never accounted for (#{abandoned_before} -> #{abandoned_after})"
    survivor = http_get(url("/info"))
    assert_equal "200", survivor.code, "request after an abandoned upload returned HTTP #{survivor.code}"

    paused = admin_stat(ADMIN_PORT, PAUSED_READING).to_i - paused_before
    assert_operator paused, :>, 0, "Envoy never stopped reading from a client the application was not keeping up with"
    stop_envoy
    summarize("queue_bytes_per_request=#{ONE_MIB}", "streamed_body_bytes=#{EIGHT_MIB}", "streamed_body_chunked=PASS",
              "unread_body_uploads=2", "unread_body_bytes=#{EIGHT_MIB}", "downstream_reads_paused=#{paused}",
              "body_control_latency_seconds=#{control_seconds}")
  end

  private

  def url(path)
    "http://127.0.0.1:#{FIBER_PORT}#{path}"
  end

  def start_envoy(name, request_limit)
    @envoy_log = File.join(self.class.results, "#{name}-envoy.log")
    @envoy = Envoy.start(config: self.class.config, modules: module_dir, log: @envoy_log, log_level: "info",
                         admin_port: ADMIN_PORT,
                         env: bundle_env.merge("RUVOY_MAX_INFLIGHT_REQUESTS" => request_limit.to_s))
    ready = @envoy.serving?("http://127.0.0.1:#{CONTROL_PORT}/", timeout: 10) && @envoy.serving?(url("/info"), timeout: 10)
    assert ready, "#{name} Envoy did not become ready:\n#{tail(@envoy_log)}"
  end

  def stop_envoy
    status = @envoy.stop
    refute_nil status, "Envoy did not stop after SIGINT"
    assert_predicate status, :success?, "Envoy exited non-zero after SIGINT"
    text = File.read(@envoy_log)
    assert_includes text, "[ruvoy] Fiber runtime stopped", "Fiber runtime did not report a clean shutdown"
    refute_match(/panic|fatal|segmentation fault|Fiber runtime shutdown failed/i, text, "Envoy log contains a fatal runtime error")
  end

  def assert_control_answers(context)
    control = nil
    seconds = elapsed { control = http_get("http://127.0.0.1:#{CONTROL_PORT}/") }
    assert_equal "fiber-control-ok", control.body, "control listener returned the wrong body #{context}"
    assert_operator seconds, :<, 0.2, "control listener took #{seconds}s #{context}"
    seconds
  end

  def chunked_post(target, data)
    uri = URI(target)
    request = Net::HTTP::Post.new(uri.request_uri, "content-type" => "application/octet-stream",
                                                   "transfer-encoding" => "chunked")
    request.body_stream = StringIO.new(data)
    Net::HTTP.start(uri.host, uri.port, read_timeout: 30) { |http| http.request(request) }
  end

  def abandon_upload(data)
    uri = URI(url("/echo"))
    request = Net::HTTP::Post.new(uri.request_uri, "content-type" => "application/octet-stream",
                                                   "content-length" => data.bytesize.to_s)
    request.body_stream = ThrottledUpload.new(data, ABANDON_BYTES_PER_SECOND)
    Timeout.timeout(1) { Net::HTTP.start(uri.host, uri.port) { |http| http.request(request) } }
  rescue Timeout::Error, IOError, SystemCallError
    nil
  end

  def module_stat(pattern)
    stats = http_get("http://127.0.0.1:#{ADMIN_PORT}/stats").body
    stats.lines.find { |line| line.match?(/dynamicmodules.*#{pattern}/) }&.split(": ", 2)&.last.to_i
  end

  def summarize(*lines)
    File.write(File.join(self.class.results, "summary.log"), ([ "result=PASS" ] + lines).map { |line| "#{line}\n" }.join, mode: "a")
    puts "raw results: #{self.class.results}"
  end
end
