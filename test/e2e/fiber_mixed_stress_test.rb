# frozen_string_literal: true

require "json"
require_relative "test_helper"

# Cycles of steady mixed load, request overload, upload pressure, disconnect
# churn and recovery against one runtime. Each cycle has to come back to the
# same place: the control listener answering, overload refused rather than
# queued, uploads slowed rather than refused, and memory and descriptors flat.
class FiberMixedStressTest < E2ETestCase
  FIBER_PORT = 19183
  CONTROL_PORT = 19184
  MAX_INFLIGHT_REQUESTS = 256
  OHA_VERSION = "oha 1.15.0"
  CONTROL_LIMIT_SECONDS = 0.5
  DEFAULTS = { cycles: 3, envoy_concurrency: 1, steady_duration: "30s", overload_duration: "15s",
               recovery_duration: "20s", disconnect_requests: 128 }.freeze
  SMOKE = { cycles: 1, steady_duration: "3s", overload_duration: "3s", recovery_duration: "3s",
            disconnect_requests: 16 }.freeze

  def test_every_cycle_recovers
    @settings = mixed_settings
    assert File.executable?(oha), "missing project-local oha"
    assert_equal OHA_VERSION, IO.popen([ oha, "--version" ], &:read).strip, "expected #{OHA_VERSION}"
    [ FIBER_PORT, CONTROL_PORT ].each { |port| assert_port_free(port) }
    @results = File.join(result_dir, "poc6-fiber-mixed-stress-#{run_id}")
    FileUtils.mkdir_p(@results)
    write_headers
    @bodies = [ 262_144, 1_048_576 ].to_h do |size|
      [ size, File.join(scratch_dir, "body-#{size}.bin").tap { |path| File.binwrite(path, "\0".b * size) } ]
    end
    write_manifest
    preflight
    start_envoy
    @phase = "startup"
    sampler = sample_resources
    record_heap("baseline")

    1.upto(@settings[:cycles]) do |cycle|
      run_steady_mix(cycle)
      run_request_overload(cycle)
      run_body_pressure(cycle)
      run_disconnect_churn(cycle)
      run_recovery(cycle)
      record_heap("cycle-#{cycle}-recovered")
    end

    first = @heap.fetch("cycle-1-recovered")
    last = @heap.fetch("cycle-#{@settings[:cycles]}-recovered")
    assert_operator last[:slots], :<=, first[:slots] * 1.10 + 5000,
                    "Ruby live slots grew beyond the mixed-stress threshold: #{first[:slots]} -> #{last[:slots]}"
    assert_operator last[:rss], :<=, [ first[:rss] * 1.25, first[:rss] + 65_536 ].max,
                    "Envoy RSS grew beyond the mixed-stress threshold: #{first[:rss]} -> #{last[:rss]} KiB"
    assert_operator last[:fds], :<=, first[:fds] + 32,
                    "Envoy FD count grew beyond the mixed-stress threshold: #{first[:fds]} -> #{last[:fds]}"

    @sampling = false
    sampler.join
    status = @envoy.stop
    assert status&.success?, "Envoy did not stop after the bounded shutdown sequence"
    assert_includes File.read(@envoy_log), "[ruvoy] Fiber runtime stopped", "Fiber runtime did not report clean shutdown"
    puts "result=PASS", "cycles=#{@settings[:cycles]}", "heap_live_slots=#{first[:slots]}->#{last[:slots]}",
         "rss_kib=#{first[:rss]}->#{last[:rss]}", "fd_count=#{first[:fds]}->#{last[:fds]}", "raw_results=#{@results}"
  ensure
    @load&.each { |pid| terminate(pid) }
    @sampling = false
    sampler&.join
    @envoy&.stop
  end

  private

  def oha
    File.join(root, ".tools", "oha", "oha")
  end

  def url(path)
    "http://127.0.0.1:#{FIBER_PORT}#{path}"
  end

  def benchmark(wait_ms, response_bytes: 0, request_bytes: 0)
    url("/benchmark?wait_ms=#{wait_ms}&response_bytes=#{response_bytes}&expected_request_bytes=#{request_bytes}")
  end

  def mixed_settings
    mode = ENV.fetch("RUVOY_MIXED_MODE", "full")
    raise ArgumentError, "RUVOY_MIXED_MODE must be full or smoke" unless %w[full smoke].include?(mode)

    settings = DEFAULTS.to_h { |key, default| [ key, ENV.fetch("RUVOY_MIXED_#{key.upcase}", default.to_s) ] }
    settings.merge!(SMOKE.transform_values(&:to_s)) if mode == "smoke"
    settings.to_h do |key, value|
      if key.end_with?("_duration")
        raise ArgumentError, "invalid duration: #{value}" unless value.match?(/\A[1-9]\d*(ms|s|m)\z/)

        [ key, value ]
      else
        number = Integer(value, 10)
        raise ArgumentError, "RUVOY_MIXED_#{key.upcase} must be positive" unless number.positive?

        [ key, number ]
      end
    end.merge(mode: mode)
  end

  def milliseconds(duration)
    amount = duration.to_i
    case duration
    when /ms\z/ then amount
    when /s\z/ then amount * 1000
    else amount * 60_000
    end
  end

  def requests_for(rate, duration)
    (rate * milliseconds(duration) + 999) / 1000
  end

  def write_headers
    @files = {
      resources: [ "resources.tsv", %w[epoch phase cpu_percent rss_kib fd_count] ],
      heap: [ "heap.tsv", %w[label heap_live_slots rss_kib fd_count] ],
      status: [ "status.tsv", %w[label total status_200 status_503 transport_errors] ],
      control: [ "control.tsv", %w[label elapsed_seconds] ]
    }.transform_values do |(name, columns)|
      File.join(@results, name).tap { |path| File.write(path, "#{columns.join("\t")}\n") }
    end
    @heap = {}
  end

  def append(file, *fields)
    File.write(@files.fetch(file), "#{fields.join("\t")}\n", mode: "a")
  end

  def write_manifest
    File.write(File.join(@results, "manifest.env"), {
      "run_id" => run_id, "mode" => @settings[:mode], "cycles" => @settings[:cycles],
      "host" => IO.popen(%w[uname -srm], &:read).strip, "envoy_concurrency" => @settings[:envoy_concurrency],
      "max_inflight_requests" => MAX_INFLIGHT_REQUESTS, "steady_duration" => @settings[:steady_duration],
      "overload_duration" => @settings[:overload_duration], "recovery_duration" => @settings[:recovery_duration],
      "disconnect_requests" => @settings[:disconnect_requests], "ruby" => RUBY_DESCRIPTION,
      "rustc" => IO.popen(%w[rustc --version], &:read).strip, "oha" => OHA_VERSION
    }.map { |key, value| "#{key}=#{value}\n" }.join)
  end

  def preflight
    log = File.join(@results, "preflight.log")
    build_module("fiber", log: log)
    check_worker_boundary(log: log)
  end

  def start_envoy
    config = File.join(scratch_dir, "envoy.yaml")
    template = File.read(File.join(root, "config", "envoy-fiber-rack.yaml"))
    File.write(config, template.sub("value: bench/config.ru", "value: #{fixture_rackup}"))
    assert_includes File.read(config), "value: #{fixture_rackup}", "failed to configure the Rack fixture"
    @envoy_log = File.join(@results, "envoy.log")
    @envoy = Envoy.start(config: config, modules: module_dir, log: @envoy_log,
                         concurrency: @settings[:envoy_concurrency],
                         env: bundle_env.merge("RUVOY_MAX_INFLIGHT_REQUESTS" => MAX_INFLIGHT_REQUESTS.to_s))
    assert @envoy.serving?(url("/gc-stats"), timeout: 10), "Fiber listener did not become ready:\n#{tail(@envoy_log)}"
    @proxy_pid = @envoy.proxy_pid
    refute_nil @proxy_pid, "could not resolve the Envoy runtime PID"
  end

  def proxy_alive?
    Process.kill(0, @proxy_pid)
    true
  rescue Errno::ESRCH
    false
  end

  def fd_count
    directory = "/proc/#{@proxy_pid}/fd"
    return Dir.children(directory).size if Dir.exist?(directory)

    IO.popen([ "lsof", "-nP", "-p", @proxy_pid.to_s ], err: File::NULL, &:read).lines.drop(1).size
  end

  def sample_resources
    @sampling = true
    Thread.new do
      while @sampling && proxy_alive?
        cpu, rss = IO.popen([ "ps", "-o", "%cpu=,rss=", "-p", @proxy_pid.to_s ], &:read).split
        break unless rss

        append(:resources, Time.now.to_i, @phase, cpu, rss, fd_count)
        sleep 1
      end
    end
  end

  def record_heap(label)
    response = http_get(url("/gc-stats"))
    File.write(File.join(@results, "#{label}-gc.headers"), response.each_header.map { |key, value| "#{key}: #{value}\n" }.join)
    slots = response["x-ruby-heap-live-slots"].to_s
    rss = IO.popen([ "ps", "-o", "rss=", "-p", @proxy_pid.to_s ], &:read).strip
    assert_match(/\A\d+\z/, slots, "#{label} did not report Ruby heap live slots")
    assert_match(/\A\d+\z/, rss, "#{label} did not report Envoy RSS")
    @heap[label] = { slots: slots.to_i, rss: rss.to_i, fds: fd_count }
    append(:heap, label, *@heap[label].values)
  end

  def start_oha(label, *args)
    json = File.join(@results, "#{label}.json")
    pid = spawn(oha, "--no-tui", "--output-format", "json", *args, out: json)
    (@load ||= []) << pid
    [ pid, label, json ]
  end

  def finish(run, expectation)
    pid, label, json = run
    _, status = Process.wait2(pid)
    @load.delete(pid)
    assert_predicate status, :success?, "#{label} oha exited non-zero"
    result = JSON.parse(File.read(json))
    codes = result.fetch("statusCodeDistribution")
    errors = result.fetch("errorDistribution")
    case expectation
    when :success
      assert result.dig("summary", "successRate") == 1 && errors.empty? && codes.keys == [ "200" ],
             "#{label} returned a non-200 response or transport error"
    # Bodies are not refused when they pile up: what the runtime has no room
    # for waits in Envoy, which stops reading from the client.
    when :throttled
      assert errors.empty? && codes.keys == [ "200" ], "#{label} was refused or errored instead of being slowed down"
    when :overload
      assert errors.empty? && codes.fetch("200", 0).positive? && codes.fetch("503", 0).positive? &&
             (codes.keys - %w[200 503]).empty?, "#{label} did not produce the expected 200/503 overload mix"
    end
    append(:status, label, codes.values.sum, codes.fetch("200", 0), codes.fetch("503", 0), errors.values.sum)
  end

  def probe_control(label)
    response = nil
    seconds = elapsed do
      uri = URI("http://127.0.0.1:#{CONTROL_PORT}/")
      response = Net::HTTP.start(uri.host, uri.port, open_timeout: 2, read_timeout: 2) { |http| http.get("/") }
    end
    assert_equal "fiber-control-ok", response.body, "#{label} control listener returned the wrong body"
    assert_operator seconds, :<, CONTROL_LIMIT_SECONDS, "#{label} control listener exceeded 500 ms"
    append(:control, label, seconds)
  end

  def run_steady_mix(cycle)
    prefix = @phase = "cycle-#{cycle}-steady"
    duration = @settings[:steady_duration]
    runs = [
      start_oha("#{prefix}-noop-h2", "--wait-ongoing-requests-after-deadline", "--http2", "-c", "4", "-p", "16",
                "-q", "1500", "--latency-correction", "-n", requests_for(1500, duration).to_s, benchmark(0)),
      start_oha("#{prefix}-wait200-h1", "--wait-ongoing-requests-after-deadline", "--http-version", "1.1", "-c", "64",
                "-q", "200", "--latency-correction", "-n", requests_for(200, duration).to_s, benchmark(200)),
      start_oha("#{prefix}-body256k-h1", "--wait-ongoing-requests-after-deadline", "--http-version", "1.1", "-c", "16",
                "-q", "40", "--latency-correction", "-n", requests_for(40, duration).to_s,
                "--method", "POST", "-H", "Expect:", "-D", @bodies.fetch(262_144),
                benchmark(100, response_bytes: 1024, request_bytes: 262_144))
    ]
    runs.each { |run| finish(run, :success) }
  end

  def run_request_overload(cycle)
    label = @phase = "cycle-#{cycle}-request-overload"
    run = start_oha(label, "--wait-ongoing-requests-after-deadline", "--http-version", "1.1", "-c", "512",
                    "-z", @settings[:overload_duration], benchmark(500))
    sleep 0.5
    probe_control(label)
    finish(run, :overload)
  end

  def run_body_pressure(cycle)
    label = @phase = "cycle-#{cycle}-body-pressure"
    run = start_oha(label, "--wait-ongoing-requests-after-deadline", "--http-version", "1.1", "-c", "128",
                    "-z", @settings[:overload_duration], "--method", "POST", "-H", "Expect:",
                    "-D", @bodies.fetch(1_048_576), benchmark(500, request_bytes: 1_048_576))
    sleep 0.5
    probe_control(label)
    finish(run, :throttled)
  end

  def run_disconnect_churn(cycle)
    label = @phase = "cycle-#{cycle}-disconnect"
    Array.new(@settings[:disconnect_requests]) do
      Thread.new do
        uri = URI(benchmark(500))
        Net::HTTP.start(uri.host, uri.port, open_timeout: 0.05, read_timeout: 0.05) { |http| http.get(uri.request_uri) }
      rescue StandardError
        nil
      end
    end.each(&:join)
    sleep 1
    assert proxy_alive?, "#{label} terminated Envoy"
    probe_control(label)
  end

  def run_recovery(cycle)
    prefix = @phase = "cycle-#{cycle}-recovery"
    duration = @settings[:recovery_duration]
    runs = [
      start_oha("#{prefix}-noop", "--wait-ongoing-requests-after-deadline", "--http-version", "1.1", "-c", "64",
                "-q", "1000", "--latency-correction", "-n", requests_for(1000, duration).to_s, benchmark(0)),
      start_oha("#{prefix}-wait200", "--wait-ongoing-requests-after-deadline", "--http-version", "1.1", "-c", "32",
                "-q", "100", "--latency-correction", "-n", requests_for(100, duration).to_s, benchmark(200))
    ]
    runs.each { |run| finish(run, :success) }
    probe_control(prefix)
  end

  def terminate(pid)
    Process.kill("TERM", pid)
    Process.wait(pid)
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end
end
