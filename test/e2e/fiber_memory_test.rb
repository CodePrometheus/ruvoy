# frozen_string_literal: true

require "json"
require_relative "test_helper"

# Retention lives in a code path, not in a request count: a queued request
# body, a fiber that parks, a fiber stopped when its client leaves, an error
# unwind. Every shape gets its own before/after measurement, wave after wave,
# and what is retained has to stop growing.
class FiberMemoryTest < E2ETestCase
  FIBER_PORT = 19183
  CONTROL_PORT = 19184
  ADMIN_PORT = 19185
  SHAPES = %w[noop request-body response-body parked raised cancelled].freeze
  DEFAULTS = { body_bytes: 65_536, requests_per_wave: 10_000, concurrency: 100, waves: 3,
               wave_timeout_seconds: 60 }.freeze
  MAX_BODY_BYTES = 2 * 1024 * 1024
  Row = Struct.new(:wave, :shape, :requests, :slots_before, :slots_after, :retained, :rss_kib, :fds, :threads)

  def test_nothing_is_retained_across_waves
    settings = memory_settings
    assert File.executable?(oha), "missing project-local oha"
    [ FIBER_PORT, CONTROL_PORT, ADMIN_PORT ].each { |port| assert_port_free(port) }
    @results = File.join(result_dir, "poc3-fiber-memory-#{run_id}")
    FileUtils.mkdir_p(@results)
    @waves = File.join(@results, "waves.tsv")
    File.write(@waves, "#{Row.members.join("\t")}\n")
    preflight(settings)
    start_envoy
    body_file = File.join(@results, "request-body.bin")
    File.binwrite(body_file, "\0".b * settings[:body_bytes])

    rows = []
    1.upto(settings[:waves]) do |wave|
      SHAPES.each do |shape|
        label = "wave-#{wave}-#{shape}"
        wait_until_idle(label, "before")
        slots_before = sample_slots("#{label}-before")
        run_shape(label, shape, settings, body_file)
        wait_until_idle(label, "after")
        slots_after = sample_slots("#{label}-after")
        rss = IO.popen([ "ps", "-o", "rss=", "-p", @proxy_pid.to_s ], &:read).strip
        assert_match(/\A\d+\z/, rss, "#{label} did not report Envoy RSS")
        rows << Row.new(wave, shape, settings[:requests_per_wave], slots_before, slots_after,
                        slots_after - slots_before, rss.to_i, process_entries("fd"), process_entries("task"))
        File.write(@waves, "#{rows.last.to_a.join("\t")}\n", mode: "a")
      end
    end

    # The first wave still pays for whatever the runtime allocates once, so
    # the budget applies from the second wave on. A single object kept per
    # request would be an order of magnitude above it.
    budget = settings[:requests_per_wave] / 20 + 2000
    leaking = rows.select { |row| row.wave > 1 && row.retained > budget }
                  .map { |row| "#{row.shape}(wave #{row.wave}, +#{row.retained})" }
    assert_empty leaking, "these shapes retained more than #{budget} slots each: #{leaking.join(" ")}"
    first, last = rows.first, rows.last
    assert_operator last.fds, :<=, first.fds + 32, "file descriptors kept growing: #{first.fds} -> #{last.fds}"
    assert_operator last.threads, :<=, first.threads + 2, "threads kept growing: #{first.threads} -> #{last.threads}"

    # Conservation: every request the runtime admitted has to end exactly
    # once, whether it answered, raised, or had its client walk away.
    wait_until_idle("the last wave", "after")
    stats = module_stats
    File.write(File.join(@results, "stats.txt"), stats.join)
    admitted = stat_value(stats, /\.requests_total: /)
    ended = stats.grep(/responses_total/).sum { |line| line.split(": ", 2).last.to_i }
    cancelled = stat_value(stats, /responses_total.*cancelled/).to_i
    inflight = stat_value(stats, /inflight_requests: /)
    refute_nil admitted, "the module did not report a request count"
    assert_equal admitted.to_i, ended, "#{admitted} requests were admitted but #{ended} ended: some path loses its accounting"
    assert_operator cancelled, :>, 0, "no request was recorded as cancelled: the abandoned shape never abandoned anything"

    assert_operator last.slots_after, :<=, first.slots_before * 1.10 + 5000,
                    "Ruby live slots grew beyond the bounded threshold: #{first.slots_before} -> #{last.slots_after}"
    # Resident memory is dominated by the allocator's high-water mark, which
    # converges as the waves repeat, while a leak adds the same amount every
    # time; measured against the first wave, since two adjacent settled waves
    # differ only by noise.
    growth = rows.map(&:rss_kib).each_cons(2).map { |before, after| after - before }
                 .each_slice(SHAPES.size).map(&:sum)
    refute_empty growth, "resident memory was never sampled"
    first_growth, settled_growth = growth.first, growth.last
    assert settled_growth <= 4096 || (first_growth.positive? && settled_growth * 4 <= first_growth),
           "resident memory grew by #{settled_growth} KiB in the last wave against #{first_growth} KiB in the first " \
           "(per wave: #{growth.join(" ")}): that is a leak, not a high-water mark"

    status = @envoy.stop
    assert status&.success?, "Envoy did not stop after the bounded shutdown sequence"
    assert_includes File.read(@envoy_log), "[ruvoy] Fiber runtime stopped", "Fiber runtime did not report clean shutdown"

    puts "result=PASS", "shapes=#{SHAPES.join(" ")}", "requests=#{settings[:requests_per_wave] * settings[:waves] * SHAPES.size}",
         "heap_live_slots=#{first.slots_before}->#{last.slots_after}", "rss_kib=#{first.rss_kib}->#{last.rss_kib}",
         "retention_budget_slots=#{budget}", "fds=#{first.fds}->#{last.fds}", "threads=#{first.threads}->#{last.threads}",
         "rss_growth_per_wave_kib=#{growth.join(" ")}", "requests_admitted=#{admitted}", "requests_ended=#{ended}",
         "ended_cancelled=#{cancelled}", "inflight_requests=#{inflight}", "raw_results=#{@results}"
  ensure
    @envoy&.stop
  end

  private

  def oha
    File.join(root, ".tools", "oha", "oha")
  end

  def url(path)
    "http://127.0.0.1:#{FIBER_PORT}#{path}"
  end

  def benchmark_url(wait_ms, response_bytes, request_bytes)
    url("/benchmark?wait_ms=#{wait_ms}&response_bytes=#{response_bytes}&expected_request_bytes=#{request_bytes}")
  end

  def memory_settings
    DEFAULTS.to_h do |key, default|
      value = Integer(ENV.fetch("RUVOY_MEMORY_#{key.upcase}", default.to_s), 10)
      raise ArgumentError, "#{key} must be non-negative" if value.negative?
      raise ArgumentError, "#{key} must be positive" if key != :body_bytes && value.zero?
      raise ArgumentError, "body_bytes exceeds the 2 MiB fixture limit" if key == :body_bytes && value > MAX_BODY_BYTES

      [ key, value ]
    end
  end

  def preflight(settings)
    log = File.join(@results, "preflight.log")
    lines = [ "run_id=#{run_id}", "ruby_version=#{RUBY_VERSION}", "async_version=#{locked_version("async")}" ]
    lines += settings.map { |key, value| "#{key}=#{value}" }
    lines += [ "envoy_package=#{Envoy::PACKAGE}", "oha=#{IO.popen([ oha, "--version" ], &:read).strip}", "" ]
    File.write(log, lines.join("\n"))
    build_module("build-fiber-module.sh", log: log)
  end

  def start_envoy
    config = File.join(@results, "envoy.yaml")
    template = File.read(File.join(root, "config", "envoy-fiber-rack.yaml"))
    File.write(config, template.sub("value: bench/config.ru", "value: #{fixture_rackup}"))
    assert_includes File.read(config), "value: #{fixture_rackup}", "failed to configure the Rack fixture"
    @envoy_log = File.join(@results, "envoy.log")
    @envoy = Envoy.start(config: config, modules: module_dir, log: @envoy_log, admin_port: ADMIN_PORT, env: bundle_env)
    assert @envoy.serving?(url("/gc-stats"), timeout: 10), "Fiber listener did not become ready:\n#{tail(@envoy_log)}"
    @proxy_pid = @envoy.proxy_pid
    refute_nil @proxy_pid, "could not resolve the Envoy runtime PID"
  end

  def shape_arguments(shape, settings, body_file)
    case shape
    when "noop" then [ "--method", "GET", benchmark_url(0, 0, 0) ]
    when "request-body" then [ "--method", "POST", "-H", "Expect:", "-D", body_file, benchmark_url(0, 0, settings[:body_bytes]) ]
    when "response-body" then [ "--method", "GET", benchmark_url(0, settings[:body_bytes], 0) ]
    when "parked" then [ "--method", "GET", benchmark_url(20, 0, 0) ]
    when "raised" then [ "--method", "GET", url("/raise") ]
    # Every request is abandoned well before the application would answer, so
    # the runtime has to reclaim a fiber it stopped rather than one that ended.
    when "cancelled" then [ "-t", "50ms", "--method", "GET", url("/async-sleep?seconds=1") ]
    end
  end

  # The status distribution a shape is allowed to produce. An abandoned
  # request has no status to report, so only the runtime's survival is
  # asserted for that shape.
  def shape_allows?(shape, result)
    statuses = result.fetch("statusCodeDistribution").keys
    case shape
    when "raised" then statuses == [ "500" ]
    when "cancelled" then true
    else result.dig("summary", "successRate") == 1 && result.fetch("errorDistribution").empty? && statuses == [ "200" ]
    end
  end

  def run_shape(label, shape, settings, body_file)
    json = File.join(@results, "#{label}.json")
    pid = spawn(oha, "--no-tui", "--output-format", "json", "--http-version", "1.1",
                "-n", settings[:requests_per_wave].to_s, "-c", settings[:concurrency].to_s,
                *shape_arguments(shape, settings, body_file), out: json)
    deadline = clock + settings[:wave_timeout_seconds]
    until Process.waitpid(pid, Process::WNOHANG)
      if clock > deadline
        terminate(pid)
        flunk "#{label} exceeded the #{settings[:wave_timeout_seconds]}s timeout"
      end
      sleep 0.05
    end
    assert shape_allows?(shape, JSON.parse(File.read(json))), "#{label} did not answer as its shape requires"
  end

  def terminate(pid)
    Process.kill("TERM", pid)
    sleep 1
    Process.kill("KILL", pid)
    Process.wait(pid)
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end

  # Live slots after a full collection, so what is reported is what is retained.
  def sample_slots(label)
    response = http_get(url("/gc-stats"))
    assert_equal "200", response.code, "#{label} could not read the heap"
    File.write(File.join(@results, "#{label}.headers"), response.each_header.map { |key, value| "#{key}: #{value}\n" }.join)
    slots = response["x-ruby-heap-live-slots"]
    assert_match(/\A\d+\z/, slots.to_s, "#{label} did not report Ruby live slots")
    slots.to_i
  end

  def module_stats
    http_get("http://127.0.0.1:#{ADMIN_PORT}/stats").body.lines.grep(/dynamicmodules/i)
  end

  def stat_value(stats, pattern)
    stats.find { |line| line.match?(pattern) }&.split(": ", 2)&.last&.strip
  end

  # A fiber whose client walked away is stopped by the reactor on its next
  # pass, so the load generator exiting does not mean the work is over. The
  # gauges are written as requests begin and end, which is why each poll
  # sends one.
  def wait_until_idle(label, moment)
    inflight = nil
    100.times do
      assert_equal "200", http_get(url("/counted")).code, "#{label} could not reach the runtime #{moment} the run"
      inflight = stat_value(module_stats, /inflight_requests: /)
      return if inflight == "0"

      sleep 0.1
    end
    flunk "#{label}: the runtime still had #{inflight} requests in flight after 10s of quiet #{moment} the run"
  end

  # File descriptors and threads of the proxy, which leak in their own right.
  def process_entries(kind)
    directory = "/proc/#{@proxy_pid}/#{kind}"
    return Dir.children(directory).size if Dir.exist?(directory)
    return IO.popen([ "lsof", "-p", @proxy_pid.to_s ], err: File::NULL, &:read).lines.drop(1).size if kind == "fd"

    0
  end
end
