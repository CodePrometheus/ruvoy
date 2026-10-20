# frozen_string_literal: true

require "async"
require "async/http/endpoint"
require "async/websocket/client"
require "json"
require_relative "test_helper"
require_relative "support/upstream_server"

# Every kind of work at once, for as long as the mode asks, while one of the
# listeners carrying it is added, changed and removed underneath. Nothing may
# crash, grow, stay behind once the load stops, slow down over time, or fail
# for a reason the run did not ask for.
#
# RUVOY_SOAK_MODE=smoke (the default) runs for minutes; full runs for hours and
# also judges growth and latency drift.
class SoakTest < E2ETestCase
  STEADY_PORT = 19260
  CHURN_PORT = 19261
  ADMIN_PORT = 19262
  OFFLOAD_LIMIT = 4
  MODES = {
    "smoke" => { load: 100, warmup: 0, idle: 60, churn_every: 10, drain: 3, sample_every: 5, heap_every: 20,
                 window: 50, session_life: 20..40 },
    "full" => { load: 3 * 3600, warmup: 1800, idle: 60, churn_every: 30, drain: 10, sample_every: 10, heap_every: 60,
                window: 60, session_life: 300..900 }
  }.freeze
  # Requests per second and connections for each kind of HTTP load.
  HTTP_LOAD = [
    [ "noop", :steady, "/noop", 300, 8 ],
    [ "wait", :steady, "/wait?ms=50", 100, 16 ],
    [ "paced", :steady, "/paced", 10, 8 ],
    [ "upload", :steady, "/upload", 10, 4 ],
    [ "call", :steady, "/call", 50, 8 ],
    [ "proxy", :steady, "/proxy", 10, 8 ],
    [ "offload", :steady, "/offload", 10, 4 ],
    [ "churn-noop", :churn, "/noop", 50, 4 ],
    [ "churn-call", :churn, "/call", 10, 2 ]
  ].freeze
  # Long-lived WebSocket sessions kept open per listener and path.
  SESSIONS = [ [ :steady, "/ws", 8 ], [ :steady, "/relay", 8 ], [ :churn, "/ws", 2 ], [ :churn, "/relay", 2 ] ].freeze
  UPLOAD_BYTES = 262_144
  STATS = {
    "inflight" => /\.inflight_requests: (\d+)$/,
    "lds_success" => /^listener_manager\.lds\.update_success: (\d+)$/,
    "lds_rejected" => /^listener_manager\.lds\.update_rejected: (\d+)$/
  }.freeze

  def test_everything_at_once_for_the_whole_run
    @mode = ENV.fetch("RUVOY_SOAK_MODE", "smoke")
    @settings = MODES.fetch(@mode) { raise ArgumentError, "RUVOY_SOAK_MODE must be smoke or full" }
    assert File.executable?(oha), "missing project-local oha; run rake tools:oha"
    [ STEADY_PORT, CHURN_PORT, ADMIN_PORT ].each { |port| assert_port_free(port) }
    @results = File.join(result_dir, "soak-#{@mode}-#{run_id}")
    FileUtils.mkdir_p(@results)
    @upstream = UpstreamServer.new.start
    @upload = File.join(scratch_dir, "upload.bin").tap { |path| File.binwrite(path, "u".b * UPLOAD_BYTES) }
    @lock = Mutex.new
    @tally = Hash.new(0)
    build_module("fiber", log: File.join(@results, "build.log"))

    start_envoy
    prime
    @baseline = idle_state("baseline")
    write_manifest

    @started = clock
    @deadline = @started + @settings[:load]
    @sampling = true
    samplers = [ sample_resources, sample_heap ]
    loads = [ churn_listeners, disconnect_clients, websocket_clients, *HTTP_LOAD.map { |load| http_load(*load) } ]
    sleep 1 while clock < @deadline && @envoy.alive?
    @deadline = clock unless @envoy.alive?
    loads.each(&:join)
    @sampling = false
    samplers.each(&:join)

    if @envoy.alive?
      apply_listeners(:a)
      sleep @settings[:idle]
      @final = idle_state("final")
    end
    judge
  ensure
    @sampling = false
    @envoy&.stop
    @upstream&.stop
  end

  private

  def oha
    File.join(root, ".tools", "oha", "oha")
  end

  def url(listener, path)
    "http://127.0.0.1:#{listener == :steady ? STEADY_PORT : CHURN_PORT}#{path}"
  end

  def elapsed_in_run
    (clock - @started).round(1)
  end

  def count(key, amount = 1)
    @lock.synchronize { @tally[key] += amount }
  end

  def record(name, *fields)
    File.write(File.join(@results, name), "#{fields.join("\t")}\n", mode: "a")
  end

  def start_envoy
    @lds = File.join(scratch_dir, "lds.json")
    @version = 0
    apply_listeners(:a)
    bootstrap = File.join(scratch_dir, "soak-bootstrap.json")
    File.write(bootstrap, JSON.pretty_generate(bootstrap_config))
    @envoy_log = File.join(@results, "envoy.log")
    @envoy = Envoy.start(config: bootstrap, modules: module_dir, log: @envoy_log, log_level: "info",
                         admin_port: ADMIN_PORT, drain_time: @settings[:drain], env: bundle_env)
    assert @envoy.serving?(url(:steady, "/noop"), timeout: 30), "Envoy did not become ready:\n#{tail(@envoy_log)}"
    @proxy_pid = @envoy.proxy_pid
    refute_nil @proxy_pid, "could not resolve the Envoy process"
  end

  def bootstrap_config
    {
      "node" => { "id" => "ruvoy-soak", "cluster" => "ruvoy" },
      "bootstrap_extensions" => [
        bootstrap_extension(rackup: File.join(root, "test", "fixtures", "soak", "config.ru"))
      ],
      # What the generated configuration runs with.
      "memory_allocator_manager" => Ruvoy::EnvoyConfig.memory_allocator_manager,
      "dynamic_resources" => {
        "lds_config" => { "resource_api_version" => "V3", "path_config_source" => { "path" => @lds } }
      },
      "static_resources" => {
        "clusters" => [ {
          "name" => "upstream",
          "type" => "STATIC",
          "connect_timeout" => "1s",
          "load_assignment" => {
            "cluster_name" => "upstream",
            "endpoints" => [ { "lb_endpoints" => [ { "endpoint" => { "address" => {
              "socket_address" => { "address" => "127.0.0.1", "port_value" => @upstream.port }
            } } } ] } ]
          },
          # Pooled connections close once idle, so descriptors settle back to
          # where they started when the load stops.
          "typed_extension_protocol_options" => {
            "envoy.extensions.upstreams.http.v3.HttpProtocolOptions" => {
              "@type" => "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
              "common_http_protocol_options" => { "idle_timeout" => "5s" },
              "explicit_http_config" => { "http_protocol_options" => {} }
            }
          }
        } ]
      }
    }
  end

  # The churn listener cycles through present, changed and absent; the steady
  # one is in every version unchanged, so Envoy never drains it.
  def apply_listeners(state)
    listeners = [ listener("steady", STEADY_PORT, 5000) ]
    listeners << listener("churn", CHURN_PORT, 5000) if state == :a
    listeners << listener("churn", CHURN_PORT, 6000) if state == :b
    @version += 1
    staged = "#{@lds}.next"
    File.write(staged, JSON.generate("version_info" => @version.to_s, "resources" => listeners))
    File.rename(staged, @lds)
  end

  def listener(name, port, timeout_ms)
    settings = { "upstream" => { "clusters" => [ "upstream" ], "timeout_ms" => timeout_ms } }
    {
      "@type" => "type.googleapis.com/envoy.config.listener.v3.Listener",
      "name" => name,
      "address" => { "socket_address" => { "address" => "127.0.0.1", "port_value" => port } },
      "filter_chains" => [ { "filters" => [ {
        "name" => "envoy.filters.network.http_connection_manager",
        "typed_config" => {
          "@type" => "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
          "stat_prefix" => name,
          "codec_type" => "AUTO",
          "upgrade_configs" => [ { "upgrade_type" => "websocket" } ],
          "route_config" => { "name" => name, "virtual_hosts" => [ { "name" => name, "domains" => [ "*" ] } ] },
          "http_filters" => [ {
            "name" => "envoy.extensions.filters.http.dynamic_modules",
            "typed_config" => {
              "@type" => "type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter",
              "dynamic_module_config" => { "name" => "ruvoy_fiber", "do_not_close" => true },
              "filter_name" => "fiber_rack",
              "terminal_filter" => true,
              "filter_config" => { "@type" => "type.googleapis.com/google.protobuf.StringValue",
                                   "value" => JSON.generate(settings) }
            }
          } ]
        }
      } ] } ]
    }
  end

  # Every kind of work once, so whatever is opened on first use is open before
  # the baseline is taken.
  def prime
    HTTP_LOAD.each do |_name, listener, path, _rate, _connections|
      response = path == "/upload" ? http_post(url(listener, path), "u" * UPLOAD_BYTES) : http_get(url(listener, path))
      assert_equal "200", response.code, "priming #{path} failed: #{response.body}"
    end
    Sync do
      SESSIONS.each { |listener, path, _count| assert websocket_session(listener, path, 2), "priming #{path} failed" }
    end
    http_get(url(:steady, "/heap"))
  end

  # Waits for the pooled upstream connections to close, then reads what
  # should be back to rest; the gauges first, before the probes below.
  def idle_state(label)
    sleep 7
    stats = admin_stats
    state = JSON.parse(http_get(url(:steady, "/state")).body)
    heap = JSON.parse(http_get(url(:steady, "/heap")).body).fetch("live_slots")
    { "label" => label, "fds" => fd_count, "threads" => thread_count, "rss_kib" => rss_kib, "heap" => heap,
      "fibers" => state.fetch("fibers"), "sessions" => state.fetch("sessions"), **stats }.tap do |snapshot|
      File.write(File.join(@results, "#{label}.json"), JSON.pretty_generate(snapshot))
    end
  end

  def admin_stats
    text = http_get("http://127.0.0.1:#{ADMIN_PORT}/stats").body
    STATS.transform_values { |pattern| text[pattern, 1].to_i }
  end

  # Where the process's memory sits: what the allocator holds for live objects
  # by its own account, what it keeps resident, the anonymous memory outside
  # it, the part of that Ruby keeps as heap pages, the rest (stacks, buffers,
  # other mappings), and the brk heap the C library allocates from first.
  def memory_layout(heap)
    allocated = JSON.parse(http_get("http://127.0.0.1:#{ADMIN_PORT}/memory").body)["allocated"].to_i / 1024
    resident, outside = anonymous_kib_by_owner(@proxy_pid)
    ruby_heap = heap.fetch("pages") * heap.fetch("page_size") / 1024
    brk = File.read("/proc/#{@proxy_pid}/smaps")[/\[heap\]\n(?:.*\n)*?Rss:\s+(\d+)/, 1]
    [ allocated, resident, outside, ruby_heap, outside - ruby_heap, brk.to_i ]
  end

  def fd_count = Dir.children("/proc/#{@proxy_pid}/fd").size
  def thread_count = File.read("/proc/#{@proxy_pid}/status")[/^Threads:\s+(\d+)/, 1].to_i
  def rss_kib = File.read("/proc/#{@proxy_pid}/status")[/^VmRSS:\s+(\d+)/, 1].to_i

  def write_manifest
    File.write(File.join(@results, "manifest.json"), JSON.pretty_generate(
      "run_id" => run_id, "mode" => @mode, "settings" => @settings.transform_values(&:to_s),
      "http_load" => HTTP_LOAD, "sessions" => SESSIONS, "host" => IO.popen(%w[uname -srm], &:read).strip,
      "nproc" => IO.popen(%w[nproc], &:read).strip, "ruby" => RUBY_DESCRIPTION,
      "rustc" => IO.popen(%w[rustc --version], &:read).strip, "build_profile" => build_profile
    ))
  end

  def sample_resources
    record("resources.tsv", "seconds", "rss_kib", "fds", "threads", "alive")
    Thread.new do
      while @sampling
        alive = @envoy.alive?
        record("resources.tsv", elapsed_in_run, rss_kib, fd_count, thread_count, alive) if alive
        unless alive
          count("envoy_died")
          break
        end
        sleep @settings[:sample_every]
      end
    end
  end

  def sample_heap
    record("heap.tsv", "seconds", "heap_live_slots", "heap_pages", "ruby_memsize", "fibers", "sessions", "inflight",
           "allocator_kib", "allocator_resident_kib", "outside_allocator_kib", "ruby_heap_kib", "other_anonymous_kib",
           "brk_kib")
    Thread.new do
      while @sampling && clock < @deadline
        sleep @settings[:heap_every]
        begin
          state = JSON.parse(http_get(url(:steady, "/state")).body)
          heap = JSON.parse(http_get(url(:steady, "/heap")).body)
          record("heap.tsv", elapsed_in_run, *heap.values_at("live_slots", "pages", "memsize"), state["fibers"],
                 state["sessions"], admin_stats["inflight"], *memory_layout(heap))
        rescue StandardError => error
          count("sampler:#{error.class}")
        end
      end
    end
  end

  def churn_listeners
    record("lds.tsv", "seconds", "version", "state")
    Thread.new do
      states = %i[b absent a].cycle
      while clock + @settings[:churn_every] < @deadline
        sleep @settings[:churn_every]
        state = states.next
        apply_listeners(state)
        count("lds_applied")
        record("lds.tsv", elapsed_in_run, @version, state)
      end
    end
  end

  # Back-to-back windows of one kind of load, each a separate oha run, so
  # latency can be compared between the start of the run and its end.
  def http_load(name, listener, path, rate, connections)
    record("windows-#{name}.tsv", "start", "seconds", "requests", "statuses", "errors", "p50", "p99")
    Thread.new do
      index = 0
      while (remaining = @deadline - clock) > 1
        length = [ @settings[:window], remaining ].min.floor
        started = elapsed_in_run
        json = File.join(@results, "oha", "#{name}-#{index}.json")
        FileUtils.mkdir_p(File.dirname(json))
        command = [ oha, "--no-tui", "--output-format", "json", "--http-version", "1.1",
                    "--wait-ongoing-requests-after-deadline", "--latency-correction",
                    "-q", rate.to_s, "-c", connections.to_s, "-z", "#{length}s" ]
        command += [ "--method", "POST", "-H", "Expect:", "-D", @upload ] if path == "/upload"
        system(*command, url(listener, path), out: json, exception: true)
        summarize_window(name, listener, started, length, json)
        index += 1
      end
    end
  end

  def summarize_window(name, listener, started, length, json)
    result = JSON.parse(File.read(json))
    statuses = result.fetch("statusCodeDistribution")
    errors = result.fetch("errorDistribution")
    latency = result.fetch("latencyPercentiles")
    count("http_requests", statuses.values.sum)
    (statuses.keys - [ "200" ]).each { |status| count("unexpected:#{name}:status #{status}", statuses[status]) }
    errors.each { |error, times| count("#{listener == :churn ? "expected" : "unexpected"}:#{name}:#{error}", times) }
    record("windows-#{name}.tsv", started, length, statuses.values.sum, JSON.generate(statuses), JSON.generate(errors),
           latency["p50"], latency["p99"])
  end

  # Clients that leave at every point a request can be at: waiting for the
  # application, partway through a streamed body, partway through an upload.
  def disconnect_clients
    leaves = [
      [ "GET /wait?ms=500 HTTP/1.1\r\nHost: soak\r\n\r\n", 0 ],
      [ "GET /paced HTTP/1.1\r\nHost: soak\r\n\r\n", 6000 ],
      [ "GET /proxy HTTP/1.1\r\nHost: soak\r\n\r\n", 200 ],
      [ "POST /upload HTTP/1.1\r\nHost: soak\r\nContent-Length: #{UPLOAD_BYTES}\r\n\r\n#{"u" * 65_536}", 0 ]
    ].cycle
    Thread.new do
      while clock < @deadline
        leave(*leaves.next)
        sleep 0.15
      end
    end
  end

  def leave(request, read)
    socket = TCPSocket.new("127.0.0.1", STEADY_PORT)
    socket.write(request)
    received = +""
    received << socket.readpartial(65_536) while received.bytesize < read && socket.wait_readable(2)
    sleep 0.05
    count("disconnects")
  rescue SystemCallError, IOError => error
    count("unexpected:disconnect:#{error.class}")
  ensure
    socket&.close
  end

  def websocket_clients
    Thread.new do
      Sync do |task|
        SESSIONS.each do |listener, path, sessions|
          sessions.times { task.async { long_sessions(listener, path) } }
        end
        task.async { short_sessions }
      end
    end
  end

  def long_sessions(listener, path)
    while clock < @deadline
      life = rand(@settings[:session_life])
      websocket_session(listener, path, [ life, @deadline - clock ].min) || sleep(1)
    end
  end

  def short_sessions
    paths = [ "/ws", "/relay" ].cycle
    while clock < @deadline
      Async { websocket_session(:steady, paths.next, 3) }
      sleep 0.5
    end
  end

  # One session sending a message a second and checking every echo. Failures
  # on the churn listener are expected: it is drained and removed on purpose.
  def websocket_session(listener, path, seconds)
    endpoint = Async::HTTP::Endpoint.parse(url(listener, path))
    Async::WebSocket::Client.connect(endpoint) do |connection|
      count("ws_sessions")
      ends = clock + seconds
      sequence = 0
      while clock < ends
        message = "#{path}-#{sequence += 1}-#{"m" * 48}"
        connection.send_text(message)
        connection.flush
        reply = Async::Task.current.with_timeout(10) { connection.read }
        raise IOError, "the session ended early" unless reply
        unless reply.to_str == message
          count("unexpected:#{listener}:#{path}:echo mismatch")
          return false
        end
        count("ws_messages")
        sleep 1
      end
    end
    true
  rescue StandardError => error
    count("#{listener == :churn ? "expected" : "unexpected"}:ws:#{listener}:#{path}:#{error.class}")
    false
  end

  def judge
    tally = @lock.synchronize { @tally.dup }
    failures = []
    failures << "Envoy died during the run" if tally["envoy_died"].positive? || !@envoy.alive?
    stopped = @envoy.stop
    log = File.read(@envoy_log)
    failures << "the log carries a crash signature" if log.match?(/\[BUG\]|panicked|Caught (Segmentation|Aborted)|SIGSEGV|SIGABRT/)
    failures << "the runtime started more than once" unless log.scan("[ruvoy] Fiber runtime started").size == 1
    unless stopped&.success? && log.include?("[ruvoy] Fiber runtime stopped")
      failures << "the runtime did not stop cleanly: #{log[/\[ruvoy\] Fiber runtime (stopped|shutdown failed).*/]}"
    end

    failures.concat(leftovers(tally)) if @final
    unexpected = tally.select { |key, _| key.start_with?("unexpected:", "sampler:") }
    failures << "unexpected failures: #{unexpected}" unless unexpected.empty?

    verdict = { "mode" => @mode, "tally" => tally, "baseline" => @baseline, "final" => @final }
    if @mode == "full" && @final
      growth = trends
      verdict["trends"] = growth
      growth.slice("allocator_kib", "other_anonymous_kib", "heap_live_slots").each do |name, trend|
        failures << "#{name} grew #{trend["percent_per_hour"]}% an hour" if trend["percent_per_hour"] > 1.0
      end
      verdict["latency"] = drift
      first, last = verdict["latency"].values_at("first", "last")
      failures << "noop p99 drifted from #{first} to #{last}" if last > first * 1.2
    end
    verdict["failures"] = failures
    File.write(File.join(@results, "verdict.json"), JSON.pretty_generate(verdict))
    puts JSON.pretty_generate(verdict), "raw_results=#{@results}"
    assert_empty failures, "soak failed; see #{@results}"
  end

  # What has to be back at rest once the load has stopped and the proxy idled.
  def leftovers(tally)
    failures = []
    failures << "descriptors did not settle: #{@baseline["fds"]} -> #{@final["fds"]}" if @final["fds"] > @baseline["fds"] + 4
    failures << "threads stayed behind: #{@baseline["threads"]} -> #{@final["threads"]}" if @final["threads"] > @baseline["threads"]
    failures << "fibers stayed behind: #{@baseline["fibers"]} -> #{@final["fibers"]}" if @final["fibers"] > @baseline["fibers"]
    failures << "WebSocket sessions stayed open: #{@final["sessions"]}" unless @final["sessions"].zero?
    failures << "requests stayed in flight: #{@final["inflight"]}" unless @final["inflight"].zero?
    peak_threads = column("resources.tsv", "threads").max
    if peak_threads > @baseline["threads"] + OFFLOAD_LIMIT
      failures << "threads exceeded the bound: #{peak_threads} > #{@baseline["threads"]} + #{OFFLOAD_LIMIT}"
    end

    failures << "Envoy rejected #{@final["lds_rejected"]} listener updates" unless @final["lds_rejected"].zero?
    if @final["lds_success"] < tally["lds_applied"] + 2
      failures << "Envoy applied #{@final["lds_success"]} of #{tally["lds_applied"] + 2} listener updates"
    end
    failures
  end

  def column(file, name)
    rows = File.readlines(File.join(@results, file), chomp: true).map { |line| line.split("\t") }
    index = rows.first.index(name)
    rows.drop(1).map { |row| row[index].to_f }
  end

  # Least-squares slope over the steady part of the run, as a percentage of the
  # mean per hour. Live allocations, anonymous memory held by neither the
  # allocator nor Ruby's heap, and Ruby's objects are judged; the rest is
  # reported.
  def trends
    from = 3600
    heap = %w[heap_live_slots heap_pages ruby_memsize allocator_kib allocator_resident_kib outside_allocator_kib
              ruby_heap_kib other_anonymous_kib brk_kib]
    {
      "rss" => [ column("resources.tsv", "seconds"), column("resources.tsv", "rss_kib") ],
      **heap.to_h { |name| [ name, [ column("heap.tsv", "seconds"), column("heap.tsv", name) ] ] }
    }.transform_values do |(seconds, values)|
      points = seconds.zip(values).select { |second, _| second >= from }
      mean_x = points.sum(&:first) / points.size
      mean_y = points.sum(&:last) / points.size
      slope = points.sum { |x, y| (x - mean_x) * (y - mean_y) } / points.sum { |x, _| (x - mean_x)**2 }
      { "points" => points.size, "mean" => mean_y.round, "per_hour" => (slope * 3600).round,
        "percent_per_hour" => mean_y.zero? ? nil : (slope * 3600 / mean_y * 100).round(3) }
    end
  end

  # The median of the per-window p99s in the ten minutes after warm-up and in
  # the last ten minutes.
  def drift
    rows = File.readlines(File.join(@results, "windows-noop.tsv"), chomp: true).drop(1).map { |line| line.split("\t") }
    windows = rows.filter_map { |row| [ row[0].to_f, row[6].to_f ] if row[1].to_i >= 30 }
    median = ->(values) { values.sort[values.size / 2] }
    first = windows.select { |start, _| start >= @settings[:warmup] && start < @settings[:warmup] + 600 }.map(&:last)
    last = windows.select { |start, _| start >= @settings[:load] - 600 }.map(&:last)
    { "first" => median.call(first), "last" => median.call(last), "windows" => [ first.size, last.size ] }
  end
end
