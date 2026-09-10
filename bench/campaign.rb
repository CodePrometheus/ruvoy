# frozen_string_literal: true

require "digest"
require "etc"
require "fileutils"
require "json"
require "net/http"
require "open3"
require "openssl"
require "socket"
require_relative "../tasks/build"
require_relative "../tasks/lockfile"
require_relative "../tasks/oha"
require_relative "campaign/settings"
require_relative "campaign/processes"
require_relative "campaign/load_generator"

module Bench
  # Runs every selected scenario against every selected architecture and writes
  # the raw evidence bench/summarize.rb turns into the published numbers.
  class Campaign
    ENVOY_PACKAGE = "envoy-server==1.39.0"
    PORTS = [ 19180, 19181, 19182, 19183, 19184, 19203, 19204, 19210, 19211, 19212 ].freeze
    ENVOY_ARCHITECTURES = {
      "baseline" => [ "envoy-baseline.yaml", 19180 ],
      "sync" => [ "envoy-sync-rack.yaml", 19181 ],
      "fiber" => [ "envoy-fiber-rack.yaml", 19183 ],
      "envoy_puma" => [ "envoy-puma-benchmark.yaml", 19203 ]
    }.freeze
    CONTROL_PORTS = { "sync" => 19182, "fiber" => 19184 }.freeze
    STOPPED = {
      "sync" => [ "[ruvoy] Ruby runtime stopped", "sync Ruby runtime did not report clean shutdown" ],
      "fiber" => [ "[ruvoy] Fiber runtime stopped", "Fiber runtime did not report clean shutdown" ]
    }.freeze
    BODY_SIZES = [ 1024, 65_536, 262_144, 1_048_576, 2_097_152 ].freeze
    MANIFEST = %w[sequence architecture scenario tls_mode round protocol concurrency wait_ms request_bytes
                  response_bytes app_params served_requests oha_json resources_csv].freeze
    SCENARIOS = %w[
      noop_h1_c1|1.1|1|0|0|0
      noop_h1_c100|1.1|100|0|0|0
      noop_h2_c100|2|100|0|0|0
      small_h1_c10|1.1|10|0|1024|1024
      large_request_h1_c10|1.1|10|0|1048576|0
      large_response_h1_c10|1.1|10|0|0|1048576
      wait10_h1_c10|1.1|10|10|0|0
      wait200_h1_c1|1.1|1|200|0|0
      wait200_h1_c10|1.1|10|200|0|0
      wait200_h1_c100|1.1|100|200|0|0
      cpu25k_h1_c100|1.1|100|0|0|0|cpu_iterations=25000
      cpu125k_h1_c100|1.1|100|0|0|0|cpu_iterations=125000
      wait50_h1_c100|1.1|100|50|0|0
      block50_h1_c100|1.1|100|0|0|0|block_ms=50
      block50_h1_c10|1.1|10|0|0|0|block_ms=50
    ].freeze
    SKIP_REASON = "direct listener does not serve cleartext HTTP/2"
    UNREACHABLE = [ SystemCallError, IOError, Net::OpenTimeout, Net::ReadTimeout, OpenSSL::SSL::SSLError ].freeze

    Row = Data.define(:scenario, :protocol, :concurrency, :wait_ms, :request_bytes, :response_bytes, :app_params)

    def self.row(line)
      scenario, protocol, concurrency, wait_ms, request_bytes, response_bytes, app_params = line.split("|")
      Row.new(scenario, protocol, Integer(concurrency), Integer(wait_ms), Integer(request_bytes),
              Integer(response_bytes), app_params.to_s)
    end

    def initialize(env)
      @settings = Settings.new(env)
    end

    def run
      @settings.validate!
      prepare
      measure
      conclude
      0
    rescue Failure => failure
      warn "FAIL: #{failure.message}\nraw results: #{result_dir}"
      1
    ensure
      @servers&.stop
      @load_generator&.cleanup
    end

    private

    def result_dir = @settings.result_dir

    def prepare
      resolve_toolchain
      write_manifest_headers
      require_tools
      @load_generator = LoadGenerator.new(@settings, local_oha: Oha::PATH)
      @load_generator.check_local unless @settings.remote?
      require_free_ports
      @tls_key = generate_tls_assets if @settings.tls_modes.include?("tls")
      render_configs
      BODY_SIZES.each { |size| File.binwrite(File.join(result_dir, "body-#{size}.bin"), "\0" * size) }
      @load_generator.prepare(result_dir)
      write("environment.txt", *describe_host, *@load_generator.describe(@settings.target_address))
      require_idle_host
      write_invocation
      preflight
    end

    def resolve_toolchain
      @ruby = @settings.ruby || RbConfig.ruby
      @path = Bench.unbundled { ENV.fetch("PATH") }
      pinned = File.read(File.join(ROOT, ".ruby-version")).strip
      actual = output_of(@ruby, "-e", "print RUBY_VERSION")
      raise Failure, "RUVOY_RUBY must be Ruby #{pinned}, got #{actual.inspect} from #{@ruby}" unless actual == pinned

      @bundle = @settings.bundle ? [ @settings.bundle ] : [ @ruby, "-S", "bundle" ]
      reported = output_of(*@bundle, "--version").to_s.strip.delete_prefix("Bundler version ").strip
      expected = Lockfile.bundler_version
      return if reported == expected

      raise Failure, "expected Bundler #{expected} from #{@bundle.join(" ")}, got #{reported.inspect}; set RUVOY_BUNDLE"
    end

    def write_manifest_headers
      FileUtils.mkdir_p(result_dir)
      write("manifest.tsv", MANIFEST.join("\t"))
      write("control-manifest.tsv", "architecture\tstate\trun\toha_json")
      write("campaign.tsv", "sequence\tscenario\ttls_mode\tround\tarchitecture\tprotocol\tstate\treason")
      write("skipped.tsv", "scenario\tround\tarchitecture\tprotocol\treason")
    end

    def require_tools
      missing = (%w[cargo pgrep ps uvx] + (@settings.remote? ? %w[ssh scp] : [])).reject { |tool| executable?(tool) }
      raise Failure, "missing required tools: #{missing.join(", ")}" if missing.any?
    end

    def require_free_ports
      PORTS.each do |port|
        TCPServer.new("0.0.0.0", port).close
      rescue Errno::EADDRINUSE
        raise Failure, "TCP port #{port} is already in use"
      end
    end

    def generate_tls_assets
      directory = File.join(result_dir, "tls")
      FileUtils.mkdir_p(directory)
      File.chmod(0o700, directory)
      details = output_of(@ruby, File.join(ROOT, "bench", "generate_tls_assets.rb"), directory,
                          @settings.listen_address, @settings.target_address, @settings.probe_address, "localhost")
      raise Failure, "failed to generate TLS assets" unless details

      details.strip
    end

    def render_configs
      @settings.tls_modes.each do |mode|
        directory = File.join(result_dir, "config", mode)
        FileUtils.mkdir_p(directory)
        Dir.glob(File.join(ROOT, "config", "envoy-*.yaml")).sort.each do |source|
          arguments = [ source, File.join(directory, File.basename(source)), @settings.listen_address, ROOT ]
          arguments += [ tls_file("cert.pem"), tls_file("key.pem") ] if mode == "tls"
          next if output_of(@ruby, File.join(ROOT, "bench", "render_envoy_config.rb"), *arguments)

          raise Failure, "failed to render #{File.basename(source)} for #{mode}"
        end
      end
    end

    # Machine facts are captured by the runner rather than trusted to a human
    # note, so a later reader can tell what the numbers were produced on.
    def describe_host
      lines = [ "[server]", "kernel=#{uname}" ]
      return lines + macos_facts unless File.readable?("/proc/cpuinfo")

      model = File.foreach("/proc/cpuinfo").find { |line| line.start_with?("model name") }.to_s.split(": ", 2).last
      lines + [ "cpu_model=#{model.to_s.strip}", "cpu_count=#{Etc.nprocessors}",
                "memory_kib=#{File.read("/proc/meminfo")[/MemTotal:\s+(\d+)/, 1]}",
                "load_average=#{File.read("/proc/loadavg").split.first(3).join(" ")}",
                "cpu_governor=#{first_line("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")}",
                "turbo_disabled=#{first_line("/sys/devices/system/cpu/intel_pstate/no_turbo")}" ]
    end

    def macos_facts
      [ "cpu_model=#{output_of("sysctl", "-n", "machdep.cpu.brand_string")&.strip || "unknown"}",
        "cpu_count=#{Etc.nprocessors}", "memory_kib=#{output_of("sysctl", "-n", "hw.memsize").to_i / 1024}",
        "load_average=#{output_of("sysctl", "-n", "vm.loadavg")&.strip}", "cpu_governor=unavailable",
        "turbo_disabled=unavailable" ]
    end

    # A busy server host makes every measurement a measurement of the neighbour.
    def require_idle_host
      return unless @settings.full? && File.readable?("/proc/loadavg")

      load = File.read("/proc/loadavg").split.first.to_f
      return if load <= @settings.idle_load_limit

      raise Failure, format("server host load average is %.2f (limit %g); the machine is not idle",
                            load, @settings.idle_load_limit)
    end

    def write_invocation
      settings = @settings
      variables = {
        "MODE" => settings.mode, "TLS" => settings.tls_selection, "ARCHITECTURES" => settings.architecture_selection,
        "LOAD_GENERATOR" => settings.load_generator, "LOAD_GENERATOR_SSH" => settings.load_generator_ssh,
        "LISTEN_ADDRESS" => settings.listen_address, "TARGET_ADDRESS" => settings.target_address,
        "ROUNDS" => settings.rounds, "DURATION" => settings.duration, "WARMUP_DURATION" => settings.warmup_duration
      }
      invocation = variables.map { |name, value| "RUVOY_BENCH_#{name}=#{value}" }.join(" ")
      write("invocation.txt", "command=#{$PROGRAM_NAME}", "invocation=#{invocation} #{$PROGRAM_NAME}")
    end

    def preflight
      log = File.join(result_dir, "preflight.log")
      write("preflight.log", *preflight_facts)
      %w[baseline sync fiber].each { |kind| Build.envoy_module(kind, profile: "release", ruby: @ruby, log: log) }
      Build.check_worker_boundary(log: log)
    rescue Build::Failed => error
      raise Failure, "build or preflight failed: #{error.message}"
    end

    def preflight_facts
      settings = @settings
      commit = output_of("git", "-C", ROOT, "rev-parse", "HEAD")&.strip || "unknown"
      dirty = output_of("git", "-C", ROOT, "status", "--porcelain").to_s.empty? ? "no" : "yes"
      [ "run_id=#{settings.run_id}", "mode=#{settings.mode}", "architectures=#{settings.architecture_selection}",
        "architecture_order_mode=#{settings.order_mode}", "scenarios=#{settings.scenario_selection}",
        "body_matrix=#{settings.body_matrix}", "host=#{uname}", "envoy_package=#{ENVOY_PACKAGE}",
        "envoy_sdk_commit=#{Lockfile.envoy_sdk_commit}", "git_commit=#{commit}", "git_dirty=#{dirty}",
        "load_generator=#{settings.load_generator}", "load_generator_ssh=#{settings.load_generator_ssh}",
        "listen_address=#{settings.listen_address}", "target_address=#{settings.target_address}",
        "cpu_source=#{proc_stat? ? "proc" : "ps"}", "clock_tick=#{Etc.sysconf(Etc::SC_CLK_TCK)}",
        "tls_modes=#{settings.tls_modes.join(" ")}", "tls_key=#{@tls_key || "none"}",
        "ruby=#{output_of(@ruby, "--version")&.strip}", "bundler=#{Lockfile.bundler_version}",
        "oha=#{@load_generator.version}", "rustc=#{output_of("rustc", "--version")&.strip}",
        *%w[rack puma falcon async].map { |name| "#{name}=#{Lockfile.version(name)}" },
        "rounds=#{settings.rounds}", "bench_duration=#{settings.duration}",
        "warmup_duration=#{settings.warmup_duration}", "warmup_concurrency=#{settings.warmup_concurrency}",
        "oha_timeout_seconds=#{settings.oha_timeout_seconds}", "envoy_concurrency=#{settings.envoy_concurrency}",
        "puma_workers=#{settings.puma_workers}", "puma_threads=#{settings.puma_threads}",
        "falcon_count=#{settings.falcon_count}" ]
    end

    def measure
      @sequence = 0
      @controlled = []
      measured = 0
      rows = selected_rows
      write("tls-negotiation.tsv", "architecture\ttls_mode\tnegotiated")
      # TLS is the outer loop so that one mode's full matrix completes before the
      # other starts; interleaving modes would mix two different systems within a
      # round and break the execution-order balance that rotation provides.
      @settings.tls_modes.each do |tls_mode|
        @tls_mode = tls_mode
        @tls_baseline = nil
        rows.each do |row|
          1.upto(@settings.rounds) do |round|
            order(round).each do |architecture|
              @sequence += 1
              measured += 1 if measure_one(architecture, row, round)
            end
          end
        end
      end
      raise Failure, "scenario selection produced no runnable measurements" if measured.zero?

      # A silently truncated campaign is worse than a failed one: it still
      # produces a summary.
      expected = @settings.tls_modes.size * @settings.rounds *
                 rows.sum { |row| @settings.architectures.count { |architecture| supports?(architecture, row.protocol) } }
      raise Failure, "campaign ran #{measured} measurements but the selection requires #{expected}" unless measured == expected
    end

    def selected_rows
      lines = if @settings.smoke?
        [ "smoke_noop_h1_c1|1.1|1|0|0|0" ]
      elsif @settings.body_matrix != "false"
        BODY_SIZES.product([ 1, 10, 100 ]).map do |bytes, concurrency|
          if %w[true request].include?(@settings.body_matrix)
            "request_#{bytes}_h1_c#{concurrency}|1.1|#{concurrency}|0|#{bytes}|0"
          else
            "response_#{bytes}_h1_c#{concurrency}|1.1|#{concurrency}|0|0|#{bytes}"
          end
        end
      else
        SCENARIOS
      end
      lines.map { |line| Campaign.row(line) }.select { |row| @settings.scenario_selected?(row.scenario) }
    end

    def order(round)
      architectures = @settings.architectures
      case @settings.order_mode
      when "forward" then architectures
      when "reverse" then architectures.reverse
      when "alternate" then round.even? ? architectures.reverse : architectures
      # Reversing only alternates first and last place; rotating gives every
      # architecture an even share of each execution slot across rounds.
      when "rotate" then architectures.rotate((round - 1) % architectures.size)
      end
    end

    def supports?(architecture, protocol)
      !%w[puma_direct falcon_direct].include?(architecture) || protocol == "1.1"
    end

    def measure_one(architecture, row, round)
      campaign_line(row, round, architecture, "planned")
      unless supports?(architecture, row.protocol)
        append("skipped.tsv", row.scenario, round, architecture, row.protocol, SKIP_REASON)
        campaign_line(row, round, architecture, "skipped", SKIP_REASON)
        return false
      end

      start_architecture(architecture)
      verify_contract(architecture)
      warm_up(architecture)
      record_round(architecture, row, round)
      probe_control(architecture) if @settings.full? && CONTROL_PORTS.key?(architecture) && !@controlled.include?(architecture)
      stop_architecture(architecture)
      campaign_line(row, round, architecture, "completed")
      sleep @settings.cooldown_seconds if @settings.cooldown_seconds.positive?
      true
    end

    def campaign_line(row, round, architecture, state, reason = "")
      append("campaign.tsv", @sequence, row.scenario, @tls_mode, round, architecture, row.protocol, state, reason)
    end

    def start_architecture(architecture)
      @dir = File.join(result_dir, architecture, "campaign-#{@sequence}")
      FileUtils.mkdir_p(@dir)
      @servers = Servers.new
      case architecture
      when "puma_direct"
        start_puma(19211, tls: tls?)
        @servers.port = 19211
      when "falcon_direct"
        start_falcon(19212)
        @servers.port = 19212
      else
        # Envoy terminates TLS and forwards cleartext to its Puma, which is the
        # deployment shape the direct listeners are compared against.
        start_puma(19210, tls: false) if architecture == "envoy_puma"
        start_envoy(architecture, *ENVOY_ARCHITECTURES.fetch(architecture))
      end
    end

    def start_envoy(architecture, config, port)
      @servers.envoy_log = File.join(@dir, "envoy.log")
      env = bundle_env.merge("ENVOY_DYNAMIC_MODULES_SEARCH_PATH" => Build::MODULE_DIR)
      @servers.envoy = Child.spawn(env, "uvx", "--from", ENVOY_PACKAGE, "envoy",
                                   "--config-path", File.join(result_dir, "config", @tls_mode, config),
                                   "--concurrency", @settings.envoy_concurrency.to_s, "--disable-hot-restart",
                                   "--log-level", "warning", out: @servers.envoy_log)
      raise Failure, "#{architecture} Envoy did not become ready" unless ready?(benchmark_url(port, host: probe), @servers.envoy)

      @servers.runtime_pid = poll(100, 0.01) { @servers.envoy.children.first } or
        raise Failure, "#{architecture} could not resolve the Envoy runtime PID"
      @servers.port = port
    end

    def start_puma(port, tls:)
      bind = "tcp://#{@settings.listen_address}:#{port}"
      if tls
        # Puma exposes no switch to forbid TLS 1.2, so the negotiated version is
        # asserted at runtime instead (see verify_tls).
        bind = "ssl://#{@settings.listen_address}:#{port}?cert=#{tls_file("cert.pem")}&key=#{tls_file("key.pem")}" \
               "&no_tlsv1=true&no_tlsv1_1=true"
      end
      @servers.puma = Child.spawn(server_env, *@bundle, "_#{Lockfile.bundler_version}_", "exec", "puma",
                                  "--no-config", "--environment", "production",
                                  "--threads", "0:#{@settings.puma_threads}", "--workers", @settings.puma_workers.to_s,
                                  "--bind", bind, bench_rackup, out: File.join(@dir, "puma.log"))
      raise Failure, "Puma did not become ready on port #{port}" unless ready?(benchmark_url(port, host: probe, tls: tls), @servers.puma)

      @servers.puma_workers = workers(@servers.puma, @settings.puma_workers, "Puma", "workers") if @settings.puma_workers.positive?
    end

    def start_falcon(port)
      env = server_env.merge("RUVOY_FALCON_URL" => "#{scheme}://#{@settings.listen_address}:#{port}",
                             "RUVOY_FALCON_COUNT" => @settings.falcon_count.to_s,
                             "RUVOY_FALCON_RACKUP" => bench_rackup, "RUVOY_FALCON_ROOT" => ROOT)
      env.merge!("RUVOY_FALCON_TLS_CERTIFICATE" => tls_file("cert.pem"), "RUVOY_FALCON_TLS_KEY" => tls_file("key.pem")) if tls?
      # falcon serve cannot take an SSL context, so both modes go through
      # bench/falcon.rb to keep Falcon's process structure identical.
      @servers.falcon = Child.spawn(env, *@bundle, "_#{Lockfile.bundler_version}_", "exec", "falcon", "host",
                                    File.join(ROOT, "bench", "falcon.rb"), out: File.join(@dir, "falcon.log"))
      raise Failure, "Falcon did not become ready on port #{port}" unless ready?(benchmark_url(port, host: probe), @servers.falcon)

      # Falcon supervises forked instances, so the workers carry the real load.
      @servers.falcon_workers = workers(@servers.falcon, @settings.falcon_count, "Falcon", "instances")
    end

    def workers(child, count, server, noun)
      found = poll(200, Child::POLL) { child.children.then { |pids| pids if pids.size >= count } }
      found or raise Failure, "#{server} started #{child.children.size} of #{count} #{noun}"
    end

    def poll(attempts, interval)
      attempts.times do
        result = yield
        return result if result

        sleep interval
      end
      nil
    end

    def ready?(url, child)
      200.times do
        return true if reachable?(url)
        return false unless child.alive?

        sleep Child::POLL
      end
      false
    end

    def verify_contract(architecture)
      port = @servers.port
      verify_tls(architecture, port) if tls?
      response = begin
        request(benchmark_url(port, host: probe, response_bytes: 1024, request_bytes: 1024),
                body: File.binread(File.join(result_dir, "body-1024.bin")), timeout: @settings.oha_timeout_seconds)
      rescue *UNREACHABLE => error
        raise Failure, "#{architecture} contract request failed: #{error.message}"
      end
      File.write(File.join(@dir, "contract.headers"), header_dump(response))
      File.binwrite(File.join(@dir, "contract.body"), response.body.to_s)
      raise Failure, "#{architecture} contract returned the wrong response size" unless response.body.to_s.bytesize == 1024
      raise Failure, "#{architecture} contract did not read the request body" unless response["x-request-bytes"] == "1024"
      return unless supports?(architecture, "2")

      json = File.join(@dir, "contract-h2.json")
      error = oha(json, [ "--no-tui", "--output-format", "json", "--http2", "-n", "2", "-c", "1", "-p", "2", *tls_args,
                          benchmark_url(port, host: target, request_bytes: 0) ])
      raise Failure, "#{architecture} HTTP/2 contract oha #{error}" if error
      raise Failure, "#{architecture} HTTP/2 contract failed" unless valid_oha?(json)
    end

    # ALPN is fixed to http/1.1: the direct servers do not offer h2 over TLS
    # while Envoy does. Version and cipher decide handshake cost and must match.
    def verify_tls(architecture, port)
      observed = negotiated_tls(port)
      raise Failure, "#{architecture} negotiated '#{observed}' instead of a TLS 1.3 cipher suite" unless observed.start_with?("TLSv1.3/TLS_")

      append("tls-negotiation.tsv", architecture, @tls_mode, observed)
      @tls_baseline ||= observed
      return if observed == @tls_baseline

      raise Failure, "#{architecture} negotiated #{observed} but the baseline is #{@tls_baseline}"
    end

    def negotiated_tls(port)
      context = OpenSSL::SSL::SSLContext.new
      context.ca_file = tls_file("ca.pem")
      context.verify_mode = OpenSSL::SSL::VERIFY_PEER
      context.alpn_protocols = [ "http/1.1" ]
      socket = Socket.tcp(probe, port, connect_timeout: 5)
      connection = OpenSSL::SSL::SSLSocket.new(socket, context)
      connection.sync_close = true
      connection.hostname = "localhost"
      connection.connect
      "#{connection.ssl_version}/#{connection.cipher.first}"
    rescue *UNREACHABLE => error
      "#{error.class}: #{error.message}"
    ensure
      connection ? connection.close : socket&.close
    end

    def warm_up(architecture)
      json = File.join(@dir, "warmup.json")
      error = oha(json, [ "--no-tui", "--output-format", "json", "--wait-ongoing-requests-after-deadline",
                          "--http-version", "1.1", "-z", @settings.warmup_duration,
                          "-c", @settings.warmup_concurrency.to_s, *tls_args, benchmark_url(@servers.port, host: target) ])
      raise Failure, "#{architecture} warmup #{error}" if error
      raise Failure, "#{architecture} warmup returned errors" unless valid_oha?(json)
    end

    def record_round(architecture, row, round)
      prefix = File.join(result_dir, architecture, "#{row.scenario}-round#{round}-seq#{@sequence}")
      json = "#{prefix}.json"
      resources = "#{prefix}.resources.csv"
      label = "#{architecture} #{row.scenario} round #{round}"
      args = [ "--no-tui", "--output-format", "json", "--wait-ongoing-requests-after-deadline",
               "-z", @settings.duration, *tls_args ]
      args += if row.protocol == "2"
        [ "--http2", "-c", "1", "-p", row.concurrency.to_s ]
      else
        [ "--http-version", "1.1", "-c", row.concurrency.to_s ]
      end
      args += [ "--method", "POST", "-H", "Expect:", "-D", @load_generator.body_path(row.request_bytes) ] if row.request_bytes.positive?
      args << benchmark_url(@servers.port, host: target, wait_ms: row.wait_ms, response_bytes: row.response_bytes,
                            request_bytes: row.request_bytes, params: row.app_params)

      served_before = served_count(label)
      pids = @servers.measured_pids
      cpu_before = cpu_seconds(pids)
      started = Bench.clock
      sampler = RssSampler.start(pids, "#{prefix}.rss.csv")
      error = oha(json, args)
      elapsed = Bench.clock - started
      cpu_after = cpu_seconds(pids)
      sampler.stop
      raise Failure, "#{label}: oha #{error}" if error
      raise Failure, "#{label} returned non-200 or transport errors" unless valid_oha?(json)

      @load_generator.check_headroom("#{json}.loadgen", label)
      served = served_count(label) - served_before
      reported = JSON.parse(File.read(json)).dig("statusCodeDistribution", "200")
      # The counter is process-wide, so warm-up leftovers and the control probe
      # also land in it. What must never happen is the server executing fewer
      # requests than the load generator counted as successful: that means work
      # was reported but never ran.
      if served < reported
        raise Failure, "#{label}: the load generator counted #{reported} successful requests but the server only executed #{served}"
      end

      cpu_percent = elapsed.positive? ? format("%.3f", (cpu_after - cpu_before) * 100 / elapsed) : "0"
      File.write(resources, "sample,cpu_percent,rss_kib\n0,#{cpu_percent},#{sampler.maximum}\n")
      append("manifest.tsv", @sequence, architecture, row.scenario, @tls_mode, round, row.protocol, row.concurrency,
             row.wait_ms, row.request_bytes, row.response_bytes, row.app_params, served, relative(json), relative(resources))
    end

    def probe_control(architecture)
      probe_args = [ "--no-tui", "--output-format", "json", "--wait-ongoing-requests-after-deadline",
                     "--http-version", "1.1", "-z", "500ms", "-c", "10",
                     "http://#{target}:#{CONTROL_PORTS.fetch(architecture)}/" ]
      1.upto(@settings.control_repetitions) do |run|
        idle = File.join(@dir, "control-idle-run#{run}.json")
        error = oha(idle, probe_args)
        raise Failure, "#{architecture} idle control probe #{error}" if error
        raise Failure, "#{architecture} idle control probe failed" unless valid_oha?(idle)

        append("control-manifest.tsv", architecture, "idle", run, relative(idle))
        slow = File.join(@dir, "control-load-run#{run}.json")
        load = @load_generator.start(slow, [ "--no-tui", "--output-format", "json", "--http-version", "1.1",
                                             "-n", "10", "-c", "10", *tls_args,
                                             benchmark_url(@servers.port, host: target, wait_ms: 200) ])
        sleep 0.05
        loaded = File.join(@dir, "control-loaded-run#{run}.json")
        error = oha(loaded, probe_args)
        if error
          @load_generator.stop(load)
          raise Failure, "#{architecture} loaded control probe #{error}"
        end
        unless load.exits_within?(@settings.oha_timeout_seconds)
          @load_generator.stop(load)
          raise Failure, "#{architecture} slow Ruby load timed out"
        end
        raise Failure, "#{architecture} slow Ruby load failed" unless load.succeeded?
        raise Failure, "#{architecture} slow Ruby load returned errors" unless valid_oha?(slow)
        raise Failure, "#{architecture} loaded control probe failed" unless valid_oha?(loaded)

        append("control-manifest.tsv", architecture, "loaded", run, relative(loaded))
      end
      @controlled << architecture
    end

    def stop_architecture(architecture)
      servers = @servers
      @servers = nil
      raise Failure, "#{architecture} did not stop cleanly" unless servers.stop

      line, message = STOPPED[architecture]
      raise Failure, message if line && !File.read(servers.envoy_log).include?(line)
    end

    def conclude
      1.upto(@settings.rounds) { |run| bridge_benchmark(run) }
      %w[summarize.rb render_charts.rb].each do |script|
        next if Bench.unbundled { system(@ruby, File.join(ROOT, "bench", script), result_dir) }

        raise Failure, "#{script} failed"
      end
      reject_unstable
      write_checksums
      puts "result=PASS", "raw_results=#{result_dir}", "summary=#{File.join(result_dir, "summary.md")}",
           "charts=#{File.join(result_dir, "charts")}"
    end

    def bridge_benchmark(run)
      path = File.join(result_dir, "bridge-run#{run}.json")
      env = { "PATH" => [ File.dirname(@ruby), @path ].join(File::PATH_SEPARATOR), "RUBY" => @ruby }
      Bench.unbundled do
        system(env, "cargo", "run", "--quiet", "--release", "--manifest-path", File.join(ROOT, "Cargo.toml"),
               "--example", "bridge_benchmark", out: path, chdir: ROOT)
      end
      document = begin
        JSON.parse(File.read(path))
      rescue JSON::ParserError, SystemCallError
        nil
      end
      pure = document&.dig("pure_rust", "p99_us")
      bridged = document&.dig("ruby_bridge", "p99_us")
      return if pure.is_a?(Numeric) && pure >= 0 && bridged.is_a?(Numeric) && bridged.positive?

      raise Failure, "standalone bridge benchmark run #{run} returned invalid JSON"
    end

    # A scenario whose rounds disagree this much is not a measurement; the
    # summary marks it and the run fails rather than publishing an unstable number.
    def reject_unstable
      unstable = JSON.parse(File.read(File.join(result_dir, "summary.json"))).fetch("unstable_measurements", [])
      return if unstable.empty? || !@settings.full?

      unstable.each do |entry|
        warn "#{entry["architecture"]}/#{entry["scenario"]}/#{entry["tls_mode"]} rps_rsd=#{entry["rps_rsd_percent"]}%"
      end
      raise Failure, "unstable scenarios exceeded the 15% RPS RSD threshold and must be re-run"
    end

    # The checksums cover the machine-readable evidence, so a later reader can
    # tell whether the raw results still match what was summarised.
    def write_checksums
      files = Dir.glob("**/*", File::FNM_DOTMATCH, base: result_dir)
                 .select { |path| File.file?(File.join(result_dir, path)) && File.basename(path) != "SHA256SUMS" }
                 .map { |path| "./#{path}" }.sort
      sums = files.map { |path| "#{Digest::SHA256.file(File.join(result_dir, path)).hexdigest}  #{path}\n" }
      File.write(File.join(result_dir, "SHA256SUMS"), sums.join)
    end

    def oha(output, args)
      @load_generator.run(output, args, timeout: @settings.oha_timeout_seconds)
    end

    def valid_oha?(path)
      document = JSON.parse(File.read(path))
      codes = document["statusCodeDistribution"]
      errors = document["errorDistribution"]
      document.dig("summary", "successRate") == 1 && document.dig("summary", "requestsPerSec").to_f.positive? &&
        errors.is_a?(Hash) && errors.empty? && codes.is_a?(Hash) && codes.keys == [ "200" ] && codes["200"].to_i.positive?
    rescue JSON::ParserError, SystemCallError
      false
    end

    # The server's own execution count, so a round that silently dropped
    # requests cannot be reported as throughput.
    def served_count(label)
      Integer(request("#{scheme}://#{probe}:#{@servers.port}/benchmark-served").body.to_s.strip, 10)
    rescue ArgumentError, *UNREACHABLE
      raise Failure, "#{label}: could not read the served counter"
    end

    def request(url, body: nil, timeout: 10)
      uri = URI(url)
      http = Net::HTTP.new(uri.host, uri.port)
      http.open_timeout = http.read_timeout = timeout
      if uri.scheme == "https"
        http.use_ssl = true
        http.ca_file = tls_file("ca.pem")
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER
      end
      http.start do
        message = body ? Net::HTTP::Post.new(uri.request_uri, "content-type" => "application/octet-stream") : Net::HTTP::Get.new(uri.request_uri)
        message.body = body if body
        http.request(message)
      end
    end

    def reachable?(url)
      request(url, timeout: 1).is_a?(Net::HTTPSuccess)
    rescue *UNREACHABLE
      false
    end

    def benchmark_url(port, host:, tls: tls?, wait_ms: 0, response_bytes: 0, request_bytes: nil, params: "")
      query = "wait_ms=#{wait_ms}&response_bytes=#{response_bytes}"
      query += "&expected_request_bytes=#{request_bytes}" if request_bytes
      query += "&#{params}" unless params.empty?
      "#{tls ? "https" : "http"}://#{host}:#{port}/benchmark?#{query}"
    end

    def header_dump(response)
      head = "HTTP/#{response.http_version} #{response.code} #{response.message}\n"
      head + response.each_header.map { |name, value| "#{name}: #{value}\n" }.join
    end

    # ps reports whole seconds, which rounds most windows to 0% or to a
    # neighbouring multiple of 1/duration; /proc counts clock ticks instead.
    def cpu_seconds(pids)
      return pids.sum { |pid| ps_seconds(pid) } unless proc_stat?

      ticks = pids.sum do |pid|
        stat = begin
          File.read("/proc/#{pid}/stat")
        rescue SystemCallError
          next 0
        end
        # comm can contain spaces, so fields are counted after the closing paren.
        fields = stat[(stat.rindex(")") + 2)..].split
        fields[11].to_i + fields[12].to_i
      end
      ticks.to_f / Etc.sysconf(Etc::SC_CLK_TCK)
    end

    def ps_seconds(pid)
      value = output_of("ps", "-o", "time=", "-p", pid.to_s).to_s.strip
      return 0.0 if value.empty?

      days, clock = value.include?("-") ? value.split("-", 2) : [ 0, value ]
      parts = clock.split(":").map(&:to_f)
      parts.unshift(0.0) while parts.size < 3
      days.to_i * 86_400 + parts[0] * 3600 + parts[1] * 60 + parts[2]
    end

    def proc_stat? = File.readable?("/proc/self/stat")

    def bundle_env
      { "BUNDLE_GEMFILE" => File.join(ROOT, "Gemfile"), "BUNDLE_PATH" => File.join(ROOT, "vendor", "bundle"),
        "BUNDLE_FROZEN" => "true" }
    end

    def server_env
      bundle_env.merge("PATH" => [ File.dirname(@ruby), @path ].join(File::PATH_SEPARATOR), "RUBY" => @ruby)
    end

    def bench_rackup = File.join(ROOT, "bench", "config.ru")

    def tls? = @tls_mode == "tls"

    def scheme = tls? ? "https" : "http"

    def tls_args = @load_generator.tls_args(tls?)

    def tls_file(name) = File.join(result_dir, "tls", name)

    def probe = @settings.probe_address

    def target = @settings.target_address

    def first_line(path)
      File.readable?(path) ? File.read(path).strip : "unavailable"
    end

    def uname
      facts = Etc.uname
      "#{facts[:sysname]} #{facts[:release]} #{facts[:machine]}"
    end

    def executable?(name)
      ENV.fetch("PATH", "").split(File::PATH_SEPARATOR).any? { |directory| File.executable?(File.join(directory, name)) }
    end

    # Nil when the command cannot run or fails.
    def output_of(*command)
      output, status = Bench.unbundled { Open3.capture2(*command, err: File::NULL) }
      status.success? ? output : nil
    rescue SystemCallError
      nil
    end

    def write(name, *lines)
      File.write(File.join(result_dir, name), lines.map { |line| "#{line}\n" }.join)
    end

    def append(name, *fields)
      File.write(File.join(result_dir, name), "#{fields.join("\t")}\n", mode: "a")
    end

    def relative(path) = path.delete_prefix("#{result_dir}/")
  end
end
