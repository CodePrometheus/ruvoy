# frozen_string_literal: true

require_relative "test_helper"

# The process-wide Ruby runtime: one CRuby VM per Envoy process, shared by
# every filter config, torn down exactly once at process exit. Lifecycle
# assertions rather than measurements, so they are safe on a busy machine.
class ProcessRuntimeTest < E2ETestCase
  STARTED = "[ruvoy] Fiber runtime started"
  STOPPED = "[ruvoy] Fiber runtime stopped"
  CRASH = /panic|segmentation fault|SIGSEGV/i

  class << self
    def results
      @results ||= File.join(result_dir, "process-runtime-#{run_id}").tap { |dir| FileUtils.mkdir_p(dir) }
    end

    def prepared
      @prepared ||= begin
        build_module("fiber", log: File.join(results, "build.log"))
        true
      end
    end
  end

  def setup
    self.class.prepared
  end

  def teardown
    @envoy&.stop
  end

  # Envoy dlopens the module, so the loader has to find libruby from the
  # module's own RUNPATH, with no LD_LIBRARY_PATH to help.
  def test_the_module_resolves_libruby_without_ld_library_path
    module_path = File.join(module_dir, "libruvoy_fiber.so")
    if command?("readelf")
      runpath = IO.popen([ "readelf", "-d", module_path ], &:read)[/(?:RUNPATH|RPATH)[^\[]*\[([^\]]*)\]/, 1]
      refute_nil runpath, "the module has no RUNPATH; Envoy would need LD_LIBRARY_PATH"
      assert_includes runpath, RbConfig::CONFIG.fetch("libdir"), "RUNPATH '#{runpath}' does not contain the Ruby libdir"
    end
    return unless command?("ldd")

    unresolved = IO.popen({ "LD_LIBRARY_PATH" => nil }, [ "ldd", module_path ], &:read).lines.grep(/not found/)
    assert_empty unresolved, "unresolved shared libraries without LD_LIBRARY_PATH: #{unresolved.join}"
  end

  def test_two_filter_configs_share_one_runtime
    log = start(config([ [ "ruvoy_first", 19190 ], [ "ruvoy_second", 19191 ] ]), "shared")
    assert ready?(19190) && ready?(19191), "two-config Envoy did not become ready:\n#{tail(log)}"
    first = http_get(url(19190))["x-ruvoy-runtime-rust-thread-id"]
    second = http_get(url(19191))["x-ruvoy-runtime-rust-thread-id"]
    refute_nil first, "the first listener reported no runtime thread"
    assert_equal first, second, "runtime threads differ: '#{first}' vs '#{second}'"
    assert_equal 1, count(log, STARTED), "the runtime was not initialized exactly once"
    @envoy.stop
    assert_equal 1, count(log, STOPPED), "shutdown did not run exactly once"
  end

  def test_an_incompatible_second_config_is_refused
    other = File.join(scratch_dir, "other.ru")
    FileUtils.cp(fixture_rackup, other)
    FileUtils.cp(File.join(root, "test", "fixtures", "rack", "app.rb"), scratch_dir)
    log = start(config([ [ "ruvoy_first", 19192 ], [ "ruvoy_second", 19193, other ] ]), "mismatch")
    refute_nil @envoy.wait_exit(timeout: 60), "Envoy stayed up with two different rackup paths"
    # What the operator needs from the message, not its wording: both rackups
    # named, and that a restart is what resolves it.
    text = File.read(log)
    assert_includes text, "active rackup=", "the refusal does not name the active rackup"
    assert_includes text, "requested rackup=", "the refusal does not name the requested rackup"
    assert_match(/restart/i, text, "the refusal does not say that a restart resolves it")
    assert_equal 1, count(log, STARTED), "the mismatch initialized a second Ruby VM"
    refute_match CRASH, text, "the mismatch case produced a crash signature"
  end

  def test_a_missing_rackup_fails_closed
    log = start(config([ [ "ruvoy_first", 19194, File.join(scratch_dir, "does-not-exist.ru") ] ]), "missing")
    refute_nil @envoy.wait_exit(timeout: 60), "Envoy stayed up with a missing rackup"
    text = File.read(log)
    assert_includes text, "failed to resolve rackup", "the startup error does not name the unresolved rackup"
    assert_includes text, "working directory", "the startup error omits the working directory"
  end

  def test_a_raising_rackup_fails_closed
    raising = File.join(scratch_dir, "raising.ru")
    File.write(raising, "raise \"intentional startup failure\"\n")
    log = start(config([ [ "ruvoy_first", 19195, raising ] ]), "raising")
    refute_nil @envoy.wait_exit(timeout: 60), "Envoy stayed up after a Ruby startup exception"
    text = File.read(log)
    assert_includes text, "intentional startup failure", "the Ruby startup exception was swallowed"
    refute_match(/segmentation fault|SIGSEGV/i, text, "the raising case produced a crash signature")
  end

  def test_shutdown_drains_an_in_flight_request
    log = start(config([ [ "ruvoy_first", 19196 ] ]), "drain")
    assert ready?(19196), "drain Envoy did not become ready:\n#{tail(log)}"
    in_flight = Thread.new { fetch(19196, "/async-sleep?duration=1", timeout: 20) }
    sleep 0.3
    Process.kill("TERM", @envoy.pid)
    response = in_flight.value
    refute_nil @envoy.wait_exit(timeout: 30), "Envoy did not exit after SIGTERM"
    puts "NOTE: in-flight request returned #{response&.code.inspect} during drain" unless response&.code == "200"
    assert_includes File.read(log), STOPPED, "no clean shutdown after SIGTERM"
  end

  # stream_idle_timeout is 5s, so a 9s Ruby request is abandoned by Envoy
  # while the fiber keeps running; its completion then lands on a destroyed
  # filter and has to go nowhere.
  def test_a_late_completion_after_the_stream_timed_out_stays_silent
    log = start(config([ [ "ruvoy_first", 19197 ] ]), "timeout")
    assert ready?(19197), "timeout Envoy did not become ready:\n#{tail(log)}"
    late = fetch(19197, "/async-sleep?duration=9", timeout: 30)
    puts "timed-out request returned #{late&.code.inspect}"
    sleep 6
    assert_equal "200", fetch(19197, "/rack-env", timeout: 10)&.code, "Envoy stopped serving after the late completion"
    assert_predicate @envoy, :alive?, "the Envoy process died after the late completion"
    @envoy.stop
    refute_match(/panic|segmentation fault|SIGSEGV|use-after-free/i, File.read(log),
                 "the late-completion case produced a crash signature")
  end

  # SIGTERM is an immediate shutdown in Envoy; draining through the admin
  # endpoint is what tells whether an in-flight Ruby request may finish.
  def test_a_graceful_drain_completes_in_flight_work
    admin = free_port(19601)
    log = start(config([ [ "ruvoy_first", 19198 ] ], admin_port: admin), "admin")
    assert ready?(19198), "admin Envoy did not become ready:\n#{tail(log)}"
    in_flight = Thread.new { fetch(19198, "/async-sleep?duration=2", timeout: 25) }
    sleep 0.4
    http_post("http://127.0.0.1:#{admin}/drain_listeners?graceful", "")
    assert_equal "200", in_flight.value&.code, "the graceful drain did not let the in-flight Ruby request finish"
  end

  # Each epoch is its own process and therefore its own Ruby VM; the point is
  # that the old epoch tears its VM down exactly once while the new one serves.
  def test_hot_restart_hands_over_without_disturbing_the_runtime
    path = config([ [ "ruvoy_first", 19199 ] ])
    epoch0, log0 = start_epoch(path, 0)
    assert epoch0.serving?(url(19199), timeout: 20), "hot-restart epoch 0 did not become ready:\n#{tail(log0)}"
    epoch1, log1 = start_epoch(path, 1)
    @envoy = epoch1
    refute_nil epoch0.wait_exit(timeout: 60), "the original epoch did not exit after the hot restart"
    assert_equal "200", fetch(19199, "/rack-env", timeout: 10)&.code, "the new epoch does not serve after the handover"
    assert_equal 1, count(log0, STOPPED), "the old epoch did not shut its runtime down exactly once"
    assert_equal 1, count(log1, STARTED), "the new epoch did not initialize exactly one Ruby VM"
    epoch1.stop
    refute_match CRASH, File.read(log0) + File.read(log1), "the hot restart produced a crash signature"
  ensure
    epoch0&.stop
  end

  private

  def url(port)
    "http://127.0.0.1:#{port}/rack-env"
  end

  def command?(name)
    ENV.fetch("PATH").split(File::PATH_SEPARATOR).any? { |directory| File.executable?(File.join(directory, name)) }
  end

  def count(log, needle)
    File.read(log).scan(needle).size
  end

  def ready?(port)
    @envoy.serving?(url(port), timeout: 20)
  end

  def fetch(port, path, timeout:)
    Net::HTTP.start("127.0.0.1", port, read_timeout: timeout) { |http| http.get(path) }
  rescue Net::ReadTimeout, EOFError, IOError, SystemCallError
    nil
  end

  # A fixed admin port makes the run fail whenever anything unrelated on the
  # machine already holds it.
  def free_port(from)
    (from..from + 40).find { |port| port_free?(port) } || flunk("no free port near #{from}")
  end

  def start(config, name)
    File.join(self.class.results, "#{name}.log").tap do |log|
      @envoy = Envoy.start(config: config, modules: module_dir, log: log, log_level: "info",
                           env: bundle_env.merge("RUVOY_DIAGNOSTICS" => "1"))
    end
  end

  def start_epoch(config, epoch)
    log = File.join(self.class.results, "hot-epoch#{epoch}.log")
    envoy = Envoy.start(config: config, modules: module_dir, log: log, log_level: "info",
                        hot_restart: { epoch: epoch, base_id: 7, drain_time: 5, parent_shutdown_time: 10 },
                        env: bundle_env.merge("RUVOY_DIAGNOSTICS" => "1"))
    [ envoy, log ]
  end

  def config(listeners, admin_port: nil)
    @configs = (@configs || 0) + 1
    File.join(scratch_dir, "config-#{@configs}.yaml").tap do |path|
      admin = admin_port ? [ "admin:", "  address:", "    socket_address:", "      address: 127.0.0.1", "      port_value: #{admin_port}" ] : []
      File.write(path, (admin + [ "static_resources:", "  listeners:" ]).map { |line| "#{line}\n" }.join +
                       listeners.map { |name, port, rackup| listener(name, port, rackup || fixture_rackup) }.join)
    end
  end

  def listener(name, port, rackup)
    <<~YAML.gsub(/^/, "    ")
      - name: #{name}
        address:
          socket_address:
            address: 127.0.0.1
            port_value: #{port}
        filter_chains:
          - filters:
              - name: envoy.filters.network.http_connection_manager
                typed_config:
                  "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                  stat_prefix: #{name}
                  codec_type: AUTO
                  stream_idle_timeout: 5s
                  request_timeout: 10s
                  route_config:
                    name: #{name}_route
                    virtual_hosts:
                      - name: #{name}_service
                        domains: ["*"]
                  http_filters:
                    - name: envoy.extensions.filters.http.dynamic_modules
                      typed_config:
                        "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
                        dynamic_module_config:
                          name: ruvoy_fiber
                          do_not_close: true
                        filter_name: fiber_rack
                        terminal_filter: true
                        filter_config:
                          "@type": type.googleapis.com/google.protobuf.StringValue
                          value: #{rackup}
    YAML
  end
end
