# frozen_string_literal: true

require "json"
require "socket"
require_relative "test_helper"

# The bootstrap extension owns the runtime: Envoy is ready only once the
# application is, the application hears about draining, Envoy's own shutdown
# stops the runtime, the admin interface reaches Ruby, and the gauges the
# runtime publishes settle without a request to carry them.
class BootstrapTest < E2ETestCase
  PORT = 19280
  ADMIN_PORT = 19281
  STOPPED = "[ruvoy] Fiber runtime stopped"

  class << self
    def results
      @results ||= File.join(result_dir, "bootstrap-#{run_id}").tap { |dir| FileUtils.mkdir_p(dir) }
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
    [ PORT, ADMIN_PORT ].each { |port| assert_port_free(port) }
    @marks = File.join(scratch_dir, "marks-#{name}.txt")
    File.write(@marks, "")
  end

  def teardown
    @envoy&.stop
  end

  def test_envoy_is_ready_only_once_the_application_is
    start("slow-boot", boot_delay: 3)
    # The listener is in the configuration, yet nothing answers on it until the
    # application's before_ready hooks have run.
    refute ready?, "Envoy reported ready before the application was"
    assert wait_for(timeout: 15) { ready? }, "Envoy never became ready:\n#{tail(@log)}"
    assert_equal "ready\n", File.read(@marks), "the hook did not run before Envoy was ready"
    assert_equal "ok\n", http_get(url("/")).body
  end

  def test_a_rack_filter_without_the_bootstrap_extension_is_refused
    config = File.join(scratch_dir, "no-bootstrap.yaml")
    File.write(config, listener_yaml)
    @log = File.join(self.class.results, "no-bootstrap.log")
    @envoy = Envoy.start(config: config, modules: module_dir, log: @log, log_level: "info", env: bundle_env)
    refute_nil @envoy.wait_exit(timeout: 60), "Envoy stayed up with a rack filter and no bootstrap extension"
    assert_includes File.read(@log), "bootstrap extension", "the refusal does not say what is missing"
  end

  def test_the_admin_interface_reaches_ruby
    start("admin")
    assert wait_for(timeout: 15) { ready? }

    overview = admin_json("/ruvoy")
    assert_equal rackup, overview["rackup"]
    assert_equal 0, overview["inflight_requests"]
    assert_match(/\A\d+\.\d+\.\d+/, overview["version"])
    assert_includes overview["ruby"], "ruby"

    gc = admin_json("/ruvoy/gc")
    assert_operator gc["heap_live_slots"], :>, 0
    compacted = admin("POST", "/ruvoy/gc?compact=1")
    assert_equal "200", compacted.code, compacted.body

    fibers = admin_json("/ruvoy/fibers")
    assert_kind_of Array, fibers

    config = admin_json("/ruvoy/config")
    assert_equal rackup, config["rackup"]
    assert_equal 1024, config["max_inflight_requests"]

    assert_equal "404", admin("GET", "/ruvoy/nope").code
  end

  # A client that leaves mid-request frees its slot on the Ruby thread, after
  # every worker is done with it; the gauge must still come back to zero
  # without another request.
  def test_inflight_requests_settles_without_another_request
    start("inflight")
    assert wait_for(timeout: 15) { ready? }
    socket = TCPSocket.new("127.0.0.1", PORT)
    socket.write("GET /wait?s=3 HTTP/1.1\r\nHost: lifecycle\r\n\r\n")
    assert wait_for(timeout: 5) { inflight == 1 }, "inflight_requests never showed the request in flight"
    socket.close
    assert wait_for(timeout: 10) { inflight == 0 }, "inflight_requests stayed at #{inflight} after the client left"
  end

  def test_draining_runs_the_application_hooks
    start("drain")
    assert wait_for(timeout: 15) { ready? }
    response = http_post("http://127.0.0.1:#{ADMIN_PORT}/drain_listeners?graceful", "")
    assert_equal "200", response.code
    assert wait_for(timeout: 10) { File.read(@marks).include?("drain\n") }, "the drain hook did not run"
  end

  def test_envoy_shutdown_stops_the_runtime_once
    start("shutdown")
    assert wait_for(timeout: 15) { ready? }
    Process.kill("TERM", @envoy.pid)
    refute_nil @envoy.wait_exit(timeout: 30), "Envoy did not exit after SIGTERM"
    text = File.read(@log)
    assert_equal 1, text.scan(STOPPED).size, "the runtime did not stop exactly once:\n#{tail(@log)}"
    refute_includes text, "shutdown failed"
    # Envoy's shutdown stage stopped it, not the process-exit fallback.
    assert_operator text.index(STOPPED), :<, text.index("exiting"), "the runtime stopped only at process exit"
  end

  private

  def rackup
    File.join(root, "test", "fixtures", "lifecycle", "config.ru")
  end

  def url(path)
    "http://127.0.0.1:#{PORT}#{path}"
  end

  def start(name, boot_delay: 0)
    config = File.join(scratch_dir, "#{name}.yaml")
    File.write(config, bootstrap_yaml(rackup: rackup, max_inflight_requests: 1024) + listener_yaml)
    @log = File.join(self.class.results, "#{name}.log")
    env = bundle_env.merge("RUVOY_TEST_MARKS" => @marks, "RUVOY_TEST_BOOT_DELAY" => boot_delay.to_s)
    @envoy = Envoy.start(config: config, modules: module_dir, log: @log, log_level: "info",
                         admin_port: ADMIN_PORT, env: env)
  end

  def ready?
    http_get("http://127.0.0.1:#{ADMIN_PORT}/ready").code == "200"
  rescue SystemCallError, IOError
    false
  end

  def inflight
    admin_stat(ADMIN_PORT, "dynamicmodulescustom.inflight_requests").to_i
  end

  def admin(method, path)
    uri = URI("http://127.0.0.1:#{ADMIN_PORT}#{path}")
    Net::HTTP.start(uri.host, uri.port) do |http|
      method == "POST" ? http.post(uri.request_uri, "") : http.get(uri.request_uri)
    end
  end

  def admin_json(path)
    response = admin("GET", path)
    assert_equal "200", response.code, "#{path}: #{response.body}"
    JSON.parse(response.body)
  end

  def wait_for(timeout:)
    deadline = clock + timeout
    loop do
      return true if yield
      return false if clock > deadline

      sleep 0.1
    end
  end

  def listener_yaml
    <<~YAML
      static_resources:
        listeners:
          - name: lifecycle
            address:
              socket_address: { address: 127.0.0.1, port_value: #{PORT} }
            filter_chains:
              - filters:
                  - name: envoy.filters.network.http_connection_manager
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                      stat_prefix: lifecycle
                      codec_type: AUTO
                      route_config:
                        name: lifecycle
                        virtual_hosts:
                          - name: lifecycle
                            domains: ["*"]
                      http_filters:
                        - name: envoy.extensions.filters.http.dynamic_modules
                          typed_config:
                            "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
                            dynamic_module_config: { name: ruvoy_fiber, do_not_close: true }
                            filter_name: fiber_rack
                            terminal_filter: true
                            filter_config:
                              "@type": type.googleapis.com/google.protobuf.StringValue
                              value: "{}"
    YAML
  end
end
