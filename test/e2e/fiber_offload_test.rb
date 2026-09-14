# frozen_string_literal: true

require "json"
require_relative "test_helper"

# Blocking C calls moved off the runtime thread with Ruvoy.offload.
#
# The fixture blocks the way a C extension does, releasing the GVL and waiting
# in the kernel, so the evidence is timing: calls that queue behind the runtime
# thread when made directly overlap once offloaded.
class FiberOffloadTest < E2ETestCase
  PORT = 19250
  CALLS = 5
  SECONDS = 0.2

  class << self
    def envoy
      @envoy ||= start_envoy
    end

    def results
      @results ||= File.join(result_dir, "offload-#{run_id}").tap { |dir| FileUtils.mkdir_p(dir) }
    end

    private

    def start_envoy
      raise "TCP port #{PORT} is already in use" unless port_free?(PORT)

      build_module("fiber", log: File.join(results, "build.log"))
      config = File.join(scratch_dir, "offload.yaml")
      File.write(config, listener_config)
      log = File.join(results, "envoy.log")
      Envoy.start(config: config, modules: module_dir, log: log, log_level: "info", env: bundle_env).tap do |envoy|
        Minitest.after_run { envoy.stop }
        raise "offload Envoy did not become ready:\n#{tail(log, lines: 5)}" unless envoy.serving?(
          "http://127.0.0.1:#{PORT}/state"
        )
      end
    end

    def listener_config
      <<~YAML
        static_resources:
          listeners:
            - name: ruvoy_offload
              address:
                socket_address:
                  address: 127.0.0.1
                  port_value: #{PORT}
              filter_chains:
                - filters:
                    - name: envoy.filters.network.http_connection_manager
                      typed_config:
                        "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                        stat_prefix: ruvoy_offload
                        codec_type: AUTO
                        stream_idle_timeout: 30s
                        route_config:
                          name: offload_route
                          virtual_hosts:
                            - name: offload_service
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
                                value: #{File.join(root, "test", "fixtures", "offload", "config.ru")}
      YAML
    end
  end

  def setup
    self.class.envoy
  end

  def test_offloaded_calls_overlap_where_direct_ones_queue
    direct = elapsed { concurrently(CALLS) { get("/direct?seconds=#{SECONDS}") } }
    offloaded = elapsed { concurrently(CALLS) { get("/offload?seconds=#{SECONDS}") } }

    assert_operator direct, :>=, CALLS * SECONDS * 0.9, "direct calls must queue, or this test proves nothing"
    assert_operator offloaded, :<, SECONDS * 2.5, "offloaded calls should wait together"
  end

  def test_the_callers_get_the_value_or_the_exception
    assert_equal "42", get("/value")

    failure = JSON.parse(get("/raise"))
    assert_equal [ "ArgumentError", "raised inside the block" ], failure.values_at("class", "message")
    assert_includes failure["backtrace"].first, "config.ru", "the backtrace should point inside the block"
    refute_includes File.read(self.class.envoy.log), "terminated with exception"
  end

  def test_a_limit_bounds_the_calls_in_flight
    duration = elapsed { concurrently(6) { get("/limited?seconds=#{SECONDS}") } }

    assert_equal 2, state["peak"]
    assert_operator duration, :>=, 3 * SECONDS * 0.9, "six calls two at a time take three rounds"
    assert_includes get("/invalid-limit"), "limit must be a positive Integer"
  end

  def test_an_interrupted_caller_waits_for_its_call_before_unwinding
    socket = TCPSocket.new("127.0.0.1", PORT)
    socket.write("GET /hold?seconds=1&key=left HTTP/1.1\r\nhost: localhost\r\n\r\n")
    wait_for { events("left").include?("start") }
    socket.close

    wait_for { events("left").include?("ensure") }
    assert_equal [ "start", "call finished", "ensure" ], events("left"),
                 "the caller must be cancelled, and its cleanup must not run while its call still does"
    assert_equal "42", get("/value")
  end

  private

  def get(path)
    response = http_get("http://127.0.0.1:#{PORT}#{path}")
    assert_equal "200", response.code, response.body
    response.body
  end

  def state
    JSON.parse(get("/state"))
  end

  def events(key)
    state.fetch("events").fetch(key, [])
  end

  def concurrently(count, &request)
    Array.new(count) { Thread.new(&request) }.each(&:join)
  end

  def wait_for(timeout: 5)
    deadline = clock + timeout
    until yield
      flunk "timed out waiting" if clock > deadline
      sleep 0.05
    end
  end
end
