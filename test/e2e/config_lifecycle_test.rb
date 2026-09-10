# frozen_string_literal: true

require_relative "test_helper"

# What happens to the Ruby runtime while Envoy's configuration changes
# underneath it.
#
# Listeners arrive over a filesystem LDS subscription, which Envoy reloads when
# the file is replaced, so this drives real configuration updates rather than
# restarting the process. The module is registered without `do_not_close`, so
# removing the last listener that references it is what would unload it.
#
# That step only carries weight where `dlclose` actually unloads: macOS keeps
# libraries resident regardless, so the run records which kind of platform it
# ran on rather than implying the check was meaningful everywhere.
class ConfigLifecycleTest < E2ETestCase
  RACK_PORT = 19220
  SECOND_PORT = 19221
  ADMIN_PORT = 19222
  REJECTED = "listener_manager.lds.update_rejected"

  def test_runtime_outlives_every_configuration_change
    @results = File.join(result_dir, "config-lifecycle-#{run_id}")
    FileUtils.mkdir_p(@results)
    build_module("fiber", log: File.join(@results, "build.log"))
    @lds = File.join(scratch_dir, "lds.yaml")
    @envoy_log = File.join(@results, "envoy.log")
    secondary_rackup = File.join(root, "bench", "config.ru")

    apply_listeners(1, rack_listener("ruvoy_primary", RACK_PORT, fixture_rackup))
    @envoy = Envoy.start(config: bootstrap, modules: module_dir, log: @envoy_log, log_level: "info",
                         env: bundle_env.merge("RUVOY_DIAGNOSTICS" => "1"))
    assert wait_for_status(url(RACK_PORT), "200"), "the primary listener never became ready:\n#{tail(@envoy_log, lines: 20)}"

    # 1. The runtime starts once and identifies its Ruby thread.
    first = probe(RACK_PORT, "first")
    runtime_thread = first["x-ruby-thread-object-id"]
    first_calls = first["x-ruby-call-count"].to_i
    refute_nil runtime_thread, "the primary listener did not report a Ruby thread"

    # 2. A second listener on the same rackup reuses the same VM.
    apply_listeners(2, rack_listener("ruvoy_primary", RACK_PORT, fixture_rackup) +
                       rack_listener("ruvoy_secondary", SECOND_PORT, fixture_rackup))
    assert wait_for_status(url(SECOND_PORT), "200"), "the added listener never became ready"
    second = probe(SECOND_PORT, "second")
    assert_equal runtime_thread, second["x-ruby-thread-object-id"], "the added listener reported a different Ruby thread"
    assert_operator second["x-ruby-call-count"].to_i, :>, first_calls, "the call counter restarted, so the VM was replaced"

    # 3. An Envoy-side update leaves the runtime untouched.
    apply_listeners(3, rack_listener("ruvoy_primary", RACK_PORT, fixture_rackup) +
                       static_listener("ruvoy_secondary", SECOND_PORT, "envoy-only"))
    assert wait_for_status("http://127.0.0.1:#{SECOND_PORT}/", "200"), "the reconfigured listener never became ready"
    assert_equal runtime_thread, probe(RACK_PORT, "third")["x-ruby-thread-object-id"], "the VM changed while a listener was reconfigured"

    # 4. A second rackup is refused, and the running configuration keeps serving.
    rejected_before = admin_stat(ADMIN_PORT, REJECTED).to_i
    apply_listeners(4, rack_listener("ruvoy_primary", RACK_PORT, fixture_rackup) +
                       rack_listener("ruvoy_secondary", SECOND_PORT, secondary_rackup))
    100.times do
      break if admin_stat(ADMIN_PORT, REJECTED).to_i > rejected_before

      sleep 0.1
    end
    assert_operator admin_stat(ADMIN_PORT, REJECTED).to_i, :>, rejected_before, "Envoy accepted a second rackup"
    assert_equal runtime_thread, probe(RACK_PORT, "fourth")["x-ruby-thread-object-id"], "the rejected update disturbed the running configuration"
    assert_includes File.read(@envoy_log), secondary_rackup, "the rejection does not say which rackup was refused"

    # 5. Removing every Ruby listener must not unload the module.
    puts "NOTE: #{unload_observable? ? "dlclose unloads here, so this is a real check" : "this platform keeps libraries resident, so this check is vacuous"}"
    apply_listeners(5, static_listener("ruvoy_secondary", SECOND_PORT, "envoy-only"))
    assert wait_for_absence(url(RACK_PORT)), "the Ruby listener was still accepting connections"
    assert_predicate @envoy, :alive?, "Envoy died when the module lost its last reference"

    # 6. The VM is still the one from step 1.
    apply_listeners(6, rack_listener("ruvoy_primary", RACK_PORT, fixture_rackup) +
                       static_listener("ruvoy_secondary", SECOND_PORT, "envoy-only"))
    assert wait_for_status(url(RACK_PORT), "200"), "the Ruby listener did not come back"
    assert_equal runtime_thread, probe(RACK_PORT, "sixth")["x-ruby-thread-object-id"], "a new VM was created after the module lost every reference"

    # 7. The module reports itself through Envoy's statistics.
    sleep 1.5
    metrics = http_get("http://127.0.0.1:#{ADMIN_PORT}/stats").body.lines.grep(/dynamicmodules/i)
    File.write(File.join(@results, "stats.txt"), metrics.join)
    assert metrics.any? { |line| line.include?("requests_total") }, "the module's statistics are missing from Envoy"
    assert metrics.any? { |line| line.match?(/responses_total.*outcome.*completed/) }, "no completed responses were counted"
    assert metrics.any? { |line| line.match?(/duration_ms.*P50/) }, "the duration histogram recorded nothing"
    assert metrics.any? { |line| line.match?(/inflight_requests: 0$/) }, "the saturation gauge did not return to zero"
    # The reactor waits with a deadline, so an unblocked runtime always reports
    # a small age here; a blocked one would grow without bound.
    reactor_idle = metrics.find { |line| line.include?("reactor_idle_ms") }&.split(": ", 2)&.last&.to_i
    refute_nil reactor_idle, "the reactor reported no idle age"
    assert_operator reactor_idle, :<, 1000, "the reactor looked stalled: #{reactor_idle} ms"

    # 8. One VM for the whole run, and no crashes.
    text = File.read(@envoy_log)
    assert_equal 1, text.scan("[ruvoy] Fiber runtime started").size, "the runtime started more than once"
    refute_match(/panic|segmentation fault|SIGSEGV/i, text, "the run produced a crash signature")
    puts "raw results: #{@results}"
  ensure
    @envoy&.stop
    if @results
      File.write(File.join(@results, "environment.txt"),
                 "run_id=#{run_id}\nhost=#{IO.popen(%w[uname -srm], &:read).strip}\nmodule_unload_observable=#{unload_observable? ? "yes" : "no"}\n")
    end
  end

  private

  def url(port)
    "http://127.0.0.1:#{port}/hello"
  end

  def unload_observable?
    RbConfig::CONFIG["host_os"].include?("linux")
  end

  def bootstrap
    File.join(scratch_dir, "bootstrap.yaml").tap do |path|
      File.write(path, <<~YAML)
        node: { id: ruvoy-config-lifecycle, cluster: ruvoy }
        # Histograms only publish on flush; a short interval keeps the run quick.
        stats_flush_interval: 1s
        admin:
          address:
            socket_address: { address: 127.0.0.1, port_value: #{ADMIN_PORT} }
        dynamic_resources:
          lds_config:
            resource_api_version: V3
            path_config_source:
              path: #{@lds}
      YAML
    end
  end

  # Envoy watches the path for moves, so the new file is staged beside it and
  # renamed into place.
  def apply_listeners(version, body)
    staged = "#{@lds}.next"
    File.write(staged, "version_info: \"#{version}\"\nresources:\n#{body}")
    File.rename(staged, @lds)
    FileUtils.cp(@lds, File.join(@results, "lds-#{version}.yaml"))
  end

  def rack_listener(name, port, rackup)
    <<~YAML
        - "@type": type.googleapis.com/envoy.config.listener.v3.Listener
          name: #{name}
          address:
            socket_address: { address: 127.0.0.1, port_value: #{port} }
          filter_chains:
            - filters:
                - name: envoy.filters.network.http_connection_manager
                  typed_config:
                    "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                    stat_prefix: #{name}
                    codec_type: AUTO
                    route_config:
                      name: #{name}_route
                      virtual_hosts:
                        - name: #{name}
                          domains: ["*"]
                    http_filters:
                      - name: envoy.extensions.filters.http.dynamic_modules
                        typed_config:
                          "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
                          dynamic_module_config:
                            name: ruvoy_fiber
                          filter_name: fiber_rack
                          terminal_filter: true
                          filter_config:
                            "@type": type.googleapis.com/google.protobuf.StringValue
                            value: '{"rackup": "#{rackup}"}'
    YAML
  end

  def static_listener(name, port, body)
    <<~YAML
        - "@type": type.googleapis.com/envoy.config.listener.v3.Listener
          name: #{name}
          address:
            socket_address: { address: 127.0.0.1, port_value: #{port} }
          filter_chains:
            - filters:
                - name: envoy.filters.network.http_connection_manager
                  typed_config:
                    "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                    stat_prefix: #{name}
                    codec_type: AUTO
                    route_config:
                      name: #{name}_route
                      virtual_hosts:
                        - name: #{name}
                          domains: ["*"]
                          routes:
                            - match: { prefix: "/" }
                              direct_response: { status: 200, body: { inline_string: "#{body}" } }
                    http_filters:
                      - name: envoy.filters.http.router
                        typed_config:
                          "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
    YAML
  end

  def probe(port, name)
    response = http_get(url(port))
    File.write(File.join(@results, "#{name}.headers"), response.each_header.map { |key, value| "#{key}: #{value}\n" }.join)
    response
  end

  def status_of(target)
    uri = URI(target)
    Net::HTTP.start(uri.host, uri.port, open_timeout: 2, read_timeout: 2) { |http| http.get(uri.request_uri).code }
  rescue SystemCallError, IOError, Net::OpenTimeout, Net::ReadTimeout
    nil
  end

  # A step never asserts against a configuration Envoy has not applied yet.
  def wait_for_status(target, expected)
    observed = nil
    100.times do
      observed = status_of(target)
      return true if observed == expected
      return false unless @envoy.alive?

      sleep 0.1
    end
    warn "last status for #{target} was #{observed.inspect}, wanted #{expected}"
    false
  end

  def wait_for_absence(target)
    100.times do
      return true if status_of(target).nil?
      return false unless @envoy.alive?

      sleep 0.1
    end
    false
  end
end
