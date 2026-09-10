# frozen_string_literal: true

require_relative "test_helper"

# One Rack::Lint application served by the fiber runtime and by Puma: whatever
# the application can observe has to look the same from both.
class RackCompatibilityTest < E2ETestCase
  FIBER_PORT = 19183
  CONTROL_PORT = 19184
  PUMA_PORT = 19210
  READY_TIMEOUT = 15

  class << self
    def ruvoy
      @ruvoy ||= start_ruvoy
    end

    def puma
      @puma ||= start_puma
    end

    private

    def start_ruvoy
      [ FIBER_PORT, CONTROL_PORT ].each { |port| raise "TCP port #{port} is already in use" unless port_free?(port) }
      config = File.join(scratch_dir, "envoy.yaml")
      template = File.read(File.join(root, "config", "envoy-fiber-rack.yaml"))
      File.write(config, template.sub("value: bench/config.ru", "value: #{fixture_rackup}"))
      raise "failed to configure the Rack fixture" unless File.read(config).include?("value: #{fixture_rackup}")

      build_module("fiber", log: File.join(scratch_dir, "build.log"))
      log = File.join(scratch_dir, "envoy.log")
      Envoy.start(config: config, modules: module_dir, log: log, env: bundle_env).tap do |envoy|
        Minitest.after_run { envoy.stop }
        raise "Ruvoy did not become ready:\n#{tail(log)}" unless envoy.serving?(base_url(FIBER_PORT) + "/closed",
                                                                                   timeout: READY_TIMEOUT)
      end
    end

    def start_puma
      raise "TCP port #{PUMA_PORT} is already in use" unless port_free?(PUMA_PORT)

      ruby_dir = File.dirname(RbConfig.ruby)
      log = File.join(scratch_dir, "puma.log")
      Server.start(RbConfig.ruby, File.join(ruby_dir, "bundle"), "_#{bundler_version}_", "exec", "puma",
                   "--no-config", "--environment", "test", "--threads", "0:1", "--workers", "0",
                   "--bind", "tcp://127.0.0.1:#{PUMA_PORT}", fixture_rackup,
                   env: bundle_env.merge("PATH" => "#{ruby_dir}:#{ENV.fetch("PATH")}"),
                   chdir: root, log: log).tap do |puma|
        Minitest.after_run { puma.stop }
        raise "Puma did not become ready:\n#{tail(log)}" unless puma.serving?(base_url(PUMA_PORT) + "/closed",
                                                                               timeout: READY_TIMEOUT)
      end
    end

    def base_url(port)
      "http://127.0.0.1:#{port}"
    end
  end

  def test_ruvoy_serves_the_lint_application
    self.class.ruvoy
    assert_rack_semantics "ruvoy", FIBER_PORT
  end

  def test_puma_serves_the_lint_application
    self.class.puma
    assert_rack_semantics "puma", PUMA_PORT
  end

  # Fibers interleave app.call, so the fiber runtime must not claim exclusivity.
  def test_ruvoy_reports_multithread
    self.class.ruvoy
    assert_includes http_get(url(FIBER_PORT, "/rack-env")).body.lines(chomp: true), "rack.multithread=true"
  end

  private

  def url(port, path)
    "http://127.0.0.1:#{port}#{path}"
  end

  def assert_rack_semantics(name, port)
    env = http_get(url(port, "/rack-env?probe=yes"), "x-ruvoy-test" => "shared-rack-app")
    assert_equal "200", env.code, "#{name} Rack env returned HTTP #{env.code}"
    lines = env.body.lines(chomp: true)
    { "REQUEST_METHOD" => '"GET"', "PATH_INFO" => '"/rack-env"', "QUERY_STRING" => '"probe=yes"',
      "SERVER_PORT" => %("#{port}"), "SERVER_PROTOCOL" => '"HTTP/1.1"', "rack.url_scheme" => '"http"',
      "HTTP_X_RUVOY_TEST" => '"shared-rack-app"' }.each do |key, value|
      assert_includes lines, "#{key}=#{value}", "#{name} did not provide #{key}"
    end
    assert_equal "active", env["x-rack-middleware"], "#{name} did not execute Rack middleware"

    echo = http_post(url(port, "/echo"), "rack-input")
    assert_equal "200", echo.code, "#{name} echo returned HTTP #{echo.code}"
    assert_equal "rack-input", echo.body, "#{name} did not preserve rack.input"

    binary = "\x00\xFFrack\x80".b
    echoed = http_post(url(port, "/echo"), binary, "content-type" => "application/octet-stream")
    assert_equal "200", echoed.code, "#{name} binary echo returned HTTP #{echoed.code}"
    assert_equal binary, echoed.body.b, "#{name} did not preserve binary request and response bytes"

    enumerable = http_get(url(port, "/enumerable"))
    assert_equal "200", enumerable.code, "#{name} enumerable body returned HTTP #{enumerable.code}"
    assert_equal "rack-body", enumerable.body, "#{name} did not consume every enumerable body chunk"
    assert_equal 2, enumerable.get_fields("set-cookie")&.size, "#{name} did not preserve multi-value response headers"

    closed = http_get(url(port, "/closed"))
    assert_equal "200", closed.code, "#{name} close probe returned HTTP #{closed.code}"
    assert_equal "1", closed.body, "#{name} did not close the enumerable response body exactly once"
  end
end
