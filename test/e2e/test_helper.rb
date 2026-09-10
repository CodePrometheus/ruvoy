# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../lib", __dir__))

require "bundler"
require "fileutils"
require "minitest/autorun"
require "net/http"
require "rbconfig"
require "socket"
require "tmpdir"
require "uri"
require_relative "support/envoy"
require_relative "support/throttled_upload"

# What every end-to-end suite needs: where the checkout is, where evidence
# goes, and which build of the module it exercises.
class E2ETestCase < Minitest::Test
  ROOT = File.expand_path("../..", __dir__)
  BUILD_PROFILES = %w[debug release].freeze

  # Available to the suites and to their class-level fixtures alike.
  module Helpers
    def root
      ROOT
    end

    def module_dir
      File.join(ROOT, "build", "modules")
    end

    def fixture_rackup
      File.join(ROOT, "test", "fixtures", "rack", "config.ru")
    end

    def build_profile
      ENV.fetch("RUVOY_BUILD_PROFILE", "release").tap do |profile|
        raise ArgumentError, "RUVOY_BUILD_PROFILE must be debug or release" unless BUILD_PROFILES.include?(profile)
      end
    end

    # Whatever a suite launches loads gems from the bundle the suite runs under.
    def bundle_env
      { "BUNDLE_GEMFILE" => File.join(ROOT, "Gemfile"),
        "BUNDLE_PATH" => File.join(ROOT, "vendor", "bundle"),
        "BUNDLE_FROZEN" => "true" }
    end

    def lockfile
      Bundler::LockfileParser.new(File.read(File.join(ROOT, "Gemfile.lock")))
    end

    def locked_version(gem_name)
      spec = lockfile.specs.find { |candidate| candidate.name == gem_name }
      raise ArgumentError, "gem #{gem_name} is not in Gemfile.lock" unless spec

      spec.version.to_s
    end

    def bundler_version
      lockfile.bundler_version.to_s
    end

    # The scripts take the Ruby and the profile from the environment.
    def run_script(script, log:)
      env = { "RUVOY_RUBY" => RbConfig.ruby, "RUVOY_BUILD_PROFILE" => build_profile }
      ran = Bundler.with_unbundled_env do
        system(env, File.join(ROOT, "scripts", script), out: [ log, "a" ], err: [ :child, :out ], chdir: ROOT)
      end
      raise "#{script} failed; see #{log}" unless ran
    end

    def build_module(script, log:)
      run_script(script, log: log)
    end

    def admin_stat(port, name)
      filter = URI.encode_www_form_component("^#{name}$")
      http_get("http://127.0.0.1:#{port}/stats?filter=#{filter}").body.lines.first&.split(": ", 2)&.last&.strip
    end

    def clock
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def elapsed
      started = clock
      yield
      clock - started
    end

    def port_free?(port)
      TCPServer.new("127.0.0.1", port).close
      true
    rescue Errno::EADDRINUSE
      false
    end

    def http_get(url, headers = {})
      uri = URI(url)
      Net::HTTP.start(uri.host, uri.port) { |http| http.get(uri.request_uri, headers) }
    end

    def http_post(url, body, headers = {})
      uri = URI(url)
      Net::HTTP.start(uri.host, uri.port) { |http| http.post(uri.request_uri, body, headers) }
    end

    def result_dir
      ENV.fetch("RUVOY_RESULTS_DIR") { File.join(ROOT, ".agents", "results") }
         .tap { |directory| FileUtils.mkdir_p(directory) }
    end

    def tail(path, lines: 100)
      File.exist?(path) ? File.readlines(path).last(lines).join : ""
    end
  end

  include Helpers
  extend Helpers

  def self.run_id
    @run_id ||= Time.now.utc.strftime("%Y%m%dT%H%M%SZ")
  end

  # Somewhere the whole suite may write, gone once the run ends unless
  # RUVOY_KEEP_SCRATCH asks for it to stay behind for inspection.
  def self.scratch_dir
    @scratch_dir ||= Dir.mktmpdir("ruvoy-e2e-").tap do |directory|
      Minitest.after_run { FileUtils.rm_rf(directory) unless ENV["RUVOY_KEEP_SCRATCH"] }
    end
  end

  def run_id
    self.class.run_id
  end

  def scratch_dir
    self.class.scratch_dir
  end

  def with_env(overrides)
    saved = overrides.to_h { |key, _| [ key, ENV[key] ] }
    overrides.each { |key, value| ENV[key] = value }
    yield
  ensure
    saved.each { |key, value| ENV[key] = value }
  end

  def assert_port_free(port)
    assert port_free?(port), "TCP port #{port} is already in use"
  end
end
