# frozen_string_literal: true

require "net/http"
require "open3"
require_relative "test_helper"

# The built gem installed the way a user would, serving from a directory
# outside the checkout: what separates a gem that carries everything it needs
# from one that only works where it was built.
class PackagedGemTest < E2ETestCase
  PORT = Integer(ENV.fetch("RUVOY_TEST_PORT", "19260"), 10)
  SLOW_REQUESTS = 8
  SERVER_LOCK = Mutex.new

  class << self
    def gem_file
      ENV.fetch("RUVOY_GEM") { newest("ruvoy-[0-9]*.gem") }
    end

    def envoy_gem
      ENV.fetch("RUVOY_ENVOY_GEM") { newest("ruvoy-envoy-*.gem") }
    end

    def gem_home
      @gem_home ||= File.join(scratch_dir, "gems").tap { |home| install(home, gem_file, envoy_gem) }
    end

    # The application gem alone: what a user who brings their own Envoy has.
    def bare_gem_home
      @bare_gem_home ||= File.join(scratch_dir, "alone").tap { |home| install(home, gem_file) }
    end

    def packaged_envoy
      File.join(gem_home, "gems", File.basename(envoy_gem, ".gem"), "exe", "envoy")
    end

    def application_dir
      @application_dir ||= File.join(scratch_dir, "app").tap do |dir|
        FileUtils.mkdir_p(dir)
        FileUtils.cp(File.join(root, "examples", "hello", "config.ru"), dir)
      end
    end

    def server
      SERVER_LOCK.synchronize do
        @server ||= start_server(gem_home, PORT).tap do |server|
          next if server.serving?(url(PORT))

          server.stop
          raise "the packaged gem never served a request:\n#{File.read(server.log)}"
        end
      end
    end

    def stop_server
      SERVER_LOCK.synchronize do
        @server&.stop
        @server = nil
      end
    end

    def start_server(home, port, env: {})
      Server.start(File.join(home, "bin", "ruvoy"), "config.ru", "--port", port.to_s,
                   env: { "GEM_HOME" => home, "GEM_PATH" => home }.merge(env),
                   chdir: application_dir, log: File.join(scratch_dir, "server-#{port}.log"))
            .tap { |server| Minitest.after_run { server.stop } }
    end

    def url(port)
      "http://127.0.0.1:#{port}"
    end

    private

    def newest(glob)
      Dir.glob(File.join(root, glob)).max_by { |file| File.mtime(file) } or raise "no #{glob} to test"
    end

    def install(home, *gems)
      Bundler.with_unbundled_env do
        system(File.join(RbConfig::CONFIG.fetch("bindir"), "gem"), "install", "--no-document",
               "--install-dir", home, *gems, out: File::NULL) or raise "gem install failed"
      end
    end
  end

  def test_a_request_is_served_from_outside_the_checkout
    assert_includes get("/").body, "hello from ruby"
  end

  def test_the_embedded_ruby_is_the_one_the_gem_was_built_for
    assert_includes get("/").body, RUBY_VERSION
  end

  def test_a_request_body_reaches_the_application
    assert_includes Net::HTTP.post(URI("#{base}/echo"), "payload").body, "payload"
  end

  def test_an_unknown_path_answers_404
    assert_equal "404", get("/nope").code
  end

  # The claim this project rests on: waiting requests overlap rather than queue.
  def test_slow_requests_overlap_instead_of_queueing
    self.class.server
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    Array.new(SLOW_REQUESTS) { Thread.new { get("/slow") } }.each(&:value)
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 4
  end

  # Envoy ships separately and installing it is optional, so the command line
  # has to work against one the machine already has.
  def test_without_the_envoy_gem_the_command_line_says_how_to_supply_one
    home = self.class.bare_gem_home
    output, = Bundler.with_unbundled_env do
      Open3.capture2e({ "GEM_HOME" => home, "GEM_PATH" => home },
                      File.join(home, "bin", "ruvoy"), "config.ru", "--port", PORT.to_s,
                      chdir: self.class.application_dir)
    end
    assert_includes output, "install the ruvoy-envoy gem"
  end

  # Envoys on one host share hot-restart state, so this one runs alone.
  def test_an_envoy_supplied_by_the_machine_is_accepted
    self.class.stop_server
    server = self.class.start_server(self.class.bare_gem_home, PORT,
                                     env: { "RUVOY_ENVOY" => self.class.packaged_envoy })
    assert server.serving?(self.class.url(PORT)), File.read(server.log)
  ensure
    server&.stop
  end

  private

  def base
    self.class.server
    self.class.url(PORT)
  end

  def get(path)
    Net::HTTP.get_response(URI("#{base}#{path}"))
  end
end
