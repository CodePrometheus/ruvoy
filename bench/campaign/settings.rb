# frozen_string_literal: true

require "rbconfig"

module Bench
  Failure = Class.new(StandardError)

  ROOT = File.expand_path("../..", __dir__)

  def self.clock
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  # Servers start from the environment the campaign itself started with, so a
  # bundle the runner happens to be inside never reaches one.
  def self.unbundled(&block)
    defined?(Bundler) ? Bundler.with_unbundled_env(&block) : yield
  end

  # Every knob the campaign reads, validated before anything runs. Full mode adds
  # the guards that keep a measurement from measuring the machine instead: a
  # remote load generator, windows long enough to mean something, an idle host
  # and a clean worktree.
  class Settings
    ARCHITECTURES = %w[baseline sync fiber envoy_puma puma_direct falcon_direct].freeze
    ORDER_MODES = %w[rotate alternate forward reverse].freeze
    BODY_MATRICES = %w[false true request response].freeze
    TLS_MODES = { "both" => %w[plain tls], "plain" => %w[plain], "tls" => %w[tls] }.freeze
    DURATION = /\A[1-9]\d*(ms|s|m)\z/
    LOOPBACK = /\A(127\.|localhost\z|::1\z|0\.0\.0\.0\z)/

    DEFAULTS = {
      mode: "full",
      duration: "30s",
      warmup_duration: "10s",
      rounds: "7",
      warmup_concurrency: "10",
      oha_timeout_seconds: "180",
      envoy_concurrency: "1",
      puma_workers: "0",
      puma_threads: "100",
      # Falcon defaults to 11 instances; the comparison needs a single process so
      # it matches Ruvoy's one Ruby runtime and direct Puma's single worker.
      falcon_count: "1",
      # baseline and sync stay available as diagnostics but must not dilute the
      # application-server comparison, so they are not selected by default.
      architectures: "fiber falcon_direct puma_direct envoy_puma",
      order_mode: "rotate",
      scenarios: "all",
      body_matrix: "false",
      # Ruvoy's premise is that Envoy already terminates TLS, so a plain-only
      # benchmark systematically understates it: the direct servers would never
      # pay for a handshake.
      tls: "both",
      load_generator: "remote",
      load_generator_ssh: "",
      load_generator_oha: "oha",
      listen_address: "127.0.0.1",
      target_address: "127.0.0.1",
      allow_dirty: "0",
      # A fresh round should not inherit the previous one's GC state, page cache
      # or thermal condition.
      cooldown_seconds: "5",
      # Above this the load generator itself is the bottleneck and the round
      # measures the generator rather than the server.
      load_generator_cpu_limit: "80",
      idle_load_limit: "1.0"
    }.freeze

    # Smoke runs prove the harness works end to end; they measure nothing.
    SMOKE = { duration: "250ms", warmup_duration: "100ms", rounds: 1, warmup_concurrency: 1 }.freeze

    attr_reader :run_id, :result_dir, :ruby, :bundle, :mode, :duration, :warmup_duration, :rounds,
                :warmup_concurrency, :control_repetitions, :oha_timeout_seconds, :envoy_concurrency,
                :puma_workers, :puma_threads, :falcon_count, :architectures, :architecture_selection,
                :order_mode, :scenario_selection, :body_matrix, :tls_selection, :tls_modes, :load_generator,
                :load_generator_ssh, :load_generator_oha, :listen_address, :target_address, :probe_address,
                :cooldown_seconds, :load_generator_cpu_limit, :idle_load_limit

    def self.milliseconds(duration)
      amount = duration.to_i
      return amount if duration.end_with?("ms")

      duration.end_with?("m") ? amount * 60_000 : amount * 1000
    end

    def initialize(env)
      @env = env
      @run_id = Time.now.utc.strftime("%Y%m%dT%H%M%SZ")
      @result_dir = File.join(env.fetch("RUVOY_RESULTS_DIR") { File.join(ROOT, ".agents", "results") }, "benchmark-#{@run_id}")
      @ruby = env["RUVOY_RUBY"]
      @bundle = env["RUVOY_BUNDLE"]
    end

    def validate!
      read_choices
      read_numbers
      guard_full_mode if full?
      raise Failure, "remote load generation requires RUVOY_BENCH_LOAD_GENERATOR_SSH" if remote? && @load_generator_ssh.empty?

      self
    end

    def full? = @mode == "full"

    def smoke? = @mode == "smoke"

    def remote? = @load_generator == "remote"

    def scenario_selected?(scenario)
      @scenario_selection == "all" || @scenario_selection.split.include?(scenario)
    end

    private

    def value(key)
      @env.fetch("RUVOY_BENCH_#{key.upcase}", DEFAULTS.fetch(key)).to_s
    end

    def choice(key, allowed)
      value(key).tap do |chosen|
        raise Failure, "RUVOY_BENCH_#{key.upcase} must be one of #{allowed.join(", ")}" unless allowed.include?(chosen)
      end
    end

    def integer(key)
      Integer(value(key), 10)
    rescue ArgumentError
      raise Failure, "RUVOY_BENCH_#{key.upcase} must be an integer"
    end

    def positive(key)
      integer(key).tap { |number| raise Failure, "RUVOY_BENCH_#{key.upcase} must be a positive integer" unless number.positive? }
    end

    def number(key)
      Float(value(key))
    rescue ArgumentError
      raise Failure, "RUVOY_BENCH_#{key.upcase} must be a number"
    end

    def read_choices
      @mode = choice(:mode, %w[full smoke])
      @body_matrix = choice(:body_matrix, BODY_MATRICES)
      raise Failure, "smoke mode does not allow RUVOY_BENCH_BODY_MATRIX" if smoke? && @body_matrix != "false"

      @tls_selection = choice(:tls, TLS_MODES.keys)
      @tls_modes = TLS_MODES.fetch(@tls_selection)
      @order_mode = choice(:order_mode, ORDER_MODES)
      @architecture_selection = value(:architectures)
      @architectures = @architecture_selection.split
      raise Failure, "RUVOY_BENCH_ARCHITECTURES must select at least one architecture" if @architectures.empty?

      unknown = @architectures - ARCHITECTURES
      raise Failure, "unknown RUVOY_BENCH_ARCHITECTURES entry: #{unknown.first}" if unknown.any?
      raise Failure, "RUVOY_BENCH_ARCHITECTURES must not contain duplicates" unless @architectures.uniq == @architectures

      @scenario_selection = value(:scenarios)
      @load_generator = choice(:load_generator, %w[local remote])
      @load_generator_ssh = value(:load_generator_ssh)
      @load_generator_oha = value(:load_generator_oha)
      @listen_address = value(:listen_address)
      @target_address = value(:target_address)
      # Readiness and contract checks run from this host rather than from the
      # load generator, so they need an address reachable from here.
      @probe_address = @listen_address == "0.0.0.0" ? "127.0.0.1" : @listen_address
    end

    def read_numbers
      smoke = smoke? ? SMOKE : {}
      @duration = smoke.fetch(:duration) { value(:duration) }
      @warmup_duration = smoke.fetch(:warmup_duration) { value(:warmup_duration) }
      { "RUVOY_BENCH_DURATION" => @duration, "RUVOY_BENCH_WARMUP_DURATION" => @warmup_duration }.each do |name, duration|
        raise Failure, "#{name} must use a positive ms, s, or m duration" unless duration.match?(DURATION)
      end
      @rounds = smoke.fetch(:rounds) { positive(:rounds) }
      @warmup_concurrency = smoke.fetch(:warmup_concurrency) { positive(:warmup_concurrency) }
      @control_repetitions = smoke? ? 1 : @rounds
      @oha_timeout_seconds = positive(:oha_timeout_seconds)
      @envoy_concurrency = positive(:envoy_concurrency)
      @puma_threads = positive(:puma_threads)
      @falcon_count = positive(:falcon_count)
      @puma_workers = integer(:puma_workers)
      raise Failure, "RUVOY_BENCH_PUMA_WORKERS must be a non-negative integer" if @puma_workers.negative?

      @cooldown_seconds = integer(:cooldown_seconds)
      @load_generator_cpu_limit = number(:load_generator_cpu_limit)
      @idle_load_limit = number(:idle_load_limit)
    end

    def guard_full_mode
      raise Failure, "full mode is forbidden on macOS; run it on an isolated Linux machine" if RbConfig::CONFIG["host_os"].include?("darwin")
      raise Failure, "full mode requires RUVOY_ALLOW_HIGH_LOAD=1" unless @env["RUVOY_ALLOW_HIGH_LOAD"] == "1"
      raise Failure, "full mode requires RUVOY_BENCH_ROUNDS to be at least 7" if @rounds < 7
      # oha competing with the servers for the same cores makes every number a
      # measurement of the machine, not of the architectures under test.
      raise Failure, "full mode requires RUVOY_BENCH_LOAD_GENERATOR=remote" unless remote?
      raise Failure, "full mode requires RUVOY_BENCH_LOAD_GENERATOR_SSH" if @load_generator_ssh.empty?
      raise Failure, "full mode requires a non-loopback RUVOY_BENCH_TARGET_ADDRESS" if @target_address.match?(LOOPBACK)
      raise Failure, "full mode requires RUVOY_BENCH_LISTEN_ADDRESS reachable by the load generator" if @listen_address == "127.0.0.1"
      # The published Linux runs used 2s windows; that is noise, not a measurement.
      raise Failure, "full mode requires RUVOY_BENCH_DURATION of at least 30s" if Settings.milliseconds(@duration) < 30_000
      raise Failure, "full mode requires RUVOY_BENCH_WARMUP_DURATION of at least 10s" if Settings.milliseconds(@warmup_duration) < 10_000
      return if value(:allow_dirty) == "1"
      return if IO.popen([ "git", "-C", ROOT, "status", "--porcelain" ], &:read).empty?

      raise Failure, "full mode requires a clean worktree; set RUVOY_BENCH_ALLOW_DIRTY=1 to override"
    end
  end
end
