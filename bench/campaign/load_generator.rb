# frozen_string_literal: true

require "open3"

module Bench
  # Where oha runs: on this host for smoke runs, over ssh on a separate machine
  # for anything meant to be a measurement.
  class LoadGenerator
    VERSION = "oha 1.15.0"
    # GNU time reports the generator's own CPU use, so saturation is detected
    # without a second connection competing for the same cores.
    TIMING = "ruvoy_load_generator user=%U sys=%S elapsed=%e"

    attr_reader :version, :cores

    def initialize(settings, local_oha:)
      @settings = settings
      @local_oha = local_oha
    end

    def remote? = @settings.remote?

    def check_local
      raise Failure, "missing project-local oha: run `rake tools:oha`" unless File.executable?(@local_oha)

      @version = IO.popen([ @local_oha, "--version" ], &:read).strip
      raise Failure, "expected #{VERSION}, got: #{@version}" unless @version == VERSION
    end

    def prepare(result_dir)
      @result_dir = result_dir
      return unless remote?

      @version = ssh!("#{quote(@settings.load_generator_oha)} --version", "failed to run oha on #{host}").strip
      raise Failure, "load generator must provide #{VERSION}, got: #{@version}" unless @version == VERSION

      cores = ssh!("nproc", "failed to read the CPU count from #{host}").strip
      raise Failure, "load generator reported an unusable CPU count: #{cores}" unless cores.match?(/\A[1-9]\d*\z/)

      @cores = cores.to_i
      @remote_dir = ssh!('mktemp -d "${TMPDIR:-/tmp}/ruvoy-bench.XXXXXX"',
                         "failed to create a request-body directory on #{host}").strip
      raise Failure, "load generator returned an empty request-body directory" if @remote_dir.empty?

      copy(Dir.glob(File.join(result_dir, "body-*.bin")).sort, "request bodies")
      ca = File.join(result_dir, "tls", "ca.pem")
      copy([ ca ], "the benchmark CA") if File.exist?(ca)
    end

    # The load generator verifies the benchmark CA instead of skipping
    # validation, so a broken certificate fails loudly rather than silently
    # downgrading.
    def tls_args(tls)
      return [] unless tls

      [ "--cacert", remote? ? "#{@remote_dir}/ca.pem" : File.join(@result_dir, "tls", "ca.pem") ]
    end

    def body_path(bytes)
      name = "body-#{bytes}.bin"
      remote? ? "#{@remote_dir}/#{name}" : File.join(@result_dir, name)
    end

    def start(output, args)
      return Child.spawn({}, @local_oha, *args, out: output, err: nil) unless remote?

      # Benchmark URLs contain '&', so every argument is quoted for the remote
      # login shell.
      command = [ "/usr/bin/time", "-f", TIMING, @settings.load_generator_oha, *args ].map { |argument| quote(argument) }
      Child.spawn({}, "ssh", "-n", "-o", "BatchMode=yes", host, command.join(" "), out: output, err: "#{output}.loadgen")
    end

    # Nil on success, otherwise what went wrong.
    def run(output, args, timeout:)
      child = start(output, args)
      unless child.exits_within?(timeout)
        stop(child)
        return "timed out after #{timeout}s"
      end
      child.succeeded? ? nil : "exited (#{child.status})"
    end

    def stop(child)
      child.signal("TERM")
      child.signal("KILL") unless child.exits_within?(1)
      child.exits_within?(1)
    end

    def check_headroom(timing_file, label)
      return unless remote? && File.size?(timing_file)

      timing = File.read(timing_file).scan(/ruvoy_load_generator (.*)$/).flatten.last.to_s
      values = timing.scan(/(\w+)=([\d.]+)/).to_h.transform_values(&:to_f)
      record = File.join(@result_dir, "load-generator-cpu.tsv")
      elapsed = values.fetch("elapsed", 0.0)
      unless elapsed.positive?
        File.write(record, "#{label}\tunmeasured\t#{@cores}\n", mode: "a")
        return
      end

      usage = (values.fetch("user", 0.0) + values.fetch("sys", 0.0)) * 100 / (elapsed * @cores)
      File.write(record, format("%s\t%.1f\t%d\n", label, usage, @cores), mode: "a")
      limit = @settings.load_generator_cpu_limit
      return if usage <= limit

      raise Failure, format("%s: load generator used %.1f%% of its CPUs (limit %g%%); the measurement is generator-bound",
                            label, usage, limit)
    end

    def describe(target_address)
      return [ "", "[load_generator]", "ssh=local" ] unless remote?

      details = ssh(%q(uname -srm; nproc; awk -F': ' '/model name/ { print $2; exit }' /proc/cpuinfo))
      rtt = begin
        IO.popen([ "ping", "-c", "5", target_address ], err: File::NULL, &:read).lines(chomp: true).last(2)
      rescue SystemCallError
        []
      end
      [ "", "[load_generator]", "ssh=#{host}" ] +
        (details ? details.lines(chomp: true).map { |line| "detail=#{line}" } : [ "detail=unavailable" ]) +
        [ "", "[network]" ] + (rtt.empty? ? [ "rtt=unavailable" ] : rtt.map { |line| "rtt=#{line}" })
    end

    # Only this run's own directory is removed, and only on the load generator.
    def cleanup
      return unless @remote_dir

      ssh("rm -rf -- #{quote(@remote_dir)}")
      @remote_dir = nil
    end

    private

    def host = @settings.load_generator_ssh

    def ssh(command)
      output, status = Open3.capture2("ssh", "-n", "-o", "BatchMode=yes", host, command, err: File::NULL)
      status.success? ? output : nil
    rescue SystemCallError
      nil
    end

    def ssh!(command, failure)
      ssh(command) || raise(Failure, failure)
    end

    def copy(files, what)
      return if system("scp", "-q", *files, "#{host}:#{@remote_dir}/")

      raise Failure, "failed to copy #{what} to #{host}"
    end

    def quote(argument)
      "'#{argument.to_s.gsub("'") { "'\\''" }}'"
    end
  end
end
