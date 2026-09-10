# frozen_string_literal: true

module Bench
  # A process the campaign started, reaped exactly once so its exit status is
  # still there to read after it has gone.
  class Child
    POLL = 0.05

    attr_reader :pid, :status

    def self.spawn(env, *command, out:, err: %i[child out])
      options = { chdir: ROOT, out: out }
      options[:err] = err if err
      new(Bench.unbundled { Process.spawn(env, *command, **options) })
    end

    def initialize(pid)
      @pid = pid
      @status = nil
    end

    def alive?
      return false if @status

      reaped = Process.waitpid2(@pid, Process::WNOHANG)
      @status = reaped.last if reaped
      @status.nil?
    rescue Errno::ECHILD
      false
    end

    def exits_within?(seconds)
      deadline = Bench.clock + seconds
      while alive?
        return false if Bench.clock > deadline

        sleep POLL
      end
      true
    end

    def signal(name)
      Process.kill(name, @pid) if alive?
    rescue Errno::ESRCH
      nil
    end

    def succeeded?
      !alive? && @status&.success?
    end

    # uvx runs the proxy as its child; Puma and Falcon fork their workers.
    def children
      IO.popen([ "pgrep", "-P", @pid.to_s ], &:read).split.map(&:to_i)
    end
  end

  # Processes the campaign did not start itself, such as the proxy uvx runs.
  module Pids
    def self.running?(pid)
      Process.kill(0, pid)
      state = IO.popen([ "ps", "-o", "stat=", "-p", pid.to_s ], err: File::NULL, &:read).strip
      !state.empty? && !state.start_with?("Z")
    rescue Errno::ESRCH
      false
    rescue Errno::EPERM
      true
    end

    def self.exits_within?(pid, seconds)
      return true unless pid

      deadline = Bench.clock + seconds
      while running?(pid)
        return false if Bench.clock > deadline

        sleep Child::POLL
      end
      true
    end
  end

  # The processes behind one measured architecture, stopped as a unit.
  class Servers
    GRACE = 5
    FINAL = 1

    attr_accessor :envoy, :envoy_log, :runtime_pid, :puma, :puma_workers, :falcon, :falcon_workers, :port

    # What a round's CPU and memory figures are taken from: the proxy plus the
    # Puma it forwards to, or the direct server with its workers.
    def measured_pids
      puma = @puma ? [ @puma.pid, *@puma_workers ] : []
      return [ @runtime_pid, *puma ] if @runtime_pid
      return puma if @puma

      [ @falcon.pid, *@falcon_workers ]
    end

    def stop
      [ stop_envoy, stop_server(@puma), stop_server(@falcon) ].all?
    end

    private

    def stop_envoy
      return true unless @envoy

      @envoy.signal("INT")
      %w[TERM KILL].each do |escalation|
        break if envoy_exits_within?(GRACE)

        signal_runtime(escalation)
        @envoy.signal(escalation)
      end
      envoy_exits_within?(FINAL) && @envoy.succeeded?
    end

    def envoy_exits_within?(seconds)
      @envoy.exits_within?(seconds) && Pids.exits_within?(@runtime_pid, seconds)
    end

    def signal_runtime(name)
      Process.kill(name, @runtime_pid) if @runtime_pid && Pids.running?(@runtime_pid)
    rescue Errno::ESRCH
      nil
    end

    def stop_server(child)
      return true unless child

      child.signal("INT")
      %w[TERM KILL].each do |escalation|
        break if child.exits_within?(GRACE)

        child.signal(escalation)
      end
      child.exits_within?(FINAL) && child.succeeded?
    end
  end

  # Resident memory of the measured processes, sampled while a round runs.
  class RssSampler
    INTERVAL = 0.1

    def self.start(pids, path)
      new(pids, path).tap(&:start)
    end

    def initialize(pids, path)
      @pids = pids
      @path = path
      @samples = []
    end

    def start
      @running = true
      @thread = Thread.new do
        File.open(@path, "w") do |file|
          file.puts "sample,rss_kib"
          while @running
            rss = IO.popen([ "ps", "-o", "rss=", "-p", @pids.join(",") ], err: File::NULL, &:read).split.map(&:to_i)
            break if rss.empty?

            file.puts "#{@samples.size},#{rss.sum}"
            @samples << rss.sum
            sleep INTERVAL
          end
        end
      end
    end

    def stop
      @running = false
      @thread.join
    end

    def maximum
      @samples.max || 0
    end
  end
end
