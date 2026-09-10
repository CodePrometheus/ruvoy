# frozen_string_literal: true

require_relative "server"

# Envoy started through `uvx`, which runs the proxy as a child rather than
# replacing itself: it forwards SIGINT, but nothing forwards SIGKILL, so a
# stop that has to escalate reaches the proxy directly.
class Envoy < Server
  PACKAGE = "envoy-server==1.39.0"
  GRACE = 10
  KILL_TIMEOUT = 5

  def self.start(config:, modules:, log:, concurrency: 1, log_level: "warning", admin_port: nil,
                 hot_restart: nil, env: {})
    command = [ "uvx", "--from", PACKAGE, "envoy",
                "--config-path", config, "--concurrency", concurrency.to_s, "--log-level", log_level ]
    command += hot_restart ? hot_restart_arguments(hot_restart) : [ "--disable-hot-restart" ]
    if admin_port
      command += [ "--config-yaml",
                   "admin: {address: {socket_address: {address: 127.0.0.1, port_value: #{admin_port}}}}" ]
    end
    super(*command, log: log, env: { "ENVOY_DYNAMIC_MODULES_SEARCH_PATH" => modules }.merge(env))
  end

  def self.hot_restart_arguments(options)
    [ "--restart-epoch", options.fetch(:epoch).to_s, "--base-id", options.fetch(:base_id).to_s,
      "--drain-time-s", options.fetch(:drain_time).to_s,
      "--parent-shutdown-time-s", options.fetch(:parent_shutdown_time).to_s ]
  end
  private_class_method :hot_restart_arguments

  # The proxy itself, which uvx runs as its child.
  def proxy_pid
    100.times do
      pid = IO.popen([ "pgrep", "-P", @pid.to_s ], &:read).split.first
      return pid.to_i if pid

      sleep 0.01
    end
    nil
  end

  # Interrupts the launcher and reports how it exited; whatever outlives the
  # grace period, launcher or proxy, is killed. The proxy is looked up first,
  # since once the launcher is gone it can no longer be found through it.
  def stop(grace: GRACE)
    return unless @pid

    children = IO.popen([ "pgrep", "-P", @pid.to_s ], &:read).split.map(&:to_i)
    status = interrupt(grace)
    [ @pid, *children ].each { |pid| kill_outright(pid) }
    status
  ensure
    @pid = nil
  end

  private

  def interrupt(grace)
    Process.kill("INT", @pid)
    deadline = clock + grace
    until (reaped = Process.waitpid2(@pid, Process::WNOHANG))
      return nil if clock > deadline

      sleep 0.05
    end
    reaped.last
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end

  def kill_outright(pid)
    return unless running?(pid)

    Process.kill("KILL", pid)
    deadline = clock + KILL_TIMEOUT
    while running?(pid)
      raise "process #{pid} survived SIGKILL" if clock > deadline

      sleep 0.05
    end
  rescue Errno::ESRCH
    nil
  end

  def running?(pid)
    Process.kill(0, pid)
    true
  rescue Errno::ESRCH
    false
  rescue Errno::EPERM
    true
  end
end
