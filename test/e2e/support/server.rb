# frozen_string_literal: true

require "bundler"
require "net/http"
require "uri"

# A process under test that serves HTTP: started, waited for, stopped.
#
# It starts from a clean environment and sees only what the suite hands it,
# so the bundle the suite runs under never leaks into what is being tested.
class Server
  READY_TIMEOUT = 30
  STOP_TIMEOUT = 5

  attr_reader :pid, :log

  def self.start(*command, log:, env: {}, chdir: nil)
    new(*command, log: log, env: env, chdir: chdir).start
  end

  def initialize(*command, log:, env:, chdir:)
    @command = command
    @log = log
    @env = env
    @chdir = chdir
  end

  def start
    options = { out: @log, err: [ :child, :out ] }
    options[:chdir] = @chdir if @chdir
    @pid = Bundler.with_unbundled_env { Process.spawn(@env, *@command, **options) }
    self
  end

  # True once the URL answers; false if the process dies or the deadline passes.
  def serving?(url, timeout: READY_TIMEOUT)
    deadline = clock + timeout
    loop do
      return true if reachable?(url)
      return false if !alive? || clock > deadline

      sleep 0.5
    end
  end

  def alive?
    !@pid.nil? && Process.waitpid(@pid, Process::WNOHANG).nil?
  rescue Errno::ECHILD
    false
  end

  # Waits for the process to exit on its own; nil if it is still running at
  # the deadline.
  def wait_exit(timeout:)
    return unless @pid

    deadline = clock + timeout
    until (reaped = Process.waitpid2(@pid, Process::WNOHANG))
      return nil if clock > deadline

      sleep 0.1
    end
    @pid = nil
    reaped.last
  rescue Errno::ECHILD
    @pid = nil
    nil
  end

  def stop
    return unless @pid

    Process.kill("TERM", @pid)
    deadline = clock + STOP_TIMEOUT
    until Process.waitpid(@pid, Process::WNOHANG)
      Process.kill("KILL", @pid) if clock > deadline
      sleep 0.1
    end
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  ensure
    @pid = nil
  end

  private

  def reachable?(url)
    uri = URI(url)
    Net::HTTP.start(uri.host, uri.port, open_timeout: 2, read_timeout: 5) do |http|
      http.get(uri.request_uri).is_a?(Net::HTTPSuccess)
    end
  rescue SystemCallError, IOError, Net::OpenTimeout, Net::ReadTimeout
    false
  end

  def clock
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
