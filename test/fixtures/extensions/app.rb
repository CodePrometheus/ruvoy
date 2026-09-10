# frozen_string_literal: true

require "json"

# Reports what the filter exposed and makes the upstream calls a suite scripts.
class ExtensionsApp
  def initialize
    @in_progress = 0
    @outcomes = []
  end

  def call(env)
    case env.fetch("PATH_INFO")
    when "/keys" then json(env.keys.grep(/\Aruvoy\./).sort)
    when "/context" then json(env["ruvoy.context"]&.to_h)
    when "/call" then json(tracked { outcome { perform(env, JSON.parse(env["rack.input"].read)) } })
    when "/calls" then json(overlapping(env, JSON.parse(env["rack.input"].read)))
    when "/background" then background(env)
    when "/state" then json("in_progress" => @in_progress, "outcomes" => @outcomes)
    else [ 404, { "content-type" => "text/plain", "content-length" => "0" }, [] ]
    end
  end

  private

  def perform(env, call)
    status, headers, body = env.fetch("ruvoy.upstream").call(
      call.fetch("cluster"), call.fetch("method", "GET"), call.fetch("path"),
      headers: call.fetch("headers", {}), body: call["body"], timeout: call["timeout"]
    )
    { "status" => status, "headers" => headers, "body" => body, "encoding" => body.encoding.name }
  end

  def outcome
    yield
  rescue StandardError => error
    { "error" => error.class.name, "message" => error.message }
  end

  # Makes every call at once from one request and reports how long they took
  # together.
  def overlapping(env, calls)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    results = calls.map { |call| Async { outcome { perform(env, call) } } }.map(&:wait)
    { "results" => results, "elapsed" => Process.clock_gettime(Process::CLOCK_MONOTONIC) - started }
  end

  # Answers at once and makes its call only once the request is over.
  def background(env)
    upstream = env.fetch("ruvoy.upstream")
    Async do
      sleep 0.2
      @outcomes << outcome { upstream.call("test_upstream", "GET", "/echo") }
    end
    json("started" => true)
  end

  def tracked
    @in_progress += 1
    yield
  ensure
    @in_progress -= 1
  end

  def json(value)
    body = JSON.generate(value)
    [ 200, { "content-type" => "application/json", "content-length" => body.bytesize.to_s }, [ body ] ]
  end
end
