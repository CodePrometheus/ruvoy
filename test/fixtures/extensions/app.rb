# frozen_string_literal: true

require "json"

# Reports what the filter exposed and makes the upstream calls a suite scripts.
class ExtensionsApp
  def initialize
    @in_progress = 0
    @outcomes = []
    @abandoned = []
  end

  def call(env)
    case env.fetch("PATH_INFO")
    when "/keys" then json(env.keys.grep(/\Aruvoy\./).sort)
    when "/context" then json(env["ruvoy.context"]&.to_h)
    when "/call" then json(tracked { outcome { perform(env, JSON.parse(env["rack.input"].read)) } })
    when "/calls" then json(overlapping(env, JSON.parse(env["rack.input"].read)))
    when "/background" then background(env)
    when "/stream" then json(tracked { outcome { consume(env, JSON.parse(env["rack.input"].read)) } })
    when "/streams" then json(overlapping_streams(env, JSON.parse(env["rack.input"].read)))
    when "/abandon" then abandon(env, JSON.parse(env["rack.input"].read))
    when %r{\A/proxy(/.*)\z} then proxy(env, $1)
    when "/state" then json("in_progress" => @in_progress, "outcomes" => @outcomes, "abandoned" => @abandoned)
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

  # Reads a stream the way the suite asks: pausing `pause` seconds after the
  # first chunk, closing it after `take` chunks, and answering `linger` seconds
  # after that. Reports what arrived and when, and what stopped it if anything.
  def consume(env, spec)
    status, headers, body = open_stream(env, spec)
    started = clock
    received = { "status" => status, "headers" => headers, "chunks" => [], "times" => [] }
    begin
      body.each do |chunk|
        received["chunks"] << chunk
        received["times"] << clock - started
        break if received["chunks"].size == spec["take"]

        sleep spec["pause"] if spec["pause"] && received["chunks"].size == 1
      end
      received["trailers"] = body.trailers
    rescue StandardError => error
      received.merge!("error" => error.class.name, "message" => error.message)
    ensure
      body.close
    end
    sleep spec["linger"] if spec["linger"]
    bytes = received["chunks"].sum(&:bytesize)
    received["chunks"] = nil if bytes > 4096
    received.merge("bytes" => bytes)
  end

  def open_stream(env, spec)
    body = spec["body"]
    body = Generated.new(spec["upload_bytes"]) if spec["upload_bytes"]
    env.fetch("ruvoy.upstream").stream(
      spec.fetch("cluster", "test_upstream"), spec.fetch("method", "GET"), spec.fetch("path"),
      headers: spec.fetch("headers", {}), body: body, idle_timeout: spec["idle_timeout"], timeout: spec["timeout"]
    )
  end

  def overlapping_streams(env, specs)
    started = clock
    results = specs.map { |spec| Async { outcome { consume(env, spec) } } }.map(&:wait)
    { "results" => results, "elapsed" => clock - started }
  end

  # Answers once the first chunk is in, and leaves the rest of the stream to a
  # fiber that reads it only after the request is over.
  def abandon(env, spec)
    _status, _headers, body = open_stream(env, spec)
    first = body.each { |chunk| break chunk }
    Async do
      sleep 0.2
      @abandoned << outcome do
        body.each { nil }
        { "finished" => true }
      end
    end
    json("first" => first)
  end

  # Passes the request on and the response back as it arrives.
  def proxy(env, path)
    env.fetch("ruvoy.upstream").stream("test_upstream", env["REQUEST_METHOD"], path, body: env["rack.input"])
  end

  def clock
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
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

  # A request body read a piece at a time, as from a client still sending it.
  class Generated
    def initialize(bytes)
      @remaining = bytes
    end

    def read(length)
      return nil unless @remaining.positive?

      piece = "g" * [ length, @remaining ].min
      @remaining -= piece.bytesize
      piece
    end
  end
end
