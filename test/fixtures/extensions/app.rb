# frozen_string_literal: true

require "async/websocket/adapters/rack"
require "digest"
require "json"

# Reports what the filter exposed and makes the upstream calls a suite scripts.
class ExtensionsApp
  def initialize
    @in_progress = 0
    @outcomes = []
    @abandoned = []
    @live_upgrades = 0
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
    when %r{\A/upgraded(/.*)\z} then json(read_upgraded(env, $1))
    when "/ws/protocols" then json(env["rack.protocol"])
    when "/ws/echo" then counted(websocket_echo(env))
    when "/ws/raise" then counted(websocket_echo(env) { raise "the application failed mid-stream" })
    when "/ws/bye" then counted(websocket_bye(env))
    when "/ws/raw" then counted(raw_upgrade(env))
    when "/duplex" then [ 200, { "content-type" => "text/plain" }, ->(stream) { stream.write(stream.read.upcase) } ]
    when "/fibers" then json(fibers)
    when "/state"
      json("in_progress" => @in_progress, "outcomes" => @outcomes, "abandoned" => @abandoned,
           "live_upgrades" => @live_upgrades)
    else [ 404, { "content-type" => "text/plain", "content-length" => "0" }, [] ]
    end
  end

  # Relays a WebSocket to the upstream path it names, as a proxy would: the
  # upstream's 101 response, returned as is, upgrades the client too.
  def relay(env)
    handshake = %w[sec-websocket-key sec-websocket-version sec-websocket-extensions].filter_map do |name|
      value = env["HTTP_#{name.upcase.tr("-", "_")}"]
      [ name, value ] if value
    end
    idle_timeout = Float(Rack::Utils.parse_query(env["QUERY_STRING"])["idle"] || 5)
    counted(env.fetch("ruvoy.upstream").stream("test_upstream", "GET", env.fetch("PATH_INFO"),
                                               headers: handshake.to_h, protocol: "websocket", idle_timeout: idle_timeout))
  end

  # Counts upgraded sessions while they run, so a test can tell that one was
  # left behind.
  def counted(response)
    status, headers, body = response
    return response unless body.respond_to?(:call) && !body.respond_to?(:each)

    app = self
    counting = Object.new
    counting.define_singleton_method(:call) do |stream|
      app.send(:live_upgrade) { body.call(stream) }
    end
    counting.define_singleton_method(:close) { body.close if body.respond_to?(:close) }
    [ status, headers, counting ]
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

  # Upgrades a stream to the upstream path and reads it to its end, after
  # `pause` seconds, reporting what arrived and how it ended.
  def read_upgraded(env, path)
    status, _headers, upstream = env.fetch("ruvoy.upstream").stream("test_upstream", "GET", path,
                                                                    protocol: "websocket", idle_timeout: 2)
    sleep Float(Rack::Utils.parse_query(env["QUERY_STRING"])["pause"] || 0)
    received = +"".b
    ending = begin
      loop { received << upstream.readpartial(65_536) }
    rescue EOFError
      "eof"
    rescue StandardError => error
      "#{error.class}: #{error.message}"
    ensure
      upstream.close
    end
    { "status" => status, "bytes" => received.bytesize, "ending" => ending }
  end

  # Where every live fiber is parked: the diagnostic for a stalled stream.
  def fibers
    ObjectSpace.each_object(Fiber).filter_map do |fiber|
      next unless fiber.alive? && fiber != Fiber.current

      trace = fiber.backtrace(0, 14) or next
      trace.map { |line| line.sub(%r{\A.*/(gems/[^/]+|ruby|lib|test)/}, "\\1/") }
    end
  end

  def live_upgrade
    @live_upgrades += 1
    yield
  ensure
    @live_upgrades -= 1
  end

  # A WebSocket served through a library written against Rack 3's upgrade; a
  # given block runs after the first message is echoed.
  def websocket_echo(env, &after_first)
    Async::WebSocket::Adapters::Rack.open(env) do |connection|
      while (message = connection.read)
        connection.write(message)
        connection.flush
        after_first&.call
      end
    end || [ 400, { "content-type" => "text/plain" }, [ "not a WebSocket request" ] ]
  end

  # Says goodbye and closes from the server's side.
  def websocket_bye(env)
    Async::WebSocket::Adapters::Rack.open(env) do |connection|
      connection.send_text("bye")
      connection.close
    end
  end

  # The same upgrade by hand, through nothing but the stream Rack 3 specifies:
  # five bytes in, the same five shouted back, then the end. The status is the
  # server's to choose once rack.protocol asks for the upgrade.
  def raw_upgrade(env)
    accept = Digest::SHA1.base64digest(env.fetch("HTTP_SEC_WEBSOCKET_KEY") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
    [ 200, { "rack.protocol" => "websocket", "sec-websocket-accept" => accept }, lambda do |stream|
      stream.write(stream.read(5).upcase)
      stream.flush
      stream.read
      stream.close
    end ]
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
