# frozen_string_literal: true

require "async/websocket/adapters/rack"
require "fiddle"
require "json"

# Every kind of work the runtime carries, behind one rackup that keeps nothing
# between requests, so whatever a soak sees grow belongs to the runtime.
module Soak
  NANOSLEEP = Fiddle::Function.new(
    Fiddle.dlopen(nil)["nanosleep"], [ Fiddle::TYPE_VOIDP, Fiddle::TYPE_VOIDP ], Fiddle::TYPE_INT
  )
  OFFLOAD = Ruvoy::Offload.new(limit: 4)
  HANDSHAKE = %w[sec-websocket-key sec-websocket-version sec-websocket-extensions].freeze
  CHUNK = ("p" * 4096).freeze

  # Yields its chunks with a pause between them, as a body produced over time.
  class Paced
    def initialize(chunks, gap)
      @chunks = chunks
      @gap = gap
    end

    def each
      @chunks.times do |index|
        sleep @gap if index.positive?
        yield CHUNK
      end
    end
  end

  class << self
    def call(env)
      case env.fetch("PATH_INFO")
      when "/noop" then text("ok")
      when "/wait" then wait(env)
      when "/paced" then [ 200, { "content-type" => "application/octet-stream" }, Paced.new(8, 0.05) ]
      when "/upload" then text(upload(env).to_s)
      when "/call" then call_upstream(env)
      when "/proxy" then env.fetch("ruvoy.upstream").stream("upstream", "GET", "/chunks/8/20", idle_timeout: 5)
      when "/offload" then offload
      when "/ws" then counted(echo(env))
      when "/relay" then counted(relay(env))
      when "/heap" then heap
      when "/state" then json("sessions" => @sessions.to_i, "fibers" => live_fibers)
      else [ 404, { "content-type" => "text/plain", "content-length" => "0" }, [] ]
      end
    end

    private

    def wait(env)
      sleep Integer(Rack::Utils.parse_query(env["QUERY_STRING"]).fetch("ms", "50"), 10) / 1000.0
      text("waited")
    end

    def upload(env)
      input = env.fetch("rack.input")
      bytes = 0
      while (chunk = input.read(65_536))
        bytes += chunk.bytesize
      end
      bytes
    end

    def call_upstream(env)
      status, _headers, body = env.fetch("ruvoy.upstream").call("upstream", "GET", "/bytes/1024", timeout: 5)
      text("#{status} #{body.bytesize}", status == 200 && body.bytesize == 1024 ? 200 : 502)
    end

    # Blocks the way a C extension does, with the GVL released.
    def offload
      OFFLOAD.call { NANOSLEEP.call([ 0, 20_000_000 ].pack("q!q!"), nil) }
      text("offloaded")
    end

    def echo(env)
      Async::WebSocket::Adapters::Rack.open(env) do |connection|
        while (message = connection.read)
          connection.write(message)
          connection.flush
        end
      end || [ 400, { "content-type" => "text/plain" }, [ "not a WebSocket request" ] ]
    end

    def relay(env)
      headers = HANDSHAKE.filter_map do |name|
        value = env["HTTP_#{name.upcase.tr("-", "_")}"]
        [ name, value ] if value
      end.to_h
      env.fetch("ruvoy.upstream").stream("upstream", "GET", "/ws/echo", headers: headers, protocol: "websocket",
                                                                        idle_timeout: 30)
    end

    # Counts upgraded sessions while they run, so one left behind shows.
    def counted(response)
      status, headers, body = response
      return response unless body.respond_to?(:call) && !body.respond_to?(:each)

      session = Object.new
      session.define_singleton_method(:call) do |stream|
        Soak.track { body.call(stream) }
      end
      session.define_singleton_method(:close) { body.close if body.respond_to?(:close) }
      [ status, headers, session ]
    end

    def heap
      GC.start
      text(GC.stat(:heap_live_slots).to_s)
    end

    def live_fibers
      ObjectSpace.each_object(Fiber).count { |fiber| fiber.alive? && fiber != Fiber.current }
    end

    def text(body, status = 200)
      [ status, { "content-type" => "text/plain", "content-length" => body.bytesize.to_s }, [ body ] ]
    end

    def json(value)
      text(JSON.generate(value)).tap { |response| response[1]["content-type"] = "application/json" }
    end
  end

  def self.track
    @sessions = @sessions.to_i + 1
    yield
  ensure
    @sessions -= 1
  end
end

run Soak
