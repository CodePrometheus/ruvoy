# frozen_string_literal: true

require "fiddle"
require "json"

# Blocks the way a C extension does: the GVL is released and the thread waits in the kernel.
NANOSLEEP = Fiddle::Function.new(
  Fiddle.dlopen(nil)["nanosleep"], [ Fiddle::TYPE_VOIDP, Fiddle::TYPE_VOIDP ], Fiddle::TYPE_INT
)
LIMITED = Ruvoy::Offload.new(limit: 2)
LOCK = Mutex.new
EVENTS = Hash.new { |events, key| events[key] = [] }
IN_FLIGHT = { now: 0, peak: 0 }

def block(seconds)
  NANOSLEEP.call([ seconds.floor, ((seconds % 1) * 1_000_000_000).round ].pack("q!q!"), nil)
end

def record(key, event)
  LOCK.synchronize { EVENTS[key] << event }
end

def counted
  LOCK.synchronize do
    IN_FLIGHT[:now] += 1
    IN_FLIGHT[:peak] = [ IN_FLIGHT[:peak], IN_FLIGHT[:now] ].max
  end
  yield
ensure
  LOCK.synchronize { IN_FLIGHT[:now] -= 1 }
end

def text(body)
  [ 200, { "content-type" => "text/plain" }, [ body.to_s ] ]
end

use Rack::Lint

run(lambda do |env|
  query = Rack::Utils.parse_query(env["QUERY_STRING"])
  seconds = Float(query.fetch("seconds", "0.2"))

  case env["PATH_INFO"]
  when "/direct" then text(block(seconds))
  when "/offload" then text(Ruvoy.offload { block(seconds) })
  when "/limited" then text(LIMITED.call { counted { block(seconds) } })
  when "/value" then text(Ruvoy.offload { 6 * 7 })
  when "/raise"
    begin
      Ruvoy.offload { raise ArgumentError, "raised inside the block" }
    rescue ArgumentError => error
      text(JSON.generate("class" => error.class.name, "message" => error.message, "backtrace" => error.backtrace.first(3)))
    end
  when "/invalid-limit"
    begin
      Ruvoy::Offload.new(limit: 0)
    rescue ArgumentError => error
      text(error.message)
    end
  when "/hold"
    key = query.fetch("key")
    begin
      record(key, "start")
      Ruvoy.offload do
        block(seconds)
        record(key, "call finished")
      end
      record(key, "returned")
      text("held")
    ensure
      record(key, "ensure")
    end
  when "/state"
    text(LOCK.synchronize { JSON.generate("events" => EVENTS, "peak" => IN_FLIGHT[:peak]) })
  else
    [ 404, { "content-type" => "text/plain" }, [ "not found" ] ]
  end
end)
