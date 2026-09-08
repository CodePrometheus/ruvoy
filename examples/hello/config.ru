# frozen_string_literal: true

# An ordinary Rack application. Nothing here knows it runs inside a proxy.
#
#   ruvoy examples/hello/config.ru
#   curl localhost:8080/
#   curl localhost:8080/slow

require "json"

SLOW_SECONDS = 1.0

application = lambda do |env|
  case env.fetch("PATH_INFO")
  when "/"
    text("hello from #{RUBY_DESCRIPTION}")
  when "/slow"
    # Waiting is where a fiber hands the thread back, so a hundred of these
    # overlap instead of queueing. Try it with `oha -c 100 -z 5s`.
    sleep(SLOW_SECONDS)
    text("waited #{SLOW_SECONDS}s")
  when "/echo"
    json("method" => env.fetch("REQUEST_METHOD"), "body" => env["rack.input"]&.read.to_s)
  else
    [ 404, { "content-type" => "text/plain" }, [ "not found\n" ] ]
  end
end

def text(body)
  [ 200, { "content-type" => "text/plain" }, [ "#{body}\n" ] ]
end

def json(payload)
  [ 200, { "content-type" => "application/json" }, [ "#{JSON.generate(payload)}\n" ] ]
end

run application
