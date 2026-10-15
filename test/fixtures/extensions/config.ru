# frozen_string_literal: true

require_relative "app"

app = ExtensionsApp.new
# Rack::Lint's stream wrapper offers only the methods the specification
# requires, and a relay reads with readpartial.
map("/relay") { run ->(env) { app.relay(env) } }
# Rack::Lint refuses this response itself; the runtime must refuse it too.
map("/unoffered") { run ->(_env) { [ 101, { "rack.protocol" => "websocket" }, ->(stream) { stream.close } ] } }
map("/") do
  use Rack::Lint
  run app
end
