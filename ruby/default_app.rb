# Minimal Rack-like application used by examples and unit tests.
Class.new do
  def call(env)
    raise "intentional boom" if env.fetch("PATH_INFO") == "/raise"

    @calls = (@calls || 0) + 1
    GC.start if env["ruvoy.force_gc"]

    body = [
      env.fetch("REQUEST_METHOD"),
      env.fetch("PATH_INFO"),
      env.fetch("rack.input").read
    ].join(" ")

    headers = {
      "content-type" => "text/plain",
      "x-ruby-call-count" => @calls.to_s
    }
    headers["x-request-header"] = env["HTTP_X_RUVOY_TEST"] if env["HTTP_X_RUVOY_TEST"]

    [ 200, headers, [ body ] ]
  end
end.new
