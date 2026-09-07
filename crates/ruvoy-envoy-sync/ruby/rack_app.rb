# Rack-like application backing the serial Envoy module in end-to-end tests.
#
# Constants are inlined because the class is anonymous: assigning one inside
# `Class.new` would land in the enclosing scope rather than in the class.
Class.new do
  def call(env)
    path = env.fetch("PATH_INFO")
    raise "intentional envoy boom" if path == "/raise"

    sleep 0.5 if path == "/slow"
    sleep 1.0 if path == "/slow-shutdown"

    @calls = (@calls || 0) + 1
    GC.start if path == "/gc" || env["ruvoy.force_gc"]

    input = env.fetch("rack.input")
    input.rewind
    request_body = input.read

    response_body = route_body(env, path, env.fetch("QUERY_STRING", ""), request_body)
    [ 200, response_headers(env, request_body, response_body), [ response_body ] ]
  end

  private

  def route_body(env, path, query, request_body)
    case path
    when "/benchmark" then benchmark_body(query)
    when "/echo" then request_body
    when "/large-response" then "R".b * (1024 * 1024)
    else [ env.fetch("REQUEST_METHOD"), path, request_body ].join(" ")
    end
  end

  def benchmark_body(query)
    parameters = query.split("&").to_h { |entry| entry.split("=", 2) }
    wait_ms = Integer(parameters.fetch("wait_ms", "0"), 10)
    response_bytes = Integer(parameters.fetch("response_bytes", "0"), 10)
    raise "invalid benchmark wait_ms" unless (0..1_000).cover?(wait_ms)
    raise "invalid benchmark response_bytes" unless (0..2 * 1024 * 1024).cover?(response_bytes)

    sleep(wait_ms / 1000.0) if wait_ms.positive?
    "B".b * response_bytes
  end

  def response_headers(env, request_body, response_body)
    headers = {
      "content-type" => "application/octet-stream",
      "content-length" => response_body.bytesize.to_s,
      "x-request-bytes" => request_body.bytesize.to_s,
      "x-ruby-call-count" => @calls.to_s,
      "x-ruby-thread-object-id" => Thread.current.object_id.to_s
    }
    headers["x-rack-request-header"] = env["HTTP_X_RUVOY_TEST"] if env["HTTP_X_RUVOY_TEST"]
    headers
  end
end.new
