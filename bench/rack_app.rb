# frozen_string_literal: true

class RuvoyBenchmarkApp
  MAX_BODY_BYTES = 2 * 1024 * 1024
  MAX_WAIT_MS = 1_000

  def initialize
    @served = 0
  end

  # The load generator only knows what it sent. Every server counts what it
  # actually executed, so a measurement that silently dropped requests can be
  # detected instead of being reported as throughput.
  def call(env)
    path = env.fetch("PATH_INFO")
    return served_response if path == "/benchmark-served"
    return not_found unless path == "/benchmark"

    @served += 1

    parameters = env.fetch("QUERY_STRING", "").split("&").to_h do |entry|
      entry.split("=", 2)
    end
    wait_ms = Integer(parameters.fetch("wait_ms", "0"), 10)
    response_bytes = Integer(parameters.fetch("response_bytes", "0"), 10)
    expected_request_bytes = Integer(parameters.fetch("expected_request_bytes", "0"), 10)
    raise ArgumentError, "invalid benchmark wait_ms" unless (0..MAX_WAIT_MS).cover?(wait_ms)
    raise ArgumentError, "invalid benchmark response_bytes" unless (0..MAX_BODY_BYTES).cover?(response_bytes)
    raise ArgumentError, "invalid expected_request_bytes" unless (0..MAX_BODY_BYTES).cover?(expected_request_bytes)

    # rack.input is optional since Rack 3.1; Falcon omits it for bodyless requests.
    request_body = env["rack.input"]&.read || ""
    raise ArgumentError, "request body exceeds 2 MiB PoC limit" if request_body.bytesize > MAX_BODY_BYTES
    raise ArgumentError, "unexpected request body size" unless request_body.bytesize == expected_request_bytes

    sleep(wait_ms / 1000.0) if wait_ms.positive?
    response_body = "B".b * response_bytes
    [
      200,
      {
        "content-type" => "application/octet-stream",
        "content-length" => response_body.bytesize.to_s,
        "x-request-bytes" => request_body.bytesize.to_s
      },
      [ response_body ]
    ]
  rescue ArgumentError => error
    body = error.message
    [ 400, { "content-type" => "text/plain", "content-length" => body.bytesize.to_s }, [ body ] ]
  end

  private

  def served_response
    body = @served.to_s
    [
      200,
      {
        "content-type" => "text/plain",
        "content-length" => body.bytesize.to_s,
        "x-benchmark-served" => body
      },
      [ body ]
    ]
  end

  def not_found
    body = "not found"
    [ 404, { "content-type" => "text/plain", "content-length" => body.bytesize.to_s }, [ body ] ]
  end
end
