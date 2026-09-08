# frozen_string_literal: true

require "fiddle"

class RuvoyBenchmarkApp
  MAX_BODY_BYTES = 2 * 1024 * 1024
  MAX_WAIT_MS = 1_000
  MAX_BLOCK_MS = 1_000
  MAX_CPU_ITERATIONS = 100_000_000

  # A native database or HTTP driver releases the GVL for the syscall, so other
  # *threads* keep running, but it knows nothing about the fiber scheduler, so
  # every other fiber on the same thread stops. Fiddle calls out with the GVL
  # released, which reproduces exactly that. `Kernel#sleep` does the opposite:
  # it yields to the scheduler, which is the best case rather than the common
  # one.
  NANOSLEEP = Fiddle::Function.new(
    Fiddle::Handle::DEFAULT["nanosleep"],
    [ Fiddle::TYPE_VOIDP, Fiddle::TYPE_VOIDP ],
    Fiddle::TYPE_INT
  )

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
    block_ms = Integer(parameters.fetch("block_ms", "0"), 10)
    cpu_iterations = Integer(parameters.fetch("cpu_iterations", "0"), 10)
    response_bytes = Integer(parameters.fetch("response_bytes", "0"), 10)
    expected_request_bytes = Integer(parameters.fetch("expected_request_bytes", "0"), 10)
    raise ArgumentError, "invalid benchmark wait_ms" unless (0..MAX_WAIT_MS).cover?(wait_ms)
    raise ArgumentError, "invalid benchmark block_ms" unless (0..MAX_BLOCK_MS).cover?(block_ms)
    unless (0..MAX_CPU_ITERATIONS).cover?(cpu_iterations)
      raise ArgumentError, "invalid benchmark cpu_iterations"
    end
    raise ArgumentError, "invalid benchmark response_bytes" unless (0..MAX_BODY_BYTES).cover?(response_bytes)
    raise ArgumentError, "invalid expected_request_bytes" unless (0..MAX_BODY_BYTES).cover?(expected_request_bytes)

    # rack.input is optional since Rack 3.1; Falcon omits it for bodyless requests.
    request_body = env["rack.input"]&.read || ""
    raise ArgumentError, "request body exceeds the 2 MiB limit" if request_body.bytesize > MAX_BODY_BYTES
    raise ArgumentError, "unexpected request body size" unless request_body.bytesize == expected_request_bytes

    sleep(wait_ms / 1000.0) if wait_ms.positive?
    native_sleep(block_ms / 1000.0) if block_ms.positive?
    checksum = burn_cpu(cpu_iterations)
    response_body = "B".b * response_bytes
    [
      200,
      {
        "content-type" => "application/octet-stream",
        "content-length" => response_body.bytesize.to_s,
        "x-request-bytes" => request_body.bytesize.to_s,
        "x-cpu-checksum" => checksum.to_s
      },
      [ response_body ]
    ]
  rescue ArgumentError => error
    body = error.message
    [ 400, { "content-type" => "text/plain", "content-length" => body.bytesize.to_s }, [ body ] ]
  end

  private

  # Reported in a response header so the work cannot be optimised away, and so
  # a run that silently skipped it is visible in the evidence.
  def burn_cpu(iterations)
    accumulator = 0
    iterations.times { |index| accumulator = (accumulator * 31 + index) & 0xFFFFFFFF }
    accumulator
  end

  def native_sleep(seconds)
    whole = seconds.floor
    timespec = [ whole, ((seconds - whole) * 1_000_000_000).round ].pack("q q")
    NANOSLEEP.call(timespec, nil)
  end

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
