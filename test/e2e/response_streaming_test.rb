# frozen_string_literal: true

require "timeout"
require_relative "test_helper"

# Rack response bodies reach the client incrementally.
#
# The distinguishing evidence is timing: with a buffered server the first byte
# cannot arrive before the last chunk was produced, so a first-byte time well
# below the total production time is what proves streaming.
class ResponseStreamingTest < E2ETestCase
  PORT = 19201
  BACKPRESSURE_CHUNKS = 512
  BACKPRESSURE_CHUNK_BYTES = 131_072
  SLOW_CLIENT_BYTES_PER_SECOND = 4 * 1024 * 1024
  FAILING_BODY_BYTES = 8 * 4096

  class << self
    def envoy
      @envoy ||= start_envoy
    end

    def results
      @results ||= File.join(result_dir, "streaming-#{run_id}").tap { |dir| FileUtils.mkdir_p(dir) }
    end

    private

    def start_envoy
      raise "TCP port #{PORT} is already in use" unless port_free?(PORT)

      build_module("build-fiber-module.sh", log: File.join(results, "build.log"))
      config = File.join(scratch_dir, "streaming.yaml")
      File.write(config, listener_config)
      log = File.join(results, "envoy.log")
      Envoy.start(config: config, modules: module_dir, log: log, log_level: "info",
                  env: bundle_env.merge("RUVOY_DIAGNOSTICS" => "1")).tap do |envoy|
        Minitest.after_run { envoy.stop }
        raise "streaming Envoy did not become ready:\n#{tail(log, lines: 5)}" unless envoy.serving?(
          "http://127.0.0.1:#{PORT}/rack-env"
        )
      end
    end

    def listener_config
      <<~YAML
        static_resources:
          listeners:
            - name: ruvoy_streaming
              per_connection_buffer_limit_bytes: 65536
              address:
                socket_address:
                  address: 127.0.0.1
                  port_value: #{PORT}
              filter_chains:
                - filters:
                    - name: envoy.filters.network.http_connection_manager
                      typed_config:
                        "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                        stat_prefix: ruvoy_streaming
                        codec_type: AUTO
                        stream_idle_timeout: 60s
                        route_config:
                          name: streaming_route
                          virtual_hosts:
                            - name: streaming_service
                              domains: ["*"]
                        http_filters:
                          - name: envoy.extensions.filters.http.dynamic_modules
                            typed_config:
                              "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
                              dynamic_module_config:
                                name: ruvoy_fiber
                                do_not_close: true
                              filter_name: fiber_rack
                              terminal_filter: true
                              filter_config:
                                "@type": type.googleapis.com/google.protobuf.StringValue
                                value: #{fixture_rackup}
      YAML
    end
  end

  def setup
    self.class.envoy
  end

  # 5 chunks with a 300 ms gap means the body needs ~1.2s to finish producing.
  def test_the_first_byte_arrives_before_the_body_is_finished
    paced = timed_get("/paced?chunks=5&chunk_bytes=32&gap_ms=300")
    puts "paced: first_byte=#{paced[:first_byte]}s total=#{paced[:total]}s status=#{paced[:status]} size=#{paced[:size]}"
    assert_equal "200", paced[:status], "the paced response returned #{paced[:status]}"
    assert_equal 160, paced[:size], "expected 160 bytes, received #{paced[:size]}"
    assert_operator paced[:first_byte], :<, paced[:total] / 2,
                    "first byte at #{paced[:first_byte]}s vs total #{paced[:total]}s: the body looks buffered"
    assert_operator paced[:total], :>, 1.0, "the paced response finished too fast to be meaningful"
  end

  def test_a_large_body_streams_without_being_buffered_whole
    large = timed_get("/paced?chunks=8&chunk_bytes=262144&gap_ms=50")
    puts "large: first_byte=#{large[:first_byte]}s total=#{large[:total]}s size=#{large[:size]}"
    assert_equal 2_097_152, large[:size], "expected 2097152 bytes, received #{large[:size]}"
    assert_operator large[:first_byte], :<, large[:total], "the large body did not stream"
  end

  def test_disconnecting_stops_the_producer_and_closes_the_body
    yielded_before = counter("/paced-yielded")
    closed_before = counter("/closed")
    abandon_after(1, "/paced?chunks=50&chunk_bytes=32&gap_ms=200")
    sleep 3
    yielded = counter("/paced-yielded") - yielded_before
    closed = counter("/closed") - closed_before
    puts "disconnect: yielded +#{yielded}, closed +#{closed}"
    assert_operator yielded, :<, 50, "the producer kept yielding all 50 chunks after the client left"
    assert_operator closed, :>, 0, "the abandoned body was never closed"
  end

  # The fixture waits before it counts the call, so a request stopped mid-wait
  # never counts. Both probes do count, so a working cancellation leaves the
  # counter one higher, and a request that ran to completion leaves it two.
  def test_a_client_that_leaves_stops_the_application
    before = counter("/counted")
    abandon_after(1, "/async-sleep?seconds=4")
    sleep 6
    after = counter("/counted")
    puts "abandoned request: call count #{before} -> #{after}"
    assert_equal before + 1, after, "the abandoned request ran to completion: #{before} -> #{after}"
  end

  def test_the_listener_survives_cancellation
    assert_equal "200", http_get(url("/rack-env")).code
    refute_match(/panic|segmentation fault|SIGSEGV/i, File.read(File.join(self.class.results, "envoy.log")),
                 "the streaming run produced a crash signature")
  end

  # The producer may run ahead of the client by everything that buffers between
  # them: the hand-off queue, Envoy's per-connection write buffer and the kernel
  # socket buffers on both ends. The body is sized so that slack stays a small
  # part of it, because a producer ignoring backpressure reaches the last chunk
  # within milliseconds.
  def test_a_slow_client_applies_backpressure_instead_of_growing_memory
    before = counter("/paced-yielded")
    slow = Thread.new do
      throttled_get("/paced?chunks=#{BACKPRESSURE_CHUNKS}&chunk_bytes=#{BACKPRESSURE_CHUNK_BYTES}&gap_ms=0",
                    bytes_per_second: SLOW_CLIENT_BYTES_PER_SECOND)
    end
    sleep 3
    produced = counter("/paced-yielded") - before
    puts "backpressure: produced #{produced} of #{BACKPRESSURE_CHUNKS} chunks while the client was still reading"
    assert_operator produced, :<, BACKPRESSURE_CHUNKS * 5 / 8,
                    "the producer ran ahead (#{produced} of #{BACKPRESSURE_CHUNKS} chunks): backpressure did not apply"
    assert_equal BACKPRESSURE_CHUNKS * BACKPRESSURE_CHUNK_BYTES, slow.value, "the slow client did not receive the whole body"
  end

  # The status and headers are already on the wire when the failure happens,
  # and a module cannot reset the stream through the current ABI, so the only
  # honest thing left is to stop. The client sees a short body.
  def test_a_body_that_fails_midway_stops_rather_than_inventing_the_rest
    failing = timed_get("/raise-mid-body", read_timeout: 10)
    assert_equal "200", failing[:status], "the failing body returned '#{failing[:status]}' after its headers were sent"
    assert_operator failing[:size], :>, 0, "the failing body delivered nothing"
    assert_operator failing[:size], :<, FAILING_BODY_BYTES,
                    "the failing body delivered #{failing[:size]} bytes instead of stopping early"
    assert_equal "200", http_get(url("/rack-env")).code, "the listener stopped serving after a mid-body failure"
  end

  private

  def url(path)
    "http://127.0.0.1:#{PORT}#{path}"
  end

  def counter(path)
    Integer(http_get(url(path)).body, 10)
  end

  def timed_get(path, read_timeout: 60)
    uri = URI(url(path))
    started = clock
    first_byte = nil
    size = 0
    status = nil
    begin
      Net::HTTP.start(uri.host, uri.port, read_timeout: read_timeout) do |http|
        http.request_get(uri.request_uri) do |response|
          status = response.code
          response.read_body do |chunk|
            first_byte ||= clock - started
            size += chunk.bytesize
          end
        end
      end
    rescue EOFError, IOError, Net::ReadTimeout
      # A body that stops early ends the connection; what arrived still counts.
    end
    { first_byte: first_byte, total: clock - started, status: status, size: size }
  end

  def abandon_after(seconds, path)
    Timeout.timeout(seconds) { http_get(url(path)) }
  rescue Timeout::Error
    nil
  end

  def throttled_get(path, bytes_per_second:)
    uri = URI(url(path))
    received = 0
    started = clock
    Net::HTTP.start(uri.host, uri.port, read_timeout: 120) do |http|
      http.request_get(uri.request_uri) do |response|
        response.read_body do |chunk|
          received += chunk.bytesize
          ahead = received / bytes_per_second.to_f - (clock - started)
          sleep ahead if ahead.positive?
        end
      end
    end
    received
  end
end
