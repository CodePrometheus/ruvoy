# frozen_string_literal: true

require "async/http/endpoint"
require "async/http/protocol/http1"
require "async/http/protocol/http2"
require "async/websocket/client"
require "digest"
require "json"
require "openssl"
require "securerandom"
require_relative "test_helper"
require_relative "support/certificates"
require_relative "support/upstream_server"

# What a filter can expose beyond Rack: Envoy's view of the request as
# `ruvoy.context`, and calls into Envoy's clusters as `ruvoy.upstream`.
class EnvoyExtensionsTest < E2ETestCase
  PORT = 19230
  TLS_PORT = 19231
  PLAIN_PORT = 19232
  TERMINATOR_PORT = 19233
  ADMIN_PORT = 19234
  UNREACHABLE_PORT = 19235
  MAX_RESPONSE_BYTES = 65_536
  STREAM_BUFFER_BYTES = 65_536

  class << self
    def envoy
      @envoy ||= start_envoy
    end

    def upstream
      @upstream ||= UpstreamServer.new.start.tap { |server| Minitest.after_run { server.stop } }
    end

    def certificates
      @certificates ||= Certificates.write(File.join(scratch_dir, "extensions-tls"))
    end

    def results
      @results ||= File.join(result_dir, "extensions-#{run_id}").tap { |dir| FileUtils.mkdir_p(dir) }
    end

    # A second Envoy with the same configuration on ports of its own, for a
    # test that has to stop it. The shared instance has built the module.
    def start_dedicated_envoy(offset, name)
      own = [ PORT, TLS_PORT, PLAIN_PORT, TERMINATOR_PORT, UNREACHABLE_PORT ]
      shift = lambda do |node|
        case node
        when Hash
          node["port_value"] += offset if own.include?(node["port_value"])
          node.each_value { |value| shift.call(value) }
        when Array then node.each { |value| shift.call(value) }
        end
      end
      config = JSON.parse(JSON.generate(envoy_config)).tap { |copy| shift.call(copy) }
      path = File.join(scratch_dir, "extensions-#{name}.json")
      File.write(path, JSON.pretty_generate(config))
      log = File.join(results, "envoy-#{name}.log")
      envoy = Envoy.start(config: path, modules: module_dir, log: log, log_level: "info", admin_port: ADMIN_PORT + offset,
                          env: bundle_env)
      raise "#{name} Envoy did not become ready:\n#{tail(log, lines: 5)}" unless envoy.serving?(
        "http://127.0.0.1:#{PLAIN_PORT + offset}/keys"
      )
      [ envoy, log ]
    end

    private

    def start_envoy
      [ PORT, TLS_PORT, PLAIN_PORT, TERMINATOR_PORT, ADMIN_PORT, UNREACHABLE_PORT ].each do |port|
        raise "TCP port #{port} is already in use" unless port_free?(port)
      end

      build_module("fiber", log: File.join(results, "build.log"))
      config = File.join(scratch_dir, "extensions.json")
      File.write(config, JSON.pretty_generate(envoy_config))
      log = File.join(results, "envoy.log")
      Envoy.start(config: config, modules: module_dir, log: log, log_level: "info", admin_port: ADMIN_PORT,
                  env: bundle_env).tap do |envoy|
        Minitest.after_run { envoy.stop }
        raise "extensions Envoy did not become ready:\n#{tail(log, lines: 5)}" unless envoy.serving?(
          "http://127.0.0.1:#{PLAIN_PORT}/keys"
        )
      end
    end

    def envoy_config
      {
        "bootstrap_extensions" => [
          bootstrap_extension(rackup: File.join(root, "test", "fixtures", "extensions", "config.ru"))
        ],
        "static_resources" => {
          "listeners" => [
            listener("extensions", PORT, ruvoy_filters(extensions: true)),
            listener("extensions_tls", TLS_PORT, ruvoy_filters(extensions: true),
                     tls: downstream_tls(require_client_certificate: false)),
            listener("plain", PLAIN_PORT, ruvoy_filters(extensions: false)),
            listener("terminator", TERMINATOR_PORT, terminator_filters,
                     tls: downstream_tls(require_client_certificate: true))
          ],
          "clusters" => [
            cluster("test_upstream", upstream.port).merge(
              "typed_extension_protocol_options" => {
                "envoy.extensions.upstreams.http.v3.HttpProtocolOptions" => {
                  "@type" => "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
                  "explicit_http_config" => { "http_protocol_options" => { "enable_trailers" => true } }
                }
              }
            ),
            cluster("breaker", upstream.port).merge(
              "circuit_breakers" => {
                "thresholds" => [ { "max_connections" => 1, "max_pending_requests" => 1, "max_requests" => 1 } ]
              }
            ),
            # Only the test that counts calls uses this one.
            cluster("counted", upstream.port),
            cluster("unreachable", UNREACHABLE_PORT),
            cluster("mtls_upstream", TERMINATOR_PORT).merge("transport_socket" => upstream_tls)
          ]
        }
      }
    end

    def listener(name, port, http_filters, tls: nil)
      chain = { "filters" => [ connection_manager(name, http_filters) ] }
      listener = {
        "name" => name,
        "address" => { "socket_address" => { "address" => "127.0.0.1", "port_value" => port } },
        "filter_chains" => [ chain ]
      }
      return listener unless tls

      chain["transport_socket"] = tls
      # Records the server name the client asked for, which the context reports.
      listener.merge("listener_filters" => [ {
        "name" => "envoy.filters.listener.tls_inspector",
        "typed_config" => {
          "@type" => "type.googleapis.com/envoy.extensions.filters.listener.tls_inspector.v3.TlsInspector"
        }
      } ])
    end

    def connection_manager(name, http_filters)
      terminator = http_filters.last["name"] == "envoy.filters.http.router"
      {
        "name" => "envoy.filters.network.http_connection_manager",
        "typed_config" => {
          "@type" => "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
          "stat_prefix" => name,
          "codec_type" => "AUTO",
          "stream_idle_timeout" => "30s",
          # WebSockets over HTTP/1.1 upgrades and over HTTP/2 extended CONNECT.
          "upgrade_configs" => [ { "upgrade_type" => "websocket" } ],
          "http2_protocol_options" => { "allow_connect" => true },
          "route_config" => { "virtual_hosts" => [ terminator ? terminator_host : rack_host ] },
          "http_filters" => http_filters
        }
      }
    end

    def rack_host
      {
        "name" => "rack",
        "domains" => [ "*" ],
        "routes" => [ {
          "name" => "context-route",
          "match" => { "prefix" => "/" },
          "non_forwarding_action" => {},
          "metadata" => { "filter_metadata" => { "acme.route" => { "tier" => "gold", "weight" => 2 } } }
        } ]
      }
    end

    # Stands in for an upstream that demands a client certificate, and tells
    # the application which identity it saw.
    def terminator_host
      {
        "name" => "terminator",
        "domains" => [ "*" ],
        "request_headers_to_add" => [
          { "header" => { "key" => "x-peer-uri-san", "value" => "%DOWNSTREAM_PEER_URI_SAN%" } }
        ],
        "routes" => [ { "match" => { "prefix" => "/" }, "route" => { "cluster" => "test_upstream" } } ]
      }
    end

    def ruvoy_filters(extensions:)
      filter_config = {}
      if extensions
        filter_config["context"] = { "metadata_namespaces" => %w[acme.tenant acme.route absent] }
        filter_config["upstream"] = {
          "clusters" => %w[test_upstream counted breaker unreachable mtls_upstream missing_cluster],
          "timeout_ms" => 2_000,
          "max_response_bytes" => MAX_RESPONSE_BYTES,
          "stream_buffer_bytes" => STREAM_BUFFER_BYTES
        }
      end
      [
        {
          "name" => "envoy.filters.http.set_metadata",
          "typed_config" => {
            "@type" => "type.googleapis.com/envoy.extensions.filters.http.set_metadata.v3.Config",
            "metadata" => [ {
              "metadata_namespace" => "acme.tenant",
              "value" => { "id" => "t-42", "weight" => 3, "beta" => true, "regions" => %w[us eu],
                           "nested" => { "plan" => "pro" } }
            } ]
          }
        },
        {
          "name" => "envoy.extensions.filters.http.dynamic_modules",
          "typed_config" => {
            "@type" => "type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter",
            "dynamic_module_config" => { "name" => "ruvoy_fiber", "do_not_close" => true },
            "filter_name" => "fiber_rack",
            "terminal_filter" => true,
            "filter_config" => {
              "@type" => "type.googleapis.com/google.protobuf.StringValue",
              "value" => JSON.generate(filter_config)
            }
          }
        }
      ]
    end

    def terminator_filters
      [ { "name" => "envoy.filters.http.router",
          "typed_config" => { "@type" => "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router" } } ]
    end

    def cluster(name, port)
      {
        "name" => name,
        "type" => "STATIC",
        "connect_timeout" => "1s",
        "load_assignment" => {
          "cluster_name" => name,
          "endpoints" => [ { "lb_endpoints" => [ {
            "endpoint" => { "address" => { "socket_address" => { "address" => "127.0.0.1", "port_value" => port } } }
          } ] } ]
        }
      }
    end

    def downstream_tls(require_client_certificate:)
      {
        "name" => "envoy.transport_sockets.tls",
        "typed_config" => {
          "@type" => "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext",
          "require_client_certificate" => require_client_certificate,
          "common_tls_context" => {
            "tls_certificates" => [ certificate_pair("server") ],
            "validation_context" => { "trusted_ca" => { "filename" => File.join(certificates, "ca.pem") } }
          }
        }
      }
    end

    def upstream_tls
      {
        "name" => "envoy.transport_sockets.tls",
        "typed_config" => {
          "@type" => "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext",
          "sni" => "localhost",
          "common_tls_context" => {
            "tls_certificates" => [ certificate_pair("client") ],
            "validation_context" => { "trusted_ca" => { "filename" => File.join(certificates, "ca.pem") } }
          }
        }
      }
    end

    def certificate_pair(name)
      { "certificate_chain" => { "filename" => File.join(certificates, "#{name}.pem") },
        "private_key" => { "filename" => File.join(certificates, "#{name}-key.pem") } }
    end
  end

  def setup
    self.class.envoy
  end

  def test_the_extensions_stay_out_of_the_environment_unless_configured
    assert_equal [ "ruvoy.force_gc" ], get_json(PLAIN_PORT, "/keys")
    assert_equal %w[ruvoy.context ruvoy.force_gc ruvoy.upstream], get_json(PORT, "/keys")
  end

  def test_the_context_carries_the_route_the_connection_and_the_listed_metadata
    context = get_json(PORT, "/context")

    assert_equal "context-route", context["route_name"]
    connection = context.fetch("connection")
    assert_equal "127.0.0.1", connection["source_address"]
    assert_equal "127.0.0.1", connection["destination_address"]
    assert_equal PORT, connection["destination_port"]
    assert_kind_of Integer, connection["source_port"]
    assert_kind_of Integer, connection["id"]
    assert_nil context["tls"]
    assert_equal(
      { "acme.tenant" => { "id" => "t-42", "weight" => 3.0, "beta" => true, "regions" => %w[us eu] } },
      context["dynamic_metadata"],
      "the nested structure has no getter, and the absent namespace does not exist"
    )
    assert_equal({ "acme.route" => { "tier" => "gold", "weight" => 2.0 } }, context["route_metadata"])
  end

  def test_the_context_reports_tls_and_only_a_presented_client_certificate
    anonymous = get_json(TLS_PORT, "/context", tls: true).fetch("tls")
    assert_match(/\ATLSv1\.[23]\z/, anonymous["version"])
    assert_equal "localhost", anonymous["server_name"]
    assert_nil anonymous["peer_certificate"]

    peer = get_json(TLS_PORT, "/context", tls: true, client_certificate: true).fetch("tls").fetch("peer_certificate")
    client = OpenSSL::X509::Certificate.new(File.read(File.join(self.class.certificates, "client.pem")))
    assert_equal Certificates::CLIENT_SUBJECT, peer["subject"]
    assert_equal Certificates::CLIENT_URI_SAN, peer["uri_san"]
    assert_equal Certificates::CLIENT_DNS_SAN, peer["dns_san"]
    assert_equal Digest::SHA256.hexdigest(client.to_der), peer["sha256"]
  end

  def test_a_call_carries_its_method_path_headers_and_body_there_and_back
    result = call("cluster" => "test_upstream", "method" => "post", "path" => "/echo?x=1",
                  "headers" => { "X-Test" => "1", "x-multi" => %w[a b] }, "body" => "payload")

    assert_equal 200, result["status"]
    assert_equal %w[a=1 b=2], result["headers"]["set-cookie"], "a repeated header arrives as an Array"
    assert_includes %w[ASCII-8BIT BINARY], result["encoding"]
    echo = JSON.parse(result["body"])
    assert_equal "POST", echo["method"]
    assert_equal "/echo?x=1", echo["target"]
    assert_equal [ "1" ], echo["headers"]["x-test"]
    assert_equal %w[a b], echo["headers"]["x-multi"]
    assert_equal [ "test_upstream" ], echo["headers"]["host"]
    assert_equal [ "7" ], echo["headers"]["content-length"], "a body goes out with its length, not chunked"
    assert_equal "payload", echo["body"]
  end

  def test_a_call_that_cannot_be_sent_raises_where_it_was_made
    refused = {
      { "cluster" => "not_listed", "path" => "/echo" } => [ "ArgumentError", "not one this filter" ],
      { "cluster" => "missing_cluster", "path" => "/echo" } => [ "Ruvoy::UpstreamError", "does not exist" ],
      { "cluster" => "test_upstream", "path" => "echo" } => [ "ArgumentError", "must start with /" ],
      { "cluster" => "test_upstream", "path" => "/echo", "headers" => { ":authority" => "x" } } =>
        [ "ArgumentError", "set from the call's arguments" ],
      { "cluster" => "test_upstream", "path" => "/echo", "timeout" => -1 } => [ "ArgumentError", "timeout" ]
    }

    refused.each do |arguments, (error, message)|
      result = call(arguments)
      assert_equal error, result["error"], "#{arguments} => #{result}"
      assert_includes result["message"], message
    end
  end

  def test_each_call_is_counted_by_how_it_ended
    before = upstream_calls
    call("cluster" => "counted", "path" => "/echo")
    call("cluster" => "missing_cluster", "path" => "/echo")
    call("cluster" => "counted", "path" => "/bytes/#{MAX_RESPONSE_BYTES + 1}")
    stream("cluster" => "counted", "path" => "/flood/#{8 * 1024 * 1024}", "pause" => 0.5)
    stream("cluster" => "counted", "path" => "/delay/1500", "idle_timeout" => 0.3)
    stream("cluster" => "counted", "path" => "/chunks/8/50", "take" => 1)
    stream("cluster" => "counted", "path" => "/cut")
    post_json(PORT, "/abandon", "cluster" => "counted", "path" => "/hang/counted")

    expected = { %w[counted completed] => 1, %w[missing_cluster refused] => 1, %w[counted too_large] => 1,
                 %w[counted overflow] => 1, %w[counted timed_out] => 1, %w[counted abandoned] => 2,
                 %w[counted reset] => 1 }
    deadline = clock + 5
    counted = nil
    loop do
      counted = upstream_calls.to_h { |key, value| [ key, value - before.fetch(key, 0) ] }.reject { |_, value| value.zero? }
      break if counted == expected || clock > deadline

      sleep 0.05
    end
    assert_equal expected, counted
    # The whole response for a call, the head for each stream that got one.
    wait_for(timeout: 12) { prometheus[/upstream_duration_ms_count\{[^}]*cluster="counted"[^}]*\} (\d+)/, 1].to_i == 5 }
  end

  def test_a_call_carries_the_requests_trace_unless_it_sets_its_own
    trace = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    incoming = { "content-type" => "application/json", "traceparent" => trace, "x-b3-traceid" => "80f198ee56343ba8" }
    forwarded = lambda do |path, arguments|
      response = http_post("http://127.0.0.1:#{PORT}#{path}", JSON.generate(arguments), incoming)
      result = JSON.parse(response.body)
      JSON.parse(path == "/call" ? result.fetch("body") : result.fetch("chunks").join).fetch("headers")
    end

    [ [ "/call", { "cluster" => "test_upstream", "path" => "/echo" } ], [ "/stream", { "path" => "/echo" } ] ].each do |path, arguments|
      headers = forwarded.call(path, arguments)
      assert_equal [ trace ], headers["traceparent"], path
      assert_equal [ "80f198ee56343ba8" ], headers["x-b3-traceid"], path
      assert_match(/\A\S+\z/, headers.fetch("x-request-id").first, path)
    end
    own = forwarded.call("/call", "cluster" => "test_upstream", "path" => "/echo", "headers" => { "traceparent" => "00-own" })
    assert_equal [ "00-own" ], own["traceparent"], "the application's own trace context wins"
  end

  def test_envoy_answers_for_an_upstream_that_is_too_slow_unreachable_or_hangs_up
    { { "cluster" => "test_upstream", "path" => "/delay/1500", "timeout" => 0.3 } => 504,
      { "cluster" => "unreachable", "path" => "/echo" } => 503,
      { "cluster" => "test_upstream", "path" => "/reset" } => 503 }.each do |arguments, status|
      result = call(arguments)
      assert_equal status, result["status"], "#{arguments} => #{result}"
    end
  end

  def test_a_response_over_the_limit_raises_and_one_at_the_limit_arrives
    at_limit = call("cluster" => "test_upstream", "path" => "/bytes/#{MAX_RESPONSE_BYTES}")
    assert_equal MAX_RESPONSE_BYTES, at_limit["body"].bytesize

    over = call("cluster" => "test_upstream", "path" => "/bytes/#{MAX_RESPONSE_BYTES + 1}")
    assert_equal "Ruvoy::UpstreamResponseTooLarge", over["error"]
    assert_includes over["message"], "limit"
  end

  def test_calls_from_one_request_overlap
    overlapped = post_json(PORT, "/calls", Array.new(2) { { "cluster" => "test_upstream", "path" => "/delay/400" } })

    assert_equal [ 200, 200 ], overlapped["results"].map { |result| result["status"] }, overlapped.to_s
    assert_operator overlapped["elapsed"], :<, 0.7, "two 400ms calls should wait together, not in turn"
  end

  def test_envoy_retries_a_call_that_asks_for_it
    result = call("cluster" => "test_upstream", "path" => "/flaky/retried",
                  "headers" => { "x-envoy-retry-on" => "5xx", "x-envoy-max-retries" => "1" })

    assert_equal [ 200, "recovered" ], result.values_at("status", "body")
    assert_equal 2, self.class.upstream.hits("/flaky/retried")
  end

  # Envoy refuses the call over the limit while it is being sent, so the
  # application hears it as an error rather than as a 503. One connection and
  # one pending slot hold two of three calls at most, whether or not the first
  # has connected by the time the others arrive.
  def test_the_clusters_circuit_breaker_turns_away_the_call_over_its_limit
    overflows = breaker_overflows
    overlapped = post_json(PORT, "/calls", Array.new(3) { { "cluster" => "breaker", "path" => "/delay/300" } })
    served, refused = overlapped["results"].partition { |result| result["status"] == 200 }

    assert_includes 1..2, served.size, overlapped.to_s
    assert_equal 3 - served.size, refused.size, overlapped.to_s
    refused.each do |result|
      assert_equal "Ruvoy::UpstreamError", result["error"]
      assert_includes result["message"], "turned the call away"
    end
    assert_equal overflows + refused.size, breaker_overflows, "the breaker, not something else, must have refused them"
  end

  def test_envoy_presents_the_clusters_client_certificate
    result = call("cluster" => "mtls_upstream", "path" => "/echo")

    assert_equal 200, result["status"]
    assert_equal [ Certificates::CLIENT_URI_SAN ], JSON.parse(result["body"])["headers"]["x-peer-uri-san"]
  end

  def test_a_call_made_after_its_request_finished_is_refused
    post_json(PORT, "/background", nil)
    outcome = wait_for { get_json(PORT, "/state")["outcomes"].last }

    assert_equal "Ruvoy::UpstreamError", outcome["error"]
    assert_includes outcome["message"], "has finished"
  end

  def test_a_client_leaving_mid_call_releases_the_fiber_waiting_on_it
    socket = TCPSocket.new("127.0.0.1", PORT)
    body = JSON.generate("cluster" => "test_upstream", "path" => "/delay/2000")
    socket.write("POST /call HTTP/1.1\r\nhost: localhost\r\ncontent-length: #{body.bytesize}\r\n\r\n#{body}")
    wait_for { get_json(PORT, "/state")["in_progress"] == 1 }
    socket.close

    wait_for { get_json(PORT, "/state")["in_progress"].zero? }
    assert_equal 200, call("cluster" => "test_upstream", "path" => "/echo")["status"]
  end

  def test_many_calls_leave_nothing_behind
    statuses = Array.new(8) do
      Thread.new do
        Net::HTTP.start("127.0.0.1", PORT) do |http|
          Array.new(50) do
            request = Net::HTTP::Post.new("/call", "content-type" => "application/json")
            request.body = JSON.generate("cluster" => "test_upstream", "path" => "/echo")
            JSON.parse(http.request(request).body)["status"]
          end
        end
      end
    end.flat_map(&:value)

    assert_equal [ 200 ] * 400, statuses
    assert_equal 0, get_json(PORT, "/state")["in_progress"]
    refute_match(/\[BUG\]|panicked/, File.read(self.class.envoy.log))
  end

  def test_a_stream_hands_over_each_chunk_as_it_arrives
    result = stream("path" => "/chunks/3/200")

    assert_equal 200, result["status"], result.to_s
    assert_equal "text/plain", result["headers"]["content-type"]
    assert_equal "chunk 0\nchunk 1\nchunk 2\n", result["chunks"].join
    assert_operator result["times"].last - result["times"].first, :>=, 0.3, "chunks 200ms apart arrived together"
    assert_nil result["trailers"]
    assert_nil result["error"]
  end

  def test_a_stream_keeps_the_trailers_it_ended_with
    result = stream("path" => "/trailers")

    assert_equal [ "hello" ], result["chunks"], result.to_s
    assert_equal({ "x-checksum" => "5d41402a" }, result["trailers"])
  end

  def test_a_stream_returned_as_the_response_reaches_the_client_as_it_arrives
    started = clock
    body = +""
    times = []
    Net::HTTP.start("127.0.0.1", PORT) do |http|
      http.request(Net::HTTP::Get.new("/proxy/chunks/3/200")) do |response|
        assert_equal "200", response.code
        response.read_body do |chunk|
          body << chunk
          times << clock - started
        end
      end
    end

    assert_equal "chunk 0\nchunk 1\nchunk 2\n", body
    assert_operator times.last - times.first, :>=, 0.3, "the client should not wait for the whole stream"
  end

  def test_a_proxied_stream_that_fails_midway_reaches_the_client_as_a_reset
    received = +""
    failure = begin
      Net::HTTP.start("127.0.0.1", PORT, read_timeout: 10) do |http|
        http.request(Net::HTTP::Get.new("/proxy/hang/proxied")) do |response|
          response.read_body { |chunk| received << chunk }
        end
      end
      nil
    rescue IOError, SystemCallError => error
      error
    end

    assert_equal "first\n", received
    refute_nil failure, "the client read a stream cut off by the idle timeout as complete"
    wait_for { self.class.upstream.hung_up?("proxied") }
  end

  def test_a_proxied_request_goes_up_with_the_body_it_came_with
    bodyless = proxied("/proxy/echo")
    assert_equal "GET", bodyless["method"]
    assert_empty bodyless["headers"].slice("content-length", "transfer-encoding").values.flatten,
                 "a GET has no body to frame"

    short = proxied("/proxy/digest", "hello")
    assert_equal [ 5, "length" ], short.values_at("bytes", "framing")

    payload = Random.new(7).bytes(300_000)
    long = proxied("/proxy/digest", payload)
    assert_equal [ payload.bytesize, Digest::SHA256.hexdigest(payload), "chunked" ],
                 long.values_at("bytes", "sha256", "framing")
  end

  def test_an_io_body_goes_up_with_its_length_unless_it_is_long
    bytes = STREAM_BUFFER_BYTES * 16
    long = streamed_json("method" => "POST", "path" => "/digest", "upload_bytes" => bytes)
    assert_equal [ bytes, Digest::SHA256.hexdigest("g" * bytes), "chunked" ],
                 long.values_at("bytes", "sha256", "framing"), "a body past the buffer goes up a piece at a time"

    short = streamed_json("method" => "POST", "path" => "/digest", "upload_bytes" => 10)
    assert_equal [ 10, "length" ], short.values_at("bytes", "framing")

    empty = streamed_json("method" => "POST", "path" => "/echo", "upload_bytes" => 0)
    assert_empty empty["headers"].fetch("transfer-encoding", []), "an empty body is sent as none"
  end

  def test_a_reader_that_falls_behind_overflows_the_buffer
    flood = 8 * 1024 * 1024
    result = stream("path" => "/flood/#{flood}", "pause" => 0.5)

    assert_equal "Ruvoy::UpstreamOverflow", result["error"], result.except("chunks").to_s
    assert_operator result["bytes"], :<, flood
  end

  def test_an_upstream_that_goes_quiet_is_given_up_and_hung_up_on_at_once
    request = Thread.new { stream("path" => "/hang/quiet", "idle_timeout" => 0.3, "linger" => 2) }
    wait_for(timeout: 1.5) { self.class.upstream.hung_up?("quiet") }
    quiet = request.value
    assert_equal "Ruvoy::UpstreamTimeout", quiet["error"], quiet.to_s
    assert_equal [ "first\n" ], quiet["chunks"]

    started = clock
    headless = stream("path" => "/delay/1500", "idle_timeout" => 0.3)
    assert_equal "Ruvoy::UpstreamTimeout", headless["error"], headless.to_s
    assert_operator clock - started, :<, 1.2
  end

  def test_closing_a_stream_early_hangs_up_on_the_upstream_at_once
    request = Thread.new { stream("path" => "/hang/closed", "take" => 1, "linger" => 2) }
    wait_for(timeout: 1.5) { self.class.upstream.hung_up?("closed") }
    result = request.value

    assert_equal [ "first\n" ], result["chunks"], result.to_s
    assert_nil result["error"]
  end

  def test_a_deadline_ends_the_exchange_however_far_it_got
    cut = stream("path" => "/chunks/8/150", "timeout" => 0.5)
    assert_equal "Ruvoy::UpstreamError", cut["error"], cut.to_s
    assert_includes 1..7, cut["chunks"].size

    before_head = stream("path" => "/delay/1500", "timeout" => 0.3)
    assert_equal 504, before_head["status"], before_head.to_s
  end

  def test_a_stream_outliving_its_request_is_hung_up_on
    assert_equal "first\n", post_json(PORT, "/abandon", "path" => "/hang/abandoned")["first"]

    wait_for { self.class.upstream.hung_up?("abandoned") }
    outcome = wait_for { get_json(PORT, "/state")["abandoned"].last }
    assert_equal "Ruvoy::UpstreamError", outcome["error"], outcome.to_s
    assert_includes outcome["message"], "request finished"
  end

  def test_a_client_leaving_a_proxied_stream_hangs_up_on_the_upstream
    socket = TCPSocket.new("127.0.0.1", PORT)
    socket.write("GET /proxy/hang/left HTTP/1.1\r\nhost: localhost\r\n\r\n")
    received = +""
    received << socket.readpartial(4096) until received.include?("first")
    socket.close

    wait_for { self.class.upstream.hung_up?("left") }
  end

  def test_streams_from_one_request_overlap
    overlapped = post_json(PORT, "/streams", Array.new(3) { { "path" => "/chunks/3/200" } })

    assert_equal [ "chunk 0\nchunk 1\nchunk 2\n" ] * 3, overlapped["results"].map { |result| result["chunks"]&.join },
                 overlapped.to_s
    assert_operator overlapped["elapsed"], :<, 0.9, "three 400ms streams should run together, not in turn"
  end

  def test_a_stream_that_cannot_be_opened_raises_where_it_was_asked_for
    refused = {
      { "cluster" => "not_listed", "path" => "/echo" } => [ "ArgumentError", "not one this filter" ],
      { "cluster" => "missing_cluster", "path" => "/echo" } => [ "Ruvoy::UpstreamError", "does not exist" ],
      { "path" => "/echo", "idle_timeout" => 0 } => [ "ArgumentError", "idle_timeout" ],
      { "path" => "/echo", "timeout" => -1 } => [ "ArgumentError", "timeout" ],
      { "path" => "/echo", "body" => 42 } => [ "ArgumentError", "body" ]
    }

    refused.each do |spec, (error, message)|
      result = stream(spec)
      assert_equal error, result["error"], "#{spec} => #{result}"
      assert_includes result["message"], message
    end
  end

  def test_many_streams_leave_nothing_behind
    outcomes = Array.new(8) do |thread|
      Thread.new do
        Net::HTTP.start("127.0.0.1", PORT) do |http|
          Array.new(25) do |index|
            request = Net::HTTP::Post.new("/stream", "content-type" => "application/json")
            # Every other stream is closed while the upstream is still sending.
            spec = index.even? ? { "path" => "/chunks/2/0" } : { "path" => "/hang/many-#{thread}-#{index}", "take" => 1 }
            request.body = JSON.generate(spec)
            JSON.parse(http.request(request).body).values_at("status", "error")
          end
        end
      end
    end.flat_map(&:value)

    assert_equal [ [ 200, nil ] ] * 200, outcomes
    assert_equal 0, get_json(PORT, "/state")["in_progress"]
    refute_match(/\[BUG\]|panicked/, File.read(self.class.envoy.log))
  end

  def test_a_websocket_application_on_rack_protocol_echoes_over_http1_and_http2
    [ Async::HTTP::Protocol::HTTP1, Async::HTTP::Protocol::HTTP2 ].each do |protocol|
      echoed = websocket("/ws/echo", protocol) do |connection|
        connection.send_text("ping")
        connection.send_binary("b" * 200_000)
        connection.flush
        [ connection.read.to_str, connection.read.to_str.bytesize ]
      end
      assert_equal [ "ping", 200_000 ], echoed, protocol.name
    end
    assert_no_live_upgrades
  end

  def test_a_bare_streaming_body_serves_an_upgrade
    key = SecureRandom.base64(16)
    socket, headers, rest = upgrade("/ws/raw", key)

    assert_equal "websocket", headers["upgrade"]
    assert_equal "upgrade", headers["connection"].downcase
    assert_equal Digest::SHA1.base64digest(key + UpstreamServer::WEBSOCKET_GUID), headers["sec-websocket-accept"]
    socket.write("hello")
    rest << socket.readpartial(16) while rest.bytesize < 5
    assert_equal "HELLO", rest
    socket.close_write
    assert_includes [ nil, "" ], (socket.read rescue nil), "the server should end once the client has"
    assert_no_live_upgrades
  ensure
    socket&.close
  end

  def test_a_streaming_body_also_answers_an_ordinary_request
    response = http_post("http://127.0.0.1:#{PORT}/duplex", "shout", {})
    assert_equal [ "200", "SHOUT" ], [ response.code, response.body ]
  end

  def test_a_protocol_the_client_did_not_offer_is_refused
    assert_equal "500", http_get("http://127.0.0.1:#{PORT}/unoffered").code
  end

  def test_a_websocket_relayed_upstream_round_trips_over_http1_and_http2
    [ Async::HTTP::Protocol::HTTP1, Async::HTTP::Protocol::HTTP2 ].each do |protocol|
      echoed = websocket("/relay/ws/echo", protocol) do |connection|
        connection.send_text("through")
        connection.send_binary("r" * 60_000)
        connection.flush
        [ connection.read.to_str, connection.read.to_str.bytesize ]
      end
      assert_equal [ "through", 60_000 ], echoed, protocol.name
    end
    assert_no_live_upgrades
  end

  def test_an_upgraded_upstream_reads_through_to_the_upstreams_close
    assert_equal({ "status" => 101, "bytes" => 11, "ending" => "eof" }, get_json(PORT, "/upgraded/ws/hello"),
                 "a text frame and a close frame, then the end rather than a failure")
  end

  def test_an_upgrade_request_names_its_protocols_in_rack_protocol
    response = http_get("http://127.0.0.1:#{PORT}/ws/protocols",
                        "connection" => "Upgrade", "upgrade" => "websocket",
                        "sec-websocket-version" => "13", "sec-websocket-key" => SecureRandom.base64(16))

    assert_equal [ "200", [ "websocket" ] ], [ response.code, JSON.parse(response.body) ]
  end

  def test_an_upgraded_stream_hands_over_what_arrived_before_its_overflow
    result = get_json(PORT, "/upgraded/ws/flood/overflowed?pause=0.5")

    assert_operator result["bytes"], :>, 0, "what arrived before the overflow should be read first"
    assert_match(/\ARuvoy::UpstreamOverflow/, result["ending"])
    wait_for { self.class.upstream.hung_up?("overflowed") }
  end

  def test_an_upstream_that_closes_closes_the_relayed_websocket
    messages = websocket("/relay/ws/hello", Async::HTTP::Protocol::HTTP1) do |connection|
      [ connection.read&.to_str, connection.read ]
    end
    assert_equal [ "hello", nil ], messages
    assert_no_live_upgrades
  end

  def test_a_client_dropping_a_websocket_ends_its_session
    socket, = upgrade("/ws/echo", SecureRandom.base64(16))
    socket.close

    assert_no_live_upgrades
  end

  def test_a_websocket_application_that_fails_ends_its_session
    first = websocket("/ws/raise", Async::HTTP::Protocol::HTTP1) do |connection|
      connection.send_text("x")
      connection.flush
      connection.read.to_str
    end

    assert_equal "x", first
    assert_no_live_upgrades
  end

  def test_a_server_closing_a_websocket_closes_the_client
    messages = websocket("/ws/bye", Async::HTTP::Protocol::HTTP1) { |connection| [ connection.read&.to_str, connection.read ] }

    assert_equal [ "bye", nil ], messages
    assert_no_live_upgrades
  end

  def test_a_relay_whose_client_falls_behind_is_cut_off
    socket, = upgrade("/relay/ws/flood/behind", SecureRandom.base64(16))
    # The client reads nothing while the upstream floods, so the stream's
    # budget overflows and both sides are let go.
    wait_for(timeout: 10) { self.class.upstream.hung_up?("behind") }
    received = 0
    begin
      received += socket.readpartial(65_536).bytesize while received < 512 * 1024 * 1024
    rescue IOError, SystemCallError
      nil
    end

    assert_operator received, :<, 512 * 1024 * 1024, "the client should have been cut off"
    assert_no_live_upgrades
  ensure
    socket&.close
  end

  def test_an_upstream_refusing_the_upgrade_answers_the_client_as_is
    socket = TCPSocket.new("127.0.0.1", PORT)
    socket.write(upgrade_request("/relay/status/404", SecureRandom.base64(16)))
    assert_match(%r{\AHTTP/1\.1 404}, socket.gets)
  ensure
    socket&.close
  end

  def test_a_client_leaving_a_relayed_websocket_hangs_up_on_the_upstream
    socket, = upgrade("/relay/ws/silent/left", SecureRandom.base64(16))
    socket.close

    wait_for { self.class.upstream.hung_up?("left") }
    assert_no_live_upgrades
  end

  def test_a_silent_upstream_is_given_up_and_the_client_reset
    started = clock
    socket, = upgrade("/relay/ws/silent/quiet?idle=0.5", SecureRandom.base64(16))
    ended = begin
      socket.read
    rescue IOError, SystemCallError
      nil
    end

    assert_includes [ nil, "" ], ended
    assert_operator clock - started, :<, 3, "the idle timeout should end the relay"
    wait_for { self.class.upstream.hung_up?("quiet") }
    assert_no_live_upgrades
  ensure
    socket&.close
  end

  # Shutting Envoy down stops the fibers parked in open WebSockets, so the
  # runtime stops without waiting them out and every client sees the end.
  def test_shutdown_ends_open_websockets_and_the_runtime_stops_cleanly
    offset = 10
    envoy, log = self.class.start_dedicated_envoy(offset, "shutdown")
    sockets = [ "/ws/echo", "/relay/ws/echo" ].map do |path|
      socket, = upgrade(path, SecureRandom.base64(16), port: PORT + offset)
      framer = Protocol::WebSocket::Framer.new(socket)
      framer.write_frame(Protocol::WebSocket::TextFrame.new(true, mask: SecureRandom.bytes(4)).pack("live"))
      assert_equal "live", framer.read_frame.unpack, "#{path} should be live before the shutdown"
      socket
    end

    stopped_in = elapsed { assert_kind_of Process::Status, envoy.stop(grace: 15), "Envoy had to be killed" }
    assert_operator stopped_in, :<, 5, "the shutdown waited on the parked fibers"
    text = File.read(log)
    assert_includes text, "Fiber runtime stopped"
    refute_match(/shutdown failed|panicked|\[BUG\]/, text)
    sockets.each do |socket|
      rest = (socket.read rescue nil)
      # The end is the connection closing, after at most one unmasked close frame.
      close_frame = rest.to_s.getbyte(0) == 0x88 && rest.bytesize == 2 + rest.getbyte(1)
      assert rest.to_s.empty? || close_frame, "the client should see the end, got #{rest.inspect}"
    end
  ensure
    sockets&.each(&:close)
    envoy&.stop
  end

  def test_many_relayed_websockets_leave_nothing_behind
    replies = Array.new(20) do |index|
      Thread.new do
        websocket("/relay/ws/echo", Async::HTTP::Protocol::HTTP1) do |connection|
          Array.new(3) do |round|
            connection.send_text("#{index}-#{round}")
            connection.flush
            connection.read.to_str
          end
        end
      end
    end.map(&:value)

    assert_equal Array.new(20) { |index| Array.new(3) { |round| "#{index}-#{round}" } }, replies
    refute_match(/\[BUG\]|panicked/, File.read(self.class.envoy.log))
    assert_no_live_upgrades
  end

  private

  def breaker_overflows
    admin_stat(ADMIN_PORT, "cluster.breaker.upstream_rq_pending_overflow").to_i
  end

  def call(arguments)
    post_json(PORT, "/call", arguments)
  end

  def prometheus
    http_get("http://127.0.0.1:#{ADMIN_PORT}/stats/prometheus").body
  end

  # Calls counted so far, by cluster and outcome.
  def upstream_calls
    prometheus.scan(/upstream_calls_total\{([^}]*)\} (\d+)/).to_h do |labels, value|
      [ labels.scan(/(\w+)="([^"]*)"/).to_h.values_at("cluster", "outcome"), value.to_i ]
    end
  end

  def stream(spec)
    post_json(PORT, "/stream", spec)
  end

  def assert_no_live_upgrades
    wait_for { get_json(PORT, "/state")["live_upgrades"].zero? }
  end

  def websocket(path, protocol, &)
    Async::WebSocket::Client.connect(Async::HTTP::Endpoint.parse("http://127.0.0.1:#{PORT}#{path}", protocol: protocol), &)
  end

  def upgrade_request(path, key)
    "GET #{path} HTTP/1.1\r\nhost: localhost\r\nconnection: Upgrade\r\nupgrade: websocket\r\n" \
      "sec-websocket-version: 13\r\nsec-websocket-key: #{key}\r\n\r\n"
  end

  # Opens an HTTP/1.1 upgrade by hand: the socket, the 101's headers, and
  # whatever arrived after them.
  def upgrade(path, key, port: PORT)
    socket = TCPSocket.new("127.0.0.1", port)
    socket.write(upgrade_request(path, key))
    received = +"".b
    received << socket.readpartial(4096) until received.include?("\r\n\r\n")
    head, rest = received.split("\r\n\r\n", 2)
    status, *fields = head.split("\r\n")
    assert_match(%r{\AHTTP/1\.1 101}, status)
    [ socket, fields.to_h { |field| field.split(": ", 2).then { |name, value| [ name.downcase, value ] } }, rest.to_s.b ]
  end

  def streamed_json(spec)
    result = stream(spec)
    assert_equal 200, result["status"], result.to_s
    JSON.parse(result.fetch("chunks").join)
  end

  def proxied(path, body = nil)
    url = "http://127.0.0.1:#{PORT}#{path}"
    response = body ? http_post(url, body, {}) : http_get(url)
    assert_equal "200", response.code, response.body
    JSON.parse(response.body)
  end

  def get_json(port, path, tls: false, client_certificate: false)
    http = Net::HTTP.new(tls ? "localhost" : "127.0.0.1", port)
    if tls
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_PEER
      http.ca_file = File.join(self.class.certificates, "ca.pem")
      if client_certificate
        http.cert = OpenSSL::X509::Certificate.new(File.read(File.join(self.class.certificates, "client.pem")))
        http.key = OpenSSL::PKey.read(File.read(File.join(self.class.certificates, "client-key.pem")))
      end
    end
    response = http.start { |connection| connection.get(path) }
    assert_equal "200", response.code, response.body
    JSON.parse(response.body)
  end

  def post_json(port, path, value)
    response = http_post("http://127.0.0.1:#{port}#{path}", JSON.generate(value), "content-type" => "application/json")
    assert_equal "200", response.code, response.body
    JSON.parse(response.body)
  end

  def wait_for(timeout: 5)
    deadline = clock + timeout
    loop do
      result = yield
      return result if result
      flunk "timed out waiting" if clock > deadline

      sleep 0.05
    end
  end
end
