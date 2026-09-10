# frozen_string_literal: true

require "digest"
require "json"
require "openssl"
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
            cluster("test_upstream", upstream.port),
            cluster("breaker", upstream.port).merge(
              "circuit_breakers" => {
                "thresholds" => [ { "max_connections" => 1, "max_pending_requests" => 1, "max_requests" => 1 } ]
              }
            ),
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
      filter_config = { "rackup" => File.join(root, "test", "fixtures", "extensions", "config.ru") }
      if extensions
        filter_config["context"] = { "metadata_namespaces" => %w[acme.tenant acme.route absent] }
        filter_config["upstream"] = {
          "clusters" => %w[test_upstream breaker unreachable mtls_upstream missing_cluster],
          "timeout_ms" => 2_000,
          "max_response_bytes" => MAX_RESPONSE_BYTES
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
    assert_equal "Ruvoy::UpstreamError", over["error"]
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
  # application hears it as an error rather than as a 503.
  def test_the_clusters_circuit_breaker_turns_away_the_call_over_its_limit
    overflows = breaker_overflows
    overlapped = post_json(PORT, "/calls", Array.new(2) { { "cluster" => "breaker", "path" => "/delay/300" } })
    served, refused = overlapped["results"].partition { |result| result["status"] == 200 }

    assert_equal [ 1, 1 ], [ served.size, refused.size ], overlapped.to_s
    assert_equal "Ruvoy::UpstreamError", refused.first["error"]
    assert_includes refused.first["message"], "turned the call away"
    assert_equal overflows + 1, breaker_overflows, "the breaker, not something else, must have refused it"
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

  private

  def breaker_overflows
    admin_stat(ADMIN_PORT, "cluster.breaker.upstream_rq_pending_overflow").to_i
  end

  def call(arguments)
    post_json(PORT, "/call", arguments)
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
