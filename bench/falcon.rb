# frozen_string_literal: true

# Falcon service definition for the benchmark.
#
# `falcon serve` cannot be used for the TLS runs: its endpoint options only carry
# (hostname, port, timeout), so a caller cannot inject an SSL context, and it
# falls back to a self-signed RSA certificate from the localhost gem. Both TLS
# and plain runs therefore go through this file, so Falcon's process structure is
# identical in both modes.
#
# The SSL context is built here rather than via Falcon::Environment::TLS because
# that module pins ssl_version to TLSv1_2_server, while Envoy negotiates TLS 1.3.
# Comparing a TLS 1.2 handshake against a TLS 1.3 handshake would measure the
# protocol, not the server.

require "falcon/environment/server"
require "falcon/environment/rackup"

falcon_url = ENV.fetch("RUVOY_FALCON_URL")
falcon_count = Integer(ENV.fetch("RUVOY_FALCON_COUNT"))
falcon_rackup = ENV.fetch("RUVOY_FALCON_RACKUP")
falcon_root = ENV.fetch("RUVOY_FALCON_ROOT")
certificate_path = ENV["RUVOY_FALCON_TLS_CERTIFICATE"]
private_key_path = ENV["RUVOY_FALCON_TLS_KEY"]

# A lambda rather than a method: the service DSL turns each block into a method
# on an anonymous evaluator class, so top-level methods are not in scope there.
# A local variable stays reachable through the block's closure.
build_ssl_context = lambda do
  certificate = OpenSSL::X509::Certificate.new(File.read(certificate_path))
  private_key = OpenSSL::PKey::RSA.new(File.read(private_key_path))

  OpenSSL::SSL::SSLContext.new.tap do |context|
    context.add_certificate(certificate, private_key)
    context.min_version = OpenSSL::SSL::TLS1_3_VERSION
    context.max_version = OpenSSL::SSL::TLS1_3_VERSION
    context.alpn_select_cb = lambda do |protocols|
      return "h2" if protocols.include?("h2")
      return "http/1.1" if protocols.include?("http/1.1")

      nil
    end
    # No context.setup here: it freezes the context, and SSLServer still needs to
    # configure it when the listener is created.
  end
end

service "ruvoy-benchmark" do
  include Falcon::Environment::Server
  include Falcon::Environment::Rackup

  root falcon_root
  rackup_path falcon_rackup
  url falcon_url
  count falcon_count
  restart false

  if certificate_path
    ssl_context(&build_ssl_context)

    # Environment::Server builds the endpoint without an SSL context, so the
    # context has to be attached here or the listener would serve cleartext.
    endpoint do
      ::Async::HTTP::Endpoint.parse(url, ssl_context: ssl_context)
        .with(**endpoint_options)
    end
  end
end
