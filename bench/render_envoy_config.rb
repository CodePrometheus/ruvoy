# frozen_string_literal: true

# Renders a benchmark copy of an Envoy config.
#
# Listener addresses must follow the benchmark listen address so a remote load
# generator can reach them, while cluster endpoints must keep pointing at the
# co-located upstream. A textual substitution cannot tell those apart, so the
# rewrite walks the parsed document instead.

require "yaml"

source, destination, listen_address, repo_root, certificate_path, private_key_path = ARGV
abort "usage: render_envoy_config.rb SOURCE DEST LISTEN_ADDRESS REPO_ROOT [CERT KEY]" unless repo_root

# TLS is attached to the benchmarked listeners only. The control listener exists
# to show that Ruby load does not block the Envoy worker, which is unrelated to
# TLS, so keeping it cleartext leaves that baseline comparable across modes.
def downstream_tls_context(certificate_path, private_key_path)
  {
    "name" => "envoy.transport_sockets.tls",
    "typed_config" => {
      "@type" =>
        "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext",
      "common_tls_context" => {
        # Falcon and Puma are pinned to TLS 1.3 as well; comparing a TLS 1.2
        # handshake against a TLS 1.3 one would measure the protocol.
        "tls_params" => {
          "tls_minimum_protocol_version" => "TLSv1_3",
          "tls_maximum_protocol_version" => "TLSv1_3"
        },
        "tls_certificates" => [
          {
            "certificate_chain" => { "filename" => certificate_path },
            "private_key" => { "filename" => private_key_path }
          }
        ],
        "alpn_protocols" => [ "h2", "http/1.1" ]
      }
    }
  }
end

def absolutize_rackup(node, repo_root)
  case node
  when Hash
    node.each do |key, value|
      if key == "value" && value.is_a?(String) && value.end_with?(".ru") &&
         !value.start_with?("/")
        node[key] = File.join(repo_root, value)
      else
        absolutize_rackup(value, repo_root)
      end
    end
  when Array
    node.each { |item| absolutize_rackup(item, repo_root) }
  end
end

config = YAML.safe_load_file(source)
listeners = config.fetch("static_resources").fetch("listeners")
listeners.each do |listener|
  listener.fetch("address").fetch("socket_address")["address"] = listen_address
  next unless certificate_path
  next if listener.fetch("name").include?("control")

  listener.fetch("filter_chains").each do |filter_chain|
    filter_chain["transport_socket"] =
      downstream_tls_context(certificate_path, private_key_path)
  end
end
absolutize_rackup(config, repo_root)

File.write(destination, YAML.dump(config))
