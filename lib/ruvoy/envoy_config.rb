# frozen_string_literal: true

require "yaml"

module Ruvoy
  # Builds the Envoy bootstrap that serves one Rack application.
  #
  # The timeouts are not defaults worth inheriting: a Ruby fiber that never
  # finishes writes nothing to the stream, so without an explicit bound the
  # request would hang for Envoy's five-minute default.
  module EnvoyConfig
    STREAM_IDLE_TIMEOUT = "15s"
    REQUEST_TIMEOUT = "30s"
    CONNECTION_BUFFER_LIMIT_BYTES = 4_194_304

    module_function

    def build(rackup:, address:, port:, admin_port: nil)
      config = { "static_resources" => { "listeners" => [ listener(rackup, address, port) ] } }
      config["admin"] = admin(address, admin_port) if admin_port
      config
    end

    def to_yaml(**options)
      YAML.dump(build(**options))
    end

    def listener(rackup, address, port)
      {
        "name" => "ruvoy",
        "per_connection_buffer_limit_bytes" => CONNECTION_BUFFER_LIMIT_BYTES,
        "address" => { "socket_address" => { "address" => address, "port_value" => port } },
        "filter_chains" => [ { "filters" => [ connection_manager(rackup) ] } ]
      }
    end

    def connection_manager(rackup)
      {
        "name" => "envoy.filters.network.http_connection_manager",
        "typed_config" => {
          "@type" => "type.googleapis.com/envoy.extensions.filters.network." \
                     "http_connection_manager.v3.HttpConnectionManager",
          "stat_prefix" => "ruvoy",
          "codec_type" => "AUTO",
          "stream_idle_timeout" => STREAM_IDLE_TIMEOUT,
          "request_timeout" => REQUEST_TIMEOUT,
          "route_config" => {
            "name" => "local_route",
            "virtual_hosts" => [ { "name" => "local_service", "domains" => [ "*" ] } ]
          },
          "http_filters" => [ rack_filter(rackup) ]
        }
      }
    end

    # Terminal: the Rack application answers the request, so no router follows.
    def rack_filter(rackup)
      {
        "name" => "envoy.extensions.filters.http.dynamic_modules",
        "typed_config" => {
          "@type" => "type.googleapis.com/envoy.extensions.filters.http." \
                     "dynamic_modules.v3.DynamicModuleFilter",
          "dynamic_module_config" => { "name" => "ruvoy_fiber", "do_not_close" => true },
          "filter_name" => "fiber_rack",
          "terminal_filter" => true,
          "filter_config" => {
            "@type" => "type.googleapis.com/google.protobuf.StringValue",
            "value" => rackup
          }
        }
      }
    end

    def admin(address, port)
      {
        "address" => { "socket_address" => { "address" => address, "port_value" => port } }
      }
    end
  end
end
