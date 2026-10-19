# frozen_string_literal: true

require "json"
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
    EXTENSION_NAME = "ruvoy"
    STRING_VALUE = "type.googleapis.com/google.protobuf.StringValue"

    module_function

    def build(rackup:, address:, port:, admin_port: nil)
      config = {
        "bootstrap_extensions" => [ bootstrap_extension(rackup: rackup) ],
        "memory_allocator_manager" => memory_allocator_manager,
        "static_resources" => { "listeners" => [ listener(address, port) ] }
      }
      config["admin"] = admin(address, admin_port) if admin_port
      config
    end

    def to_yaml(**options)
      YAML.dump(build(**options))
    end

    # The extension that starts the Ruby runtime and names the application.
    # Every listener's rack filter joins the runtime it started, so there is
    # exactly one per Envoy.
    def bootstrap_extension(rackup:, **settings)
      {
        "name" => EXTENSION_NAME,
        "typed_config" => {
          "@type" => "type.googleapis.com/envoy.extensions.bootstrap.dynamic_modules.v3." \
                     "DynamicModuleBootstrapExtension",
          "dynamic_module_config" => module_config,
          "extension_name" => EXTENSION_NAME,
          "extension_config" => {
            "@type" => STRING_VALUE,
            "value" => JSON.generate({ "rackup" => rackup }.merge(settings.transform_keys(&:to_s)))
          }
        }
      }
    end

    # The same as a top-level YAML section, to head a configuration written
    # by hand.
    def bootstrap_yaml(rackup:, **settings)
      YAML.dump("bootstrap_extensions" => [ bootstrap_extension(rackup: rackup, **settings) ])
          .delete_prefix("---\n")
    end

    def listener(address, port)
      {
        "name" => "ruvoy",
        "per_connection_buffer_limit_bytes" => CONNECTION_BUFFER_LIMIT_BYTES,
        "address" => { "socket_address" => { "address" => address, "port_value" => port } },
        "filter_chains" => [ { "filters" => [ connection_manager ] } ]
      }
    end

    def connection_manager
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
          "http_filters" => [ rack_filter ]
        }
      }
    end

    # Terminal: the Rack application answers the request, so no router follows.
    # The filter has nothing of its own to configure here; the application is
    # the bootstrap extension's.
    def rack_filter
      {
        "name" => "envoy.extensions.filters.http.dynamic_modules",
        "typed_config" => {
          "@type" => "type.googleapis.com/envoy.extensions.filters.http." \
                     "dynamic_modules.v3.DynamicModuleFilter",
          "dynamic_module_config" => module_config,
          "filter_name" => "fiber_rack",
          "terminal_filter" => true,
          "filter_config" => { "@type" => STRING_VALUE, "value" => "{}" }
        }
      }
    end

    # Envoy's allocator keeps what Ruby and Envoy free unless told to return
    # it, so the resident set would only ever grow. Its own background thread
    # returns a megabyte a second, more than a busy process frees.
    def memory_allocator_manager
      { "bytes_to_release" => 1_048_576, "memory_release_interval" => "1s" }
    end

    # A fresh hash each time: one object in two places would come out of the
    # YAML dumper as an anchor and an alias.
    def module_config
      { "name" => "ruvoy_fiber", "do_not_close" => true }
    end

    def admin(address, port)
      {
        "address" => { "socket_address" => { "address" => address, "port_value" => port } }
      }
    end
  end
end
