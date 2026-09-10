# frozen_string_literal: true

# What a Rack application can reach beyond the Rack environment once the filter
# turns the extensions on: Envoy's view of the request, and Envoy's clusters.
# envoy.yaml next to this file turns both on and stands up a `users` service.
#
#   ENVOY_DYNAMIC_MODULES_SEARCH_PATH=build/modules envoy -c examples/extensions/envoy.yaml
#   curl localhost:8080/whoami
#   curl localhost:8080/users/42
#   curl -w '%{time_total}s\n' localhost:8080/profile/42

require "json"

application = lambda do |env|
  context = env.fetch("ruvoy.context")
  upstream = env.fetch("ruvoy.upstream")

  case env.fetch("PATH_INFO")
  when "/whoami"
    # Envoy copied this when the headers arrived; reading it builds plain Ruby values.
    json(context.to_h)
  when %r{\A/users/(\d+)\z}
    # Envoy sends the call through the `users` cluster, so its connection pool,
    # TLS settings, retries and circuit breakers apply.
    status, _headers, body = upstream.call("users", "GET", "/users/#{$1}")
    [ status, { "content-type" => "application/json" }, [ body ] ]
  when %r{\A/profile/(\d+)\z}
    # A waiting call holds only its fiber, so both of these are in flight at
    # once: the page takes one upstream delay, not two.
    user, orders = [ "/users/#{$1}", "/users/#{$1}/orders" ].map do |path|
      Async { JSON.parse(upstream.call("users", "GET", path, timeout: 1).last) }
    end.map(&:wait)
    json("tenant" => context.dynamic_metadata.dig("acme.tenant", "id"), "user" => user, "orders" => orders)
  else
    [ 404, { "content-type" => "text/plain" }, [ "not found\n" ] ]
  end
rescue Ruvoy::UpstreamError => error
  [ 502, { "content-type" => "text/plain" }, [ "#{error.message}\n" ] ]
end

def json(payload)
  [ 200, { "content-type" => "application/json" }, [ "#{JSON.generate(payload)}\n" ] ]
end

run application
