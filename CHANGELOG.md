# Changelog

Notable changes to the `ruvoy` gem, newest first. Changes not yet released are
listed under Unreleased. The `ruvoy-envoy` gem carries an Envoy release as it
is, and its version is that Envoy version.

## Unreleased

### Changed

- The `ruvoy` bootstrap extension is required. It names the rackup and the
  runtime's settings, `max_inflight_requests`, `shutdown_timeout_ms` and
  `diagnostics`, which the filter configuration and the
  `RUVOY_MAX_INFLIGHT_REQUESTS`, `RUVOY_SHUTDOWN_TIMEOUT_MS` and
  `RUVOY_DIAGNOSTICS` variables carried before. A rack filter configured
  without it, or still naming a rackup, is refused. `ruvoy` and
  `--print-config` generate it.
- The generated Envoy configuration sets `memory_allocator_manager` to return
  1 MiB of freed memory a second. Envoy's allocator keeps freed memory by
  default, which a three-hour soak showed as a resident set growing about 3%
  an hour with no growth in what was allocated.
- `inflight_requests`, `reactor_idle_ms` and `resident_bytes` are read once a
  second on the main thread instead of being written on every request, so the
  request path publishes nothing and an idle proxy no longer shows a stale
  value.

### Added

- Envoy becomes ready only once the application has loaded and its
  `Ruvoy::Lifecycle.before_ready` hooks have run. `Ruvoy::Lifecycle.on_drain`
  hooks run when Envoy begins draining, and Envoy's own shutdown stops the
  runtime.
- Admin endpoints answered by Ruby: `/ruvoy`, `/ruvoy/gc`, `/ruvoy/fibers`
  and `/ruvoy/config`.
- Statistics for calls to clusters: `upstream_calls_total{cluster, outcome}`
  counts how each call ended as the application saw it, and
  `upstream_duration_ms{cluster}` times a call to its whole response and a
  stream to its head.
- Calls and streams carry the request's trace context on: `traceparent`,
  `tracestate`, `x-request-id` and the B3 headers, unless the call sets them.

## 0.3.0 - 2026-10-15

### Added

- Protocol upgrades such as WebSocket, through Rack 3's `rack.protocol`: an
  application accepts one by naming the protocol in its response headers and
  answering with a streaming body, which gets a duplex stream to the client.
  Works over HTTP/1.1 and HTTP/2 once Envoy allows the upgrade
  (`upgrade_configs`, plus `allow_connect` for HTTP/2).
- A streaming body, one that responds to `call`, can answer any request. The
  stream it is given reads the request body and writes the response.
- `Upstream#stream` takes `protocol:`, such as `"websocket"`. When the upstream
  answers `101`, returning the response upgrades the client as well and relays
  bytes both ways until either side closes.

### Fixed

- A fiber still reading a request body is woken when Envoy destroys the stream,
  for example at shutdown, instead of waiting out the runtime's shutdown
  timeout.

## 0.2.1 - 2026-10-15

### Added

- `Upstream#stream` takes the same arguments as `call` but returns once the
  response head arrives, with a body that streams and can be returned as the
  Rack response. A request body may be any IO, such as `rack.input`. A reader
  that falls more than `upstream.stream_buffer_bytes` (1 MiB by default) behind
  has the stream reset and gets `Ruvoy::UpstreamOverflow`.

### Changed

- A streaming response body waiting for the client to catch up is woken when
  there is room, instead of polling for it.

### Fixed

- A response body that fails after its headers are sent resets the stream, so
  the client sees an error instead of a short response that looks complete.

## 0.2.0 - 2026-10-14

### Added

- `env["ruvoy.context"]`: Envoy's view of the request, including the route
  name, the connection, TLS, and dynamic and route metadata. Enabled by
  `context` in the filter configuration.
- `env["ruvoy.upstream"]`: `call` sends a request through an Envoy cluster
  listed in `upstream.clusters` and returns a Rack-style triple, while the
  calling fiber waits without holding the runtime thread. A call that cannot be
  sent raises `Ruvoy::UpstreamError`, and a response over `max_response_bytes`
  raises `Ruvoy::UpstreamResponseTooLarge`.
- `Ruvoy.offload` runs a blocking call on its own thread while the calling
  fiber waits; `Ruvoy::Offload.new(limit:)` bounds how many run at once.

### Changed

- The Rack environment reuses interned strings for its keys.

### Fixed

- `rack.input` can be rewound and read again, as Rails applications expect.
  What has been read is kept in memory up to 128 KiB and in a temporary file
  beyond that.
- A chunk larger than the room left in a streaming response's buffer is split
  to fit, instead of exceeding the buffer.
- A request stopped while waiting for its body, for example because the client
  went away, no longer leaves its waiter behind.

## 0.1.0 - 2026-09-08

First release.

- Runs a Rack application from `config.ru` inside Envoy, as a dynamic module
  with a dedicated Ruby thread where requests run as fibers. HTTP/1.1, HTTP/2
  and connections stay under Envoy's control.
- Streams response and request bodies, with backpressure in both directions.
- Rejects work beyond a request-count budget before it reaches Ruby.
- Stops the fiber of a request whose client has gone away, and closes its
  response body.
- Ships as the `ruvoy` gem, which carries the module and a command line, and
  the `ruvoy-envoy` gem, which carries Envoy.
