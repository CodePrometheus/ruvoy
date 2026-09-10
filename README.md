<p align="center">
  <img src="assets/ruvoy-logo.png" alt="Ruvoy" width="360">
</p>

<p align="center"><strong>Ruby, carried by Envoy.</strong></p>

<p align="center">
  <img alt="Rust 2024" src="https://img.shields.io/badge/Rust-2024-000000?logo=rust&logoColor=white">
  <img alt="Ruby 4.0.5" src="https://img.shields.io/badge/Ruby-4.0.5-CC342D?logo=ruby&logoColor=white">
  <img alt="Envoy 1.39.1" src="https://img.shields.io/badge/Envoy-1.39.1-AC6199?logo=envoyproxy&logoColor=white">
  <a href="LICENSE"><img alt="Apache License 2.0" src="https://img.shields.io/badge/License-Apache--2.0-blue.svg"></a>
</p>

<p align="center">English | <a href="README.zh-CN.md">简体中文</a> | <a href="README.ja.md">日本語</a></p>

Ruvoy is a Ruby application runtime inside Envoy. Envoy speaks HTTP, Ruby
speaks Rack, and owned Rust messages bridge the two.

## Install

```console
$ gem install ruvoy
```

The gem carries the dynamic module and the Envoy binary, so there is nothing
else to install and no version to match by hand.

| | |
|---|---|
| Ruby | 4.0.x. The module links against the Ruby it was built for, so the gem is published per ABI. |
| Platform | Linux `x86_64` and `aarch64`, matching the platforms Envoy publishes binaries for. |
| Envoy | Bundled (1.39.1, dynamic module ABI 0.1.0). Not installed separately. |

## Quickstart

```console
$ ruvoy examples/hello/config.ru
$ curl localhost:8080/
hello from ruby 4.0.5
```

`ruvoy` takes a rackup and serves it. It generates the Envoy configuration and
replaces itself with Envoy, so signals and exit status belong to the proxy and
a container needs no process supervisor.

```console
$ ruvoy --help
Usage: ruvoy [options] [config.ru]
    -p, --port PORT                  Listen on PORT (default 8080)
    -a, --address ADDRESS            Bind to ADDRESS (default 127.0.0.1)
        --admin-port PORT            Expose Envoy's admin interface on PORT
        --print-config               Print the generated Envoy configuration and exit
```

To run it under an Envoy you configure yourself, `--print-config` prints the
listener Ruvoy would have used; the module is `ruvoy_fiber` and its filter
config is the path to the rackup, or a JSON object naming it (see
[Envoy context and upstream calls](#envoy-context-and-upstream-calls)).

## Status

The data path is implemented and covered by suites that run against a real
Envoy in CI. Configuration reload has been driven through a running Envoy, and
a one-hour soak of 1.8 million requests left the heap, file descriptors and
threads flat. Interfaces may change.

## Features

- **Envoy-native HTTP** — HTTP/1.1, HTTP/2, connections, timeouts, and
  downstream lifecycle remain under Envoy's control.
- **Rack applications** — load an ordinary `config.ru` and receive a Rack
  environment built from the Envoy request.
- **Fiber concurrency** — scheduler-aware Ruby I/O overlaps on a dedicated
  CRuby owner thread without entering Ruby from Envoy workers.
- **Owned-data boundary** — only Rust-owned requests and responses cross
  threads; Ruby objects never do.
- **Streaming responses** — response bodies are delivered chunk by chunk as the
  application produces them, with downstream backpressure applied to the Ruby
  producer instead of buffering.
- **Streaming requests** — the application is called as soon as the headers
  arrive and reads the body as it lands, with a client the application cannot
  keep up with slowed down instead of buffered.
- **Bounded admission** — a request-count budget rejects excess work before it
  reaches Ruby.
- **No upstream HTTP hop** — Rack calls do not require a loopback connection to
  a separate Ruby application server.
- **Envoy context and upstream calls** — optionally, the application sees the
  route, connection, TLS and metadata Envoy has for the request, and calls
  Envoy's clusters from inside it.

## With and without Ruvoy

| | Without Ruvoy | With Ruvoy |
|---|---|---|
| Request path | Envoy → upstream HTTP → Ruby server | Envoy → owned Rust message → Ruby |
| Ruby host | Separate server process or processes | Dedicated CRuby owner thread inside Envoy |
| Protocol handoff | Request is serialized onto another HTTP connection | Request crosses the worker boundary as owned data |
| I/O concurrency | Server threads or workers | Async fibers on the Ruby owner thread |
| Failure boundary | Envoy and Ruby are separate processes | Envoy and Ruby share one process |
| CPU-bound Ruby | Multiple Ruby workers can use multiple cores | One CRuby VM remains limited by the GVL |

Ruvoy is designed for scheduler-aware I/O and short CPU-light Rack work. It
does not make CPU-bound Ruby execute in parallel.

## Architecture

```text
client
  → Envoy worker
  → owned Rust request
  → CRuby owner thread
  → Async Fiber
  → owned Rust response
  → original Envoy worker
  → client
```

Envoy workers never acquire a Ruby handle or enter the VM. The Ruby owner
thread runs the Rack application, while the result is committed back onto the
worker that owns the downstream stream.

## Current scope

Ruvoy constructs the Rack environment, provides binary `rack.input`, preserves
repeated response headers, consumes enumerable response bodies, and calls
`close` when the body supports it.

Response bodies stream incrementally: headers are sent once and each chunk is
forwarded as the application yields it, so the first bytes reach the client
before the body is complete. A slow client applies backpressure to the Ruby
producer rather than accumulating in memory, and a client that disconnects
stops the enumeration and closes the body.

Request bodies stream as well: the application is called once the headers
arrive and reads through `rack.input` as the body lands, so uploading and
processing overlap. What the application has not read yet waits in Envoy, which
stops reading from the client until there is room again. Raw socket hijacking
(`rack.hijack`) is not supported; the connection always belongs to Envoy.

A response body that fails after its headers are already on the wire can only
stop: a dynamic module cannot reset a stream it has started answering, so the
client receives a short body rather than an error. Applications that can fail
partway through should send their own length or checksum.

## Envoy context and upstream calls

Two optional capabilities give the application more of Envoy than a Rack
request carries. Both stay off unless the filter configuration asks for them,
and both are available on the fiber runtime only. The configuration is then a
JSON object:

```json
{
  "rackup": "/srv/app/config.ru",
  "context": { "metadata_namespaces": ["acme.tenant"] },
  "upstream": { "clusters": ["users"], "timeout_ms": 15000, "max_response_bytes": 1048576 }
}
```

### `ruvoy.context`

Envoy's view of the request, copied on the worker when the headers arrive.
Ruby values are only built when the application reads them.

```ruby
context = env["ruvoy.context"]
context.route_name        # => "api", or nil for an unnamed route
context.connection        # => {"id"=>7, "source_address"=>"10.0.0.7", "source_port"=>51234, ...}
context.tls               # => nil on plaintext, else {"version"=>"TLSv1.3", "server_name"=>..., "peer_certificate"=>...}
context.dynamic_metadata  # => {"acme.tenant"=>{"id"=>"t-42"}}, from filters ahead of Ruvoy
context.route_metadata    # => the same namespaces, from the matched route
context.to_h              # => all of the above
```

What Envoy's module interface can report sets some limits:

- A route has a name only if the route configuration gives it one. A terminal
  filter still needs a route entry to be named: give it
  `non_forwarding_action: {}`.
- `server_name` is the SNI the client sent, which Envoy records only when the
  listener has the TLS inspector listener filter.
- `peer_certificate` means the client presented a certificate. It identifies
  the client only on a listener that requires client certificates signed by a
  trusted CA.
- Only the first URI and the first DNS subject alternative name are reported.
- Metadata values are strings, numbers (always Float), booleans and lists of
  those. Nested structures are left out.

### `ruvoy.upstream`

Calls a cluster Envoy was configured with and returns a Rack-style triple:

```ruby
status, headers, body = env["ruvoy.upstream"].call(
  "users", "GET", "/users/42",
  headers: { "accept" => "application/json" }, timeout: 2
)
```

The request's worker sends the call through the cluster, so the cluster's
connection pool, TLS settings and circuit breakers apply, and headers such as
`x-envoy-retry-on` ask Envoy to retry. The calling fiber waits without holding
the runtime thread, so other requests keep being served and calls made from
one request overlap. A repeated response header arrives as an Array.

- Only the clusters in `upstream.clusters` can be called; any other raises
  `ArgumentError`.
- Answers Envoy produces while the call is under way arrive as ordinary
  responses: 504 when it timed out, 503 when the upstream could not be reached
  or hung up.
- `Ruvoy::UpstreamError` is raised when the call could not be sent: the cluster
  does not exist, or turned the call away at once because it has no healthy
  host or a circuit breaker is open. It is also raised when the response body is
  over `max_response_bytes`, and when the request finished before the call did.
- Responses are buffered whole, so `max_response_bytes` bounds what one call
  can hold in memory.

[`examples/extensions`](examples/extensions) runs both against a stand-in
service, including two calls from one request overlapping.

## Scaling

Each Ruvoy process runs exactly one CRuby VM, so Ruby work in one process is
limited to one core by the GVL. Scaling out means more processes, not more
threads: run one Ruvoy per replica and scale horizontally as with any
single-VM Ruby server.

## Benchmarks

### Environment

| Component | Configuration |
|---|---|
| Machine | 4 vCPU, 32 GiB memory, x86_64 |
| System | Linux |
| Envoy | `1.39.0`, concurrency `1` |
| Ruby | `4.0.5` |
| Rust | `1.97.1` |
| Rack | `3.2.6` |
| Load generator | oha `1.15.0` |

### Mixed stress

This run tested admission control, recovery, client cancellation, and resource
stability. It used three complete cycles:

- steady traffic: 45,000 HTTP/2 no-op requests, 6,000 HTTP/1.1 requests with a
  200 ms wait, and 1,200 HTTP/1.1 requests with a 256 KiB body per cycle;
- request-count overload: 15 seconds with at most 256 admitted requests;
- aggregate-body overload: 15 seconds with a 64 MiB admitted-body budget;
- client cancellation: 128 deliberately cancelled requests;
- recovery: 20,000 no-op and 2,000 waiting requests per cycle.

| Result | Value |
|---|---:|
| Total requests | `2,503,763` |
| HTTP 200 | `251,400` |
| Expected overload HTTP 503 | `2,252,363` |
| Transport errors | `0` |
| Steady and recovery failures | `0` |
| Control-listener maximum latency | `35.615 ms` |
| File descriptors after every cycle | `59` |
| Ruby live-heap change, recovered cycle 1 → 3 | `+2 slots` |
| Peak RSS during body overload | `489,332 KiB` |

Every steady and recovery request returned HTTP 200. All HTTP 503 responses
were generated during deliberate overload and are reported as rejected work,
not successful throughput.

The load generator ran on the same machine, and the server did not saturate
the host CPU. This result therefore supports resilience and bounded-overload
claims only; it is not a maximum-throughput measurement.

### Server comparison

A fixed-commit comparison of Ruvoy, direct Falcon `0.55.6`, direct Puma
`7.2.0`, and Envoy → Puma, all serving the same Rack fixture:

- oha ran on a separate 2 vCPU host over a local network (RTT ≈ 0.03 ms), so
  the load generator never competed with the servers for CPU.
- Each measurement is 10 s warm-up plus 30 s under load; 7 rounds per cell
  with the architecture order rotated every round.
- A round only counts if every request returned HTTP 200 with zero transport
  errors. Reported values are medians with relative standard deviation.
- Every architecture runs one Ruby execution context: Puma in single mode
  with 100 threads, one Falcon instance, one Ruvoy owner thread, and Envoy
  `concurrency 1`.
- Judgement was fixed before the run: an advantage is claimed only when both
  throughput and p99 improve by more than the larger of 5% and the observed
  round-to-round variance.

No-op Rack application, HTTP/1.1, 100 connections, plaintext:

| Server | RPS median (RSD) | p50 ms | p99 ms | CPU | RSS MiB |
|---|---:|---:|---:|---:|---:|
| Ruvoy | `31,270` (1.9%) | `3.06` | `5.01` | 189% | 94 |
| Falcon | `12,716` (0.5%) | `7.67` | `9.63` | 99% | 83 |
| Puma | `10,410` (5.5%) | `8.84` | `20.96` | 87% | 127 |
| Envoy → Puma | `9,754` (4.0%) | `9.92` | `17.63` | 159% | 185 |

The same workload with TLS 1.3, terminated by Envoy for Ruvoy and
Envoy → Puma and in-process by Falcon and Puma:

| Server | RPS median (RSD) | p50 ms | p99 ms | CPU | RSS MiB |
|---|---:|---:|---:|---:|---:|
| Ruvoy | `32,065` (2.9%) | `2.98` | `4.85` | 192% | 95 |
| Falcon | `11,401` (3.0%) | `8.73` | `10.61` | 99% | 91 |
| Envoy → Puma | `9,674` (4.4%) | `9.97` | `17.44` | 162% | 191 |
| Puma | `8,728` (6.7%) | `9.87` | `25.52` | 87% | 148 |

Falcon is the comparison that isolates the architecture: both are
fiber-per-request, so the difference is carrying requests as owned data
inside one process instead of over a loopback HTTP hop. Against Falcon,
Ruvoy holds `+146%` throughput with `−48%` p99 in plaintext and `+181%`
with `−54%` under TLS — both past the pre-registered noise threshold.
Against Envoy → Puma the margin is `+221%` and `+231%`.

Four honest qualifications:

- The no-op scenario measures per-request framework overhead, so these are
  ceiling numbers. As real application time grows, the relative margin
  shrinks: with a 200 ms simulated wait at 100 connections, all four servers
  sit at the same ~495 RPS concurrency limit.
- Ruvoy's CPU column includes the Envoy worker handling HTTP in the same
  process (~1.9 cores versus Falcon's ~1.0). Per core the margin is roughly
  `+29%`; the rest comes from pairing the Ruby thread with a proxy-grade
  HTTP front end.
- Every number here comes from an application that yields while it waits. A
  driver written as a C extension releases the interpreter lock but not the
  fiber, so a single such call stalls every other request on the thread. The
  `wait50` and `block50` scenarios measure that difference directly; they are
  not part of the campaign above.
- A 1 MiB-response scenario was also run and produced no ranking: above
  ~1,000 such responses per second all four architectures converged on the
  network path's throughput limit with 59–96% variance, so that scenario
  measures the link, not the servers.

Not measured: CPU-bound Rack, real database or HTTP-client drivers, and
multi-process deployment.

Measurements were produced by `bench/run.rb`, which enforces the
protocol above — remote load generation, rotation, warm-up, per-round
validation, and the pre-registered judgement — and writes raw oha output,
CPU and RSS samples, and the generated summary for every run.

## License

Apache License 2.0. See [LICENSE](LICENSE).
