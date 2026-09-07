#!/usr/bin/env bash

# Verifies that Rack response bodies reach the client incrementally.
#
# The distinguishing evidence is timing: with a buffered server the first byte
# cannot arrive before the last chunk was produced, so a first-byte time well
# below the total production time is what proves streaming.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_dir="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}/streaming-$run_id"
build_profile="${RUVOY_BUILD_PROFILE:-release}"
envoy_package="envoy-server==1.39.0"
ruby_version="$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
ruby_bin="${RUVOY_RUBY:-"$HOME/.rbenv/versions/$ruby_version/bin/ruby"}"
temporary_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-streaming.XXXXXX")"
port=18101
envoy_pid=""
failures=0

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  failures=$((failures + 1))
}

pass() {
  printf 'PASS: %s\n' "$*"
}

process_is_alive() {
  local pid="$1"
  local state

  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null || return 1
  state="$(ps -o stat= -p "$pid" 2>/dev/null | tr -d '[:space:]')"
  [[ -n "$state" && "$state" != Z* ]]
}

cleanup() {
  if [[ -n "$envoy_pid" ]]; then
    kill -INT "$envoy_pid" 2>/dev/null || true
    for _ in $(seq 1 100); do
      process_is_alive "$envoy_pid" || break
      sleep 0.05
    done
    kill -KILL "$envoy_pid" 2>/dev/null || true
    wait "$envoy_pid" 2>/dev/null || true
  fi
  rm -rf "$temporary_dir"
}
trap cleanup EXIT

mkdir -p "$result_dir"
[[ -x "$ruby_bin" ]] || {
  printf 'FAIL: missing Ruby %s; set RUVOY_RUBY\n' "$ruby_version" >&2
  exit 1
}
RUVOY_RUBY="$ruby_bin" RUVOY_BUILD_PROFILE="$build_profile" \
  "$repo_root/scripts/build-fiber-module.sh" >"$result_dir/build.log" 2>&1 || {
  printf 'FAIL: fiber module build failed; see %s\n' "$result_dir/build.log" >&2
  exit 1
}

cat >"$temporary_dir/streaming.yaml" <<YAML
static_resources:
  listeners:
    - name: ruvoy_streaming
      per_connection_buffer_limit_bytes: 65536
      address:
        socket_address:
          address: 127.0.0.1
          port_value: $port
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
                        value: $repo_root/test/fixtures/rack/config.ru
YAML

BUNDLE_GEMFILE="$repo_root/Gemfile" \
  BUNDLE_PATH="$repo_root/vendor/bundle" \
  BUNDLE_FROZEN=true \
  ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
  RUVOY_DIAGNOSTICS=1 \
  uvx --from "$envoy_package" envoy \
  --config-path "$temporary_dir/streaming.yaml" \
  --concurrency 1 \
  --disable-hot-restart \
  --log-level info \
  >"$result_dir/envoy.log" 2>&1 &
envoy_pid=$!

for _ in $(seq 1 300); do
  curl --silent --fail --max-time 1 --output /dev/null \
    "http://127.0.0.1:$port/rack-env" && break
  process_is_alive "$envoy_pid" || break
  sleep 0.1
done
curl --silent --fail --max-time 5 --output /dev/null \
  "http://127.0.0.1:$port/rack-env" || {
  printf 'FAIL: streaming Envoy did not become ready\n' >&2
  tail -5 "$result_dir/envoy.log" >&2
  exit 1
}

# --- 1. The first byte arrives long before the body is finished --------------
# 5 chunks with a 300 ms gap means the body needs ~1.2s to finish producing.
timing="$(
  curl --silent --output "$temporary_dir/paced.body" \
    --write-out '%{time_starttransfer} %{time_total} %{http_code} %{size_download}' \
    "http://127.0.0.1:$port/paced?chunks=5&chunk_bytes=32&gap_ms=300"
)"
read -r first_byte total status size <<<"$timing"
printf 'paced: first_byte=%ss total=%ss status=%s size=%s\n' \
  "$first_byte" "$total" "$status" "$size"

[[ "$status" == 200 ]] &&
  pass "the paced response completed with HTTP 200" ||
  fail "the paced response returned $status"
[[ "$size" == 160 ]] &&
  pass "every chunk arrived (160 bytes)" ||
  fail "expected 160 bytes, received $size"
awk -v first="$first_byte" -v total="$total" \
  'BEGIN { exit !(first < total / 2) }' &&
  pass "the first byte arrived before half the production time (streaming)" ||
  fail "first byte at ${first_byte}s vs total ${total}s: the body looks buffered"
awk -v total="$total" 'BEGIN { exit !(total > 1.0) }' &&
  pass "the response really took the paced duration (${total}s)" ||
  fail "the paced response finished too fast to be meaningful"

# --- 2. A large body streams without buffering it whole ----------------------
large_timing="$(
  curl --silent --output "$temporary_dir/large.body" \
    --write-out '%{time_starttransfer} %{time_total} %{size_download}' \
    "http://127.0.0.1:$port/paced?chunks=8&chunk_bytes=262144&gap_ms=50"
)"
read -r large_first large_total large_size <<<"$large_timing"
printf 'large: first_byte=%ss total=%ss size=%s\n' \
  "$large_first" "$large_total" "$large_size"
[[ "$large_size" == 2097152 ]] &&
  pass "the 2 MiB streamed body arrived intact" ||
  fail "expected 2097152 bytes, received $large_size"
awk -v first="$large_first" -v total="$large_total" \
  'BEGIN { exit !(first < total) }' &&
  pass "the large body also started before it finished" ||
  fail "the large body did not stream"

# --- 3. Disconnecting stops the producer and closes the body -----------------
before_yielded="$(curl --silent --max-time 5 "http://127.0.0.1:$port/paced-yielded")"
before_closed="$(curl --silent --max-time 5 "http://127.0.0.1:$port/closed")"
curl --silent --output /dev/null --max-time 1 \
  "http://127.0.0.1:$port/paced?chunks=50&chunk_bytes=32&gap_ms=200" || true
sleep 3
after_yielded="$(curl --silent --max-time 5 "http://127.0.0.1:$port/paced-yielded")"
after_closed="$(curl --silent --max-time 5 "http://127.0.0.1:$port/closed")"
printf 'disconnect: yielded %s -> %s, closed %s -> %s\n' \
  "$before_yielded" "$after_yielded" "$before_closed" "$after_closed"

[[ "$((after_yielded - before_yielded))" -lt 50 ]] &&
  pass "the producer stopped early after the client left ($((after_yielded - before_yielded)) of 50 chunks)" ||
  fail "the producer kept yielding all 50 chunks after the client left"
[[ "$after_closed" -gt "$before_closed" ]] &&
  pass "the abandoned body was closed" ||
  fail "the abandoned body was never closed"

# --- 4. A client that leaves stops the application, not just its output -------
# The fixture waits before it counts the call, so a request that was stopped
# mid-wait never counts. Both probes do count, so a working cancellation leaves
# the counter one higher, and a request that ran to completion leaves it two.
calls_before="$(curl --silent --max-time 5 "http://127.0.0.1:$port/counted")"
curl --silent --output /dev/null --max-time 1 \
  "http://127.0.0.1:$port/async-sleep?seconds=4" || true
sleep 6
calls_after="$(curl --silent --max-time 5 "http://127.0.0.1:$port/counted")"
printf 'abandoned request: call count %s -> %s\n' "$calls_before" "$calls_after"

[[ "$calls_after" -eq "$((calls_before + 1))" ]] &&
  pass "the abandoned request was stopped before it finished" ||
  fail "the abandoned request ran to completion: $calls_before -> $calls_after"

# --- 5. The server still works afterwards ------------------------------------
survivor="$(curl --silent --max-time 5 --output /dev/null --write-out '%{http_code}' \
  "http://127.0.0.1:$port/rack-env")"
[[ "$survivor" == 200 ]] &&
  pass "the listener still serves requests after the cancellation" ||
  fail "the listener returned '$survivor' after the cancellation"
grep -Eqi 'panic|segmentation fault|SIGSEGV' "$result_dir/envoy.log" &&
  fail "the streaming run produced a crash signature" ||
  pass "no crash signature in the streaming run"

# --- 6. A slow client applies backpressure instead of growing memory ---------
# The producer may run ahead by at most the queue (1 MiB) plus Envoy's write
# buffer (64 KiB). With a 4 MiB body that is well under half the chunks, so a
# producer that ignored backpressure would be obvious.
backpressure_before="$(curl --silent --max-time 5 "http://127.0.0.1:$port/paced-yielded")"
curl --silent --limit-rate 2m --max-time 120 \
  --output "$temporary_dir/slow.body" \
  "http://127.0.0.1:$port/paced?chunks=128&chunk_bytes=131072&gap_ms=0" &
slow_curl=$!
sleep 3
mid_yielded="$(curl --silent --max-time 5 "http://127.0.0.1:$port/paced-yielded")"
mid_produced=$((mid_yielded - backpressure_before))
printf 'backpressure: produced %s of 128 chunks while the client was still reading\n' \
  "$mid_produced"

[[ -n "$mid_yielded" ]] &&
  pass "the runtime served another request while the producer was blocked" ||
  fail "the runtime stopped answering while the producer was blocked"
[[ "$mid_produced" -lt 110 ]] &&
  pass "the producer stayed behind the slow client ($mid_produced of 128 chunks)" ||
  fail "the producer ran ahead ($mid_produced of 128 chunks): backpressure did not apply"

wait "$slow_curl" 2>/dev/null || true
slow_size="$(wc -c <"$temporary_dir/slow.body" | tr -d ' ')"
[[ "$slow_size" == 16777216 ]] &&
  pass "the slow client still received the whole 16 MiB body" ||
  fail "the slow client received $slow_size of 16777216 bytes"

printf '\nraw results: %s\n' "$result_dir"
if [[ "$failures" -gt 0 ]]; then
  printf 'RESULT: FAIL (%d checks failed)\n' "$failures"
  exit 1
fi
printf 'RESULT: PASS\n'
