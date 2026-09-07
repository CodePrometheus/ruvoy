#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_root="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
result_dir="$result_root/poc4-fiber-backpressure-$run_id"
temporary_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-backpressure.XXXXXX")"
fiber_port=18083
control_port=18084
envoy_pid=""
envoy_log=""

mkdir -p "$result_dir"

cleanup() {
  if [[ -n "$envoy_pid" ]] && kill -0 "$envoy_pid" 2>/dev/null; then
    kill -INT "$envoy_pid" 2>/dev/null || true
    wait "$envoy_pid" 2>/dev/null || true
  fi
  rm -rf -- "$temporary_dir"
}
trap cleanup EXIT INT TERM

fail() {
  {
    echo "result=FAIL"
    echo "failure=$*"
    if [[ -n "$envoy_log" && -f "$envoy_log" ]]; then
      echo
      echo "===== envoy log ====="
      cat "$envoy_log"
    fi
  } >>"$result_dir/summary.log"
  echo "FAIL: $*" >&2
  echo "raw results: $result_dir" >&2
  exit 1
}

header_value() {
  local file="$1"
  local wanted="$2"
  awk -v wanted="$wanted" '
    {
      sub(/\r$/, "")
      separator = index($0, ":")
      if (separator > 0 && tolower(substr($0, 1, separator - 1)) == tolower(wanted)) {
        value = substr($0, separator + 1)
        sub(/^[[:space:]]+/, "", value)
        print value
      }
    }
  ' "$file" | tail -1
}

curl_fiber() {
  local prefix="$1"
  shift
  curl --http1.1 \
    --silent \
    --show-error \
    --max-time 5 \
    --dump-header "$prefix.headers" \
    --output "$prefix.body" \
    --write-out '%{http_code}' \
    "$@"
}

assert_status() {
  local actual="$1"
  local expected="$2"
  local context="$3"
  [[ "$actual" == "$expected" ]] ||
    fail "$context returned HTTP $actual, expected $expected"
}

start_envoy() {
  local name="$1"
  local request_limit="$2"
  local body_limit="$3"
  envoy_log="$result_dir/$name-envoy.log"

  RUVOY_MAX_INFLIGHT_REQUESTS="$request_limit" \
    RUVOY_MAX_INFLIGHT_BODY_BYTES="$body_limit" \
    BUNDLE_GEMFILE="$repo_root/Gemfile" \
    BUNDLE_PATH="$repo_root/vendor/bundle" \
    BUNDLE_FROZEN=true \
    ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
    uvx --from envoy-server==1.39.0 envoy \
    --config-path "$repo_root/config/envoy-fiber-rack.yaml" \
    --concurrency 1 \
    --disable-hot-restart \
    --log-level info \
    >"$envoy_log" 2>&1 &
  envoy_pid=$!

  local ready=false
  for _ in $(seq 1 200); do
    if curl --http1.1 --silent --fail --output /dev/null \
      "http://127.0.0.1:$control_port/" &&
      curl --http1.1 --silent --fail --output /dev/null \
        "http://127.0.0.1:$fiber_port/info"; then
      ready=true
      break
    fi
    if ! kill -0 "$envoy_pid" 2>/dev/null; then
      fail "$name Envoy exited before becoming ready"
    fi
    sleep 0.05
  done
  [[ "$ready" == "true" ]] || fail "$name Envoy did not become ready"
}

stop_envoy() {
  kill -INT "$envoy_pid"
  local stopped=false
  for _ in $(seq 1 200); do
    if ! kill -0 "$envoy_pid" 2>/dev/null; then
      stopped=true
      break
    fi
    sleep 0.05
  done
  [[ "$stopped" == "true" ]] || fail "Envoy did not stop after SIGINT"
  wait "$envoy_pid" || fail "Envoy exited non-zero after SIGINT"
  envoy_pid=""
  grep -q '\[ruvoy\] Fiber runtime stopped' "$envoy_log" ||
    fail "Fiber runtime did not report a clean shutdown"
  if grep -Eiq 'panic|fatal|segmentation fault|Fiber runtime shutdown failed' "$envoy_log"; then
    fail "Envoy log contains a fatal runtime error"
  fi
}

for tool in cargo curl lsof uvx; do
  command -v "$tool" >/dev/null || fail "$tool is required"
done
for port in "$fiber_port" "$control_port"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "TCP port $port is already in use"
  fi
done

{
  echo "run_id=$run_id"
  echo "envoy_package=envoy-server==1.39.0"
  echo "ruby_version=$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
  echo "rustc=$(rustc --version)"
  RUVOY_BUILD_PROFILE=release "$repo_root/scripts/build-fiber-module.sh"
  "$repo_root/scripts/check-worker-boundary.sh"
} >"$result_dir/preflight.log" 2>&1 || fail "release build or worker-boundary preflight failed"

start_envoy request-limit 2 16777216

request_dir="$temporary_dir/request-limit"
mkdir -p "$request_dir"
holder_pids=()
for index in 1 2; do
  (
    curl_fiber "$request_dir/holder-$index" \
      "http://127.0.0.1:$fiber_port/async-sleep?seconds=0.6" \
      >"$request_dir/holder-$index.status"
  ) &
  holder_pids+=("$!")
done
sleep 0.15

request_overload_prefix="$request_dir/overload"
request_overload_status="$(
  curl_fiber "$request_overload_prefix" "http://127.0.0.1:$fiber_port/info"
)"
assert_status "$request_overload_status" 503 "request admission overload"
[[ "$(header_value "$request_overload_prefix.headers" "x-ruvoy-error")" == "true" ]] ||
  fail "request admission overload response omitted x-ruvoy-error"
grep -qE 'Ruby runtime admission limit reached' "$request_overload_prefix.body" ||
  fail "request admission overload response returned the wrong reason"

request_control_time="$(
  curl --http1.1 \
    --silent \
    --show-error \
    --output "$request_dir/control.body" \
    --write-out '%{time_total}' \
    "http://127.0.0.1:$control_port/"
)"
[[ "$(<"$request_dir/control.body")" == "fiber-control-ok" ]] ||
  fail "control listener returned the wrong body during request overload"
awk -v seconds="$request_control_time" 'BEGIN { exit !(seconds < 0.2) }' ||
  fail "control listener took ${request_control_time}s during request overload"

for pid in "${holder_pids[@]}"; do
  wait "$pid" || fail "request admission holder failed"
done
for index in 1 2; do
  assert_status "$(<"$request_dir/holder-$index.status")" 200 \
    "request admission holder $index"
done

request_released_prefix="$request_dir/released"
request_released_status="$(
  curl_fiber "$request_released_prefix" "http://127.0.0.1:$fiber_port/info"
)"
assert_status "$request_released_status" 200 "request after admission release"
stop_envoy

dd if=/dev/zero of="$temporary_dir/one-mib.bin" bs=1048576 count=1 2>/dev/null
start_envoy body-limit 16 1048576

body_dir="$temporary_dir/body-limit"
mkdir -p "$body_dir"
(
  curl_fiber "$body_dir/holder" \
    --request POST \
    --header 'Expect:' \
    --data-binary "@$temporary_dir/one-mib.bin" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=600&response_bytes=0&expected_request_bytes=1048576" \
    >"$body_dir/holder.status"
) &
body_holder_pid=$!
sleep 0.15

body_overload_prefix="$body_dir/overload"
body_overload_status="$(
  curl_fiber "$body_overload_prefix" \
    --request POST \
    --header 'Expect:' \
    --data-binary "@$temporary_dir/one-mib.bin" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=1048576"
)"
assert_status "$body_overload_status" 503 "body-byte admission overload"
[[ "$(header_value "$body_overload_prefix.headers" "x-ruvoy-error")" == "true" ]] ||
  fail "body-byte overload response omitted x-ruvoy-error"
grep -qE 'request body admission limit reached' "$body_overload_prefix.body" ||
  fail "body-byte overload response returned the wrong reason"

body_control_time="$(
  curl --http1.1 \
    --silent \
    --show-error \
    --output "$body_dir/control.body" \
    --write-out '%{time_total}' \
    "http://127.0.0.1:$control_port/"
)"
[[ "$(<"$body_dir/control.body")" == "fiber-control-ok" ]] ||
  fail "control listener returned the wrong body during body-byte overload"
awk -v seconds="$body_control_time" 'BEGIN { exit !(seconds < 0.2) }' ||
  fail "control listener took ${body_control_time}s during body-byte overload"

wait "$body_holder_pid" || fail "body-byte admission holder failed"
assert_status "$(<"$body_dir/holder.status")" 200 "body-byte admission holder"

body_released_prefix="$body_dir/released"
body_released_status="$(
  curl_fiber "$body_released_prefix" \
    --request POST \
    --header 'Expect:' \
    --data-binary "@$temporary_dir/one-mib.bin" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=1048576"
)"
assert_status "$body_released_status" 200 "body request after admission release"
[[ "$(header_value "$body_released_prefix.headers" "x-request-bytes")" == "1048576" ]] ||
  fail "body request after admission release was truncated"

chunked_prefix="$body_dir/chunked"
chunked_status="$(
  curl_fiber "$chunked_prefix" \
    --request POST \
    --header 'Expect:' \
    --header 'Content-Length:' \
    --header 'Transfer-Encoding: chunked' \
    --data-binary "@$temporary_dir/one-mib.bin" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=1048576"
)"
assert_status "$chunked_status" 200 "chunked body at the admission limit"
[[ "$(header_value "$chunked_prefix.headers" "x-request-bytes")" == "1048576" ]] ||
  fail "chunked body at the admission limit was truncated"
stop_envoy

{
  echo "result=PASS"
  echo "request_limit=2"
  echo "request_overload_status=503"
  echo "request_control_latency_seconds=$request_control_time"
  echo "request_capacity_release=PASS"
  echo "body_limit_bytes=1048576"
  echo "body_overload_status=503"
  echo "body_control_latency_seconds=$body_control_time"
  echo "body_capacity_release=PASS"
  echo "chunked_incremental_admission=PASS"
} >"$result_dir/summary.log"

echo "PASS: Fiber request and body-byte backpressure"
echo "raw results: $result_dir"
