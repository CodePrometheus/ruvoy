#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_dir="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_file="$result_dir/poc2-envoy-sync-rack-$run_id.log"
temporary_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-sync.XXXXXX")"
envoy_log="$temporary_dir/envoy.log"
envoy_pid=""
sync_port=18081
control_port=18082
build_profile="${RUVOY_BUILD_PROFILE:-release}"

cleanup() {
  if [[ -n "$envoy_pid" ]] && kill -0 "$envoy_pid" 2>/dev/null; then
    kill -INT "$envoy_pid" 2>/dev/null || true
    wait "$envoy_pid" 2>/dev/null || true
  fi
  rm -rf -- "$temporary_dir"
}
trap cleanup EXIT INT TERM

fail() {
  local message="$*"
  {
    echo "result=FAIL"
    echo "failure=$message"
    if [[ -f "$envoy_log" ]]; then
      echo
      echo "===== envoy log ====="
      cat "$envoy_log"
    fi
  } >>"$result_file"
  echo "FAIL: $message" >&2
  if [[ -f "$envoy_log" ]]; then
    tail -100 "$envoy_log" >&2
  fi
  echo "raw result: $result_file" >&2
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

curl_sync() {
  local prefix="$1"
  shift
  curl --http1.1 \
    --silent \
    --show-error \
    --dump-header "$prefix.headers" \
    --output "$prefix.body" \
    --write-out '%{http_code}' \
    "$@"
}

assert_status() {
  local actual="$1"
  local expected="$2"
  local context="$3"
  [[ "$actual" == "$expected" ]] || fail "$context returned HTTP $actual, expected $expected"
}

command -v cargo >/dev/null || fail "cargo is required"
command -v curl >/dev/null || fail "curl is required"
command -v uvx >/dev/null || fail "uvx is required"
command -v shasum >/dev/null || fail "shasum is required"

for port in "$sync_port" "$control_port"; do
  if command -v lsof >/dev/null && lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "TCP port $port is already in use"
  fi
done

mkdir -p "$result_dir"
{
  echo "run_id=$run_id"
  echo "envoy_package=envoy-server==1.39.0"
  echo "envoy_sdk_commit=8eea3285d6bdb89f8ea34632cfe7ce1608a8f374"
  echo "ruby_version=4.0.5"
  echo "build_profile=$build_profile"
  echo "rustc=$(rustc --version)"
  echo "host=$(uname -srm)"
  "$repo_root/scripts/build-sync-module.sh"
  "$repo_root/scripts/check-worker-boundary.sh"
  uvx --from envoy-server==1.39.0 envoy --version
} >"$result_file" 2>&1 || fail "release module build or preflight failed"

ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
  RUVOY_DIAGNOSTICS=1 \
  uvx --from envoy-server==1.39.0 envoy \
  --config-path "$repo_root/config/envoy-sync-rack.yaml" \
  --concurrency 1 \
  --disable-hot-restart \
  --log-level info \
  >"$envoy_log" 2>&1 &
envoy_pid=$!

ready=false
for _ in $(seq 1 150); do
  if curl --http1.1 --silent --fail --output /dev/null \
    "http://127.0.0.1:$control_port/" &&
    curl --http1.1 --silent --fail --output /dev/null \
      "http://127.0.0.1:$sync_port/info"; then
    ready=true
    break
  fi
  if ! kill -0 "$envoy_pid" 2>/dev/null; then
    fail "Envoy exited before becoming ready"
  fi
  sleep 0.1
done
[[ "$ready" == "true" ]] || fail "Envoy did not become ready"

get_prefix="$temporary_dir/get"
get_status="$(curl_sync "$get_prefix" "http://127.0.0.1:$sync_port/info")"
assert_status "$get_status" 200 "GET /info"
printf 'GET /info ' >"$temporary_dir/get.expected"
cmp "$temporary_dir/get.expected" "$get_prefix.body" ||
  fail "GET /info returned an unexpected body"

runtime_thread_id="$(header_value "$get_prefix.headers" "x-ruvoy-runtime-rust-thread-id")"
worker_thread_id="$(header_value "$get_prefix.headers" "x-ruvoy-worker-rust-thread-id")"
ruby_thread_object_id="$(header_value "$get_prefix.headers" "x-ruby-thread-object-id")"
[[ -n "$runtime_thread_id" ]] || fail "missing runtime Rust thread id"
[[ -n "$worker_thread_id" ]] || fail "missing worker Rust thread id"
[[ -n "$ruby_thread_object_id" ]] || fail "missing Ruby thread object id"
[[ "$runtime_thread_id" != "$worker_thread_id" ]] ||
  fail "Ruby runtime and Envoy worker used the same Rust thread"

small_prefix="$temporary_dir/small"
small_status="$(
  curl_sync "$small_prefix" \
    --request POST \
    --header 'x-ruvoy-test: copied-header' \
    --data-binary 'hello-rack' \
    "http://127.0.0.1:$sync_port/echo"
)"
assert_status "$small_status" 200 "small POST /echo"
printf 'hello-rack' >"$temporary_dir/small.expected"
cmp "$temporary_dir/small.expected" "$small_prefix.body" ||
  fail "small POST body was not echoed exactly"
[[ "$(header_value "$small_prefix.headers" "x-rack-request-header")" == "copied-header" ]] ||
  fail "owned request header was not copied into the Rack env"

empty_prefix="$temporary_dir/empty"
empty_status="$(
  curl_sync "$empty_prefix" \
    --request POST \
    --data-binary '' \
    "http://127.0.0.1:$sync_port/echo"
)"
assert_status "$empty_status" 200 "empty POST /echo"
[[ ! -s "$empty_prefix.body" ]] || fail "empty POST did not return an empty body"

dd if=/dev/zero of="$temporary_dir/one-mib.bin" bs=1048576 count=1 2>/dev/null
large_request_prefix="$temporary_dir/large-request"
large_request_status="$(
  curl_sync "$large_request_prefix" \
    --request POST \
    --header 'Expect:' \
    --data-binary "@$temporary_dir/one-mib.bin" \
    "http://127.0.0.1:$sync_port/echo"
)"
assert_status "$large_request_status" 200 "1 MiB POST /echo"
cmp "$temporary_dir/one-mib.bin" "$large_request_prefix.body" ||
  fail "1 MiB request body was not echoed exactly"

tr '\000' 'R' <"$temporary_dir/one-mib.bin" >"$temporary_dir/one-mib-r.bin"
large_response_prefix="$temporary_dir/large-response"
large_response_status="$(
  curl_sync "$large_response_prefix" \
    "http://127.0.0.1:$sync_port/large-response"
)"
assert_status "$large_response_status" 200 "GET /large-response"
cmp "$temporary_dir/one-mib-r.bin" "$large_response_prefix.body" ||
  fail "1 MiB response body was not returned exactly"

for index in $(seq 1 10); do
  gc_prefix="$temporary_dir/gc-$index"
  gc_status="$(curl_sync "$gc_prefix" "http://127.0.0.1:$sync_port/gc")"
  assert_status "$gc_status" 200 "GC request $index"
  [[ "$(header_value "$gc_prefix.headers" "x-ruby-thread-object-id")" == "$ruby_thread_object_id" ]] ||
    fail "GC request $index changed the Ruby runtime thread"
done

concurrency_dir="$temporary_dir/concurrency"
mkdir -p "$concurrency_dir"
concurrent_pids=()
for index in $(seq 1 64); do
  (
    curl --http1.1 \
      --silent \
      --show-error \
      --max-time 10 \
      --dump-header "$concurrency_dir/$index.headers" \
      --output "$concurrency_dir/$index.body" \
      --write-out '%{http_code}' \
      --request POST \
      --data-binary "body-$index" \
      "http://127.0.0.1:$sync_port/echo" \
      >"$concurrency_dir/$index.status" \
      2>"$concurrency_dir/$index.stderr"
  ) &
  concurrent_pids+=("$!")
done
for pid in "${concurrent_pids[@]}"; do
  wait "$pid" || fail "a concurrent curl process failed"
done
for index in $(seq 1 64); do
  [[ "$(<"$concurrency_dir/$index.status")" == "200" ]] ||
    fail "concurrent request $index did not return 200"
  printf 'body-%s' "$index" >"$concurrency_dir/$index.expected"
  cmp "$concurrency_dir/$index.expected" "$concurrency_dir/$index.body" ||
    fail "concurrent request $index returned the wrong body"
  [[ "$(header_value "$concurrency_dir/$index.headers" "x-ruby-thread-object-id")" == "$ruby_thread_object_id" ]] ||
    fail "concurrent request $index ran on a different Ruby thread"
done

error_prefix="$temporary_dir/error"
error_status="$(curl_sync "$error_prefix" "http://127.0.0.1:$sync_port/raise")"
assert_status "$error_status" 500 "GET /raise"
[[ "$(header_value "$error_prefix.headers" "x-ruvoy-error")" == "true" ]] ||
  fail "Ruby exception response did not carry x-ruvoy-error"
rg -q 'intentional envoy boom' "$error_prefix.body" ||
  fail "Ruby exception text did not cross the owned bridge"

slow_prefix="$temporary_dir/slow"
(
  curl_sync "$slow_prefix" "http://127.0.0.1:$sync_port/slow" >"$slow_prefix.status"
) &
slow_pid=$!
sleep 0.05
control_time="$(
  curl --http1.1 \
    --silent \
    --show-error \
    --output "$temporary_dir/control.body" \
    --write-out '%{time_total}' \
    "http://127.0.0.1:$control_port/"
)"
wait "$slow_pid" || fail "slow Rack request failed"
[[ "$(<"$slow_prefix.status")" == "200" ]] || fail "slow Rack request did not return 200"
[[ "$(<"$temporary_dir/control.body")" == "control-ok" ]] ||
  fail "control listener returned the wrong body"
awk -v seconds="$control_time" 'BEGIN { exit !(seconds < 0.2) }' ||
  fail "control listener took ${control_time}s while Ruby was blocked"

set +e
curl --http1.1 \
  --silent \
  --show-error \
  --max-time 0.02 \
  --output "$temporary_dir/cancel.body" \
  "http://127.0.0.1:$sync_port/slow" \
  2>"$temporary_dir/cancel.stderr"
cancel_exit=$?
set -e
[[ "$cancel_exit" == "28" ]] || fail "cancel probe exited $cancel_exit instead of timing out"
sleep 0.65
after_cancel_prefix="$temporary_dir/after-cancel"
after_cancel_status="$(
  curl_sync "$after_cancel_prefix" "http://127.0.0.1:$sync_port/info"
)"
assert_status "$after_cancel_status" 200 "request after cancelled late response"

shutdown_prefix="$temporary_dir/slow-shutdown"
(
  curl_sync "$shutdown_prefix" \
    "http://127.0.0.1:$sync_port/slow-shutdown" \
    >"$shutdown_prefix.status" \
    2>"$shutdown_prefix.stderr"
) &
shutdown_curl_pid=$!
sleep 0.1
kill -INT "$envoy_pid"

envoy_stopped=false
for _ in $(seq 1 150); do
  if ! kill -0 "$envoy_pid" 2>/dev/null; then
    envoy_stopped=true
    break
  fi
  sleep 0.1
done
[[ "$envoy_stopped" == "true" ]] || fail "Envoy hung during SIGINT with an active Ruby request"
if ! wait "$envoy_pid"; then
  envoy_pid=""
  fail "Envoy exited non-zero after SIGINT"
fi
envoy_pid=""
wait "$shutdown_curl_pid" 2>/dev/null || true

grep -q 'Dynamic module ABI version v0.1.0 matched' "$envoy_log" ||
  fail "Envoy did not report a matching dynamic-module ABI"
grep -q 'caught SIGINT' "$envoy_log" || fail "Envoy did not record SIGINT"
grep -q '\[ruvoy\] Ruby runtime stopped' "$envoy_log" ||
  fail "Ruby runtime did not report a clean shutdown"
if grep -Eiq 'panic|fatal|segmentation fault|Ruby runtime shutdown failed' "$envoy_log"; then
  fail "Envoy log contains a fatal runtime error"
fi

{
  echo "result=PASS"
  echo "get_post_headers=PASS"
  echo "empty_small_1mib_request=PASS"
  echo "one_mib_response=PASS"
  echo "forced_gc_requests=10"
  echo "concurrent_requests=64"
  echo "ruby_exception=PASS"
  echo "runtime_thread_id=$runtime_thread_id"
  echo "worker_thread_id=$worker_thread_id"
  echo "ruby_thread_object_id=$ruby_thread_object_id"
  echo "control_latency_while_ruby_blocked_seconds=$control_time"
  echo "client_cancel_late_response=PASS"
  echo "sigint_with_active_request=PASS"
  echo "ruby_runtime_shutdown=PASS"
  echo "large_request_sha256=$(shasum -a 256 "$large_request_prefix.body" | awk '{ print $1 }')"
  echo "large_response_sha256=$(shasum -a 256 "$large_response_prefix.body" | awk '{ print $1 }')"
  echo
  echo "===== envoy log ====="
  cat "$envoy_log"
} >>"$result_file"

echo "PASS: Envoy + synchronous Rack bridge"
echo "raw result: $result_file"
