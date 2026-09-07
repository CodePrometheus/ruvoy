#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_dir="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
build_profile="${RUVOY_BUILD_PROFILE:-release}"
envoy_concurrency="${RUVOY_ENVOY_CONCURRENCY:-1}"
result_file="$result_dir/poc3-envoy-fiber-rack-$build_profile-$run_id.log"
temporary_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-fiber.XXXXXX")"
envoy_log="$temporary_dir/envoy.log"
envoy_config="$temporary_dir/envoy.yaml"
envoy_pid=""
fiber_port=18083
control_port=18084
ruby_bin="${RUVOY_RUBY:-"$HOME/.rbenv/versions/4.0.5/bin/ruby"}"

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

curl_fiber() {
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

monotonic_seconds() {
  "$ruby_bin" -e 'printf "%.9f", Process.clock_gettime(Process::CLOCK_MONOTONIC)'
}

[[ "$envoy_concurrency" =~ ^[1-9][0-9]*$ ]] ||
  fail "RUVOY_ENVOY_CONCURRENCY must be a positive integer"
command -v cargo >/dev/null || fail "cargo is required"
command -v curl >/dev/null || fail "curl is required"
command -v uvx >/dev/null || fail "uvx is required"

sed \
  "s|value: bench/config.ru|value: $repo_root/test/fixtures/rack/config.ru|" \
  "$repo_root/config/envoy-fiber-rack.yaml" >"$envoy_config"
grep -Fq "value: $repo_root/test/fixtures/rack/config.ru" "$envoy_config" ||
  fail "failed to configure the Rack fixture"

for port in "$fiber_port" "$control_port"; do
  if command -v lsof >/dev/null && lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "TCP port $port is already in use"
  fi
done

mkdir -p "$result_dir"
{
  echo "run_id=$run_id"
  echo "build_profile=$build_profile"
  echo "envoy_concurrency=$envoy_concurrency"
  echo "envoy_package=envoy-server==1.39.0"
  echo "envoy_sdk_commit=8eea3285d6bdb89f8ea34632cfe7ce1608a8f374"
  echo "ruby_version=4.0.5"
  echo "async_version=$("$repo_root/scripts/gem-version.sh" async)"
  echo "rustc=$(rustc --version)"
  echo "host=$(uname -srm)"
  "$repo_root/scripts/build-fiber-module.sh"
  "$repo_root/scripts/check-worker-boundary.sh"
  uvx --from envoy-server==1.39.0 envoy --version
} >"$result_file" 2>&1 || fail "Fiber module build or preflight failed"

BUNDLE_GEMFILE="$repo_root/Gemfile" \
  BUNDLE_PATH="$repo_root/vendor/bundle" \
  BUNDLE_FROZEN=true \
  ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
  RUVOY_DIAGNOSTICS=1 \
  uvx --from envoy-server==1.39.0 envoy \
  --config-path "$envoy_config" \
  --concurrency "$envoy_concurrency" \
  --disable-hot-restart \
  --log-level info \
  >"$envoy_log" 2>&1 &
envoy_pid=$!

ready=false
for _ in $(seq 1 150); do
  if curl --http1.1 --silent --fail --output /dev/null \
    "http://127.0.0.1:$control_port/" &&
    curl --http1.1 --silent --fail --output /dev/null \
      "http://127.0.0.1:$fiber_port/info"; then
    ready=true
    break
  fi
  if ! kill -0 "$envoy_pid" 2>/dev/null; then
    fail "Envoy exited before Fiber listener became ready"
  fi
  sleep 0.1
done
[[ "$ready" == "true" ]] || fail "Fiber listener did not become ready"

get_prefix="$temporary_dir/get"
get_status="$(curl_fiber "$get_prefix" "http://127.0.0.1:$fiber_port/info")"
assert_status "$get_status" 200 "GET /info"
expected_async="$("$repo_root/scripts/gem-version.sh" async)"
[[ "$(header_value "$get_prefix.headers" "x-async-version")" == "$expected_async" ]] ||
  fail "Envoy Fiber response did not report Async $expected_async"
runtime_thread_id="$(header_value "$get_prefix.headers" "x-ruvoy-runtime-rust-thread-id")"
worker_thread_id="$(header_value "$get_prefix.headers" "x-ruvoy-worker-rust-thread-id")"
ruby_thread_object_id="$(header_value "$get_prefix.headers" "x-ruby-thread-object-id")"
[[ -n "$runtime_thread_id" && -n "$worker_thread_id" && -n "$ruby_thread_object_id" ]] ||
  fail "missing runtime/worker thread identity headers"
[[ "$runtime_thread_id" != "$worker_thread_id" ]] ||
  fail "Fiber runtime and Envoy worker used the same Rust thread"

small_prefix="$temporary_dir/small"
small_status="$(
  curl_fiber "$small_prefix" \
    --request POST \
    --header 'x-ruvoy-test: fiber-header' \
    --data-binary 'fiber-body' \
    "http://127.0.0.1:$fiber_port/echo"
)"
assert_status "$small_status" 200 "Fiber POST /echo"
printf 'fiber-body' >"$temporary_dir/small.expected"
cmp "$temporary_dir/small.expected" "$small_prefix.body" ||
  fail "Fiber POST body was not echoed exactly"
[[ "$(header_value "$small_prefix.headers" "x-rack-request-header")" == "fiber-header" ]] ||
  fail "Fiber request header was not copied into the Rack env"

dd if=/dev/zero of="$temporary_dir/one-mib.bin" bs=1048576 count=1 2>/dev/null
large_request_prefix="$temporary_dir/large-request"
large_request_status="$(
  curl_fiber "$large_request_prefix" \
    --request POST \
    --header 'Expect:' \
    --header 'x-ruvoy-stage-timing: 1' \
    --data-binary "@$temporary_dir/one-mib.bin" \
    "http://127.0.0.1:$fiber_port/echo"
)"
assert_status "$large_request_status" 200 "Fiber 1 MiB POST /echo"
cmp "$temporary_dir/one-mib.bin" "$large_request_prefix.body" ||
  fail "Fiber 1 MiB request body was not echoed exactly"
stage_ingress_ns="$(header_value "$large_request_prefix.headers" "x-ruvoy-stage-ingress-ns")"
stage_body_copy_ns="$(header_value "$large_request_prefix.headers" "x-ruvoy-stage-body-copy-ns")"
stage_body_callbacks="$(header_value "$large_request_prefix.headers" "x-ruvoy-stage-body-callbacks")"
stage_runtime_queue_ns="$(
  header_value "$large_request_prefix.headers" "x-ruvoy-stage-runtime-queue-ns"
)"
stage_rack_input_ns="$(
  header_value "$large_request_prefix.headers" "x-ruvoy-stage-rack-input-ns"
)"
stage_rack_call_ns="$(header_value "$large_request_prefix.headers" "x-ruvoy-stage-rack-call-ns")"
stage_response_copy_ns="$(
  header_value "$large_request_prefix.headers" "x-ruvoy-stage-response-copy-ns"
)"
for value in \
  "$stage_ingress_ns" \
  "$stage_body_copy_ns" \
  "$stage_body_callbacks" \
  "$stage_runtime_queue_ns" \
  "$stage_rack_input_ns" \
  "$stage_rack_call_ns" \
  "$stage_response_copy_ns"; do
  [[ "$value" =~ ^[0-9]+$ ]] || fail "invalid or missing Fiber stage timing header: $value"
done

chunked_prefix="$temporary_dir/chunked-request"
chunked_status="$(
  curl_fiber "$chunked_prefix" \
    --request POST \
    --header 'Expect:' \
    --header 'Content-Length:' \
    --header 'Transfer-Encoding: chunked' \
    --header 'x-ruvoy-stage-timing: 1' \
    --data-binary "@$temporary_dir/one-mib.bin" \
    "http://127.0.0.1:$fiber_port/echo"
)"
assert_status "$chunked_status" 200 "Fiber chunked 1 MiB POST /echo"
cmp "$temporary_dir/one-mib.bin" "$chunked_prefix.body" ||
  fail "Fiber chunked 1 MiB request body was not echoed exactly"

# Several times the runtime's own buffer, so it can only be served by taking
# the body in pieces while the rest waits in Envoy.
dd if=/dev/zero of="$temporary_dir/large.bin" bs=1048576 count=8 2>/dev/null
large_prefix="$temporary_dir/beyond-buffer"
large_status="$(
  curl_fiber "$large_prefix" \
    --request POST \
    --header 'Expect:' \
    --data-binary "@$temporary_dir/large.bin" \
    "http://127.0.0.1:$fiber_port/echo"
)"
assert_status "$large_status" 200 "Fiber 8 MiB POST /echo"
cmp "$temporary_dir/large.bin" "$large_prefix.body" ||
  fail "Fiber 8 MiB request body was not echoed exactly"

stage_benchmark_prefix="$temporary_dir/stage-benchmark"
stage_benchmark_status="$(
  curl_fiber "$stage_benchmark_prefix" \
    --request POST \
    --header 'Expect:' \
    --header 'x-ruvoy-stage-timing: 1' \
    --data-binary "@$temporary_dir/one-mib.bin" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=1048576"
)"
assert_status "$stage_benchmark_status" 200 "Fiber timed 1 MiB POST /benchmark"
[[ ! -s "$stage_benchmark_prefix.body" ]] ||
  fail "timed Fiber benchmark unexpectedly returned a response body"
[[ "$(header_value "$stage_benchmark_prefix.headers" "x-request-bytes")" == "1048576" ]] ||
  fail "timed Fiber benchmark did not receive the complete request body"
stage_ingress_ns="$(header_value "$stage_benchmark_prefix.headers" "x-ruvoy-stage-ingress-ns")"
stage_body_copy_ns="$(header_value "$stage_benchmark_prefix.headers" "x-ruvoy-stage-body-copy-ns")"
stage_body_callbacks="$(header_value "$stage_benchmark_prefix.headers" "x-ruvoy-stage-body-callbacks")"
stage_runtime_queue_ns="$(
  header_value "$stage_benchmark_prefix.headers" "x-ruvoy-stage-runtime-queue-ns"
)"
stage_rack_input_ns="$(
  header_value "$stage_benchmark_prefix.headers" "x-ruvoy-stage-rack-input-ns"
)"
stage_rack_call_ns="$(
  header_value "$stage_benchmark_prefix.headers" "x-ruvoy-stage-rack-call-ns"
)"
stage_response_copy_ns="$(
  header_value "$stage_benchmark_prefix.headers" "x-ruvoy-stage-response-copy-ns"
)"
stage_scheduler_return_ns="$(
  header_value "$stage_benchmark_prefix.headers" "x-ruvoy-stage-scheduler-return-ns"
)"

tr '\000' 'F' <"$temporary_dir/one-mib.bin" >"$temporary_dir/one-mib-f.bin"
large_response_prefix="$temporary_dir/large-response"
large_response_status="$(
  curl_fiber "$large_response_prefix" \
    "http://127.0.0.1:$fiber_port/large-response"
)"
assert_status "$large_response_status" 200 "Fiber GET /large-response"
cmp "$temporary_dir/one-mib-f.bin" "$large_response_prefix.body" ||
  fail "Fiber 1 MiB response body was not returned exactly"

error_prefix="$temporary_dir/error"
error_status="$(curl_fiber "$error_prefix" "http://127.0.0.1:$fiber_port/raise")"
assert_status "$error_status" 500 "Fiber GET /raise"
grep -qE 'intentional fiber envoy boom' "$error_prefix.body" ||
  fail "Fiber Ruby exception did not cross the owned bridge"

for index in $(seq 1 5); do
  gc_prefix="$temporary_dir/gc-$index"
  gc_status="$(curl_fiber "$gc_prefix" "http://127.0.0.1:$fiber_port/gc")"
  assert_status "$gc_status" 200 "Fiber GC request $index"
  [[ "$(header_value "$gc_prefix.headers" "x-ruby-thread-object-id")" == "$ruby_thread_object_id" ]] ||
    fail "Fiber GC request $index changed the Ruby runtime thread"
done

async_dir="$temporary_dir/async-batch"
mkdir -p "$async_dir"
async_started="$(monotonic_seconds)"
async_pids=()
for index in $(seq 1 10); do
  (
    curl --http1.1 \
      --silent \
      --show-error \
      --max-time 5 \
      --dump-header "$async_dir/$index.headers" \
      --output "$async_dir/$index.body" \
      --write-out '%{http_code}' \
      "http://127.0.0.1:$fiber_port/async-sleep?seconds=0.2" \
      >"$async_dir/$index.status" \
      2>"$async_dir/$index.stderr"
  ) &
  async_pids+=("$!")
done
for pid in "${async_pids[@]}"; do
  wait "$pid" || fail "an Async sleep request failed"
done
async_finished="$(monotonic_seconds)"
async_elapsed="$(awk -v start="$async_started" -v finish="$async_finished" 'BEGIN { printf "%.6f", finish - start }')"
awk -v seconds="$async_elapsed" 'BEGIN { exit !(seconds < 0.8) }' ||
  fail "10 scheduler-aware 200 ms requests took ${async_elapsed}s"

: >"$async_dir/fiber-ids"
for index in $(seq 1 10); do
  [[ "$(<"$async_dir/$index.status")" == "200" ]] ||
    fail "Async sleep request $index did not return 200"
  header_value "$async_dir/$index.headers" "x-ruby-fiber-object-id" \
    >>"$async_dir/fiber-ids"
  [[ "$(header_value "$async_dir/$index.headers" "x-ruby-thread-object-id")" == "$ruby_thread_object_id" ]] ||
    fail "Async sleep request $index ran on a different Ruby thread"
done
unique_async_fibers="$(sort -u "$async_dir/fiber-ids" | wc -l | tr -d '[:space:]')"
[[ "$unique_async_fibers" == "10" ]] ||
  fail "expected 10 request Fibers, observed $unique_async_fibers"

worker_probe_dir="$temporary_dir/worker-probe"
mkdir -p "$worker_probe_dir"
worker_probe_pids=()
for index in $(seq 1 64); do
  (
    curl --http1.1 \
      --silent \
      --show-error \
      --max-time 5 \
      --header 'Connection: close' \
      --dump-header "$worker_probe_dir/$index.headers" \
      --output "$worker_probe_dir/$index.body" \
      --write-out '%{http_code}' \
      "http://127.0.0.1:$fiber_port/info" \
      >"$worker_probe_dir/$index.status" \
      2>"$worker_probe_dir/$index.stderr"
  ) &
  worker_probe_pids+=("$!")
done
for pid in "${worker_probe_pids[@]}"; do
  wait "$pid" || fail "a worker distribution request failed"
done
: >"$worker_probe_dir/worker-ids"
for index in $(seq 1 64); do
  [[ "$(<"$worker_probe_dir/$index.status")" == "200" ]] ||
    fail "worker distribution request $index did not return 200"
  probe_runtime_thread_id="$(
    header_value "$worker_probe_dir/$index.headers" "x-ruvoy-runtime-rust-thread-id"
  )"
  probe_worker_thread_id="$(
    header_value "$worker_probe_dir/$index.headers" "x-ruvoy-worker-rust-thread-id"
  )"
  [[ "$probe_runtime_thread_id" == "$runtime_thread_id" ]] ||
    fail "worker distribution request $index changed the runtime thread"
  [[ -n "$probe_worker_thread_id" && "$probe_worker_thread_id" != "$runtime_thread_id" ]] ||
    fail "worker distribution request $index did not stay outside the runtime thread"
  printf '%s\n' "$probe_worker_thread_id" >>"$worker_probe_dir/worker-ids"
done
unique_worker_threads="$(
  sort -u "$worker_probe_dir/worker-ids" | wc -l | tr -d '[:space:]'
)"
if [[ "$envoy_concurrency" -gt 1 && "$unique_worker_threads" -lt 2 ]]; then
  fail "multi-worker probe observed only $unique_worker_threads Envoy worker"
fi

blocking_dir="$temporary_dir/blocking-batch"
mkdir -p "$blocking_dir"
blocking_started="$(monotonic_seconds)"
blocking_pids=()
for index in $(seq 1 5); do
  (
    curl --http1.1 \
      --silent \
      --show-error \
      --max-time 5 \
      --output "$blocking_dir/$index.body" \
      --write-out '%{http_code}' \
      "http://127.0.0.1:$fiber_port/blocking?seconds=0.1" \
      >"$blocking_dir/$index.status" \
      2>"$blocking_dir/$index.stderr"
  ) &
  blocking_pids+=("$!")
done
for pid in "${blocking_pids[@]}"; do
  wait "$pid" || fail "a scheduler-unaware blocking request failed"
done
blocking_finished="$(monotonic_seconds)"
blocking_elapsed="$(awk -v start="$blocking_started" -v finish="$blocking_finished" 'BEGIN { printf "%.6f", finish - start }')"
awk -v seconds="$blocking_elapsed" 'BEGIN { exit !(seconds >= 0.45 && seconds < 2.0) }' ||
  fail "5 scheduler-unaware 100 ms requests took ${blocking_elapsed}s"

control_blocking_prefix="$temporary_dir/control-blocking"
(
  curl_fiber "$control_blocking_prefix" \
    "http://127.0.0.1:$fiber_port/blocking?seconds=0.5" \
    >"$control_blocking_prefix.status"
) &
control_blocking_pid=$!
sleep 0.05
control_time="$(
  curl --http1.1 \
    --silent \
    --show-error \
    --output "$temporary_dir/control.body" \
    --write-out '%{time_total}' \
    "http://127.0.0.1:$control_port/"
)"
wait "$control_blocking_pid" || fail "long blocking Fiber request failed"
[[ "$(<"$temporary_dir/control.body")" == "fiber-control-ok" ]] ||
  fail "Fiber control listener returned the wrong body"
awk -v seconds="$control_time" 'BEGIN { exit !(seconds < 0.2) }' ||
  fail "control listener took ${control_time}s while the Fiber reactor was blocked"

set +e
curl --http1.1 \
  --silent \
  --show-error \
  --max-time 0.02 \
  --output "$temporary_dir/cancel.body" \
  "http://127.0.0.1:$fiber_port/async-sleep?seconds=0.5" \
  2>"$temporary_dir/cancel.stderr"
cancel_exit=$?
set -e
[[ "$cancel_exit" == "28" ]] || fail "Fiber cancel probe exited $cancel_exit instead of 28"
sleep 0.6

set +e
curl --http1.1 \
  --silent \
  --show-error \
  --max-time 0.05 \
  --limit-rate 64k \
  --request POST \
  --header 'Expect:' \
  --data-binary "@$temporary_dir/one-mib.bin" \
  --output "$temporary_dir/body-cancel.body" \
  "http://127.0.0.1:$fiber_port/echo" \
  2>"$temporary_dir/body-cancel.stderr"
body_cancel_exit=$?
set -e
[[ "$body_cancel_exit" == "28" ]] ||
  fail "Fiber body cancel probe exited $body_cancel_exit instead of 28"

after_cancel_prefix="$temporary_dir/after-cancel"
after_cancel_status="$(
  curl_fiber "$after_cancel_prefix" "http://127.0.0.1:$fiber_port/info"
)"
assert_status "$after_cancel_status" 200 "request after cancelled Fiber response"

shutdown_prefix="$temporary_dir/slow-shutdown"
(
  curl_fiber "$shutdown_prefix" \
    "http://127.0.0.1:$fiber_port/slow-shutdown?seconds=1.0" \
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
[[ "$envoy_stopped" == "true" ]] ||
  fail "Envoy hung during SIGINT with an active Fiber request"
if ! wait "$envoy_pid"; then
  envoy_pid=""
  fail "Envoy Fiber process exited non-zero after SIGINT"
fi
envoy_pid=""
wait "$shutdown_curl_pid" 2>/dev/null || true

grep -q 'Dynamic module ABI version v0.1.0 matched' "$envoy_log" ||
  fail "Envoy did not report a matching Fiber module ABI"
grep -q 'caught SIGINT' "$envoy_log" || fail "Envoy did not record SIGINT"
grep -q '\[ruvoy\] Fiber runtime stopped' "$envoy_log" ||
  fail "Fiber runtime did not report a clean shutdown"
if grep -Eiq 'panic|fatal|segmentation fault|Fiber runtime shutdown failed' "$envoy_log"; then
  fail "Envoy Fiber log contains a fatal runtime error"
fi

{
  echo "result=PASS"
  echo "async_version=$("$repo_root/scripts/gem-version.sh" async)"
  echo "get_post_headers_1mib=PASS"
  echo "chunked_1mib=PASS"
  echo "beyond_buffer_body=PASS"
  echo "stage_ingress_ns=$stage_ingress_ns"
  echo "stage_body_copy_ns=$stage_body_copy_ns"
  echo "stage_body_callbacks=$stage_body_callbacks"
  echo "stage_runtime_queue_ns=$stage_runtime_queue_ns"
  echo "stage_rack_input_ns=$stage_rack_input_ns"
  echo "stage_rack_call_ns=$stage_rack_call_ns"
  echo "stage_response_copy_ns=$stage_response_copy_ns"
  echo "stage_scheduler_return_ns=$stage_scheduler_return_ns"
  echo "ruby_exception_gc=PASS"
  echo "scheduler_aware_requests=10"
  echo "scheduler_aware_elapsed_seconds=$async_elapsed"
  echo "scheduler_aware_unique_fibers=$unique_async_fibers"
  echo "configured_envoy_workers=$envoy_concurrency"
  echo "observed_envoy_worker_threads=$unique_worker_threads"
  echo "scheduler_unaware_requests=5"
  echo "scheduler_unaware_elapsed_seconds=$blocking_elapsed"
  echo "runtime_thread_id=$runtime_thread_id"
  echo "worker_thread_id=$worker_thread_id"
  echo "ruby_thread_object_id=$ruby_thread_object_id"
  echo "control_latency_while_fiber_blocked_seconds=$control_time"
  echo "client_cancel_late_response=PASS"
  echo "client_cancel_during_body=PASS"
  echo "sigint_with_active_fiber=PASS"
  echo "fiber_runtime_shutdown=PASS"
  echo
  echo "===== envoy log ====="
  cat "$envoy_log"
} >>"$result_file"

echo "PASS: Envoy + Fiber Rack bridge ($build_profile)"
echo "raw result: $result_file"
