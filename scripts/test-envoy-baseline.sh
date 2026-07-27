#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_dir="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_file="$result_dir/poc1-envoy-baseline-$run_id.log"
temporary_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-baseline.XXXXXX")"
envoy_log="$temporary_dir/envoy.log"
envoy_pid=""

cleanup() {
  if [[ -n "$envoy_pid" ]] && kill -0 "$envoy_pid" 2>/dev/null; then
    kill -INT "$envoy_pid" 2>/dev/null || true
    wait "$envoy_pid" 2>/dev/null || true
  fi
  rm -rf -- "$temporary_dir"
}
trap cleanup EXIT INT TERM

fail() {
  echo "FAIL: $*" >&2
  if [[ -f "$envoy_log" ]]; then
    tail -100 "$envoy_log" >&2
  fi
  exit 1
}

assert_response() {
  local path="$1"
  local expected_mode="$2"
  local expected_body="$3"
  local prefix="$temporary_dir/${expected_mode}"
  local status

  status="$(
    curl --silent --show-error \
      --dump-header "$prefix.headers" \
      --output "$prefix.body" \
      --write-out '%{http_code}' \
      "http://127.0.0.1:18080$path"
  )"

  [[ "$status" == "200" ]] || fail "$path returned HTTP $status"
  [[ "$(tr -d '\r' <"$prefix.headers" | tr '[:upper:]' '[:lower:]' | awk '/^x-ruvoy-mode:/ { print $2 }')" == "$expected_mode" ]] ||
    fail "$path did not return x-ruvoy-mode: $expected_mode"
  [[ "$(LC_ALL=C tr -d '\n' <"$prefix.body")" == "$expected_body" ]] ||
    fail "$path returned an unexpected body"
}

command -v cargo >/dev/null || fail "cargo is required"
command -v curl >/dev/null || fail "curl is required"
command -v uvx >/dev/null || fail "uvx is required"

if command -v lsof >/dev/null && lsof -nP -iTCP:18080 -sTCP:LISTEN >/dev/null 2>&1; then
  fail "TCP port 18080 is already in use"
fi

mkdir -p "$result_dir"

{
  echo "run_id=$run_id"
  echo "envoy_package=envoy-server==1.39.0"
  echo "envoy_sdk_commit=8eea3285d6bdb89f8ea34632cfe7ce1608a8f374"
  echo "rustc=$(rustc --version)"
  echo "host=$(uname -srm)"
  "$repo_root/scripts/build-modules.sh"
  uvx --from envoy-server==1.39.0 envoy --version
} >"$result_file" 2>&1

ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
  uvx --from envoy-server==1.39.0 envoy \
  --config-path "$repo_root/config/envoy-baseline.yaml" \
  --concurrency 2 \
  --disable-hot-restart \
  --log-level info \
  >"$envoy_log" 2>&1 &
envoy_pid=$!

ready=false
for _ in $(seq 1 100); do
  if curl --silent --fail --output /dev/null http://127.0.0.1:18080/direct; then
    ready=true
    break
  fi
  if ! kill -0 "$envoy_pid" 2>/dev/null; then
    fail "Envoy exited before becoming ready"
  fi
  sleep 0.1
done
[[ "$ready" == "true" ]] || fail "Envoy did not become ready"

assert_response /direct direct rust-baseline
assert_response /scheduler scheduler rust-scheduler

for _ in $(seq 1 50); do
  assert_response /scheduler scheduler rust-scheduler
done

kill -INT "$envoy_pid"
if ! wait "$envoy_pid"; then
  envoy_pid=""
  fail "Envoy did not exit cleanly after SIGINT"
fi
envoy_pid=""

grep -q 'Dynamic module ABI version v0.1.0 matched' "$envoy_log" ||
  fail "Envoy did not report a matching dynamic-module ABI"
if grep -Eiq 'panic|fatal|segmentation fault' "$envoy_log"; then
  fail "Envoy log contains a fatal runtime error"
fi

{
  echo "direct_response=PASS"
  echo "foreign_thread_scheduler_responses=51"
  echo "sigint_cleanup=PASS"
  echo
  echo "===== envoy log ====="
  cat "$envoy_log"
} >>"$result_file"

echo "PASS: pure Rust Envoy dynamic-module baseline"
echo "raw result: $result_file"
