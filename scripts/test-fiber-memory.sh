#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_root="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
result_dir="$result_root/poc3-fiber-memory-$run_id"
envoy_log="$result_dir/envoy.log"
waves_file="$result_dir/waves.tsv"
oha="$repo_root/.tools/oha/oha"
ruby_version="$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
fiber_port=18083
control_port=18084
envoy_launcher_pid=""
envoy_runtime_pid=""
request_bytes="${RUVOY_MEMORY_REQUEST_BYTES:-0}"
requests_per_wave="${RUVOY_MEMORY_REQUESTS_PER_WAVE:-10000}"
concurrency="${RUVOY_MEMORY_CONCURRENCY:-100}"
waves="${RUVOY_MEMORY_WAVES:-5}"
wave_timeout_seconds="${RUVOY_MEMORY_WAVE_TIMEOUT_SECONDS:-60}"

mkdir -p "$result_dir"
printf 'wave\trequests\theap_live_slots\trss_kib\n' >"$waves_file"

process_is_alive() {
  local pid="$1"
  local state

  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null || return 1
  state="$(ps -o stat= -p "$pid" 2>/dev/null | tr -d '[:space:]')"
  [[ -n "$state" && "$state" != Z* ]]
}

wait_for_exit() {
  local pid="$1"
  local attempts="$2"

  [[ -z "$pid" ]] && return 0
  for _ in $(seq 1 "$attempts"); do
    if ! process_is_alive "$pid"; then
      return 0
    fi
    sleep 0.05
  done
  ! process_is_alive "$pid"
}

signal_if_alive() {
  local signal="$1"
  local pid="$2"

  if process_is_alive "$pid"; then
    kill -"$signal" "$pid" 2>/dev/null || true
  fi
}

stop_envoy() {
  local stopped=true

  signal_if_alive INT "$envoy_launcher_pid"
  if ! wait_for_exit "$envoy_launcher_pid" 100 ||
    ! wait_for_exit "$envoy_runtime_pid" 100; then
    signal_if_alive TERM "$envoy_runtime_pid"
    signal_if_alive TERM "$envoy_launcher_pid"
  fi
  if ! wait_for_exit "$envoy_launcher_pid" 100 ||
    ! wait_for_exit "$envoy_runtime_pid" 100; then
    signal_if_alive KILL "$envoy_runtime_pid"
    signal_if_alive KILL "$envoy_launcher_pid"
  fi

  wait_for_exit "$envoy_launcher_pid" 20 || stopped=false
  wait_for_exit "$envoy_runtime_pid" 20 || stopped=false
  if [[ -n "$envoy_launcher_pid" ]]; then
    if ! wait "$envoy_launcher_pid" 2>/dev/null; then
      stopped=false
    fi
  fi

  [[ "$stopped" == true ]]
}

cleanup() {
  stop_envoy || true
}
trap cleanup EXIT INT TERM

fail() {
  printf 'FAIL: %s\nraw results: %s\n' "$*" "$result_dir" >&2
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

for tool in curl jq lsof pgrep ps rg uvx; do
  command -v "$tool" >/dev/null || fail "$tool is required"
done
for value_name in request_bytes requests_per_wave concurrency waves wave_timeout_seconds; do
  value="${!value_name}"
  [[ "$value" =~ ^[0-9]+$ ]] || fail "$value_name must be a non-negative integer"
done
[[ "$request_bytes" -le 2097152 ]] || fail "request_bytes exceeds the 2 MiB PoC limit"
[[ "$requests_per_wave" -gt 0 ]] || fail "requests_per_wave must be positive"
[[ "$concurrency" -gt 0 ]] || fail "concurrency must be positive"
[[ "$waves" -gt 0 ]] || fail "waves must be positive"
[[ "$wave_timeout_seconds" -gt 0 ]] || fail "wave_timeout_seconds must be positive"
[[ -x "$oha" ]] || fail "missing project-local oha"
for port in "$fiber_port" "$control_port"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "TCP port $port is already in use"
  fi
done

{
  printf 'run_id=%s\nruby_version=%s\nasync_version=%s\n' "$run_id" "$ruby_version" "$("$repo_root/scripts/gem-version.sh" async)"
  printf 'request_bytes=%s\nrequests_per_wave=%s\nconcurrency=%s\nwaves=%s\nwave_timeout_seconds=%s\n' \
    "$request_bytes" "$requests_per_wave" "$concurrency" "$waves" "$wave_timeout_seconds"
  printf 'envoy_package=envoy-server==1.39.0\noha=%s\n' "$("$oha" --version)"
  RUVOY_BUILD_PROFILE=release "$repo_root/scripts/build-fiber-module.sh"
} >"$result_dir/preflight.log" 2>&1 || fail "Fiber release build failed"

BUNDLE_GEMFILE="$repo_root/Gemfile" \
  BUNDLE_PATH="$repo_root/vendor/bundle" \
  BUNDLE_FROZEN=true \
  ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
  uvx --from envoy-server==1.39.0 envoy \
  --config-path "$repo_root/config/envoy-fiber-rack.yaml" \
  --concurrency 1 \
  --disable-hot-restart \
  --log-level warning \
  >"$envoy_log" 2>&1 &
envoy_launcher_pid=$!

ready=false
for _ in $(seq 1 200); do
  if curl --http1.1 --silent --fail --output /dev/null \
    "http://127.0.0.1:$fiber_port/gc-stats"; then
    ready=true
    break
  fi
  if ! kill -0 "$envoy_launcher_pid" 2>/dev/null; then
    fail "Envoy exited before the Fiber listener became ready"
  fi
  sleep 0.05
done
[[ "$ready" == true ]] || fail "Fiber listener did not become ready"

for _ in $(seq 1 100); do
  envoy_runtime_pid="$(pgrep -P "$envoy_launcher_pid" | head -1 || true)"
  [[ -n "$envoy_runtime_pid" ]] && break
  sleep 0.01
done
[[ -n "$envoy_runtime_pid" ]] || fail "could not resolve the Envoy runtime PID"

oha_body_args=(--method GET)
if [[ "$request_bytes" -gt 0 ]]; then
  dd if=/dev/zero of="$result_dir/body-$request_bytes.bin" \
    bs="$request_bytes" count=1 2>/dev/null
  oha_body_args=(--method POST -H 'Expect:' -D "$result_dir/body-$request_bytes.bin")
fi

for wave in $(seq 1 "$waves"); do
  oha_json="$result_dir/wave-$wave.json"
  "$oha" \
    --no-tui \
    --output-format json \
    --http-version 1.1 \
    -n "$requests_per_wave" \
    -c "$concurrency" \
    "${oha_body_args[@]}" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=$request_bytes" \
    >"$oha_json" &
  oha_pid=$!
  wave_started_at="$(date +%s)"
  while process_is_alive "$oha_pid"; do
    if (( $(date +%s) - wave_started_at >= wave_timeout_seconds )); then
      signal_if_alive TERM "$oha_pid"
      wait_for_exit "$oha_pid" 20 || signal_if_alive KILL "$oha_pid"
      wait "$oha_pid" 2>/dev/null || true
      fail "wave $wave exceeded the ${wave_timeout_seconds}s timeout"
    fi
    sleep 0.05
  done
  wait "$oha_pid" 2>/dev/null || fail "wave $wave oha exited non-zero"
  jq -e '
    .summary.successRate == 1
    and (.errorDistribution | length) == 0
    and (.statusCodeDistribution | keys) == ["200"]
  ' "$oha_json" >/dev/null || fail "wave $wave returned request errors"

  headers="$result_dir/wave-$wave-gc.headers"
  curl --http1.1 \
    --silent \
    --show-error \
    --fail \
    --dump-header "$headers" \
    --output "$result_dir/wave-$wave-gc.body" \
    "http://127.0.0.1:$fiber_port/gc-stats"
  live_slots="$(header_value "$headers" x-ruby-heap-live-slots)"
  rss_kib="$(ps -o rss= -p "$envoy_runtime_pid" | tr -d '[:space:]')"
  [[ "$live_slots" =~ ^[0-9]+$ ]] || fail "wave $wave did not report Ruby live slots"
  [[ "$rss_kib" =~ ^[0-9]+$ ]] || fail "wave $wave did not report Envoy RSS"
  printf '%s\t%s\t%s\t%s\n' \
    "$wave" "$requests_per_wave" "$live_slots" "$rss_kib" >>"$waves_file"
done

first_live="$(awk -F '\t' 'NR == 2 { print $3 }' "$waves_file")"
last_live="$(awk -F '\t' 'END { print $3 }' "$waves_file")"
first_rss="$(awk -F '\t' 'NR == 2 { print $4 }' "$waves_file")"
last_rss="$(awk -F '\t' 'END { print $4 }' "$waves_file")"

awk -v first="$first_live" -v last="$last_live" '
  BEGIN {
    limit = first * 1.10 + 5000
    exit !(last <= limit)
  }
' || fail "Ruby live slots grew beyond the bounded threshold: $first_live -> $last_live"
awk -v first="$first_rss" -v last="$last_rss" '
  BEGIN {
    proportional = first * 1.25
    absolute = first + 65536
    limit = proportional > absolute ? proportional : absolute
    exit !(last <= limit)
  }
' || fail "Envoy RSS kept growing beyond the bounded threshold: $first_rss -> $last_rss KiB"

stop_envoy || fail "Envoy did not stop after the bounded shutdown sequence"
envoy_launcher_pid=""
envoy_runtime_pid=""
rg -Fq '[ruvoy] Fiber runtime stopped' "$envoy_log" ||
  fail "Fiber runtime did not report clean shutdown"

printf 'result=PASS\nheap_live_slots=%s->%s\nrss_kib=%s->%s\nraw_results=%s\n' \
  "$first_live" "$last_live" "$first_rss" "$last_rss" "$result_dir"
