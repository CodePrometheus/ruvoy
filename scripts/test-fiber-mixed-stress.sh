#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_root="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
result_dir="$result_root/poc6-fiber-mixed-stress-$run_id"
temporary_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-mixed.XXXXXX")"
envoy_config="$temporary_dir/envoy.yaml"
envoy_log="$result_dir/envoy.log"
resource_file="$result_dir/resources.tsv"
heap_file="$result_dir/heap.tsv"
status_file="$result_dir/status.tsv"
control_file="$result_dir/control.tsv"
phase_file="$temporary_dir/phase"
oha="$repo_root/.tools/oha/oha"
ruby_version="$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
fiber_port=18083
control_port=18084
max_inflight_requests=256
max_inflight_body_bytes=67108864
mode="${RUVOY_MIXED_MODE:-full}"
cycles="${RUVOY_MIXED_CYCLES:-3}"
envoy_concurrency="${RUVOY_MIXED_ENVOY_CONCURRENCY:-1}"
steady_duration="${RUVOY_MIXED_STEADY_DURATION:-30s}"
overload_duration="${RUVOY_MIXED_OVERLOAD_DURATION:-15s}"
recovery_duration="${RUVOY_MIXED_RECOVERY_DURATION:-20s}"
disconnect_requests="${RUVOY_MIXED_DISCONNECT_REQUESTS:-128}"
envoy_launcher_pid=""
envoy_runtime_pid=""
sampler_pid=""
last_load_pid=""
current_load_pids=()

fail() {
  printf 'FAIL: %s\nraw results: %s\n' "$*" "$result_dir" >&2
  exit 1
}

duration_milliseconds() {
  local duration="$1"

  case "$duration" in
    *ms)
      printf '%s\n' "${duration%ms}"
      ;;
    *s)
      printf '%s\n' "$(( ${duration%s} * 1000 ))"
      ;;
    *m)
      printf '%s\n' "$(( ${duration%m} * 60000 ))"
      ;;
  esac
}

request_count_for_rate() {
  local rate="$1"
  local duration="$2"
  local milliseconds

  milliseconds="$(duration_milliseconds "$duration")"
  printf '%s\n' "$(( (rate * milliseconds + 999) / 1000 ))"
}

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

stop_envoy() {
  if process_is_alive "$envoy_launcher_pid"; then
    kill -INT "$envoy_launcher_pid" 2>/dev/null || true
  fi
  if ! wait_for_exit "$envoy_launcher_pid" 100 ||
    ! wait_for_exit "$envoy_runtime_pid" 100; then
    process_is_alive "$envoy_runtime_pid" && kill -TERM "$envoy_runtime_pid" 2>/dev/null || true
    process_is_alive "$envoy_launcher_pid" && kill -TERM "$envoy_launcher_pid" 2>/dev/null || true
  fi
  if ! wait_for_exit "$envoy_launcher_pid" 100 ||
    ! wait_for_exit "$envoy_runtime_pid" 100; then
    process_is_alive "$envoy_runtime_pid" && kill -KILL "$envoy_runtime_pid" 2>/dev/null || true
    process_is_alive "$envoy_launcher_pid" && kill -KILL "$envoy_launcher_pid" 2>/dev/null || true
  fi
  wait_for_exit "$envoy_launcher_pid" 20 && wait_for_exit "$envoy_runtime_pid" 20
  if [[ -n "$envoy_launcher_pid" ]]; then
    wait "$envoy_launcher_pid" 2>/dev/null || true
  fi
}

cleanup() {
  local pid

  if [[ "${#current_load_pids[@]}" -gt 0 ]]; then
    for pid in "${current_load_pids[@]}"; do
      process_is_alive "$pid" && kill -TERM "$pid" 2>/dev/null || true
    done
    for pid in "${current_load_pids[@]}"; do
      wait "$pid" 2>/dev/null || true
    done
  fi
  if process_is_alive "$sampler_pid"; then
    kill -TERM "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
  fi
  stop_envoy || true
  if [[ -n "$temporary_dir" && "$temporary_dir" == */ruvoy-mixed.* ]]; then
    rm -rf -- "$temporary_dir"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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

set_phase() {
  printf '%s\n' "$1" >"$phase_file"
}

sample_resources() {
  while process_is_alive "$envoy_runtime_pid"; do
    local phase
    local stats
    local cpu
    local rss
    local fds

    phase="$(<"$phase_file")"
    stats="$(ps -o %cpu=,rss= -p "$envoy_runtime_pid" 2>/dev/null | awk 'NF >= 2 { print $1 "\t" $2 }')"
    [[ -n "$stats" ]] || break
    cpu="${stats%%$'\t'*}"
    rss="${stats#*$'\t'}"
    fds="$(lsof -nP -p "$envoy_runtime_pid" 2>/dev/null | awk 'NR > 1 { count++ } END { print count + 0 }')"
    printf '%s\t%s\t%s\t%s\t%s\n' "$(date +%s)" "$phase" "$cpu" "$rss" "$fds" >>"$resource_file"
    sleep 1
  done
}

record_heap() {
  local label="$1"
  local headers="$result_dir/$label-gc.headers"
  local body="$result_dir/$label-gc.body"
  local live_slots
  local rss
  local fds

  curl --http1.1 --silent --show-error --fail \
    --dump-header "$headers" \
    --output "$body" \
    "http://127.0.0.1:$fiber_port/gc-stats"
  live_slots="$(header_value "$headers" x-ruby-heap-live-slots)"
  rss="$(ps -o rss= -p "$envoy_runtime_pid" | tr -d '[:space:]')"
  fds="$(lsof -nP -p "$envoy_runtime_pid" 2>/dev/null | awk 'NR > 1 { count++ } END { print count + 0 }')"
  [[ "$live_slots" =~ ^[0-9]+$ ]] || fail "$label did not report Ruby heap live slots"
  [[ "$rss" =~ ^[0-9]+$ ]] || fail "$label did not report Envoy RSS"
  printf '%s\t%s\t%s\t%s\n' "$label" "$live_slots" "$rss" "$fds" >>"$heap_file"
}

start_oha() {
  local output="$1"
  shift

  "$oha" --no-tui --output-format json "$@" >"$output" &
  last_load_pid=$!
}

record_status() {
  local label="$1"
  local json_file="$2"

  jq -r --arg label "$label" '
    [
      $label,
      ([.statusCodeDistribution[]] | add // 0),
      (.statusCodeDistribution["200"] // 0),
      (.statusCodeDistribution["503"] // 0),
      ([.errorDistribution[]] | add // 0)
    ] | @tsv
  ' "$json_file" >>"$status_file"
}

validate_success() {
  local label="$1"
  local json_file="$2"

  jq -e '
    .summary.successRate == 1
    and (.errorDistribution | length) == 0
    and (.statusCodeDistribution | keys) == ["200"]
  ' "$json_file" >/dev/null || fail "$label returned a non-200 response or transport error"
  record_status "$label" "$json_file"
}

validate_overload() {
  local label="$1"
  local json_file="$2"

  jq -e '
    (.errorDistribution | length) == 0
    and ((.statusCodeDistribution["200"] // 0) > 0)
    and ((.statusCodeDistribution["503"] // 0) > 0)
    and ([
      (.statusCodeDistribution | keys[])
      | select(. != "200" and . != "503")
    ] | length) == 0
  ' "$json_file" >/dev/null || fail "$label did not produce the expected 200/503 overload mix"
  record_status "$label" "$json_file"
}

wait_and_validate() {
  local pid="$1"
  local label="$2"
  local json_file="$3"
  local expectation="$4"

  wait "$pid" || fail "$label oha exited non-zero"
  case "$expectation" in
    success)
      validate_success "$label" "$json_file"
      ;;
    overload)
      validate_overload "$label" "$json_file"
      ;;
    *)
      fail "unknown expectation: $expectation"
      ;;
  esac
}

probe_control() {
  local label="$1"
  local body="$result_dir/$label-control.body"
  local elapsed

  elapsed="$(curl --http1.1 --silent --show-error --fail --max-time 2 \
    --write-out '%{time_total}' \
    --output "$body" \
    "http://127.0.0.1:$control_port/")"
  [[ "$(<"$body")" == "fiber-control-ok" ]] || fail "$label control listener returned the wrong body"
  awk -v elapsed="$elapsed" 'BEGIN { exit !(elapsed < 0.5) }' ||
    fail "$label control listener exceeded 500 ms"
  printf '%s\t%s\n' "$label" "$elapsed" >>"$control_file"
}

run_steady_mix() {
  local cycle="$1"
  local prefix="cycle-$cycle-steady"
  local noop_json="$result_dir/$prefix-noop-h2.json"
  local wait_json="$result_dir/$prefix-wait200-h1.json"
  local body_json="$result_dir/$prefix-body256k-h1.json"
  local noop_pid
  local wait_pid
  local body_pid

  set_phase "$prefix"
  start_oha "$noop_json" \
    --wait-ongoing-requests-after-deadline --http2 -c 4 -p 16 \
    -q 1500 --latency-correction -n "$steady_noop_requests" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=0"
  noop_pid=$last_load_pid
  start_oha "$wait_json" \
    --wait-ongoing-requests-after-deadline --http-version 1.1 -c 64 \
    -q 200 --latency-correction -n "$steady_wait_requests" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=200&response_bytes=0&expected_request_bytes=0"
  wait_pid=$last_load_pid
  start_oha "$body_json" \
    --wait-ongoing-requests-after-deadline --http-version 1.1 -c 16 \
    -q 40 --latency-correction -n "$steady_body_requests" \
    --method POST -H 'Expect:' -D "$temporary_dir/body-262144.bin" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=100&response_bytes=1024&expected_request_bytes=262144"
  body_pid=$last_load_pid
  current_load_pids=("$noop_pid" "$wait_pid" "$body_pid")

  wait_and_validate "$noop_pid" "$prefix-noop-h2" "$noop_json" success
  wait_and_validate "$wait_pid" "$prefix-wait200-h1" "$wait_json" success
  wait_and_validate "$body_pid" "$prefix-body256k-h1" "$body_json" success
  current_load_pids=()
}

run_request_overload() {
  local cycle="$1"
  local label="cycle-$cycle-request-overload"
  local json_file="$result_dir/$label.json"
  local pid

  set_phase "$label"
  start_oha "$json_file" \
    --wait-ongoing-requests-after-deadline --http-version 1.1 -c 512 \
    -z "$overload_duration" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=500&response_bytes=0&expected_request_bytes=0"
  pid=$last_load_pid
  current_load_pids=("$pid")
  sleep 0.5
  probe_control "$label"
  wait_and_validate "$pid" "$label" "$json_file" overload
  current_load_pids=()
}

run_body_overload() {
  local cycle="$1"
  local label="cycle-$cycle-body-overload"
  local json_file="$result_dir/$label.json"
  local pid

  set_phase "$label"
  start_oha "$json_file" \
    --wait-ongoing-requests-after-deadline --http-version 1.1 -c 128 \
    -z "$overload_duration" \
    --method POST -H 'Expect:' -D "$temporary_dir/body-1048576.bin" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=500&response_bytes=0&expected_request_bytes=1048576"
  pid=$last_load_pid
  current_load_pids=("$pid")
  sleep 0.5
  probe_control "$label"
  wait_and_validate "$pid" "$label" "$json_file" overload
  current_load_pids=()
}

run_disconnect_churn() {
  local cycle="$1"
  local label="cycle-$cycle-disconnect"
  local pid

  set_phase "$label"
  current_load_pids=()
  for _ in $(seq 1 "$disconnect_requests"); do
    curl --http1.1 --silent --output /dev/null --max-time 0.05 \
      "http://127.0.0.1:$fiber_port/benchmark?wait_ms=500&response_bytes=0&expected_request_bytes=0" &
    current_load_pids+=("$!")
  done
  for pid in "${current_load_pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  current_load_pids=()
  sleep 1
  process_is_alive "$envoy_runtime_pid" || fail "$label terminated Envoy"
  probe_control "$label"
}

run_recovery() {
  local cycle="$1"
  local prefix="cycle-$cycle-recovery"
  local noop_json="$result_dir/$prefix-noop.json"
  local wait_json="$result_dir/$prefix-wait200.json"
  local noop_pid
  local wait_pid

  set_phase "$prefix"
  start_oha "$noop_json" \
    --wait-ongoing-requests-after-deadline --http-version 1.1 -c 64 \
    -q 1000 --latency-correction -n "$recovery_noop_requests" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=0"
  noop_pid=$last_load_pid
  start_oha "$wait_json" \
    --wait-ongoing-requests-after-deadline --http-version 1.1 -c 32 \
    -q 100 --latency-correction -n "$recovery_wait_requests" \
    "http://127.0.0.1:$fiber_port/benchmark?wait_ms=200&response_bytes=0&expected_request_bytes=0"
  wait_pid=$last_load_pid
  current_load_pids=("$noop_pid" "$wait_pid")

  wait_and_validate "$noop_pid" "$prefix-noop" "$noop_json" success
  wait_and_validate "$wait_pid" "$prefix-wait200" "$wait_json" success
  current_load_pids=()
  probe_control "$prefix"
}

for tool in cargo curl dd jq lsof pgrep ps sed seq uvx; do
  command -v "$tool" >/dev/null || fail "$tool is required"
done
[[ -x "$oha" ]] || fail "missing project-local oha"
[[ "$("$oha" --version)" == "oha 1.15.0" ]] || fail "expected oha 1.15.0"
[[ "$cycles" =~ ^[1-9][0-9]*$ ]] || fail "RUVOY_MIXED_CYCLES must be positive"
[[ "$envoy_concurrency" =~ ^[1-9][0-9]*$ ]] || fail "RUVOY_MIXED_ENVOY_CONCURRENCY must be positive"
[[ "$disconnect_requests" =~ ^[1-9][0-9]*$ ]] || fail "RUVOY_MIXED_DISCONNECT_REQUESTS must be positive"
case "$mode" in
  full)
    ;;
  smoke)
    cycles=1
    steady_duration=3s
    overload_duration=3s
    recovery_duration=3s
    disconnect_requests=16
    ;;
  *)
    fail "RUVOY_MIXED_MODE must be full or smoke"
    ;;
esac
for duration in "$steady_duration" "$overload_duration" "$recovery_duration"; do
  [[ "$duration" =~ ^[1-9][0-9]*(ms|s|m)$ ]] || fail "invalid duration: $duration"
done
steady_noop_requests="$(request_count_for_rate 1500 "$steady_duration")"
steady_wait_requests="$(request_count_for_rate 200 "$steady_duration")"
steady_body_requests="$(request_count_for_rate 40 "$steady_duration")"
recovery_noop_requests="$(request_count_for_rate 1000 "$recovery_duration")"
recovery_wait_requests="$(request_count_for_rate 100 "$recovery_duration")"
for port in "$fiber_port" "$control_port"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "TCP port $port is already in use"
  fi
done

mkdir -p "$result_dir"
printf 'epoch\tphase\tcpu_percent\trss_kib\tfd_count\n' >"$resource_file"
printf 'label\theap_live_slots\trss_kib\tfd_count\n' >"$heap_file"
printf 'label\ttotal\tstatus_200\tstatus_503\ttransport_errors\n' >"$status_file"
printf 'label\telapsed_seconds\n' >"$control_file"
printf 'startup\n' >"$phase_file"
dd if=/dev/zero of="$temporary_dir/body-262144.bin" bs=262144 count=1 2>/dev/null
dd if=/dev/zero of="$temporary_dir/body-1048576.bin" bs=1048576 count=1 2>/dev/null
sed \
  "s|value: bench/config.ru|value: $repo_root/test/fixtures/rack/config.ru|" \
  "$repo_root/config/envoy-fiber-rack.yaml" >"$envoy_config"
grep -Fq "value: $repo_root/test/fixtures/rack/config.ru" "$envoy_config" ||
  fail "failed to configure the Rack fixture"

{
  printf 'run_id=%s\nmode=%s\ncycles=%s\nhost=%s\n' "$run_id" "$mode" "$cycles" "$(uname -srm)"
  printf 'envoy_concurrency=%s\n' "$envoy_concurrency"
  printf 'max_inflight_requests=%s\nmax_inflight_body_bytes=%s\n' "$max_inflight_requests" "$max_inflight_body_bytes"
  printf 'steady_duration=%s\noverload_duration=%s\nrecovery_duration=%s\ndisconnect_requests=%s\n' \
    "$steady_duration" "$overload_duration" "$recovery_duration" "$disconnect_requests"
  printf 'ruby=%s\nrustc=%s\noha=%s\n' \
    "$("$HOME/.rbenv/versions/$ruby_version/bin/ruby" --version)" \
    "$(rustc --version)" \
    "$("$oha" --version)"
  printf 'steady_noop_requests=%s\nsteady_wait_requests=%s\nsteady_body_requests=%s\n' \
    "$steady_noop_requests" "$steady_wait_requests" "$steady_body_requests"
  printf 'recovery_noop_requests=%s\nrecovery_wait_requests=%s\n' \
    "$recovery_noop_requests" "$recovery_wait_requests"
} >"$result_dir/manifest.env"

{
  RUVOY_BUILD_PROFILE=release "$repo_root/scripts/build-fiber-module.sh"
  "$repo_root/scripts/check-worker-boundary.sh"
} >"$result_dir/preflight.log" 2>&1 || fail "Fiber build or worker-boundary preflight failed"

RUVOY_MAX_INFLIGHT_REQUESTS="$max_inflight_requests" \
  RUVOY_MAX_INFLIGHT_BODY_BYTES="$max_inflight_body_bytes" \
  BUNDLE_GEMFILE="$repo_root/Gemfile" \
  BUNDLE_PATH="$repo_root/vendor/bundle" \
  BUNDLE_FROZEN=true \
  ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
  uvx --from envoy-server==1.39.0 envoy \
  --config-path "$envoy_config" \
  --concurrency "$envoy_concurrency" \
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
  process_is_alive "$envoy_launcher_pid" || fail "Envoy exited before becoming ready"
  sleep 0.05
done
[[ "$ready" == true ]] || fail "Fiber listener did not become ready"

for _ in $(seq 1 100); do
  envoy_runtime_pid="$(pgrep -P "$envoy_launcher_pid" | head -1 || true)"
  [[ -n "$envoy_runtime_pid" ]] && break
  sleep 0.01
done
[[ -n "$envoy_runtime_pid" ]] || fail "could not resolve the Envoy runtime PID"

sample_resources &
sampler_pid=$!
record_heap baseline

for cycle in $(seq 1 "$cycles"); do
  run_steady_mix "$cycle"
  run_request_overload "$cycle"
  run_body_overload "$cycle"
  run_disconnect_churn "$cycle"
  run_recovery "$cycle"
  record_heap "cycle-$cycle-recovered"
done

first_cycle_heap="$(awk -F '\t' '$1 == "cycle-1-recovered" { print $2 }' "$heap_file")"
last_cycle_heap="$(awk -F '\t' -v label="cycle-$cycles-recovered" '$1 == label { print $2 }' "$heap_file")"
first_cycle_rss="$(awk -F '\t' '$1 == "cycle-1-recovered" { print $3 }' "$heap_file")"
last_cycle_rss="$(awk -F '\t' -v label="cycle-$cycles-recovered" '$1 == label { print $3 }' "$heap_file")"
first_cycle_fds="$(awk -F '\t' '$1 == "cycle-1-recovered" { print $4 }' "$heap_file")"
last_cycle_fds="$(awk -F '\t' -v label="cycle-$cycles-recovered" '$1 == label { print $4 }' "$heap_file")"
awk -v first="$first_cycle_heap" -v last="$last_cycle_heap" '
  BEGIN {
    limit = first * 1.10 + 5000
    exit !(last <= limit)
  }
' || fail "Ruby live slots grew beyond the mixed-stress threshold: $first_cycle_heap -> $last_cycle_heap"
awk -v first="$first_cycle_rss" -v last="$last_cycle_rss" '
  BEGIN {
    proportional = first * 1.25
    absolute = first + 65536
    limit = proportional > absolute ? proportional : absolute
    exit !(last <= limit)
  }
' || fail "Envoy RSS grew beyond the mixed-stress threshold: $first_cycle_rss -> $last_cycle_rss KiB"
awk -v first="$first_cycle_fds" -v last="$last_cycle_fds" '
  BEGIN {
    exit !(last <= first + 32)
  }
' || fail "Envoy FD count grew beyond the mixed-stress threshold: $first_cycle_fds -> $last_cycle_fds"

if process_is_alive "$sampler_pid"; then
  kill -TERM "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
fi
sampler_pid=""
stop_envoy || fail "Envoy did not stop after the bounded shutdown sequence"
envoy_launcher_pid=""
envoy_runtime_pid=""
grep -Fq '[ruvoy] Fiber runtime stopped' "$envoy_log" || fail "Fiber runtime did not report clean shutdown"

printf 'result=PASS\ncycles=%s\nheap_live_slots=%s->%s\nrss_kib=%s->%s\nfd_count=%s->%s\nraw_results=%s\n' \
  "$cycles" "$first_cycle_heap" "$last_cycle_heap" "$first_cycle_rss" "$last_cycle_rss" \
  "$first_cycle_fds" "$last_cycle_fds" "$result_dir"
