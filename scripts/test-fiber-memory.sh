#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_root="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
result_dir="$result_root/poc3-fiber-memory-$run_id"
envoy_log="$result_dir/envoy.log"
envoy_config="$result_dir/envoy.yaml"
waves_file="$result_dir/waves.tsv"
oha="$repo_root/.tools/oha/oha"
ruby_version="$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
fiber_port=19183
control_port=19184
admin_port=19185
envoy_launcher_pid=""
envoy_runtime_pid=""
body_bytes="${RUVOY_MEMORY_BODY_BYTES:-65536}"
requests_per_wave="${RUVOY_MEMORY_REQUESTS_PER_WAVE:-10000}"
concurrency="${RUVOY_MEMORY_CONCURRENCY:-100}"
waves="${RUVOY_MEMORY_WAVES:-3}"
wave_timeout_seconds="${RUVOY_MEMORY_WAVE_TIMEOUT_SECONDS:-60}"

mkdir -p "$result_dir"
printf 'wave\tshape\trequests\tslots_before\tslots_after\tretained\trss_kib\tfds\tthreads\n' \
  >"$waves_file"

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

for tool in curl jq lsof pgrep ps uvx; do
  command -v "$tool" >/dev/null || fail "$tool is required"
done
for value_name in body_bytes requests_per_wave concurrency waves wave_timeout_seconds; do
  value="${!value_name}"
  [[ "$value" =~ ^[0-9]+$ ]] || fail "$value_name must be a non-negative integer"
done
[[ "$body_bytes" -le 2097152 ]] || fail "body_bytes exceeds the 2 MiB fixture limit"
[[ "$requests_per_wave" -gt 0 ]] || fail "requests_per_wave must be positive"
[[ "$concurrency" -gt 0 ]] || fail "concurrency must be positive"
[[ "$waves" -gt 0 ]] || fail "waves must be positive"
[[ "$wave_timeout_seconds" -gt 0 ]] || fail "wave_timeout_seconds must be positive"
[[ -x "$oha" ]] || fail "missing project-local oha"
for port in "$fiber_port" "$control_port" "$admin_port"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "TCP port $port is already in use"
  fi
done

sed \
  "s|value: bench/config.ru|value: $repo_root/test/fixtures/rack/config.ru|" \
  "$repo_root/config/envoy-fiber-rack.yaml" >"$envoy_config"
grep -Fq "value: $repo_root/test/fixtures/rack/config.ru" "$envoy_config" ||
  fail "failed to configure the Rack fixture"

{
  printf 'run_id=%s\nruby_version=%s\nasync_version=%s\n' "$run_id" "$ruby_version" "$("$repo_root/scripts/gem-version.sh" async)"
  printf 'body_bytes=%s\nrequests_per_wave=%s\nconcurrency=%s\nwaves=%s\nwave_timeout_seconds=%s\n' \
    "$body_bytes" "$requests_per_wave" "$concurrency" "$waves" "$wave_timeout_seconds"
  printf 'envoy_package=envoy-server==1.39.0\noha=%s\n' "$("$oha" --version)"
  RUVOY_BUILD_PROFILE=release "$repo_root/scripts/build-fiber-module.sh"
} >"$result_dir/preflight.log" 2>&1 || fail "Fiber release build failed"

BUNDLE_GEMFILE="$repo_root/Gemfile" \
  BUNDLE_PATH="$repo_root/vendor/bundle" \
  BUNDLE_FROZEN=true \
  ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
  uvx --from envoy-server==1.39.0 envoy \
  --config-path "$envoy_config" \
  --config-yaml "admin: {address: {socket_address: {address: 127.0.0.1, port_value: $admin_port}}}" \
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

# Retention lives in a code path, not in a request count: a queued request
# body, a fiber that parks, a fiber that is stopped when its client leaves, an
# error unwind. Driving one shape only proves that one path is clean, so every
# shape gets its own before/after measurement.
shapes=(noop request-body response-body parked raised cancelled)
body_file="$result_dir/request-body.bin"
dd if=/dev/zero of="$body_file" bs="$body_bytes" count=1 2>/dev/null

benchmark_url() {
  printf 'http://127.0.0.1:%s/benchmark?wait_ms=%s&response_bytes=%s&expected_request_bytes=%s' \
    "$fiber_port" "$1" "$2" "$3"
}

# Fills `shape_args` with the oha arguments for one shape.
set_shape_arguments() {
  case "$1" in
    noop) shape_args=(--method GET "$(benchmark_url 0 0 0)") ;;
    request-body) shape_args=(--method POST -H 'Expect:' -D "$body_file"
      "$(benchmark_url 0 0 "$body_bytes")") ;;
    response-body) shape_args=(--method GET "$(benchmark_url 0 "$body_bytes" 0)") ;;
    parked) shape_args=(--method GET "$(benchmark_url 20 0 0)") ;;
    raised) shape_args=(--method GET "http://127.0.0.1:$fiber_port/raise") ;;
    # Every request is abandoned well before the application would answer, so
    # the runtime has to reclaim a fiber it stopped rather than one that ended.
    cancelled) shape_args=(-t 50ms --method GET
      "http://127.0.0.1:$fiber_port/async-sleep?seconds=1") ;;
    *) fail "unknown request shape: $1" ;;
  esac
}

# The status distribution a shape is allowed to produce, as a jq filter.
shape_expectation() {
  case "$1" in
    raised) printf '(.statusCodeDistribution | keys) == ["500"]' ;;
    # An abandoned request has no status to report, so only the runtime's
    # survival is asserted for this shape.
    cancelled) printf 'true' ;;
    *) printf '.summary.successRate == 1 and (.errorDistribution | length) == 0 and (.statusCodeDistribution | keys) == ["200"]' ;;
  esac
}

# Live slots after a full collection, so what is reported is what is retained.
sample_slots() {
  local headers="$result_dir/$1.headers"
  curl --http1.1 --silent --show-error --fail \
    --dump-header "$headers" --output /dev/null \
    "http://127.0.0.1:$fiber_port/gc-stats" || return 1
  header_value "$headers" x-ruby-heap-live-slots
}

# One statistic the module publishes, read through Envoy's admin endpoint.
module_stat() {
  curl --silent --max-time 5 "http://127.0.0.1:$admin_port/stats" |
    awk -F': ' -v name="$1" '$0 ~ "dynamicmodules.*" name ": " { print $2; exit }'
}

# Blocks until the runtime reports nothing in flight.
#
# A fiber whose client walked away is stopped by the reactor on its next pass,
# so the load generator exiting does not mean the work is over; sampling then
# measures a runtime still winding down. The gauges are written as requests
# begin and end, which is why each poll sends one.
wait_until_idle() {
  local attempt inflight
  for attempt in $(seq 1 100); do
    curl --http1.1 --silent --show-error --fail --output /dev/null \
      "http://127.0.0.1:$fiber_port/counted" || return 1
    inflight="$(module_stat inflight_requests)"
    [[ "$inflight" == 0 ]] && return 0
    sleep 0.1
  done
  fail "the runtime still had $inflight requests in flight after 10s of quiet"
}

# File descriptors and threads of the proxy, which leak in their own right.
process_entries() {
  if [[ -d "/proc/$envoy_runtime_pid/$1" ]]; then
    find "/proc/$envoy_runtime_pid/$1" -mindepth 1 -maxdepth 1 | wc -l | tr -d ' '
  elif [[ "$1" == fd ]]; then
    lsof -p "$envoy_runtime_pid" 2>/dev/null | tail -n +2 | wc -l | tr -d ' '
  else
    echo 0
  fi
}

for wave in $(seq 1 "$waves"); do
  for shape in "${shapes[@]}"; do
    label="wave-$wave-$shape"
    wait_until_idle || fail "$label could not reach a quiet runtime before the run"
    slots_before="$(sample_slots "$label-before")" ||
      fail "$label could not read the heap before the run"

    set_shape_arguments "$shape"
    oha_json="$result_dir/$label.json"
    "$oha" --no-tui --output-format json --http-version 1.1 \
      -n "$requests_per_wave" -c "$concurrency" \
      "${shape_args[@]}" >"$oha_json" &
    oha_pid=$!
    wave_started_at="$(date +%s)"
    while process_is_alive "$oha_pid"; do
      if (( $(date +%s) - wave_started_at >= wave_timeout_seconds )); then
        signal_if_alive TERM "$oha_pid"
        wait_for_exit "$oha_pid" 20 || signal_if_alive KILL "$oha_pid"
        wait "$oha_pid" 2>/dev/null || true
        fail "$label exceeded the ${wave_timeout_seconds}s timeout"
      fi
      sleep 0.05
    done
    wait "$oha_pid" 2>/dev/null || true
    jq -e "$(shape_expectation "$shape")" "$oha_json" >/dev/null ||
      fail "$label did not answer as its shape requires"

    wait_until_idle || fail "$label could not reach a quiet runtime after the run"
    slots_after="$(sample_slots "$label-after")" ||
      fail "$label could not read the heap after the run"
    rss_kib="$(ps -o rss= -p "$envoy_runtime_pid" | tr -d '[:space:]')"
    [[ "$slots_after" =~ ^[0-9]+$ ]] || fail "$label did not report Ruby live slots"
    [[ "$rss_kib" =~ ^[0-9]+$ ]] || fail "$label did not report Envoy RSS"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$wave" "$shape" "$requests_per_wave" "$slots_before" "$slots_after" \
      "$((slots_after - slots_before))" "$rss_kib" \
      "$(process_entries fd)" "$(process_entries task)" >>"$waves_file"
  done
done

# The first wave still pays for whatever the runtime allocates once, so the
# budget applies from the second wave on. A single object kept per request
# would be an order of magnitude above it.
retention_budget=$((requests_per_wave / 20 + 2000))
leaking="$(awk -F '\t' -v budget="$retention_budget" '
  NR > 1 && $1 > 1 && $6 > budget { printf "%s(wave %s, +%s) ", $2, $1, $6 }
' "$waves_file")"
[[ -z "$leaking" ]] ||
  fail "these shapes retained more than $retention_budget slots each: $leaking"

first_fds="$(awk -F '\t' 'NR == 2 { print $8 }' "$waves_file")"
last_fds="$(awk -F '\t' 'END { print $8 }' "$waves_file")"
first_threads="$(awk -F '\t' 'NR == 2 { print $9 }' "$waves_file")"
last_threads="$(awk -F '\t' 'END { print $9 }' "$waves_file")"
for value_name in first_fds last_fds first_threads last_threads; do
  [[ "${!value_name}" =~ ^[0-9]+$ ]] || fail "$value_name was not recorded"
done
(( last_fds <= first_fds + 32 )) ||
  fail "file descriptors kept growing: $first_fds -> $last_fds"
(( last_threads <= first_threads + 2 )) ||
  fail "threads kept growing: $first_threads -> $last_threads"

# Conservation: every request the runtime admitted has to end exactly once,
# whether it answered, raised, or had its client walk away. A path that admits
# work without accounting for its end drives these two apart, and no assertion
# about any single shape would notice.
wait_until_idle || fail "the runtime never went quiet after the last wave"
stats="$(curl --silent --max-time 5 "http://127.0.0.1:$admin_port/stats" |
  grep -i dynamicmodules || true)"
printf '%s\n' "$stats" >"$result_dir/stats.txt"
admitted="$(awk -F': ' '/\.requests_total: /  { print $2; exit }' <<<"$stats")"
ended="$(awk -F': ' '/responses_total/ { total += $2 } END { print total + 0 }' <<<"$stats")"
cancelled="$(awk -F': ' '/responses_total.*cancelled/ { print $2; exit }' <<<"$stats")"
inflight="$(awk -F': ' '/inflight_requests: / { print $2; exit }' <<<"$stats")"

[[ "$admitted" =~ ^[0-9]+$ ]] || fail "the module did not report a request count"
[[ "$admitted" == "$ended" ]] ||
  fail "$admitted requests were admitted but $ended ended: some path loses its accounting"
[[ "${cancelled:-0}" -gt 0 ]] ||
  fail "no request was recorded as cancelled: the abandoned shape never abandoned anything"

first_live="$(awk -F '\t' 'NR == 2 { print $4 }' "$waves_file")"
last_live="$(awk -F '\t' 'END { print $5 }' "$waves_file")"
first_rss="$(awk -F '\t' 'NR == 2 { print $7 }' "$waves_file")"
last_rss="$(awk -F '\t' 'END { print $7 }' "$waves_file")"

awk -v first="$first_live" -v last="$last_live" '
  BEGIN {
    limit = first * 1.10 + 5000
    exit !(last <= limit)
  }
' || fail "Ruby live slots grew beyond the bounded threshold: $first_live -> $last_live"
# Resident memory is dominated by the allocator's high-water mark and by
# Envoy's own buffers, neither of which the runtime hands back. What separates
# that from a leak is the shape: a high-water mark converges as the waves repeat
# while a leak adds the same amount every time. Growth is therefore required to
# shrink, or to already be too small to be worth distinguishing.
rss_growth="$(awk -F '\t' '
  NR > 1 { if (previous != "") { print $7 - previous } previous = $7 }
' "$waves_file" | awk -v shapes="${#shapes[@]}" '
  { wave = int((NR - 1) / shapes) + 1; growth[wave] += $1 }
  END { for (index_ = 1; index_ <= wave; index_++) printf "%s ", growth[index_] }
')"
read -r -a rss_growth_per_wave <<<"$rss_growth"
measured_waves="${#rss_growth_per_wave[@]}"
(( measured_waves > 0 )) || fail "resident memory was never sampled"
settled_growth="${rss_growth_per_wave[$((measured_waves - 1))]}"
first_growth="${rss_growth_per_wave[0]}"

# Measured against the first wave rather than the wave before it. Two adjacent
# waves of a curve that has already flattened differ by noise, so comparing
# them decides on the noise: the same commit produced 11324 then 11608 on one
# machine and 16148 then 11316 on another, with the same settled value.
#
# The first wave is where a high-water mark is paid, so what separates it from
# a leak is that later waves add a fraction of it while a leak keeps adding the
# same amount. This bounds a leak larger than a quarter of the startup cost;
# anything finer is caught by the live-slot assertion above, which counts Ruby
# objects rather than pages the allocator never returns.
(( settled_growth <= 4096 || (first_growth > 0 && settled_growth * 4 <= first_growth) )) ||
  fail "resident memory grew by ${settled_growth} KiB in the last wave against ${first_growth} KiB in the first (per wave: ${rss_growth_per_wave[*]}): that is a leak, not a high-water mark"

stop_envoy || fail "Envoy did not stop after the bounded shutdown sequence"
envoy_launcher_pid=""
envoy_runtime_pid=""
grep -Fq '[ruvoy] Fiber runtime stopped' "$envoy_log" ||
  fail "Fiber runtime did not report clean shutdown"

printf 'result=PASS\nshapes=%s\nrequests=%s\nheap_live_slots=%s->%s\nrss_kib=%s->%s\n' \
  "${shapes[*]}" "$((requests_per_wave * waves * ${#shapes[@]}))" \
  "$first_live" "$last_live" "$first_rss" "$last_rss"
printf 'retention_budget_slots=%s\nfds=%s->%s\nthreads=%s->%s\n' \
  "$retention_budget" "$first_fds" "$last_fds" "$first_threads" "$last_threads"
printf 'rss_growth_per_wave_kib=%s\nrequests_admitted=%s\nrequests_ended=%s\n' \
  "${rss_growth_per_wave[*]}" "$admitted" "$ended"
printf 'ended_cancelled=%s\ninflight_requests=%s\nraw_results=%s\n' \
  "$cancelled" "$inflight" "$result_dir"
