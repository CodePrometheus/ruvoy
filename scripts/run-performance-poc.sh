#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_root="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
result_dir="$result_root/poc4-performance-$run_id"
mode="${RUVOY_BENCH_MODE:-full}"
envoy_package="envoy-server==1.39.0"
oha="$repo_root/.tools/oha/oha"
ruby_version="$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
ruby_bin="${RUVOY_RUBY:-"$HOME/.rbenv/versions/$ruby_version/bin/ruby"}"
bundle_bin="$HOME/.rbenv/versions/$ruby_version/bin/bundle"
bundle_path="$repo_root/vendor/bundle"
bench_duration="2s"
warmup_duration="1s"
control_repetitions=5
oha_timeout_seconds="${RUVOY_BENCH_OHA_TIMEOUT_SECONDS:-60}"
architecture_selection="${RUVOY_BENCH_ARCHITECTURES:-baseline sync fiber puma}"
IFS=' ' read -r -a architectures <<<"$architecture_selection"
scenario_selection="${RUVOY_BENCH_SCENARIOS:-all}"
IFS=' ' read -r -a selected_scenarios <<<"$scenario_selection"
body_matrix="${RUVOY_BENCH_BODY_MATRIX:-false}"

case "$mode" in
  full)
    ;;
  smoke)
    bench_duration="300ms"
    warmup_duration="200ms"
    control_repetitions=1
    ;;
  *)
    echo "RUVOY_BENCH_MODE must be full or smoke" >&2
    exit 1
    ;;
esac
case "$body_matrix" in
  true | false)
    ;;
  *)
    echo "RUVOY_BENCH_BODY_MATRIX must be true or false" >&2
    exit 1
    ;;
esac
[[ "$oha_timeout_seconds" =~ ^[0-9]+$ ]] && [[ "$oha_timeout_seconds" -gt 0 ]] || {
  echo "RUVOY_BENCH_OHA_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 1
}

[[ "${#architectures[@]}" -gt 0 ]] || {
  echo "RUVOY_BENCH_ARCHITECTURES must select at least one architecture" >&2
  exit 1
}
for architecture in "${architectures[@]}"; do
  case "$architecture" in
    baseline | sync | fiber | puma)
      ;;
    *)
      echo "unknown RUVOY_BENCH_ARCHITECTURES entry: $architecture" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$result_dir"
manifest="$result_dir/manifest.tsv"
control_manifest="$result_dir/control-manifest.tsv"
printf 'architecture\tscenario\trun\tprotocol\tconcurrency\twait_ms\trequest_bytes\tresponse_bytes\toha_json\tresources_csv\n' >"$manifest"
printf 'architecture\tstate\trun\toha_json\n' >"$control_manifest"

current_envoy_pid=""
current_envoy_runtime_pid=""
current_puma_pid=""
current_architecture=""
current_envoy_log=""
current_listener_port=""
server_pid_csv=""
stop_status=0
oha_timeout_attempts=$((oha_timeout_seconds * 20))
shutdown_attempts=100

fail() {
  local message="$*"
  printf 'FAIL: %s\nraw results: %s\n' "$message" "$result_dir" >&2
  exit 1
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

signal_if_alive() {
  local signal="$1"
  local pid="$2"

  if process_is_alive "$pid"; then
    kill -"$signal" "$pid" 2>/dev/null || true
  fi
}

stop_envoy() {
  local stopped=true

  signal_if_alive INT "$current_envoy_pid"
  if ! wait_for_exit "$current_envoy_pid" "$shutdown_attempts" ||
    ! wait_for_exit "$current_envoy_runtime_pid" "$shutdown_attempts"; then
    signal_if_alive TERM "$current_envoy_runtime_pid"
    signal_if_alive TERM "$current_envoy_pid"
  fi
  if ! wait_for_exit "$current_envoy_pid" "$shutdown_attempts" ||
    ! wait_for_exit "$current_envoy_runtime_pid" "$shutdown_attempts"; then
    signal_if_alive KILL "$current_envoy_runtime_pid"
    signal_if_alive KILL "$current_envoy_pid"
  fi

  wait_for_exit "$current_envoy_pid" 20 || stopped=false
  wait_for_exit "$current_envoy_runtime_pid" 20 || stopped=false
  if [[ -n "$current_envoy_pid" ]]; then
    if ! wait "$current_envoy_pid" 2>/dev/null; then
      stopped=false
    fi
  fi

  [[ "$stopped" == true ]]
}

stop_puma() {
  local stopped=true

  signal_if_alive INT "$current_puma_pid"
  if ! wait_for_exit "$current_puma_pid" "$shutdown_attempts"; then
    signal_if_alive TERM "$current_puma_pid"
  fi
  if ! wait_for_exit "$current_puma_pid" "$shutdown_attempts"; then
    signal_if_alive KILL "$current_puma_pid"
  fi

  wait_for_exit "$current_puma_pid" 20 || stopped=false
  if [[ -n "$current_puma_pid" ]]; then
    if ! wait "$current_puma_pid" 2>/dev/null; then
      stopped=false
    fi
  fi

  [[ "$stopped" == true ]]
}

cleanup_current() {
  stop_status=0
  stop_envoy || stop_status=1
  stop_puma || stop_status=1
  current_envoy_pid=""
  current_envoy_runtime_pid=""
  current_puma_pid=""
  current_listener_port=""
  server_pid_csv=""
}

trap cleanup_current EXIT INT TERM

wait_for_url() {
  local url="$1"
  local pid="$2"
  for _ in $(seq 1 200); do
    if curl --http1.1 --silent --fail --max-time 1 --output /dev/null "$url"; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
    sleep 0.05
  done
  return 1
}

run_oha_with_timeout() {
  local output="$1"
  shift

  "$@" >"$output" &
  local command_pid=$!
  if ! wait_for_exit "$command_pid" "$oha_timeout_attempts"; then
    signal_if_alive TERM "$command_pid"
    if ! wait_for_exit "$command_pid" 20; then
      signal_if_alive KILL "$command_pid"
    fi
    wait "$command_pid" 2>/dev/null || true
    return 124
  fi
  wait "$command_pid"
}

sample_rss() {
  local pid_csv="$1"
  local output="$2"
  printf 'sample,rss_kib\n' >"$output"
  local sample=0
  while true; do
    local process_stats
    process_stats="$(ps -o rss= -p "$pid_csv" 2>/dev/null || true)"
    if [[ -z "$process_stats" ]]; then
      break
    fi
    local rss
    rss="$(printf '%s\n' "$process_stats" | awk '{ rss += $1 } END { print rss }')"
    printf '%d,%s\n' "$sample" "$rss" >>"$output"
    sample=$((sample + 1))
    sleep 0.1
  done
}

process_cpu_seconds() {
  local pid_csv="$1"
  ps -o time= -p "$pid_csv" 2>/dev/null |
    awk '
      function as_seconds(value, parts, count, days, clock) {
        days = 0
        clock = value
        if (index(value, "-") > 0) {
          split(value, day_parts, "-")
          days = day_parts[1]
          clock = day_parts[2]
        }
        count = split(clock, parts, ":")
        if (count == 3) {
          return days * 86400 + parts[1] * 3600 + parts[2] * 60 + parts[3]
        }
        return days * 86400 + parts[1] * 60 + parts[2]
      }
      { total += as_seconds($1) }
      END { printf "%.6f", total }
    '
}

monotonic_seconds() {
  "$ruby_bin" -e 'printf "%.9f", Process.clock_gettime(Process::CLOCK_MONOTONIC)'
}

validate_oha_result() {
  local json_file="$1"
  jq -e '
    .summary.successRate == 1
    and (.errorDistribution | length) == 0
    and (.statusCodeDistribution | keys) == ["200"]
  ' "$json_file" >/dev/null
}

start_architecture() {
  local architecture="$1"
  local config=""
  local listener_port=""
  local architecture_dir="$result_dir/$architecture"
  mkdir -p "$architecture_dir"
  current_architecture="$architecture"
  current_envoy_log="$architecture_dir/envoy.log"

  case "$architecture" in
    baseline)
      config="$repo_root/config/envoy-baseline.yaml"
      listener_port=18080
      ;;
    sync)
      config="$repo_root/config/envoy-sync-rack.yaml"
      listener_port=18081
      ;;
    fiber)
      config="$repo_root/config/envoy-fiber-rack.yaml"
      listener_port=18083
      ;;
    puma)
      config="$repo_root/config/envoy-puma-benchmark.yaml"
      listener_port=18103
      PATH="$(dirname "$ruby_bin"):$PATH" \
        BUNDLE_GEMFILE="$repo_root/Gemfile" \
        BUNDLE_PATH="$bundle_path" \
        "$ruby_bin" "$bundle_bin" _4.0.10_ exec puma \
        --no-config \
        --environment production \
        --threads 0:100 \
        --workers 0 \
        --bind tcp://127.0.0.1:18110 \
        "$repo_root/bench/config.ru" \
        >"$architecture_dir/puma.log" 2>&1 &
      current_puma_pid=$!
      wait_for_url \
        'http://127.0.0.1:18110/benchmark?wait_ms=0&response_bytes=0' \
        "$current_puma_pid" ||
        fail "Puma did not become ready"
      ;;
    *)
      fail "unknown architecture: $architecture"
      ;;
  esac

  BUNDLE_GEMFILE="$repo_root/Gemfile" \
    BUNDLE_PATH="$bundle_path" \
    BUNDLE_FROZEN=true \
    ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
    uvx --from "$envoy_package" envoy \
    --config-path "$config" \
    --concurrency 1 \
    --disable-hot-restart \
    --log-level warning \
    >"$current_envoy_log" 2>&1 &
  current_envoy_pid=$!

  wait_for_url \
    "http://127.0.0.1:$listener_port/benchmark?wait_ms=0&response_bytes=0" \
    "$current_envoy_pid" ||
    fail "$architecture Envoy did not become ready"

  for _ in $(seq 1 100); do
    current_envoy_runtime_pid="$(pgrep -P "$current_envoy_pid" | head -1 || true)"
    if [[ -n "$current_envoy_runtime_pid" ]]; then
      break
    fi
    sleep 0.01
  done
  [[ -n "$current_envoy_runtime_pid" ]] ||
    fail "$architecture could not resolve the Envoy runtime PID"

  if [[ -n "$current_puma_pid" ]]; then
    server_pid_csv="$current_envoy_runtime_pid,$current_puma_pid"
  else
    server_pid_csv="$current_envoy_runtime_pid"
  fi
  current_listener_port="$listener_port"
}

verify_contract() {
  local architecture="$1"
  local listener_port="$2"
  local architecture_dir="$result_dir/$architecture"
  curl --http1.1 \
    --silent \
    --show-error \
    --request POST \
    --max-time "$oha_timeout_seconds" \
    --header 'Expect:' \
    --data-binary "@$result_dir/body-1024.bin" \
    --dump-header "$architecture_dir/contract.headers" \
    --output "$architecture_dir/contract.body" \
    "http://127.0.0.1:$listener_port/benchmark?wait_ms=0&response_bytes=1024&expected_request_bytes=1024"
  [[ "$(wc -c <"$architecture_dir/contract.body" | tr -d ' ')" == 1024 ]] ||
    fail "$architecture contract returned the wrong response size"
  rg -qi '^x-request-bytes: 1024' "$architecture_dir/contract.headers" ||
    fail "$architecture contract did not read the request body"

  run_oha_with_timeout \
    "$architecture_dir/contract-h2.json" \
    "$oha" \
    --no-tui \
    --output-format json \
    --http2 \
    -n 2 \
    -c 1 \
    -p 2 \
    "http://127.0.0.1:$listener_port/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=0" ||
    fail "$architecture HTTP/2 contract oha timed out or exited non-zero"
  validate_oha_result "$architecture_dir/contract-h2.json" ||
    fail "$architecture HTTP/2 contract failed"
}

run_oha() {
  local architecture="$1"
  local listener_port="$2"
  local scenario="$3"
  local protocol="$4"
  local concurrency="$5"
  local wait_ms="$6"
  local request_bytes="$7"
  local response_bytes="$8"
  local run="$9"
  local run_prefix="$result_dir/$architecture/${scenario}-run${run}"
  local json_file="$run_prefix.json"
  local resource_file="$run_prefix.resources.csv"
  local rss_sample_file="$run_prefix.rss.csv"
  local url="http://127.0.0.1:$listener_port/benchmark?wait_ms=$wait_ms&response_bytes=$response_bytes&expected_request_bytes=$request_bytes"
  local args=(--no-tui --output-format json --wait-ongoing-requests-after-deadline)

  if [[ "$body_matrix" == "true" ]]; then
    local request_count=$((concurrency * 10))
    if [[ "$request_count" -lt 100 ]]; then
      request_count=100
    fi
    args+=(-n "$request_count")
  elif [[ "$request_bytes" -ge 1048576 || "$response_bytes" -ge 1048576 ]]; then
    args+=(-n 20)
  else
    args+=(-z "$bench_duration")
  fi
  if [[ "$protocol" == "2" ]]; then
    args+=(--http2 -c 1 -p "$concurrency")
  else
    args+=(--http-version 1.1 -c "$concurrency")
  fi
  if [[ "$request_bytes" -gt 0 ]]; then
    args+=(--method POST -H 'Expect:' -D "$result_dir/body-$request_bytes.bin")
  fi

  local cpu_before
  local wall_before
  cpu_before="$(process_cpu_seconds "$server_pid_csv")"
  wall_before="$(monotonic_seconds)"
  sample_rss "$server_pid_csv" "$rss_sample_file" &
  local sampler_pid=$!
  set +e
  run_oha_with_timeout "$json_file" "$oha" "${args[@]}" "$url"
  local oha_status=$?
  set -e
  local wall_after
  local cpu_after
  wall_after="$(monotonic_seconds)"
  cpu_after="$(process_cpu_seconds "$server_pid_csv")"
  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  [[ "$oha_status" -eq 0 ]] || fail "$architecture $scenario run $run: oha exited $oha_status"
  validate_oha_result "$json_file" ||
    fail "$architecture $scenario run $run returned errors"

  local cpu_percent
  local rss_max
  cpu_percent="$(
    awk \
      -v before="$cpu_before" \
      -v after="$cpu_after" \
      -v start="$wall_before" \
      -v finish="$wall_after" \
      'BEGIN {
        elapsed = finish - start
        if (elapsed <= 0) {
          print 0
        } else {
          printf "%.3f", (after - before) * 100 / elapsed
        }
      }'
  )"
  rss_max="$(
    awk -F ',' 'NR > 1 && $2 > maximum { maximum = $2 } END { print maximum + 0 }' \
      "$rss_sample_file"
  )"
  printf 'sample,cpu_percent,rss_kib\n0,%s,%s\n' \
    "$cpu_percent" "$rss_max" >"$resource_file"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$architecture" \
    "$scenario" \
    "$run" \
    "$protocol" \
    "$concurrency" \
    "$wait_ms" \
    "$request_bytes" \
    "$response_bytes" \
    "${json_file#"$result_dir/"}" \
    "${resource_file#"$result_dir/"}" \
    >>"$manifest"
}

run_control_probe() {
  local architecture="$1"
  local listener_port="$2"
  local control_port="$3"
  local architecture_dir="$result_dir/$architecture"

  for run in $(seq 1 "$control_repetitions"); do
    local idle_json="$architecture_dir/control-idle-run$run.json"
    run_oha_with_timeout \
      "$idle_json" \
      "$oha" \
      --no-tui \
      --output-format json \
      --wait-ongoing-requests-after-deadline \
      --http-version 1.1 \
      -z 500ms \
      -c 10 \
      "http://127.0.0.1:$control_port/" ||
      fail "$architecture idle control probe timed out or exited non-zero"
    validate_oha_result "$idle_json" || fail "$architecture idle control probe failed"
    printf '%s\tidle\t%s\t%s\n' \
      "$architecture" "$run" "${idle_json#"$result_dir/"}" >>"$control_manifest"

    local load_json="$architecture_dir/control-load-run$run.json"
    "$oha" \
      --no-tui \
      --output-format json \
      --http-version 1.1 \
      -n 10 \
      -c 10 \
      "http://127.0.0.1:$listener_port/benchmark?wait_ms=200&response_bytes=0" \
      >"$load_json" &
    local load_pid=$!
    sleep 0.05
    local loaded_json="$architecture_dir/control-loaded-run$run.json"
    run_oha_with_timeout \
      "$loaded_json" \
      "$oha" \
      --no-tui \
      --output-format json \
      --wait-ongoing-requests-after-deadline \
      --http-version 1.1 \
      -z 500ms \
      -c 10 \
      "http://127.0.0.1:$control_port/" || {
        signal_if_alive TERM "$load_pid"
        wait "$load_pid" 2>/dev/null || true
        fail "$architecture loaded control probe timed out or exited non-zero"
      }
    if ! wait_for_exit "$load_pid" "$oha_timeout_attempts"; then
      signal_if_alive TERM "$load_pid"
      wait_for_exit "$load_pid" 20 || signal_if_alive KILL "$load_pid"
      wait "$load_pid" 2>/dev/null || true
      fail "$architecture slow Ruby load timed out"
    fi
    wait "$load_pid" || fail "$architecture slow Ruby load failed"
    validate_oha_result "$load_json" || fail "$architecture slow Ruby load returned errors"
    validate_oha_result "$loaded_json" || fail "$architecture loaded control probe failed"
    printf '%s\tloaded\t%s\t%s\n' \
      "$architecture" "$run" "${loaded_json#"$result_dir/"}" >>"$control_manifest"
  done
}

scenario_rows() {
  if [[ "$body_matrix" == "true" ]]; then
    local request_bytes
    local concurrency
    for request_bytes in 1024 65536 262144 1048576 2097152; do
      for concurrency in 1 10 100; do
        printf 'request_%s_h1_c%s|1.1|%s|0|%s|0|5\n' \
          "$request_bytes" "$concurrency" "$concurrency" "$request_bytes"
      done
    done
    return
  fi

  printf '%s\n' \
    'noop_h1_c1|1.1|1|0|0|0|5' \
    'noop_h1_c100|1.1|100|0|0|0|5' \
    'noop_h2_c100|2|100|0|0|0|5' \
    'small_h1_c10|1.1|10|0|1024|1024|1' \
    'large_request_h1_c10|1.1|10|0|1048576|0|5' \
    'large_response_h1_c10|1.1|10|0|0|1048576|5' \
    'wait10_h1_c10|1.1|10|10|0|0|1' \
    'wait200_h1_c1|1.1|1|200|0|0|1' \
    'wait200_h1_c10|1.1|10|200|0|0|5' \
    'wait200_h1_c100|1.1|100|200|0|0|5'
}

scenario_is_selected() {
  local wanted="$1"
  if [[ "$scenario_selection" == "all" ]]; then
    return 0
  fi
  for selected in "${selected_scenarios[@]}"; do
    if [[ "$selected" == "$wanted" ]]; then
      return 0
    fi
  done
  return 1
}

for tool in cargo curl dd jq pgrep ps rg uvx; do
  command -v "$tool" >/dev/null || fail "$tool is required"
done
[[ -x "$oha" ]] ||
  fail "missing project-local oha: run scripts/install-oha.sh"
[[ "$("$oha" --version)" == "oha 1.15.0" ]] || fail "expected oha 1.15.0"
[[ -x "$ruby_bin" ]] || fail "missing Ruby $ruby_version: $ruby_bin"
[[ "$("$ruby_bin" -e 'print RUBY_VERSION')" == "$ruby_version" ]] ||
  fail "expected Ruby $ruby_version"

for port in 18080 18081 18082 18083 18084 18103 18104 18110; do
  if command -v lsof >/dev/null &&
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "TCP port $port is already in use"
  fi
done

for request_bytes in 1024 65536 262144 1048576 2097152; do
  dd if=/dev/zero of="$result_dir/body-$request_bytes.bin" \
    bs="$request_bytes" count=1 2>/dev/null
done

{
  printf 'run_id=%s\nmode=%s\narchitectures=%s\nscenarios=%s\nbody_matrix=%s\nhost=%s\n' \
    "$run_id" "$mode" "$architecture_selection" "$scenario_selection" "$body_matrix" "$(uname -srm)"
  printf 'envoy_package=%s\nenvoy_sdk_commit=%s\n' \
    "$envoy_package" "8eea3285d6bdb89f8ea34632cfe7ce1608a8f374"
  printf 'ruby=%s\noha=%s\n' "$("$ruby_bin" --version)" "$("$oha" --version)"
  printf 'rustc=%s\nrack=3.2.6\npuma=7.2.0\n' "$(rustc --version)"
  printf 'bench_duration=%s\nwarmup_duration=%s\noha_timeout_seconds=%s\n' \
    "$bench_duration" "$warmup_duration" "$oha_timeout_seconds"
  RUVOY_BUILD_PROFILE=release "$repo_root/scripts/build-modules.sh"
  RUVOY_BUILD_PROFILE=release "$repo_root/scripts/build-sync-module.sh"
  RUVOY_BUILD_PROFILE=release "$repo_root/scripts/build-fiber-module.sh"
  "$repo_root/scripts/check-worker-boundary.sh"
} >"$result_dir/preflight.log" 2>&1 || fail "build or preflight failed"

for architecture in "${architectures[@]}"; do
  start_architecture "$architecture"
  listener_port="$current_listener_port"
  verify_contract "$architecture" "$listener_port"
  run_oha_with_timeout \
    "$result_dir/$architecture/warmup.json" \
    "$oha" \
    --no-tui \
    --output-format json \
    --wait-ongoing-requests-after-deadline \
    --http-version 1.1 \
    -z "$warmup_duration" \
    -c 10 \
    "http://127.0.0.1:$listener_port/benchmark?wait_ms=0&response_bytes=0" ||
    fail "$architecture warmup timed out or exited non-zero"
  validate_oha_result "$result_dir/$architecture/warmup.json" ||
    fail "$architecture warmup returned errors"

  while IFS='|' read -r scenario protocol concurrency wait_ms request_bytes response_bytes repetitions; do
    if ! scenario_is_selected "$scenario"; then
      continue
    fi
    if [[ "$mode" == "smoke" ]]; then
      repetitions=1
    fi
    for run in $(seq 1 "$repetitions"); do
      run_oha \
        "$architecture" \
        "$listener_port" \
        "$scenario" \
        "$protocol" \
        "$concurrency" \
        "$wait_ms" \
        "$request_bytes" \
        "$response_bytes" \
        "$run"
    done
  done < <(scenario_rows)

  case "$architecture" in
    sync)
      run_control_probe "$architecture" "$listener_port" 18082
      ;;
    fiber)
      run_control_probe "$architecture" "$listener_port" 18084
      ;;
  esac

  cleanup_current
  [[ "$stop_status" -eq 0 ]] || fail "$architecture did not stop cleanly"
  case "$architecture" in
    sync)
      rg -Fq '[ruvoy] Ruby runtime stopped' "$current_envoy_log" ||
        fail "sync Ruby runtime did not report clean shutdown"
      ;;
    fiber)
      rg -Fq '[ruvoy] Fiber runtime stopped' "$current_envoy_log" ||
        fail "Fiber runtime did not report clean shutdown"
      ;;
  esac
done

bridge_repetitions=5
if [[ "$mode" == "smoke" ]]; then
  bridge_repetitions=1
fi
for run in $(seq 1 "$bridge_repetitions"); do
  PATH="$(dirname "$ruby_bin"):$PATH" \
    RUBY="$ruby_bin" \
    cargo run \
    --quiet \
    --release \
    --manifest-path "$repo_root/Cargo.toml" \
    --example bridge_benchmark \
    >"$result_dir/bridge-run$run.json"
  jq -e '.pure_rust.p99_us >= 0 and .ruby_bridge.p99_us > 0' \
    "$result_dir/bridge-run$run.json" >/dev/null ||
    fail "standalone bridge benchmark run $run returned invalid JSON"
done

"$ruby_bin" "$repo_root/bench/summarize.rb" "$result_dir"
printf 'result=PASS\nraw_results=%s\nsummary=%s\n' \
  "$result_dir" "$result_dir/summary.md"
