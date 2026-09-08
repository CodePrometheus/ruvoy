#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_root="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
result_dir="$result_root/benchmark-$run_id"
mode="${RUVOY_BENCH_MODE:-full}"
envoy_package="envoy-server==1.39.0"
oha="$repo_root/.tools/oha/oha"
ruby_version="$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
ruby_bin="${RUVOY_RUBY:-}"
bundle_bin="${RUVOY_BUNDLE:-}"
bundle_path="$repo_root/vendor/bundle"
bench_duration="${RUVOY_BENCH_DURATION:-30s}"
warmup_duration="${RUVOY_BENCH_WARMUP_DURATION:-10s}"
measurement_rounds="${RUVOY_BENCH_ROUNDS:-7}"
warmup_concurrency="${RUVOY_BENCH_WARMUP_CONCURRENCY:-10}"
control_repetitions="$measurement_rounds"
oha_timeout_seconds="${RUVOY_BENCH_OHA_TIMEOUT_SECONDS:-180}"
envoy_concurrency="${RUVOY_BENCH_ENVOY_CONCURRENCY:-1}"
puma_workers="${RUVOY_BENCH_PUMA_WORKERS:-0}"
puma_threads="${RUVOY_BENCH_PUMA_THREADS:-100}"
# Falcon defaults to 11 instances; the comparison needs a single process so
# it matches Ruvoy's one Ruby runtime and direct Puma's single worker.
falcon_count="${RUVOY_BENCH_FALCON_COUNT:-1}"
# baseline and sync stay available as diagnostics but must not dilute the
# application-server comparison, so they are no longer selected by default.
architecture_selection="${RUVOY_BENCH_ARCHITECTURES:-fiber falcon_direct puma_direct envoy_puma}"
load_generator="${RUVOY_BENCH_LOAD_GENERATOR:-remote}"
load_generator_ssh="${RUVOY_BENCH_LOAD_GENERATOR_SSH:-}"
load_generator_oha="${RUVOY_BENCH_LOAD_GENERATOR_OHA:-oha}"
listen_address="${RUVOY_BENCH_LISTEN_ADDRESS:-127.0.0.1}"
target_address="${RUVOY_BENCH_TARGET_ADDRESS:-127.0.0.1}"
allow_dirty="${RUVOY_BENCH_ALLOW_DIRTY:-0}"
remote_body_dir=""
oha_version=""
# Readiness and contract checks run from the runner host, not the load
# generator, so they need a locally reachable address.
probe_address="$listen_address"
[[ "$probe_address" == "0.0.0.0" ]] && probe_address=127.0.0.1
IFS=' ' read -r -a architectures <<<"$architecture_selection"
architecture_order_mode="${RUVOY_BENCH_ORDER_MODE:-rotate}"
scenario_selection="${RUVOY_BENCH_SCENARIOS:-all}"
IFS=' ' read -r -a selected_scenarios <<<"$scenario_selection"
body_matrix="${RUVOY_BENCH_BODY_MATRIX:-false}"
# Ruvoy's premise is that Envoy already terminates TLS, so a plain-only
# benchmark systematically understates it: the direct servers would never pay
# for a handshake. Both modes run by default.
tls_selection="${RUVOY_BENCH_TLS:-both}"
tls_negotiation_baseline=""
# A fresh round should not inherit the previous one's GC state, page cache or
# thermal condition.
cooldown_seconds="${RUVOY_BENCH_COOLDOWN_SECONDS:-5}"
# Above this the load generator itself is the bottleneck and the round measures
# the generator rather than the server.
load_generator_cpu_limit="${RUVOY_BENCH_LOAD_GENERATOR_CPU_LIMIT:-80}"
load_generator_cores=""
idle_load_limit="${RUVOY_BENCH_IDLE_LOAD_LIMIT:-1.0}"

fail() {
  local message="$*"
  printf 'FAIL: %s\nraw results: %s\n' "$message" "$result_dir" >&2
  exit 1
}

case "$mode" in
  full)
    ;;
  smoke)
    bench_duration="250ms"
    warmup_duration="100ms"
    measurement_rounds=1
    warmup_concurrency=1
    control_repetitions=1
    ;;
  *)
    fail "RUVOY_BENCH_MODE must be full or smoke"
    ;;
esac

case "$body_matrix" in
  false | true | request | response)
    ;;
  *)
    fail "RUVOY_BENCH_BODY_MATRIX must be false, true, request, or response"
    ;;
esac

case "$tls_selection" in
  both)
    tls_modes=(plain tls)
    ;;
  plain)
    tls_modes=(plain)
    ;;
  tls)
    tls_modes=(tls)
    ;;
  *)
    fail "RUVOY_BENCH_TLS must be both, plain, or tls"
    ;;
esac
if [[ "$mode" == "smoke" && "$body_matrix" != "false" ]]; then
  fail "smoke mode does not allow RUVOY_BENCH_BODY_MATRIX"
fi

case "$architecture_order_mode" in
  rotate | alternate | forward | reverse)
    ;;
  *)
    fail "RUVOY_BENCH_ORDER_MODE must be rotate, alternate, forward, or reverse"
    ;;
esac

for numeric_name in \
  measurement_rounds \
  warmup_concurrency \
  control_repetitions \
  oha_timeout_seconds \
  envoy_concurrency \
  puma_threads \
  falcon_count; do
  numeric_value="${!numeric_name}"
  [[ "$numeric_value" =~ ^[0-9]+$ ]] && [[ "$numeric_value" -gt 0 ]] ||
    fail "$numeric_name must be a positive integer"
done
[[ "$puma_workers" =~ ^[0-9]+$ ]] || fail "puma_workers must be a non-negative integer"
if [[ "$mode" == "full" && "$measurement_rounds" -lt 7 ]]; then
  fail "full mode requires RUVOY_BENCH_ROUNDS to be at least 7"
fi
for duration_name in bench_duration warmup_duration; do
  duration="${!duration_name}"
  [[ "$duration" =~ ^[1-9][0-9]*(ms|s|m)$ ]] ||
    fail "$duration_name must use a positive ms, s, or m duration"
done

[[ "${#architectures[@]}" -gt 0 ]] ||
  fail "RUVOY_BENCH_ARCHITECTURES must select at least one architecture"
seen_architectures=" "
for architecture in "${architectures[@]}"; do
  case "$architecture" in
    baseline | sync | fiber | envoy_puma | puma_direct | falcon_direct)
      ;;
    *)
      fail "unknown RUVOY_BENCH_ARCHITECTURES entry: $architecture"
      ;;
  esac
  [[ "$seen_architectures" != *" $architecture "* ]] ||
    fail "RUVOY_BENCH_ARCHITECTURES must not contain duplicates"
  seen_architectures+="$architecture "
done

duration_ms() {
  local value="$1"

  case "$value" in
    *ms)
      printf '%s' "${value%ms}"
      ;;
    *s)
      awk -v value="${value%s}" 'BEGIN { printf "%d", value * 1000 }'
      ;;
    *m)
      awk -v value="${value%m}" 'BEGIN { printf "%d", value * 60000 }'
      ;;
    *)
      return 1
      ;;
  esac
}

is_loopback_address() {
  case "$1" in
    127.* | localhost | ::1 | 0.0.0.0)
      return 0
      ;;
  esac
  return 1
}

case "$load_generator" in
  local | remote)
    ;;
  *)
    fail "RUVOY_BENCH_LOAD_GENERATOR must be local or remote"
    ;;
esac

if [[ "$mode" == "full" ]]; then
  [[ "$(uname -s)" != "Darwin" ]] ||
    fail "full mode is forbidden on macOS; run it on an isolated Linux machine"
  [[ "${RUVOY_ALLOW_HIGH_LOAD:-}" == "1" ]] ||
    fail "full mode requires RUVOY_ALLOW_HIGH_LOAD=1"

  # oha competing with Envoy and Puma for the same cores makes every number a
  # measurement of the machine, not of the architectures under test.
  [[ "$load_generator" == "remote" ]] ||
    fail "full mode requires RUVOY_BENCH_LOAD_GENERATOR=remote"
  [[ -n "$load_generator_ssh" ]] ||
    fail "full mode requires RUVOY_BENCH_LOAD_GENERATOR_SSH"
  ! is_loopback_address "$target_address" ||
    fail "full mode requires a non-loopback RUVOY_BENCH_TARGET_ADDRESS"
  [[ "$listen_address" != "127.0.0.1" ]] ||
    fail "full mode requires RUVOY_BENCH_LISTEN_ADDRESS reachable by the load generator"

  # The published Linux runs used 2s windows; that is noise, not a measurement.
  [[ "$(duration_ms "$bench_duration")" -ge 30000 ]] ||
    fail "full mode requires RUVOY_BENCH_DURATION of at least 30s"
  [[ "$(duration_ms "$warmup_duration")" -ge 10000 ]] ||
    fail "full mode requires RUVOY_BENCH_WARMUP_DURATION of at least 10s"

  if [[ "$allow_dirty" != "1" ]]; then
    [[ -z "$(git -C "$repo_root" status --porcelain)" ]] ||
      fail "full mode requires a clean worktree; set RUVOY_BENCH_ALLOW_DIRTY=1 to override"
  fi
fi

if [[ "$load_generator" == "remote" ]]; then
  [[ -n "$load_generator_ssh" ]] ||
    fail "remote load generation requires RUVOY_BENCH_LOAD_GENERATOR_SSH"
fi

resolve_ruby() {
  local candidate=""

  if [[ -n "$ruby_bin" ]]; then
    return
  fi
  if command -v rbenv >/dev/null 2>&1; then
    candidate="$(RBENV_VERSION="$ruby_version" rbenv which ruby 2>/dev/null || true)"
    if [[ -x "$candidate" ]]; then
      ruby_bin="$candidate"
      return
    fi
  fi
  ruby_bin="$(command -v ruby || true)"
}

resolve_ruby
[[ -x "$ruby_bin" ]] || fail "missing Ruby $ruby_version; set RUVOY_RUBY"
[[ "$("$ruby_bin" -e 'print RUBY_VERSION')" == "$ruby_version" ]] ||
  fail "RUVOY_RUBY must be Ruby $ruby_version"

bundle_command=()
if [[ -n "$bundle_bin" ]]; then
  [[ -x "$bundle_bin" ]] || fail "RUVOY_BUNDLE is not executable: $bundle_bin"
  bundle_command=("$bundle_bin")
elif "$ruby_bin" -S bundle --version >/dev/null 2>&1; then
  bundle_command=("$ruby_bin" -S bundle)
else
  fail "missing Bundler for $ruby_bin; set RUVOY_BUNDLE or install Bundler 4.0.10"
fi
# Bundler 4 prints a bare version; older releases print "Bundler version X".
bundle_version="$("${bundle_command[@]}" --version | tr -d '[:space:]')"
bundle_version="${bundle_version#Bundlerversion}"
[[ "$bundle_version" == "4.0.10" ]] ||
  fail "expected Bundler 4.0.10, got: $bundle_version"

mkdir -p "$result_dir"
manifest="$result_dir/manifest.tsv"
control_manifest="$result_dir/control-manifest.tsv"
campaign_manifest="$result_dir/campaign.tsv"
skipped_manifest="$result_dir/skipped.tsv"
printf 'sequence\tarchitecture\tscenario\ttls_mode\tround\tprotocol\tconcurrency\twait_ms\trequest_bytes\tresponse_bytes\tapp_params\tserved_requests\toha_json\tresources_csv\n' >"$manifest"
printf 'architecture\tstate\trun\toha_json\n' >"$control_manifest"
printf 'sequence\tscenario\ttls_mode\tround\tarchitecture\tprotocol\tstate\treason\n' >"$campaign_manifest"
printf 'scenario\tround\tarchitecture\tprotocol\treason\n' >"$skipped_manifest"

current_envoy_pid=""
current_envoy_runtime_pid=""
current_puma_pid=""
current_puma_pid_csv=""
current_falcon_pid=""
current_falcon_pid_csv=""
current_envoy_log=""
current_architecture_dir=""
current_listener_port=""
current_tls_mode="plain"
current_scheme="http"
server_pid_csv=""
stop_status=0
oha_timeout_attempts=$((oha_timeout_seconds * 20))
shutdown_attempts=100
control_done_architectures=" "

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
  if [[ -n "$current_envoy_pid" ]] && ! wait "$current_envoy_pid" 2>/dev/null; then
    stopped=false
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
  if [[ -n "$current_puma_pid" ]] && ! wait "$current_puma_pid" 2>/dev/null; then
    stopped=false
  fi

  [[ "$stopped" == true ]]
}

stop_falcon() {
  local stopped=true

  signal_if_alive INT "$current_falcon_pid"
  if ! wait_for_exit "$current_falcon_pid" "$shutdown_attempts"; then
    signal_if_alive TERM "$current_falcon_pid"
  fi
  if ! wait_for_exit "$current_falcon_pid" "$shutdown_attempts"; then
    signal_if_alive KILL "$current_falcon_pid"
  fi

  wait_for_exit "$current_falcon_pid" 20 || stopped=false
  if [[ -n "$current_falcon_pid" ]] && ! wait "$current_falcon_pid" 2>/dev/null; then
    stopped=false
  fi

  [[ "$stopped" == true ]]
}

cleanup_current() {
  stop_status=0
  stop_envoy || stop_status=1
  stop_puma || stop_status=1
  stop_falcon || stop_status=1
  current_envoy_pid=""
  current_envoy_runtime_pid=""
  current_puma_pid=""
  current_puma_pid_csv=""
  current_falcon_pid=""
  current_falcon_pid_csv=""
  current_listener_port=""
  server_pid_csv=""
}

# Only this run's own directory is removed, and only on the load generator.
remove_remote_body_dir() {
  [[ -n "$remote_body_dir" ]] || return 0
  ssh -n -o BatchMode=yes "$load_generator_ssh" \
    "rm -rf -- $(shell_quote "$remote_body_dir")" 2>/dev/null || true
  remote_body_dir=""
}

cleanup_all() {
  cleanup_current
  remove_remote_body_dir
}

trap cleanup_all EXIT INT TERM

wait_for_url() {
  local url="$1"
  local pid="$2"

  local tls_args=()
  [[ "$current_tls_mode" == tls ]] && tls_args=(--cacert "$result_dir/tls/ca.pem")
  for _ in $(seq 1 200); do
    if curl --http1.1 --silent --fail --max-time 1 --output /dev/null \
      ${tls_args[@]+"${tls_args[@]}"} "$url"; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
    sleep 0.05
  done
  return 1
}

shell_quote() {
  printf "'%s'" "${1//\'/\'\\\'\'}"
}

# Benchmark URLs contain '&', so remote arguments must survive the login shell.
start_oha() {
  local output="$1"
  shift

  if [[ "$load_generator" == "remote" ]]; then
    local command
    # GNU time writes the generator's own CPU usage to stderr so saturation can
    # be detected without a second connection competing for the same cores.
    command="/usr/bin/time -f 'ruvoy_load_generator user=%U sys=%S elapsed=%e'"
    command+=" $(shell_quote "$load_generator_oha")"
    local argument
    for argument in "$@"; do
      command+=" $(shell_quote "$argument")"
    done
    ssh -n -o BatchMode=yes "$load_generator_ssh" "$command" \
      >"$output" 2>"$output.loadgen" &
  else
    "$oha" "$@" >"$output" &
  fi
}

# The load generator verifies the benchmark CA instead of skipping validation,
# so a broken certificate fails loudly rather than silently downgrading.
oha_tls_args() {
  [[ "$current_tls_mode" == tls ]] || return 0
  printf '%s\n%s\n' --cacert "$(ca_path)"
}

ca_path() {
  if [[ "$load_generator" == "remote" ]]; then
    printf '%s/ca.pem' "$remote_body_dir"
  else
    printf '%s/tls/ca.pem' "$result_dir"
  fi
}

body_path() {
  local request_bytes="$1"

  if [[ "$load_generator" == "remote" ]]; then
    printf '%s/body-%s.bin' "$remote_body_dir" "$request_bytes"
  else
    printf '%s/body-%s.bin' "$result_dir" "$request_bytes"
  fi
}

run_oha_with_timeout() {
  local output="$1"
  shift

  start_oha "$output" "$@"
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
    [[ -n "$process_stats" ]] || break
    local rss
    rss="$(printf '%s\n' "$process_stats" | awk '{ rss += $1 } END { print rss }')"
    printf '%d,%s\n' "$sample" "$rss" >>"$output"
    sample=$((sample + 1))
    sleep 0.1
  done
}

# ps reports whole seconds, which rounds most measurement windows to 0% or to a
# neighbouring multiple of 1/duration. /proc gives clock ticks instead.
clock_tick="$(getconf CLK_TCK 2>/dev/null || printf '100')"
cpu_source=ps
[[ -r /proc/self/stat ]] && cpu_source=proc

proc_cpu_seconds() {
  local pid_csv="$1"
  local pid
  local stat_files=()

  for pid in ${pid_csv//,/ }; do
    [[ -r "/proc/$pid/stat" ]] && stat_files+=("/proc/$pid/stat")
  done
  if [[ "${#stat_files[@]}" -eq 0 ]]; then
    printf '0.000000'
    return
  fi
  awk -v ticks="$clock_tick" '
    {
      # comm can contain spaces, so fields are counted after the closing paren.
      rest = substr($0, index($0, ") ") + 2)
      split(rest, fields, " ")
      total += fields[12] + fields[13]
    }
    END { printf "%.6f", total / ticks }
  ' "${stat_files[@]}"
}

process_cpu_seconds() {
  local pid_csv="$1"

  if [[ "$cpu_source" == "proc" ]]; then
    proc_cpu_seconds "$pid_csv"
    return
  fi

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

# Reads the server's own execution count so a round that silently dropped
# requests cannot be reported as throughput.
served_count() {
  local port="$1"
  local tls_args=()

  [[ "$current_tls_mode" == tls ]] && tls_args=(--cacert "$result_dir/tls/ca.pem")
  curl --http1.1 --silent --fail --max-time 10 \
    ${tls_args[@]+"${tls_args[@]}"} \
    "$current_scheme://$probe_address:$port/benchmark-served" 2>/dev/null
}

assert_load_generator_headroom() {
  local timing_file="$1"
  local label="$2"

  [[ "$load_generator" == "remote" ]] || return 0
  [[ -s "$timing_file" ]] || return 0

  local usage
  usage="$(
    awk -v cores="$load_generator_cores" '
      /ruvoy_load_generator/ {
        for (index_ = 1; index_ <= NF; index_ += 1) {
          split($index_, pair, "=")
          values[pair[1]] = pair[2]
        }
      }
      END {
        if (values["elapsed"] <= 0 || cores <= 0) { print -1; exit }
        printf "%.1f", (values["user"] + values["sys"]) * 100 / (values["elapsed"] * cores)
      }
    ' "$timing_file"
  )"
  if [[ "$usage" == "-1" ]]; then
    printf '%s\tunmeasured\t%s\n' "$label" "$load_generator_cores" \
      >>"$result_dir/load-generator-cpu.tsv"
    return 0
  fi

  printf '%s\t%s\t%s\n' "$label" "$usage" "$load_generator_cores" \
    >>"$result_dir/load-generator-cpu.tsv"
  awk -v usage="$usage" -v limit="$load_generator_cpu_limit" \
    'BEGIN { exit !(usage > limit) }' &&
    fail "$label: load generator used ${usage}% of its CPUs (limit ${load_generator_cpu_limit}%); the measurement is generator-bound"
  return 0
}

# Machine facts are captured by the runner rather than trusted to a human
# note, so a later reader can tell what the numbers were produced on.
describe_host() {
  printf 'kernel=%s\n' "$(uname -srm)"
  if [[ -r /proc/cpuinfo ]]; then
    printf 'cpu_model=%s\n' \
      "$(awk -F': ' '/model name/ { print $2; exit }' /proc/cpuinfo)"
    printf 'cpu_count=%s\n' "$(nproc)"
    printf 'memory_kib=%s\n' "$(awk '/MemTotal/ { print $2 }' /proc/meminfo)"
    printf 'load_average=%s\n' "$(cut -d' ' -f1-3 /proc/loadavg)"
    printf 'cpu_governor=%s\n' \
      "$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || printf 'unavailable')"
    printf 'turbo_disabled=%s\n' \
      "$(cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || printf 'unavailable')"
  else
    printf 'cpu_model=%s\n' "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || printf 'unknown')"
    printf 'cpu_count=%s\n' "$(sysctl -n hw.ncpu 2>/dev/null || printf 'unknown')"
    printf 'memory_kib=%s\n' \
      "$(awk -v bytes="$(sysctl -n hw.memsize 2>/dev/null || printf 0)" 'BEGIN { print int(bytes / 1024) }')"
    printf 'load_average=%s\n' "$(uptime | sed 's/.*averages*: //')"
    printf 'cpu_governor=%s\nturbo_disabled=%s\n' unavailable unavailable
  fi
}

# A busy server host makes every measurement a measurement of the neighbour.
assert_server_host_idle() {
  [[ "$mode" == "full" ]] || return 0
  [[ -r /proc/loadavg ]] || return 0

  local current_load
  current_load="$(cut -d' ' -f1 /proc/loadavg)"
  awk -v current="$current_load" -v limit="$idle_load_limit" \
    'BEGIN { exit !(current > limit) }' &&
    fail "server host load average is $current_load (limit $idle_load_limit); the machine is not idle"
  return 0
}

monotonic_seconds() {
  "$ruby_bin" -e 'printf "%.9f", Process.clock_gettime(Process::CLOCK_MONOTONIC)'
}

validate_oha_result() {
  local json_file="$1"

  jq -e '
    .summary.successRate == 1
    and .summary.requestsPerSec > 0
    and (.errorDistribution | type == "object" and length == 0)
    and (.statusCodeDistribution | type == "object")
    and (.statusCodeDistribution | keys == ["200"])
    and (.statusCodeDistribution["200"] > 0)
  ' "$json_file" >/dev/null
}

architecture_supports_protocol() {
  local architecture="$1"
  local protocol="$2"

  case "$architecture" in
    puma_direct | falcon_direct)
      [[ "$protocol" == "1.1" ]]
      ;;
    *)
      return 0
      ;;
  esac
}

control_port_for_architecture() {
  case "$1" in
    sync)
      printf '19182'
      ;;
    fiber)
      printf '19184'
      ;;
    *)
      return 1
      ;;
  esac
}

# The TLS mode is passed in rather than read from current_tls_mode: when Puma is
# Envoy's upstream, Envoy terminates TLS and forwards cleartext, which is the
# deployment shape being compared against. Only the direct listener serves TLS.
start_puma() {
  local port="$1"
  local puma_tls_mode="$2"
  local bind="tcp://$listen_address:$port"
  local puma_scheme=http

  if [[ "$puma_tls_mode" == tls ]]; then
    puma_scheme=https
    # Puma exposes no switch to forbid TLS 1.2, so the negotiated version is
    # asserted at runtime instead (see verify_tls_negotiation).
    # Puma exposes no switch to forbid TLS 1.2, so the negotiated version is
    # asserted at runtime instead (see verify_tls_negotiation).
    bind="ssl://$listen_address:$port?cert=$result_dir/tls/cert.pem&key=$result_dir/tls/key.pem&no_tlsv1=true&no_tlsv1_1=true"
  fi

  PATH="$(dirname "$ruby_bin"):$PATH" \
    RUBY="$ruby_bin" \
    BUNDLE_GEMFILE="$repo_root/Gemfile" \
    BUNDLE_PATH="$bundle_path" \
    BUNDLE_FROZEN=true \
    "${bundle_command[@]}" _4.0.10_ exec puma \
    --no-config \
    --environment production \
    --threads "0:$puma_threads" \
    --workers "$puma_workers" \
    --bind "$bind" \
    "$repo_root/bench/config.ru" \
    >"$current_architecture_dir/puma.log" 2>&1 &
  current_puma_pid=$!
  wait_for_url \
    "$puma_scheme://$probe_address:$port/benchmark?wait_ms=0&response_bytes=0" \
    "$current_puma_pid" ||
    fail "Puma did not become ready on port $port"

  current_puma_pid_csv="$current_puma_pid"
  if [[ "$puma_workers" -gt 0 ]]; then
    local worker_pid_list=""
    local worker_count=0
    for _ in $(seq 1 200); do
      worker_pid_list="$(pgrep -P "$current_puma_pid" || true)"
      worker_count="$(printf '%s\n' "$worker_pid_list" | awk 'NF { count += 1 } END { print count + 0 }')"
      [[ "$worker_count" -ge "$puma_workers" ]] && break
      sleep 0.05
    done
    [[ "$worker_count" -ge "$puma_workers" ]] ||
      fail "Puma started $worker_count of $puma_workers workers"
    local worker_pid
    while IFS= read -r worker_pid; do
      [[ -n "$worker_pid" ]] || continue
      current_puma_pid_csv+=",$worker_pid"
    done <<<"$worker_pid_list"
  fi
}

start_falcon() {
  local port="$1"

  local falcon_tls_env=()
  if [[ "$current_tls_mode" == tls ]]; then
    falcon_tls_env=(
      "RUVOY_FALCON_TLS_CERTIFICATE=$result_dir/tls/cert.pem"
      "RUVOY_FALCON_TLS_KEY=$result_dir/tls/key.pem"
    )
  fi

  # falcon serve cannot take an SSL context, so both modes go through
  # bench/falcon.rb to keep Falcon's process structure identical.
  env \
    PATH="$(dirname "$ruby_bin"):$PATH" \
    RUBY="$ruby_bin" \
    BUNDLE_GEMFILE="$repo_root/Gemfile" \
    BUNDLE_PATH="$bundle_path" \
    BUNDLE_FROZEN=true \
    RUVOY_FALCON_URL="$current_scheme://$listen_address:$port" \
    RUVOY_FALCON_COUNT="$falcon_count" \
    RUVOY_FALCON_RACKUP="$repo_root/bench/config.ru" \
    RUVOY_FALCON_ROOT="$repo_root" \
    ${falcon_tls_env[@]+"${falcon_tls_env[@]}"} \
    "${bundle_command[@]}" _4.0.10_ exec falcon host "$repo_root/bench/falcon.rb" \
    >"$current_architecture_dir/falcon.log" 2>&1 &
  current_falcon_pid=$!
  wait_for_url \
    "$current_scheme://$probe_address:$port/benchmark?wait_ms=0&response_bytes=0" \
    "$current_falcon_pid" ||
    fail "Falcon did not become ready on port $port"

  # Falcon supervises forked instances, so the workers carry the real load.
  current_falcon_pid_csv="$current_falcon_pid"
  local worker_pid_list=""
  local worker_count=0
  for _ in $(seq 1 200); do
    worker_pid_list="$(pgrep -P "$current_falcon_pid" || true)"
    worker_count="$(printf '%s\n' "$worker_pid_list" | awk 'NF { count += 1 } END { print count + 0 }')"
    [[ "$worker_count" -ge "$falcon_count" ]] && break
    sleep 0.05
  done
  [[ "$worker_count" -ge "$falcon_count" ]] ||
    fail "Falcon started $worker_count of $falcon_count instances"
  local worker_pid
  while IFS= read -r worker_pid; do
    [[ -n "$worker_pid" ]] || continue
    current_falcon_pid_csv+=",$worker_pid"
  done <<<"$worker_pid_list"
}

start_architecture() {
  local architecture="$1"
  local sequence="$2"
  local config=""
  local listener_port=""

  current_envoy_pid=""
  current_envoy_runtime_pid=""
  current_puma_pid=""
  current_puma_pid_csv=""
  current_falcon_pid=""
  current_falcon_pid_csv=""
  current_envoy_log=""
  current_architecture_dir="$result_dir/$architecture/campaign-$sequence"
  mkdir -p "$current_architecture_dir"

  case "$architecture" in
    baseline)
      config="$result_dir/config/$current_tls_mode/envoy-baseline.yaml"
      listener_port=19180
      ;;
    sync)
      config="$result_dir/config/$current_tls_mode/envoy-sync-rack.yaml"
      listener_port=19181
      ;;
    fiber)
      config="$result_dir/config/$current_tls_mode/envoy-fiber-rack.yaml"
      listener_port=19183
      ;;
    envoy_puma)
      start_puma 19210 plain
      config="$result_dir/config/$current_tls_mode/envoy-puma-benchmark.yaml"
      listener_port=19203
      ;;
    puma_direct)
      start_puma 19211 "$current_tls_mode"
      current_listener_port=19211
      server_pid_csv="$current_puma_pid_csv"
      return
      ;;
    falcon_direct)
      start_falcon 19212
      current_listener_port=19212
      server_pid_csv="$current_falcon_pid_csv"
      return
      ;;
  esac

  current_envoy_log="$current_architecture_dir/envoy.log"
  BUNDLE_GEMFILE="$repo_root/Gemfile" \
    BUNDLE_PATH="$bundle_path" \
    BUNDLE_FROZEN=true \
    ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
    uvx --from "$envoy_package" envoy \
    --config-path "$config" \
    --concurrency "$envoy_concurrency" \
    --disable-hot-restart \
    --log-level warning \
    >"$current_envoy_log" 2>&1 &
  current_envoy_pid=$!
  wait_for_url \
    "$current_scheme://$probe_address:$listener_port/benchmark?wait_ms=0&response_bytes=0" \
    "$current_envoy_pid" ||
    fail "$architecture Envoy did not become ready"

  for _ in $(seq 1 100); do
    current_envoy_runtime_pid="$(pgrep -P "$current_envoy_pid" | head -1 || true)"
    [[ -n "$current_envoy_runtime_pid" ]] && break
    sleep 0.01
  done
  [[ -n "$current_envoy_runtime_pid" ]] ||
    fail "$architecture could not resolve the Envoy runtime PID"

  if [[ -n "$current_puma_pid_csv" ]]; then
    server_pid_csv="$current_envoy_runtime_pid,$current_puma_pid_csv"
  else
    server_pid_csv="$current_envoy_runtime_pid"
  fi
  current_listener_port="$listener_port"
}

# ALPN is deliberately excluded: the direct servers do not offer h2 over TLS,
# while Envoy does. Version and cipher decide handshake cost and must match.
verify_tls_negotiation() {
  local architecture="$1"
  local listener_port="$2"
  local observed

  observed="$(
    openssl s_client \
      -connect "$probe_address:$listener_port" \
      -CAfile "$result_dir/tls/ca.pem" \
      -alpn http/1.1 \
      -servername localhost \
      </dev/null 2>/dev/null |
      awk '
        /^New, TLSv/ {
          sub(/^New, /, "")
          split($0, parts, ", Cipher is ")
          protocol = parts[1]
          cipher = parts[2]
        }
        END { printf "%s/%s", protocol, cipher }
      '
  )"
  [[ "$observed" == TLSv1.3/TLS_* ]] ||
    fail "$architecture negotiated '$observed' instead of a TLS 1.3 cipher suite"
  printf '%s\t%s\t%s\n' "$architecture" "$current_tls_mode" "$observed" \
    >>"$result_dir/tls-negotiation.tsv"

  if [[ -z "$tls_negotiation_baseline" ]]; then
    tls_negotiation_baseline="$observed"
  elif [[ "$observed" != "$tls_negotiation_baseline" ]]; then
    fail "$architecture negotiated $observed but the baseline is $tls_negotiation_baseline"
  fi
}

verify_contract() {
  local architecture="$1"
  local listener_port="$2"

  if [[ "$current_tls_mode" == tls ]]; then
    verify_tls_negotiation "$architecture" "$listener_port"
  fi

  local curl_tls_args=()
  [[ "$current_tls_mode" == tls ]] && curl_tls_args=(--cacert "$result_dir/tls/ca.pem")
  curl --http1.1 \
    --silent \
    --show-error \
    --request POST \
    --max-time "$oha_timeout_seconds" \
    --header 'Expect:' \
    ${curl_tls_args[@]+"${curl_tls_args[@]}"} \
    --data-binary "@$result_dir/body-1024.bin" \
    --dump-header "$current_architecture_dir/contract.headers" \
    --output "$current_architecture_dir/contract.body" \
    "$current_scheme://$probe_address:$listener_port/benchmark?wait_ms=0&response_bytes=1024&expected_request_bytes=1024"
  [[ "$(wc -c <"$current_architecture_dir/contract.body" | tr -d ' ')" == 1024 ]] ||
    fail "$architecture contract returned the wrong response size"
  grep -qiE '^x-request-bytes: 1024' "$current_architecture_dir/contract.headers" ||
    fail "$architecture contract did not read the request body"

  if architecture_supports_protocol "$architecture" 2; then
    local contract_tls_args=()
    while IFS= read -r tls_argument; do
      [[ -n "$tls_argument" ]] && contract_tls_args+=("$tls_argument")
    done < <(oha_tls_args)
    run_oha_with_timeout \
      "$current_architecture_dir/contract-h2.json" \
      --no-tui \
      --output-format json \
      --http2 \
      -n 2 \
      -c 1 \
      -p 2 \
      ${contract_tls_args[@]+"${contract_tls_args[@]}"} \
      "$current_scheme://$target_address:$listener_port/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=0" ||
      fail "$architecture HTTP/2 contract oha timed out or exited non-zero"
    validate_oha_result "$current_architecture_dir/contract-h2.json" ||
      fail "$architecture HTTP/2 contract failed"
  fi
}

warmup_architecture() {
  local architecture="$1"
  local listener_port="$2"

  local warmup_tls_args=()
  while IFS= read -r tls_argument; do
    [[ -n "$tls_argument" ]] && warmup_tls_args+=("$tls_argument")
  done < <(oha_tls_args)
  run_oha_with_timeout \
    "$current_architecture_dir/warmup.json" \
    --no-tui \
    --output-format json \
    --wait-ongoing-requests-after-deadline \
    --http-version 1.1 \
    -z "$warmup_duration" \
    -c "$warmup_concurrency" \
    ${warmup_tls_args[@]+"${warmup_tls_args[@]}"} \
    "$current_scheme://$target_address:$listener_port/benchmark?wait_ms=0&response_bytes=0" ||
    fail "$architecture warmup timed out or exited non-zero"
  validate_oha_result "$current_architecture_dir/warmup.json" ||
    fail "$architecture warmup returned errors"
}

run_oha() {
  local sequence="$1"
  local architecture="$2"
  local scenario="$3"
  local round="$4"
  local protocol="$5"
  local concurrency="$6"
  local wait_ms="$7"
  local request_bytes="$8"
  local response_bytes="$9"
  local app_params="${10}"
  local run_prefix="$result_dir/$architecture/${scenario}-round${round}-seq${sequence}"
  local json_file="$run_prefix.json"
  local resource_file="$run_prefix.resources.csv"
  local rss_sample_file="$run_prefix.rss.csv"
  local url="$current_scheme://$target_address:$current_listener_port/benchmark?wait_ms=$wait_ms&response_bytes=$response_bytes&expected_request_bytes=$request_bytes"
  [[ -n "$app_params" ]] && url+="&$app_params"
  local args=(--no-tui --output-format json --wait-ongoing-requests-after-deadline -z "$bench_duration")
  while IFS= read -r tls_argument; do
    [[ -n "$tls_argument" ]] && args+=("$tls_argument")
  done < <(oha_tls_args)

  if [[ "$protocol" == "2" ]]; then
    args+=(--http2 -c 1 -p "$concurrency")
  else
    args+=(--http-version 1.1 -c "$concurrency")
  fi
  if [[ "$request_bytes" -gt 0 ]]; then
    args+=(--method POST -H 'Expect:' -D "$(body_path "$request_bytes")")
  fi

  local served_before
  served_before="$(served_count "$current_listener_port")" ||
    fail "$architecture $scenario round $round: could not read the served counter"
  local cpu_before
  local wall_before
  cpu_before="$(process_cpu_seconds "$server_pid_csv")"
  wall_before="$(monotonic_seconds)"
  sample_rss "$server_pid_csv" "$rss_sample_file" &
  local sampler_pid=$!
  set +e
  run_oha_with_timeout "$json_file" "${args[@]}" "$url"
  local oha_status=$?
  set -e
  local wall_after
  local cpu_after
  wall_after="$(monotonic_seconds)"
  cpu_after="$(process_cpu_seconds "$server_pid_csv")"
  signal_if_alive TERM "$sampler_pid"
  wait "$sampler_pid" 2>/dev/null || true
  [[ "$oha_status" -eq 0 ]] || fail "$architecture $scenario round $round: oha exited $oha_status"
  validate_oha_result "$json_file" ||
    fail "$architecture $scenario round $round returned non-200 or transport errors"

  assert_load_generator_headroom "$json_file.loadgen" \
    "$architecture $scenario round $round"

  local served_after
  served_after="$(served_count "$current_listener_port")" ||
    fail "$architecture $scenario round $round: could not read the served counter"
  local served_delta=$((served_after - served_before))
  local reported_requests
  reported_requests="$(jq -r '.statusCodeDistribution["200"]' "$json_file")"
  # The counter is process-wide, so warm-up leftovers and the control probe also
  # land in it. What must never happen is the server executing FEWER requests
  # than the load generator counted as successful: that means work was reported
  # but never ran.
  if [[ "$served_delta" -lt "$reported_requests" ]]; then
    fail "$architecture $scenario round $round: the load generator counted $reported_requests successful requests but the server only executed $served_delta"
  fi

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

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$sequence" \
    "$architecture" \
    "$scenario" \
    "$current_tls_mode" \
    "$round" \
    "$protocol" \
    "$concurrency" \
    "$wait_ms" \
    "$request_bytes" \
    "$response_bytes" \
    "$app_params" \
    "$served_delta" \
    "${json_file#"$result_dir/"}" \
    "${resource_file#"$result_dir/"}" \
    >>"$manifest"
}

run_control_probe() {
  local architecture="$1"
  local control_port="$2"

  for run in $(seq 1 "$control_repetitions"); do
    local idle_json="$current_architecture_dir/control-idle-run$run.json"
    run_oha_with_timeout \
      "$idle_json" \
      --no-tui \
      --output-format json \
      --wait-ongoing-requests-after-deadline \
      --http-version 1.1 \
      -z 500ms \
      -c 10 \
      "http://$target_address:$control_port/" ||
      fail "$architecture idle control probe timed out or exited non-zero"
    validate_oha_result "$idle_json" || fail "$architecture idle control probe failed"
    printf '%s\tidle\t%s\t%s\n' \
      "$architecture" "$run" "${idle_json#"$result_dir/"}" >>"$control_manifest"

    local load_json="$current_architecture_dir/control-load-run$run.json"
    local load_tls_args=()
    while IFS= read -r tls_argument; do
      [[ -n "$tls_argument" ]] && load_tls_args+=("$tls_argument")
    done < <(oha_tls_args)
    start_oha "$load_json" \
      --no-tui \
      --output-format json \
      --http-version 1.1 \
      -n 10 \
      -c 10 \
      ${load_tls_args[@]+"${load_tls_args[@]}"} \
      "$current_scheme://$target_address:$current_listener_port/benchmark?wait_ms=200&response_bytes=0"
    local load_pid=$!
    sleep 0.05
    local loaded_json="$current_architecture_dir/control-loaded-run$run.json"
    run_oha_with_timeout \
      "$loaded_json" \
      --no-tui \
      --output-format json \
      --wait-ongoing-requests-after-deadline \
      --http-version 1.1 \
      -z 500ms \
      -c 10 \
      "http://$target_address:$control_port/" || {
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
  if [[ "$mode" == "smoke" ]]; then
    printf 'smoke_noop_h1_c1|1.1|1|0|0|0\n'
    return
  fi

  if [[ "$body_matrix" != "false" ]]; then
    local body_bytes
    local concurrency
    for body_bytes in 1024 65536 262144 1048576 2097152; do
      for concurrency in 1 10 100; do
        if [[ "$body_matrix" == "true" || "$body_matrix" == "request" ]]; then
          printf 'request_%s_h1_c%s|1.1|%s|0|%s|0\n' \
            "$body_bytes" "$concurrency" "$concurrency" "$body_bytes"
        else
          printf 'response_%s_h1_c%s|1.1|%s|0|0|%s\n' \
            "$body_bytes" "$concurrency" "$concurrency" "$body_bytes"
        fi
      done
    done
    return
  fi

  printf '%s\n' \
    'noop_h1_c1|1.1|1|0|0|0' \
    'noop_h1_c100|1.1|100|0|0|0' \
    'noop_h2_c100|2|100|0|0|0' \
    'small_h1_c10|1.1|10|0|1024|1024' \
    'large_request_h1_c10|1.1|10|0|1048576|0' \
    'large_response_h1_c10|1.1|10|0|0|1048576' \
    'wait10_h1_c10|1.1|10|10|0|0' \
    'wait200_h1_c1|1.1|1|200|0|0' \
    'wait200_h1_c10|1.1|10|200|0|0' \
    'wait200_h1_c100|1.1|100|200|0|0' \
    'cpu25k_h1_c100|1.1|100|0|0|0|cpu_iterations=25000' \
    'cpu125k_h1_c100|1.1|100|0|0|0|cpu_iterations=125000' \
    'wait50_h1_c100|1.1|100|50|0|0' \
    'block50_h1_c100|1.1|100|0|0|0|block_ms=50' \
    'block50_h1_c10|1.1|10|0|0|0|block_ms=50'
}

scenario_is_selected() {
  local wanted="$1"

  if [[ "$scenario_selection" == "all" ]]; then
    return 0
  fi
  local selected
  for selected in "${selected_scenarios[@]}"; do
    [[ "$selected" == "$wanted" ]] && return 0
  done
  return 1
}

control_has_run() {
  [[ "$control_done_architectures" == *" $1 "* ]]
}

run_measurement() {
  local sequence="$1"
  local architecture="$2"
  local scenario="$3"
  local round="$4"
  local protocol="$5"
  local concurrency="$6"
  local wait_ms="$7"
  local request_bytes="$8"
  local response_bytes="$9"
  local app_params="${10}"

  printf '%s\t%s\t%s\t%s\t%s\t%s\tplanned\t\n' \
    "$sequence" "$scenario" "$current_tls_mode" "$round" "$architecture" "$protocol" \
    >>"$campaign_manifest"
  if ! architecture_supports_protocol "$architecture" "$protocol"; then
    printf '%s\t%s\t%s\t%s\tdirect listener does not serve cleartext HTTP/2\n' \
      "$scenario" "$round" "$architecture" "$protocol" >>"$skipped_manifest"
    printf '%s\t%s\t%s\t%s\t%s\t%s\tskipped\tdirect listener does not serve cleartext HTTP/2\n' \
      "$sequence" "$scenario" "$current_tls_mode" "$round" "$architecture" "$protocol" \
      >>"$campaign_manifest"
    return
  fi

  start_architecture "$architecture" "$sequence"
  verify_contract "$architecture" "$current_listener_port"
  warmup_architecture "$architecture" "$current_listener_port"
  run_oha \
    "$sequence" \
    "$architecture" \
    "$scenario" \
    "$round" \
    "$protocol" \
    "$concurrency" \
    "$wait_ms" \
    "$request_bytes" \
    "$response_bytes" \
    "$app_params"

  if [[ "$mode" == "full" ]] && ! control_has_run "$architecture"; then
    if control_port="$(control_port_for_architecture "$architecture")"; then
      run_control_probe "$architecture" "$control_port"
      control_done_architectures+="$architecture "
    fi
  fi

  cleanup_current
  [[ "$stop_status" -eq 0 ]] || fail "$architecture did not stop cleanly"
  case "$architecture" in
    sync)
      grep -Fq '[ruvoy] Ruby runtime stopped' "$current_envoy_log" ||
        fail "sync Ruby runtime did not report clean shutdown"
      ;;
    fiber)
      grep -Fq '[ruvoy] Fiber runtime stopped' "$current_envoy_log" ||
        fail "Fiber runtime did not report clean shutdown"
      ;;
  esac
  printf '%s\t%s\t%s\t%s\t%s\t%s\tcompleted\t\n' \
    "$sequence" "$scenario" "$current_tls_mode" "$round" "$architecture" "$protocol" \
    >>"$campaign_manifest"
  if [[ "$cooldown_seconds" -gt 0 ]]; then
    sleep "$cooldown_seconds"
  fi
  return 0
}

for tool in cargo curl dd jq pgrep ps uvx; do
  command -v "$tool" >/dev/null || fail "$tool is required"
done
if [[ "$load_generator" == "local" ]]; then
  [[ -x "$oha" ]] || fail "missing project-local oha: run scripts/install-oha.sh"
  oha_version="$("$oha" --version)"
  [[ "$oha_version" == "oha 1.15.0" ]] || fail "expected oha 1.15.0, got: $oha_version"
fi

for port in 19180 19181 19182 19183 19184 19203 19204 19210 19211 19212; do
  if command -v lsof >/dev/null &&
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "TCP port $port is already in use"
  fi
done

if printf '%s\n' "${tls_modes[@]}" | grep -qx tls; then
  mkdir -p "$result_dir/tls"
  chmod 700 "$result_dir/tls"
  tls_key_details="$(
    "$ruby_bin" "$repo_root/bench/generate_tls_assets.rb" \
      "$result_dir/tls" "$listen_address" "$target_address" "$probe_address" localhost
  )" || fail "failed to generate TLS assets"
fi

for tls_mode in "${tls_modes[@]}"; do
  mkdir -p "$result_dir/config/$tls_mode"
  for source_config in "$repo_root"/config/envoy-*.yaml; do
    render_arguments=(
      "$source_config"
      "$result_dir/config/$tls_mode/$(basename "$source_config")"
      "$listen_address"
      "$repo_root"
    )
    if [[ "$tls_mode" == tls ]]; then
      render_arguments+=("$result_dir/tls/cert.pem" "$result_dir/tls/key.pem")
    fi
    "$ruby_bin" "$repo_root/bench/render_envoy_config.rb" "${render_arguments[@]}" ||
      fail "failed to render $(basename "$source_config") for $tls_mode"
  done
done

for request_bytes in 1024 65536 262144 1048576 2097152; do
  dd if=/dev/zero of="$result_dir/body-$request_bytes.bin" \
    bs="$request_bytes" count=1 2>/dev/null
done

if [[ "$load_generator" == "remote" ]]; then
  oha_version="$(
    ssh -n -o BatchMode=yes "$load_generator_ssh" \
      "$(shell_quote "$load_generator_oha") --version"
  )" || fail "failed to run oha on $load_generator_ssh"
  [[ "$oha_version" == "oha 1.15.0" ]] ||
    fail "load generator must provide oha 1.15.0, got: $oha_version"

  load_generator_cores="$(
    ssh -n -o BatchMode=yes "$load_generator_ssh" "nproc"
  )" || fail "failed to read the CPU count from $load_generator_ssh"
  [[ "$load_generator_cores" =~ ^[0-9]+$ ]] && [[ "$load_generator_cores" -gt 0 ]] ||
    fail "load generator reported an unusable CPU count: $load_generator_cores"

  remote_body_dir="$(
    ssh -n -o BatchMode=yes "$load_generator_ssh" \
      "mktemp -d \"\${TMPDIR:-/tmp}/ruvoy-bench.XXXXXX\""
  )" || fail "failed to create a request-body directory on $load_generator_ssh"
  [[ -n "$remote_body_dir" ]] ||
    fail "load generator returned an empty request-body directory"
  scp -q "$result_dir"/body-*.bin "$load_generator_ssh:$remote_body_dir/" ||
    fail "failed to copy request bodies to $load_generator_ssh"
  if [[ -f "$result_dir/tls/ca.pem" ]]; then
    scp -q "$result_dir/tls/ca.pem" "$load_generator_ssh:$remote_body_dir/" ||
      fail "failed to copy the benchmark CA to $load_generator_ssh"
  fi
fi

{
  printf '[server]\n'
  describe_host
  if [[ "$load_generator" == "remote" ]]; then
    printf '\n[load_generator]\nssh=%s\n' "$load_generator_ssh"
    ssh -n -o BatchMode=yes "$load_generator_ssh" \
      "uname -srm; nproc; awk -F': ' '/model name/ { print \$2; exit }' /proc/cpuinfo" \
      2>/dev/null | sed 's/^/detail=/' || printf 'detail=unavailable\n'
    printf '\n[network]\n'
    ping -c 5 "$target_address" 2>/dev/null | tail -2 | sed 's/^/rtt=/' ||
      printf 'rtt=unavailable\n'
  else
    printf '\n[load_generator]\nssh=local\n'
  fi
} >"$result_dir/environment.txt"

assert_server_host_idle

{
  printf 'command=%s\n' "$0"
  printf 'invocation=RUVOY_BENCH_MODE=%s RUVOY_BENCH_TLS=%s RUVOY_BENCH_ARCHITECTURES=%s RUVOY_BENCH_LOAD_GENERATOR=%s RUVOY_BENCH_LOAD_GENERATOR_SSH=%s RUVOY_BENCH_LISTEN_ADDRESS=%s RUVOY_BENCH_TARGET_ADDRESS=%s RUVOY_BENCH_ROUNDS=%s RUVOY_BENCH_DURATION=%s RUVOY_BENCH_WARMUP_DURATION=%s %s\n' \
    "$mode" "$tls_selection" "$architecture_selection" "$load_generator" \
    "$load_generator_ssh" "$listen_address" "$target_address" \
    "$measurement_rounds" "$bench_duration" "$warmup_duration" "$0"
} >"$result_dir/invocation.txt"

{
  printf 'run_id=%s\nmode=%s\narchitectures=%s\narchitecture_order_mode=%s\nscenarios=%s\nbody_matrix=%s\nhost=%s\n' \
    "$run_id" "$mode" "$architecture_selection" "$architecture_order_mode" "$scenario_selection" "$body_matrix" "$(uname -srm)"
  printf 'envoy_package=%s\nenvoy_sdk_commit=%s\n' \
    "$envoy_package" "8eea3285d6bdb89f8ea34632cfe7ce1608a8f374"
  printf 'git_commit=%s\ngit_dirty=%s\n' \
    "$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || printf 'unknown')" \
    "$([[ -n "$(git -C "$repo_root" status --porcelain 2>/dev/null)" ]] && printf 'yes' || printf 'no')"
  printf 'load_generator=%s\nload_generator_ssh=%s\nlisten_address=%s\ntarget_address=%s\n' \
    "$load_generator" "$load_generator_ssh" "$listen_address" "$target_address"
  printf 'cpu_source=%s\nclock_tick=%s\n' "$cpu_source" "$clock_tick"
  printf 'tls_modes=%s\ntls_key=%s\n' "${tls_modes[*]}" "${tls_key_details:-none}"
  printf 'ruby=%s\nbundler=%s\noha=%s\n' \
    "$("$ruby_bin" --version)" "$bundle_version" "$oha_version"
  printf 'rustc=%s\nrack=%s\npuma=%s\nfalcon=%s\nasync=%s\n' \
    "$(rustc --version)" "$("$repo_root/scripts/gem-version.sh" rack)" "$("$repo_root/scripts/gem-version.sh" puma)" \
    "$("$repo_root/scripts/gem-version.sh" falcon)" "$("$repo_root/scripts/gem-version.sh" async)"
  printf 'rounds=%s\nbench_duration=%s\nwarmup_duration=%s\nwarmup_concurrency=%s\noha_timeout_seconds=%s\n' \
    "$measurement_rounds" "$bench_duration" "$warmup_duration" "$warmup_concurrency" "$oha_timeout_seconds"
  printf 'envoy_concurrency=%s\npuma_workers=%s\npuma_threads=%s\nfalcon_count=%s\n' \
    "$envoy_concurrency" "$puma_workers" "$puma_threads" "$falcon_count"
  RUVOY_RUBY="$ruby_bin" RUVOY_BUILD_PROFILE=release "$repo_root/scripts/build-modules.sh"
  RUVOY_RUBY="$ruby_bin" RUVOY_BUILD_PROFILE=release "$repo_root/scripts/build-sync-module.sh"
  RUVOY_RUBY="$ruby_bin" RUVOY_BUILD_PROFILE=release "$repo_root/scripts/build-fiber-module.sh"
  "$repo_root/scripts/check-worker-boundary.sh"
} >"$result_dir/preflight.log" 2>&1 || fail "build or preflight failed"

campaign_sequence=0
measurement_count=0
printf 'architecture\ttls_mode\tnegotiated\n' >"$result_dir/tls-negotiation.tsv"
# TLS is the outer loop so that one mode's full matrix completes before the
# other starts; interleaving modes would mix two different systems within a
# round and break the execution-order balance that rotate provides.
for current_tls_mode in "${tls_modes[@]}"; do
  if [[ "$current_tls_mode" == tls ]]; then
    current_scheme=https
  else
    current_scheme=http
  fi
  tls_negotiation_baseline=""
while IFS='|' read -r scenario protocol concurrency wait_ms request_bytes response_bytes app_params; do
  scenario_is_selected "$scenario" || continue
  for round in $(seq 1 "$measurement_rounds"); do
    architecture_count="${#architectures[@]}"
    round_order=()
    case "$architecture_order_mode" in
      forward)
        round_order=("${architectures[@]}")
        ;;
      reverse)
        for ((index = architecture_count - 1; index >= 0; index--)); do
          round_order+=("${architectures[$index]}")
        done
        ;;
      alternate)
        if [[ $((round % 2)) -eq 0 ]]; then
          for ((index = architecture_count - 1; index >= 0; index--)); do
            round_order+=("${architectures[$index]}")
          done
        else
          round_order=("${architectures[@]}")
        fi
        ;;
      rotate)
        # Reversing only alternates first and last place; rotating gives every
        # architecture an even share of each execution slot across rounds.
        offset=$(((round - 1) % architecture_count))
        for ((position = 0; position < architecture_count; position++)); do
          round_order+=("${architectures[$(((offset + position) % architecture_count))]}")
        done
        ;;
    esac
    for architecture in "${round_order[@]}"; do
      campaign_sequence=$((campaign_sequence + 1))
      run_measurement \
        "$campaign_sequence" \
        "$architecture" \
        "$scenario" \
        "$round" \
        "$protocol" \
        "$concurrency" \
        "$wait_ms" \
        "$request_bytes" \
        "$response_bytes" \
        "$app_params"
      if architecture_supports_protocol "$architecture" "$protocol"; then
        measurement_count=$((measurement_count + 1))
      fi
    done
  done
done < <(scenario_rows)
done

[[ "$measurement_count" -gt 0 ]] || fail "scenario selection produced no runnable measurements"

# A silently truncated campaign is worse than a failed one: it still produces a
# summary. Recompute what the selection should have produced and refuse to
# report anything less.
expected_measurements=0
for expected_tls_mode in "${tls_modes[@]}"; do
  while IFS='|' read -r expected_scenario expected_protocol _ _ _ _ _; do
    scenario_is_selected "$expected_scenario" || continue
    for expected_architecture in "${architectures[@]}"; do
      architecture_supports_protocol "$expected_architecture" "$expected_protocol" || continue
      expected_measurements=$((expected_measurements + measurement_rounds))
    done
  done < <(scenario_rows)
done
[[ "$measurement_count" -eq "$expected_measurements" ]] ||
  fail "campaign ran $measurement_count measurements but the selection requires $expected_measurements"

bridge_repetitions="$measurement_rounds"
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
"$ruby_bin" "$repo_root/bench/render_charts.rb" "$result_dir"

# A scenario whose rounds disagree this much is not a measurement; the summary
# marks it and the run fails rather than publishing an unstable number.
unstable="$(jq -r '.unstable_measurements[]? | "\(.architecture)/\(.scenario)/\(.tls_mode) rps_rsd=\(.rps_rsd_percent)%"' \
  "$result_dir/summary.json")"
if [[ -n "$unstable" && "$mode" == "full" ]]; then
  printf '%s\n' "$unstable" >&2
  fail "unstable scenarios exceeded the 15% RPS RSD threshold and must be re-run"
fi

# The checksums cover the machine-readable evidence, so a later reader can tell
# whether the raw results still match what was summarised.
(
  cd "$result_dir" || exit 1
  find . -type f ! -name SHA256SUMS -print0 |
    sort -z |
    xargs -0 shasum -a 256
) >"$result_dir/SHA256SUMS" || fail "failed to record checksums"

printf 'result=PASS\nraw_results=%s\nsummary=%s\ncharts=%s\n' \
  "$result_dir" "$result_dir/summary.md" "$result_dir/charts"
