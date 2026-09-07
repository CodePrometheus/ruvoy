#!/usr/bin/env bash

# Verifies what happens to the Ruby runtime while Envoy's configuration changes
# underneath it.
#
# Listeners arrive over a filesystem LDS subscription, which Envoy reloads when
# the file is replaced, so this drives real configuration updates rather than
# restarting the process. The module is registered without `do_not_close`, so
# removing the last listener that references it is what would unload it.
#
# That step only carries weight where `dlclose` actually unloads: macOS keeps
# libraries resident regardless, so the run reports which kind of platform it
# ran on rather than implying the check was meaningful everywhere.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_dir="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}/config-lifecycle-$run_id"
build_profile="${RUVOY_BUILD_PROFILE:-release}"
envoy_package="envoy-server==1.39.0"
ruby_version="$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
ruby_bin="${RUVOY_RUBY:-"$HOME/.rbenv/versions/$ruby_version/bin/ruby"}"
temporary_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-config-lifecycle.XXXXXX")"
rack_port=18120
second_port=18121
admin_port=18122
envoy_log="$result_dir/envoy.log"
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
  local pid="$1" state
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

header_of() {
  awk -v name="$1" 'BEGIN { IGNORECASE = 1 }
    tolower($1) == tolower(name) ":" { sub(/\r$/, "", $2); print $2; exit }' "$2"
}

# Replaces the listener set. Envoy watches the path for moves, so the new file
# is staged beside it and renamed into place.
apply_listeners() {
  local version="$1" body="$2"
  {
    printf 'version_info: "%s"\nresources:\n' "$version"
    printf '%s\n' "$body"
  } >"$temporary_dir/lds.next"
  mv "$temporary_dir/lds.next" "$temporary_dir/lds.yaml"
  cp "$temporary_dir/lds.yaml" "$result_dir/lds-$version.yaml"
}

admin_stat() {
  curl --silent --max-time 5 "http://127.0.0.1:$admin_port/stats?filter=^$1\$" |
    awk -F': ' '{ print $2; exit }'
}

# Waits for a URL to answer with the expected status, so a step never asserts
# against a configuration Envoy has not applied yet.
wait_for_status() {
  local url="$1" expected="$2" observed=""
  for _ in $(seq 1 100); do
    observed="$(curl --silent --output /dev/null --max-time 2 --write-out '%{http_code}' "$url" || true)"
    [[ "$observed" == "$expected" ]] && return 0
    process_is_alive "$envoy_pid" || return 1
    sleep 0.1
  done
  printf 'last status for %s was %s, wanted %s\n' "$url" "$observed" "$expected" >&2
  return 1
}

wait_for_absence() {
  local url="$1"
  for _ in $(seq 1 100); do
    curl --silent --output /dev/null --max-time 2 "$url" || return 0
    process_is_alive "$envoy_pid" || return 1
    sleep 0.1
  done
  return 1
}

rack_listener() {
  local name="$1" port="$2" rackup="$3"
  cat <<YAML
  - "@type": type.googleapis.com/envoy.config.listener.v3.Listener
    name: $name
    address:
      socket_address: { address: 127.0.0.1, port_value: $port }
    filter_chains:
      - filters:
          - name: envoy.filters.network.http_connection_manager
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
              stat_prefix: $name
              codec_type: AUTO
              route_config:
                name: ${name}_route
                virtual_hosts:
                  - name: $name
                    domains: ["*"]
              http_filters:
                - name: envoy.extensions.filters.http.dynamic_modules
                  typed_config:
                    "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
                    dynamic_module_config:
                      name: ruvoy_fiber
                    filter_name: fiber_rack
                    terminal_filter: true
                    filter_config:
                      "@type": type.googleapis.com/google.protobuf.StringValue
                      value: '{"rackup": "$rackup"}'
YAML
}

static_listener() {
  local name="$1" port="$2" body="$3"
  cat <<YAML
  - "@type": type.googleapis.com/envoy.config.listener.v3.Listener
    name: $name
    address:
      socket_address: { address: 127.0.0.1, port_value: $port }
    filter_chains:
      - filters:
          - name: envoy.filters.network.http_connection_manager
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
              stat_prefix: $name
              codec_type: AUTO
              route_config:
                name: ${name}_route
                virtual_hosts:
                  - name: $name
                    domains: ["*"]
                    routes:
                      - match: { prefix: "/" }
                        direct_response: { status: 200, body: { inline_string: "$body" } }
              http_filters:
                - name: envoy.filters.http.router
                  typed_config:
                    "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
YAML
}

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

primary_rackup="$repo_root/test/fixtures/rack/config.ru"
secondary_rackup="$repo_root/bench/config.ru"

cat >"$temporary_dir/bootstrap.yaml" <<YAML
node: { id: ruvoy-config-lifecycle, cluster: ruvoy }
# Histograms only publish on flush; a short interval keeps the run quick.
stats_flush_interval: 1s
admin:
  address:
    socket_address: { address: 127.0.0.1, port_value: $admin_port }
dynamic_resources:
  lds_config:
    resource_api_version: V3
    path_config_source:
      path: $temporary_dir/lds.yaml
YAML

apply_listeners 1 "$(rack_listener ruvoy_primary "$rack_port" "$primary_rackup")"

BUNDLE_GEMFILE="$repo_root/Gemfile" \
  BUNDLE_PATH="$repo_root/vendor/bundle" \
  BUNDLE_FROZEN=true \
  ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
  RUVOY_DIAGNOSTICS=1 \
  uvx --from "$envoy_package" envoy \
  --config-path "$temporary_dir/bootstrap.yaml" \
  --concurrency 1 \
  --disable-hot-restart \
  --log-level info \
  >"$envoy_log" 2>&1 &
envoy_pid=$!

wait_for_status "http://127.0.0.1:$rack_port/hello" 200 || {
  printf 'FAIL: the primary listener never became ready\n' >&2
  tail -20 "$envoy_log" >&2
  exit 1
}

probe() {
  local port="$1" prefix="$2"
  curl --silent --max-time 5 --output "$prefix.body" --dump-header "$prefix.headers" \
    "http://127.0.0.1:$port/hello"
}

# --- 1. The runtime starts once and identifies its Ruby thread ---------------
probe "$rack_port" "$temporary_dir/first"
runtime_thread="$(header_of x-ruby-thread-object-id "$temporary_dir/first.headers")"
first_calls="$(header_of x-ruby-call-count "$temporary_dir/first.headers")"
[[ -n "$runtime_thread" ]] &&
  pass "the primary listener served from Ruby thread $runtime_thread" ||
  fail "the primary listener did not report a Ruby thread"

# --- 2. A second listener on the same rackup reuses the same VM --------------
apply_listeners 2 "$(
  rack_listener ruvoy_primary "$rack_port" "$primary_rackup"
  rack_listener ruvoy_secondary "$second_port" "$primary_rackup"
)"
wait_for_status "http://127.0.0.1:$second_port/hello" 200 &&
  pass "the added listener started serving" ||
  fail "the added listener never became ready"

probe "$second_port" "$temporary_dir/second"
[[ "$(header_of x-ruby-thread-object-id "$temporary_dir/second.headers")" == "$runtime_thread" ]] &&
  pass "both listeners share one Ruby VM" ||
  fail "the added listener reported a different Ruby thread"
[[ "$(header_of x-ruby-call-count "$temporary_dir/second.headers")" -gt "$first_calls" ]] &&
  pass "the call counter continued rather than restarting" ||
  fail "the call counter restarted, so the VM was replaced"

# --- 3. An Envoy-side update leaves the runtime untouched --------------------
apply_listeners 3 "$(
  rack_listener ruvoy_primary "$rack_port" "$primary_rackup"
  static_listener ruvoy_secondary "$second_port" "envoy-only"
)"
wait_for_status "http://127.0.0.1:$second_port/" 200 &&
  pass "the listener was reconfigured to answer without Ruby" ||
  fail "the reconfigured listener never became ready"

probe "$rack_port" "$temporary_dir/third"
[[ "$(header_of x-ruby-thread-object-id "$temporary_dir/third.headers")" == "$runtime_thread" ]] &&
  pass "reconfiguring a listener did not restart the VM" ||
  fail "the VM changed while a listener was reconfigured"

# --- 4. A second rackup is refused, and the running config keeps serving -----
rejected_before="$(admin_stat listener_manager.lds.update_rejected)"
apply_listeners 4 "$(
  rack_listener ruvoy_primary "$rack_port" "$primary_rackup"
  rack_listener ruvoy_secondary "$second_port" "$secondary_rackup"
)"
for _ in $(seq 1 100); do
  [[ "$(admin_stat listener_manager.lds.update_rejected)" -gt "$rejected_before" ]] && break
  sleep 0.1
done
[[ "$(admin_stat listener_manager.lds.update_rejected)" -gt "$rejected_before" ]] &&
  pass "Envoy rejected the configuration asking for a second rackup" ||
  fail "Envoy accepted a second rackup"

probe "$rack_port" "$temporary_dir/fourth"
[[ "$(header_of x-ruby-thread-object-id "$temporary_dir/fourth.headers")" == "$runtime_thread" ]] &&
  pass "the rejected update left the running configuration serving" ||
  fail "the rejected update disturbed the running configuration"
grep -q "$secondary_rackup" "$envoy_log" &&
  pass "the rejection names the rackup that was refused" ||
  fail "the rejection does not say which rackup was refused"

# --- 5. Removing every Ruby listener must not unload the module --------------
case "$(uname -s)" in
  Linux) unload_note="dlclose unloads here, so this is a real check" ;;
  *) unload_note="this platform keeps libraries resident, so this check is vacuous" ;;
esac
printf 'NOTE: %s\n' "$unload_note"
apply_listeners 5 "$(static_listener ruvoy_secondary "$second_port" "envoy-only")"
wait_for_absence "http://127.0.0.1:$rack_port/hello" &&
  pass "the last Ruby listener was removed" ||
  fail "the Ruby listener was still accepting connections"
process_is_alive "$envoy_pid" &&
  pass "Envoy survived losing its last reference to the module" ||
  fail "Envoy died when the module lost its last reference"

# --- 6. The VM is still the one from step 1 ----------------------------------
apply_listeners 6 "$(
  rack_listener ruvoy_primary "$rack_port" "$primary_rackup"
  static_listener ruvoy_secondary "$second_port" "envoy-only"
)"
wait_for_status "http://127.0.0.1:$rack_port/hello" 200 &&
  pass "the Ruby listener came back" ||
  fail "the Ruby listener did not come back"

probe "$rack_port" "$temporary_dir/sixth"
[[ "$(header_of x-ruby-thread-object-id "$temporary_dir/sixth.headers")" == "$runtime_thread" ]] &&
  pass "the VM outlived having no listeners at all" ||
  fail "a new VM was created after the module lost every reference"

# --- 7. The module reports itself through Envoy's statistics -----------------
sleep 1.5
metrics="$(curl --silent --max-time 5 "http://127.0.0.1:$admin_port/stats" |
  grep -i 'dynamicmodules' || true)"
printf '%s\n' "$metrics" >"$result_dir/stats.txt"
grep -q 'requests_total' <<<"$metrics" &&
  pass "Envoy exposes the module's request counter" ||
  fail "the module's statistics are missing from Envoy"
grep -qE 'responses_total.*outcome.*completed' <<<"$metrics" &&
  pass "completed responses are counted by outcome" ||
  fail "no completed responses were counted"
grep -qE 'duration_ms.*P50' <<<"$metrics" &&
  pass "request durations reach the histogram" ||
  fail "the duration histogram recorded nothing"
grep -qE 'inflight_requests: 0$' <<<"$metrics" &&
  pass "the saturation gauge returned to zero once the work finished" ||
  fail "the saturation gauge did not return to zero"
# The reactor waits with a deadline, so an unblocked runtime always reports a
# small age here; a blocked one would grow without bound.
reactor_idle="$(awk -F': ' '/reactor_idle_ms/ { print $2; exit }' <<<"$metrics")"
[[ -n "$reactor_idle" && "$reactor_idle" -lt 1000 ]] &&
  pass "the reactor reported in recently (${reactor_idle} ms)" ||
  fail "the reactor looked stalled: ${reactor_idle:-missing} ms"

# --- 8. One VM for the whole run, and no crashes -----------------------------
started="$(grep -c '\[ruvoy\] Fiber runtime started' "$envoy_log" || true)"
[[ "$started" -eq 1 ]] &&
  pass "the runtime started exactly once across every configuration change" ||
  fail "the runtime started $started times"
grep -Eqi 'panic|segmentation fault|SIGSEGV' "$envoy_log" &&
  fail "the run produced a crash signature" ||
  pass "no crash signature across the configuration changes"

cp "$temporary_dir"/*.headers "$result_dir/" 2>/dev/null || true
{
  printf 'run_id=%s\n' "$run_id"
  printf 'host=%s\n' "$(uname -srm)"
  printf 'module_unload_observable=%s\n' \
    "$([[ "$(uname -s)" == Linux ]] && echo yes || echo no)"
} >"$result_dir/environment.txt"

printf '\nraw results: %s\n' "$result_dir"
if [[ "$failures" -gt 0 ]]; then
  printf 'RESULT: FAIL (%d checks failed)\n' "$failures"
  exit 1
fi
printf 'RESULT: PASS\n'
