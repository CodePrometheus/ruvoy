#!/usr/bin/env bash

# Verifies the process-wide Ruby runtime: one CRuby VM per Envoy process, shared
# by every filter config, torn down exactly once at process exit.
#
# These are lifecycle assertions, not measurements, so they are safe to run on a
# machine that is doing other work.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_dir="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}/process-runtime-$run_id"
build_profile="${RUVOY_BUILD_PROFILE:-release}"
envoy_package="envoy-server==1.39.0"
ruby_version="$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
ruby_bin="${RUVOY_RUBY:-"$HOME/.rbenv/versions/$ruby_version/bin/ruby"}"
temporary_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-process-runtime.XXXXXX")"
rackup="$repo_root/test/fixtures/rack/config.ru"
envoy_pid=""
failures=0

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  failures=$((failures + 1))
}

pass() {
  printf 'PASS: %s\n' "$*"
}

cleanup() {
  stop_envoy
  rm -rf "$temporary_dir"
}
trap cleanup EXIT

stop_envoy() {
  [[ -n "$envoy_pid" ]] || return 0
  kill -INT "$envoy_pid" 2>/dev/null || true
  for _ in $(seq 1 200); do
    kill -0 "$envoy_pid" 2>/dev/null || break
    sleep 0.05
  done
  kill -KILL "$envoy_pid" 2>/dev/null || true
  wait "$envoy_pid" 2>/dev/null || true
  envoy_pid=""
}

# Envoy exits non-zero when a filter config refuses to build, which is exactly
# what the fail-closed cases assert, so the launcher must not use set -e.
start_envoy() {
  local config="$1"
  local log="$2"

  BUNDLE_GEMFILE="$repo_root/Gemfile" \
    BUNDLE_PATH="$repo_root/vendor/bundle" \
    BUNDLE_FROZEN=true \
    ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
    RUVOY_DIAGNOSTICS=1 \
    uvx --from "$envoy_package" envoy \
    --config-path "$config" \
    --concurrency 1 \
    --disable-hot-restart \
    --log-level info \
    >"$log" 2>&1 &
  envoy_pid=$!
}

wait_for_port() {
  local port="$1"

  for _ in $(seq 1 200); do
    if curl --silent --fail --max-time 1 --output /dev/null \
      "http://127.0.0.1:$port/rack-env"; then
      return 0
    fi
    process_is_alive "$envoy_pid" || return 1
    sleep 0.1
  done
  return 1
}

# A backgrounded process stays reapable after it exits, and kill -0 succeeds on
# a zombie, so liveness has to exclude the Z state.
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

  for _ in $(seq 1 "$attempts"); do
    process_is_alive "$pid" || return 0
    sleep 0.1
  done
  return 1
}

runtime_started_count() {
  grep -c '\[ruvoy\] Fiber runtime started' "$1" || true
}

listener_config() {
  local name="$1"
  local port="$2"
  local rackup_path="$3"

  cat <<YAML
    - name: $name
      address:
        socket_address:
          address: 127.0.0.1
          port_value: $port
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: $name
                codec_type: AUTO
                stream_idle_timeout: 5s
                request_timeout: 10s
                route_config:
                  name: ${name}_route
                  virtual_hosts:
                    - name: ${name}_service
                      domains: ["*"]
                http_filters:
                  - name: envoy.extensions.filters.http.dynamic_modules
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
                      dynamic_module_config:
                        name: ruvoy_fiber
                        do_not_close: true
                      filter_name: fiber_rack
                      terminal_filter: true
                      filter_config:
                        "@type": type.googleapis.com/google.protobuf.StringValue
                        value: $rackup_path
YAML
}

write_config() {
  local path="$1"
  shift

  {
    printf 'static_resources:\n  listeners:\n'
    while [[ "$#" -ge 3 ]]; do
      listener_config "$1" "$2" "$3"
      shift 3
    done
  } >"$path"
}

mkdir -p "$result_dir"
[[ -x "$ruby_bin" ]] || {
  printf 'FAIL: missing Ruby %s; set RUVOY_RUBY\n' "$ruby_version" >&2
  exit 1
}
RUVOY_RUBY="$ruby_bin" RUVOY_BUILD_PROFILE="$build_profile" \
  "$repo_root/scripts/build-fiber-module.sh" >"$result_dir/build.log" 2>&1 ||
  {
    printf 'FAIL: fiber module build failed; see %s\n' "$result_dir/build.log" >&2
    exit 1
  }

# --- 0. The module resolves libruby without LD_LIBRARY_PATH ------------------
# Envoy dlopens the module, so the loader must find libruby from the module's
# own RUNPATH. This whole script deliberately runs with LD_LIBRARY_PATH unset.
module_path="$repo_root/build/modules/libruvoy_fiber.so"
if command -v readelf >/dev/null; then
  runpath="$(readelf -d "$module_path" 2>/dev/null |
    awk -F'[][]' '/RUNPATH|RPATH/ { print $2 }')"
  ruby_libdir="$("$ruby_bin" -e "print RbConfig::CONFIG['libdir']")"
  [[ -n "$runpath" ]] &&
    pass "the module records a RUNPATH ($runpath)" ||
    fail "the module has no RUNPATH; Envoy would need LD_LIBRARY_PATH"
  [[ "$runpath" == *"$ruby_libdir"* ]] &&
    pass "the RUNPATH points at the Ruby libdir" ||
    fail "RUNPATH '$runpath' does not contain '$ruby_libdir'"
fi
if command -v ldd >/dev/null; then
  unresolved="$(env -u LD_LIBRARY_PATH ldd "$module_path" 2>/dev/null |
    awk '/not found/ { print $1 }')"
  [[ -z "$unresolved" ]] &&
    pass "every shared library resolves without LD_LIBRARY_PATH" ||
    fail "unresolved shared libraries without LD_LIBRARY_PATH: $unresolved"
fi

# --- 1. Two filter configs share one runtime ---------------------------------
write_config "$temporary_dir/shared.yaml" \
  ruvoy_first 18090 "$rackup" \
  ruvoy_second 18091 "$rackup"
start_envoy "$temporary_dir/shared.yaml" "$result_dir/shared.log"
if wait_for_port 18090 && wait_for_port 18091; then
  first_thread="$(curl --silent --dump-header - --output /dev/null \
    "http://127.0.0.1:18090/rack-env" |
    awk -F': ' 'tolower($1) == "x-ruvoy-runtime-rust-thread-id" { print $2 }' | tr -d '\r')"
  second_thread="$(curl --silent --dump-header - --output /dev/null \
    "http://127.0.0.1:18091/rack-env" |
    awk -F': ' 'tolower($1) == "x-ruvoy-runtime-rust-thread-id" { print $2 }' | tr -d '\r')"
  started="$(runtime_started_count "$result_dir/shared.log")"

  [[ -n "$first_thread" && "$first_thread" == "$second_thread" ]] &&
    pass "two filter configs share one Ruby runtime thread ($first_thread)" ||
    fail "runtime threads differ: '$first_thread' vs '$second_thread'"
  [[ "$started" -eq 1 ]] &&
    pass "the runtime was initialized exactly once ($started)" ||
    fail "expected one runtime startup, found $started"
else
  fail "two-config Envoy did not become ready"
fi
stop_envoy
grep -q '\[ruvoy\] Fiber runtime stopped' "$result_dir/shared.log" &&
  pass "the runtime reported a clean shutdown" ||
  fail "no clean shutdown line in shared.log"
[[ "$(grep -c '\[ruvoy\] Fiber runtime stopped' "$result_dir/shared.log" || true)" -eq 1 ]] &&
  pass "shutdown ran exactly once" ||
  fail "shutdown did not run exactly once"

# --- 2. An incompatible second config is refused -----------------------------
cp "$rackup" "$temporary_dir/other.ru"
cp "$repo_root/test/fixtures/rack/app.rb" "$temporary_dir/app.rb"
write_config "$temporary_dir/mismatch.yaml" \
  ruvoy_first 18092 "$rackup" \
  ruvoy_second 18093 "$temporary_dir/other.ru"
start_envoy "$temporary_dir/mismatch.yaml" "$result_dir/mismatch.log"
if wait_for_exit "$envoy_pid" 600; then
  pass "Envoy refused the mismatched configuration instead of starting"
else
  fail "Envoy stayed up with two different rackup paths"
fi
envoy_pid=""
# Assert what the operator needs from the message, not its wording: both
# rackups named, and that a restart is what resolves it.
grep -q 'active rackup=' "$result_dir/mismatch.log" &&
  grep -q 'requested rackup=' "$result_dir/mismatch.log" &&
  grep -qi 'restart' "$result_dir/mismatch.log" &&
  pass "the refusal names the active rackup" ||
  fail "no explicit mismatch error in mismatch.log"
[[ "$(runtime_started_count "$result_dir/mismatch.log")" -eq 1 ]] &&
  pass "the mismatch did not initialize a second Ruby VM" ||
  fail "expected exactly one runtime startup during the mismatch case"
grep -Eqi 'panic|segmentation fault|SIGSEGV' "$result_dir/mismatch.log" &&
  fail "mismatch case produced a crash signature" ||
  pass "no crash signature in the mismatch case"

# --- 3. A missing rackup fails closed ----------------------------------------
write_config "$temporary_dir/missing.yaml" \
  ruvoy_first 18094 "$temporary_dir/does-not-exist.ru"
start_envoy "$temporary_dir/missing.yaml" "$result_dir/missing.log"
if wait_for_exit "$envoy_pid" 600; then
  pass "Envoy refused to start with a missing rackup"
else
  fail "Envoy stayed up with a missing rackup"
fi
envoy_pid=""
grep -q 'failed to resolve rackup' "$result_dir/missing.log" &&
  pass "the startup error names the unresolved rackup" ||
  fail "no rackup resolution error in missing.log"
grep -q 'working directory' "$result_dir/missing.log" &&
  pass "the startup error reports the working directory" ||
  fail "the startup error omits the working directory"

# --- 4. A raising rackup fails closed ----------------------------------------
printf 'raise "intentional startup failure"\n' >"$temporary_dir/raising.ru"
write_config "$temporary_dir/raising.yaml" \
  ruvoy_first 18095 "$temporary_dir/raising.ru"
start_envoy "$temporary_dir/raising.yaml" "$result_dir/raising.log"
if wait_for_exit "$envoy_pid" 600; then
  pass "Envoy refused to start after a Ruby startup exception"
else
  fail "Envoy stayed up after a Ruby startup exception"
fi
envoy_pid=""
grep -q 'intentional startup failure' "$result_dir/raising.log" &&
  pass "the Ruby exception is surfaced in the startup error" ||
  fail "the Ruby startup exception was swallowed"
grep -Eqi 'segmentation fault|SIGSEGV' "$result_dir/raising.log" &&
  fail "the raising case produced a crash signature" ||
  pass "no crash signature in the raising case"

# --- 5. Shutdown drains an in-flight request ---------------------------------
write_config "$temporary_dir/drain.yaml" ruvoy_first 18096 "$rackup"
start_envoy "$temporary_dir/drain.yaml" "$result_dir/drain.log"
if wait_for_port 18096; then
  curl --silent --max-time 20 --output "$temporary_dir/drain.body" \
    --write-out '%{http_code}' \
    "http://127.0.0.1:18096/async-sleep?duration=1" >"$temporary_dir/drain.status" &
  drain_curl=$!
  sleep 0.3
  kill -TERM "$envoy_pid" 2>/dev/null || true
  wait "$drain_curl" 2>/dev/null || true

  if wait_for_exit "$envoy_pid" 300; then
    pass "Envoy exited after SIGTERM with a request in flight"
  else
    fail "Envoy did not exit after SIGTERM"
  fi
  wait "$envoy_pid" 2>/dev/null
  envoy_pid=""
  [[ "$(cat "$temporary_dir/drain.status" 2>/dev/null)" == 200 ]] &&
    pass "the in-flight request completed during drain" ||
    printf 'NOTE: in-flight request returned %s during drain\n' \
      "$(cat "$temporary_dir/drain.status" 2>/dev/null)"
  grep -q '\[ruvoy\] Fiber runtime stopped' "$result_dir/drain.log" &&
    pass "the runtime stopped cleanly after SIGTERM" ||
    fail "no clean shutdown after SIGTERM"
else
  fail "drain Envoy did not become ready"
  stop_envoy
fi

# --- 6. A late completion after the stream timed out must stay silent --------
write_config "$temporary_dir/timeout.yaml" ruvoy_first 18097 "$rackup"
start_envoy "$temporary_dir/timeout.yaml" "$result_dir/timeout.log"
if wait_for_port 18097; then
  # stream_idle_timeout is 5s, so a 9s Ruby request is abandoned by Envoy while
  # the Fiber keeps running; its completion then commits onto a destroyed filter.
  timeout_status="$(curl --silent --max-time 30 --output /dev/null \
    --write-out '%{http_code}' \
    "http://127.0.0.1:18097/async-sleep?duration=9" || true)"
  printf 'timed-out request returned %s\n' "$timeout_status"
  sleep 6

  survivor="$(curl --silent --max-time 10 --output /dev/null \
    --write-out '%{http_code}' "http://127.0.0.1:18097/rack-env" || true)"
  [[ "$survivor" == 200 ]] &&
    pass "Envoy still serves requests after a late Fiber completion" ||
    fail "Envoy stopped serving after the late completion (got '$survivor')"
  kill -0 "$envoy_pid" 2>/dev/null &&
    pass "the Envoy process survived the late completion" ||
    fail "the Envoy process died after the late completion"
else
  fail "timeout Envoy did not become ready"
fi
stop_envoy
grep -Eqi 'panic|segmentation fault|SIGSEGV|use-after-free' "$result_dir/timeout.log" &&
  fail "the late-completion case produced a crash signature" ||
  pass "no crash signature after the late completion"

# --- 7. Graceful drain completes in-flight work ------------------------------
# SIGTERM is an immediate shutdown in Envoy, so the earlier case only proves the
# runtime stops cleanly. Draining through the admin endpoint is what tells us
# whether an in-flight Ruby request is allowed to finish.
{
  printf 'admin:\n  address:\n    socket_address:\n      address: 127.0.0.1\n      port_value: 19001\n'
  printf 'static_resources:\n  listeners:\n'
  listener_config ruvoy_first 18098 "$rackup"
} >"$temporary_dir/admin.yaml"
start_envoy "$temporary_dir/admin.yaml" "$result_dir/admin.log"
if wait_for_port 18098; then
  curl --silent --max-time 25 --output /dev/null --write-out '%{http_code}' \
    "http://127.0.0.1:18098/async-sleep?duration=2" >"$temporary_dir/drain2.status" &
  drain2_curl=$!
  sleep 0.4
  curl --silent --max-time 10 --request POST --output /dev/null \
    "http://127.0.0.1:19001/drain_listeners?graceful" || true
  wait "$drain2_curl" 2>/dev/null || true

  drain2_status="$(cat "$temporary_dir/drain2.status" 2>/dev/null)"
  [[ "$drain2_status" == 200 ]] &&
    pass "a graceful drain let the in-flight Ruby request finish ($drain2_status)" ||
    fail "graceful drain returned '$drain2_status' for the in-flight request"
else
  fail "admin Envoy did not become ready"
fi
stop_envoy

# --- 8. Hot restart hands over without disturbing the runtime ----------------
# Each epoch is its own process and therefore its own Ruby VM; the point is that
# the old epoch tears its VM down exactly once while the new one serves.
write_config "$temporary_dir/hot.yaml" ruvoy_first 18099 "$rackup"
BUNDLE_GEMFILE="$repo_root/Gemfile" \
  BUNDLE_PATH="$repo_root/vendor/bundle" \
  BUNDLE_FROZEN=true \
  ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
  RUVOY_DIAGNOSTICS=1 \
  uvx --from "$envoy_package" envoy \
  --config-path "$temporary_dir/hot.yaml" \
  --concurrency 1 \
  --restart-epoch 0 \
  --base-id 7 \
  --drain-time-s 5 \
  --parent-shutdown-time-s 10 \
  --log-level info \
  >"$result_dir/hot-epoch0.log" 2>&1 &
epoch0_pid=$!
envoy_pid="$epoch0_pid"

if wait_for_port 18099; then
  BUNDLE_GEMFILE="$repo_root/Gemfile" \
    BUNDLE_PATH="$repo_root/vendor/bundle" \
    BUNDLE_FROZEN=true \
    ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
    RUVOY_DIAGNOSTICS=1 \
    uvx --from "$envoy_package" envoy \
    --config-path "$temporary_dir/hot.yaml" \
    --concurrency 1 \
    --restart-epoch 1 \
    --base-id 7 \
    --drain-time-s 5 \
    --parent-shutdown-time-s 10 \
    --log-level info \
    >"$result_dir/hot-epoch1.log" 2>&1 &
  epoch1_pid=$!

  if wait_for_exit "$epoch0_pid" 600; then
    pass "the original epoch exited after the hot restart"
  else
    fail "the original epoch did not exit after the hot restart"
    kill -KILL "$epoch0_pid" 2>/dev/null || true
  fi
  wait "$epoch0_pid" 2>/dev/null || true
  envoy_pid="$epoch1_pid"

  handover="$(curl --silent --max-time 10 --output /dev/null \
    --write-out '%{http_code}' "http://127.0.0.1:18099/rack-env" || true)"
  [[ "$handover" == 200 ]] &&
    pass "the new epoch serves requests after the handover" ||
    fail "the new epoch returned '$handover' after the handover"

  [[ "$(grep -c '\[ruvoy\] Fiber runtime stopped' "$result_dir/hot-epoch0.log" || true)" -eq 1 ]] &&
    pass "the old epoch tore its Ruby VM down exactly once" ||
    fail "the old epoch did not shut its runtime down exactly once"
  [[ "$(runtime_started_count "$result_dir/hot-epoch1.log")" -eq 1 ]] &&
    pass "the new epoch initialized exactly one Ruby VM" ||
    fail "the new epoch did not initialize exactly one Ruby VM"
else
  fail "hot-restart epoch 0 did not become ready"
fi
stop_envoy
grep -Eqi 'panic|segmentation fault|SIGSEGV' \
  "$result_dir/hot-epoch0.log" "$result_dir/hot-epoch1.log" &&
  fail "the hot restart produced a crash signature" ||
  pass "no crash signature across the hot restart"

printf '\nraw results: %s\n' "$result_dir"
if [[ "$failures" -gt 0 ]]; then
  printf 'RESULT: FAIL (%d checks failed)\n' "$failures"
  exit 1
fi
printf 'RESULT: PASS\n'
