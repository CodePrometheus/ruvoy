#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-rack.XXXXXX")"
envoy_config="$temporary_dir/envoy.yaml"
envoy_log="$temporary_dir/envoy.log"
puma_log="$temporary_dir/puma.log"
envoy_pid=""
puma_pid=""
fiber_port=19183
control_port=19184
puma_port=19210
ruby_version="$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
ruby_bin="${RUVOY_RUBY:-}"
bundle_path="${RUVOY_BUNDLE_PATH:-"$repo_root/vendor/bundle"}"
rackup="$repo_root/test/fixtures/rack/config.ru"

if [[ -z "$ruby_bin" ]]; then
  if command -v rbenv >/dev/null 2>&1; then
    ruby_bin="$(RBENV_VERSION="$ruby_version" rbenv which ruby)"
  elif [[ -x "$HOME/.rbenv/versions/$ruby_version/bin/ruby" ]]; then
    ruby_bin="$HOME/.rbenv/versions/$ruby_version/bin/ruby"
  else
    ruby_bin="$(command -v ruby)"
  fi
fi
bundle_bin="${RUVOY_BUNDLE:-"$(dirname "$ruby_bin")/bundle"}"

cleanup() {
  if [[ -n "$envoy_pid" ]] && kill -0 "$envoy_pid" 2>/dev/null; then
    "$repo_root/scripts/stop-envoy.sh" "$envoy_pid" 10 || true
    wait "$envoy_pid" 2>/dev/null || true
  fi
  if [[ -n "$puma_pid" ]] && kill -0 "$puma_pid" 2>/dev/null; then
    kill -TERM "$puma_pid" 2>/dev/null || true
    wait "$puma_pid" 2>/dev/null || true
  fi
  rm -rf -- "$temporary_dir"
}
trap cleanup EXIT INT TERM

fail() {
  local message="$1"
  echo "FAIL: $message" >&2
  if [[ -f "$envoy_log" ]]; then
    tail -80 "$envoy_log" >&2
  fi
  if [[ -f "$puma_log" ]]; then
    tail -80 "$puma_log" >&2
  fi
  exit 1
}

wait_for_url() {
  local url="$1"
  local pid="$2"
  for _ in $(seq 1 150); do
    if curl --http1.1 --silent --fail --output /dev/null "$url"; then
      return 0
    fi
    kill -0 "$pid" 2>/dev/null || return 1
    sleep 0.1
  done
  return 1
}

print_response() {
  local url="$1"
  local prefix="$temporary_dir/readiness"
  local status

  status="$(
    curl --http1.1 \
      --silent \
      --show-error \
      --dump-header "$prefix.headers" \
      --output "$prefix.body" \
      --write-out '%{http_code}' \
      "$url" || true
  )"
  echo "Readiness response: HTTP $status" >&2
  [[ -f "$prefix.headers" ]] && sed -n '1,30p' "$prefix.headers" >&2
  [[ -f "$prefix.body" ]] && sed -n '1,80p' "$prefix.body" >&2
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

request() {
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

assert_server() {
  local name="$1"
  local base_url="$2"
  local expected_port="$3"
  local prefix="$temporary_dir/$name"

  local env_status
  env_status="$(
    request "$prefix-env" \
      --header 'x-ruvoy-test: shared-rack-app' \
      "$base_url/rack-env?probe=yes"
  )"
  [[ "$env_status" == "200" ]] || fail "$name Rack env returned HTTP $env_status"
  grep -Fxq 'REQUEST_METHOD="GET"' "$prefix-env.body" ||
    fail "$name did not provide REQUEST_METHOD"
  grep -Fxq 'PATH_INFO="/rack-env"' "$prefix-env.body" ||
    fail "$name did not provide PATH_INFO"
  grep -Fxq 'QUERY_STRING="probe=yes"' "$prefix-env.body" ||
    fail "$name did not provide QUERY_STRING"
  grep -Fxq "SERVER_PORT=\"$expected_port\"" "$prefix-env.body" ||
    fail "$name did not provide the listener port"
  grep -Fxq 'SERVER_PROTOCOL="HTTP/1.1"' "$prefix-env.body" ||
    fail "$name did not provide HTTP/1.1"
  grep -Fxq 'rack.url_scheme="http"' "$prefix-env.body" ||
    fail "$name did not provide rack.url_scheme"
  grep -Fxq 'HTTP_X_RUVOY_TEST="shared-rack-app"' "$prefix-env.body" ||
    fail "$name did not copy request headers"
  [[ "$(header_value "$prefix-env.headers" "x-rack-middleware")" == "active" ]] ||
    fail "$name did not execute Rack middleware"

  local echo_status
  echo_status="$(
    request "$prefix-echo" \
      --request POST \
      --data-binary 'rack-input' \
      "$base_url/echo"
  )"
  [[ "$echo_status" == "200" ]] || fail "$name echo returned HTTP $echo_status"
  [[ "$(<"$prefix-echo.body")" == "rack-input" ]] ||
    fail "$name did not preserve rack.input"

  local binary_input="$prefix-binary-input"
  printf '\000\377rack\200' >"$binary_input"
  local binary_status
  binary_status="$(
    request "$prefix-binary" \
      --request POST \
      --header 'content-type: application/octet-stream' \
      --data-binary "@$binary_input" \
      "$base_url/echo"
  )"
  [[ "$binary_status" == "200" ]] || fail "$name binary echo returned HTTP $binary_status"
  cmp -s "$binary_input" "$prefix-binary.body" ||
    fail "$name did not preserve binary request and response bytes"

  local enumerable_status
  enumerable_status="$(request "$prefix-enumerable" "$base_url/enumerable")"
  [[ "$enumerable_status" == "200" ]] ||
    fail "$name enumerable body returned HTTP $enumerable_status"
  [[ "$(<"$prefix-enumerable.body")" == "rack-body" ]] ||
    fail "$name did not consume every enumerable body chunk"
  [[ "$(grep -Eic '^set-cookie:' "$prefix-enumerable.headers")" == "2" ]] ||
    fail "$name did not preserve multi-value response headers"

  local closed_status
  closed_status="$(request "$prefix-closed" "$base_url/closed")"
  [[ "$closed_status" == "200" ]] || fail "$name close probe returned HTTP $closed_status"
  [[ "$(<"$prefix-closed.body")" == "1" ]] ||
    fail "$name did not close the enumerable response body exactly once"
}

for command in cargo cmp curl sed uvx; do
  command -v "$command" >/dev/null || fail "$command is required"
done
[[ -x "$ruby_bin" ]] || fail "Ruby is not executable: $ruby_bin"
[[ -x "$bundle_bin" ]] || fail "Bundler is not executable: $bundle_bin"
[[ "$("$ruby_bin" -e 'print RUBY_VERSION')" == "$ruby_version" ]] ||
  fail "expected Ruby $ruby_version from $ruby_bin"

for port in "$fiber_port" "$control_port" "$puma_port"; do
  if command -v lsof >/dev/null && lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "TCP port $port is already in use"
  fi
done

sed \
  "s|value: bench/config.ru|value: $rackup|" \
  "$repo_root/config/envoy-fiber-rack.yaml" >"$envoy_config"
grep -Fq "value: $rackup" "$envoy_config" ||
  fail "failed to configure the Rack fixture"

"$repo_root/scripts/build-fiber-module.sh"

(
  cd "$repo_root"
  exec env \
    BUNDLE_GEMFILE="$repo_root/Gemfile" \
    BUNDLE_PATH="$bundle_path" \
    BUNDLE_FROZEN=true \
    ENVOY_DYNAMIC_MODULES_SEARCH_PATH="$repo_root/build/modules" \
    uvx --from envoy-server==1.39.0 envoy \
    --config-path "$envoy_config" \
    --concurrency 1 \
    --disable-hot-restart \
    --log-level warning
) >"$envoy_log" 2>&1 &
envoy_pid=$!

(
  cd "$repo_root"
  exec env \
    PATH="$(dirname "$ruby_bin"):$PATH" \
    BUNDLE_GEMFILE="$repo_root/Gemfile" \
    BUNDLE_PATH="$bundle_path" \
    "$ruby_bin" "$bundle_bin" _4.0.10_ exec puma \
    --no-config \
    --environment test \
    --threads 0:1 \
    --workers 0 \
    --bind "tcp://127.0.0.1:$puma_port" \
    "$rackup"
) >"$puma_log" 2>&1 &
puma_pid=$!

if ! wait_for_url "http://127.0.0.1:$fiber_port/closed" "$envoy_pid"; then
  print_response "http://127.0.0.1:$fiber_port/closed"
  fail "Ruvoy did not become ready"
fi
wait_for_url "http://127.0.0.1:$puma_port/closed" "$puma_pid" ||
  fail "Puma did not become ready"

assert_server ruvoy "http://127.0.0.1:$fiber_port" "$fiber_port"
assert_server puma "http://127.0.0.1:$puma_port" "$puma_port"

# Fibers interleave app.call, so the Fiber runtime must not claim exclusivity.
grep -Fxq 'rack.multithread=true' "$temporary_dir/ruvoy-env.body" ||
  fail "Ruvoy did not report rack.multithread for the Fiber runtime"

echo "PASS: Ruvoy and Puma served the same Rack::Lint application"
