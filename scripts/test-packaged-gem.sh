#!/usr/bin/env bash

# Installs the built gem the way a user would and serves requests through it.
#
# Running from a directory outside the checkout is the point: it is what
# separates a gem that carries everything it needs from one that only works
# where it was built.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gem_file="${1:-$(ls -t "$repo_root"/ruvoy-[0-9]*.gem 2>/dev/null | head -1)}"
envoy_gem="${2:-$(ls -t "$repo_root"/ruvoy-envoy-*.gem 2>/dev/null | head -1)}"
port="${RUVOY_TEST_PORT:-19260}"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-packaged.XXXXXX")"
server_pid=""
failures=0

cleanup() {
  [[ -n "$server_pid" ]] && kill "$server_pid" 2>/dev/null
  wait "$server_pid" 2>/dev/null
  rm -rf "$work_dir"
}
trap cleanup EXIT

check() {
  local description="$1"
  shift
  if "$@"; then
    printf 'PASS  %s\n' "$description"
  else
    printf 'FAIL  %s\n' "$description"
    failures=$((failures + 1))
  fi
}

[[ -f "$gem_file" ]] || { printf 'no gem to test\n' >&2; exit 1; }
[[ -f "$envoy_gem" ]] || { printf 'no ruvoy-envoy gem to test\n' >&2; exit 1; }
printf 'Testing %s with %s\n\n' "$(basename "$gem_file")" "$(basename "$envoy_gem")"

export GEM_HOME="$work_dir/gems"
export GEM_PATH="$GEM_HOME"
gem install --no-document --install-dir "$GEM_HOME" "$gem_file" "$envoy_gem" >/dev/null

application_dir="$work_dir/app"
mkdir -p "$application_dir"
cp "$repo_root/examples/hello/config.ru" "$application_dir/config.ru"

cd "$application_dir"
"$GEM_HOME/bin/ruvoy" config.ru --port "$port" >"$work_dir/server.log" 2>&1 &
server_pid=$!

ready=""
for _ in $(seq 1 60); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$port/" 2>/dev/null; then
    ready=yes
    break
  fi
  kill -0 "$server_pid" 2>/dev/null || break
  sleep 0.5
done

if [[ -z "$ready" ]]; then
  printf 'FAIL  the packaged gem never served a request\n'
  sed 's/^/      /' "$work_dir/server.log" | tail -20
  exit 1
fi

check 'a request is served from outside the checkout' \
  bash -c 'curl -fsS "http://127.0.0.1:'"$port"'/" | grep -q "hello from ruby"'
check 'the embedded Ruby is the one the gem was built for' \
  bash -c 'curl -fsS "http://127.0.0.1:'"$port"'/" | grep -q "'"$(ruby -e 'print RUBY_VERSION')"'"'
check 'a request body reaches the application' \
  bash -c 'curl -fsS -X POST --data-binary payload "http://127.0.0.1:'"$port"'/echo" | grep -q payload'
check 'an unknown path answers 404' \
  bash -c '[[ "$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:'"$port"'/nope")" == 404 ]]'

# The claim this project rests on: waiting requests overlap rather than queue.
started="$(date +%s)"
slow_pids=()
for _ in $(seq 1 8); do
  curl -fsS -o /dev/null "http://127.0.0.1:$port/slow" &
  slow_pids+=("$!")
done
# Named explicitly: a bare `wait` would also wait on the server, which never exits.
wait "${slow_pids[@]}"
elapsed=$(( $(date +%s) - started ))
check 'eight one-second requests overlap instead of queueing' test "$elapsed" -lt 4

# Envoy ships separately and installing it is optional, so the command line has
# to work against one the machine already has.
kill "$server_pid" 2>/dev/null
wait "$server_pid" 2>/dev/null
server_pid=""
alone="$work_dir/alone"
gem install --no-document --install-dir "$alone" "$gem_file" >/dev/null
check 'without the Envoy gem, the command line says how to supply one' \
  bash -c 'GEM_HOME="$1" GEM_PATH="$1" "$1/bin/ruvoy" config.ru --port '"$port"' 2>&1 |
           grep -q "install the ruvoy-envoy gem"' _ "$alone"
check 'an Envoy supplied by the machine is accepted' \
  bash -c 'GEM_HOME="$1" GEM_PATH="$1" RUVOY_ENVOY="$2" timeout 20 "$1/bin/ruvoy" config.ru \
             --port '"$port"' >/dev/null 2>&1 &
           for _ in $(seq 1 40); do
             curl -fsS -o /dev/null "http://127.0.0.1:'"$port"'/" && exit 0
             sleep 0.5
           done
           exit 1' _ "$alone" "$GEM_HOME/gems/$(basename "${envoy_gem%.gem}")/exe/envoy"
pkill -f "ruvoy.*--port $port" 2>/dev/null || true

printf '\n%s assertions failed\n' "$failures"
[[ "$failures" -eq 0 ]]
