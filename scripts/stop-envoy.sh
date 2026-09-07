#!/usr/bin/env bash

# Stops an Envoy started through `uvx`, and the proxy it launched.
#
# `uvx` runs the proxy as a child rather than replacing itself with it. It
# forwards SIGINT, but nothing can forward SIGKILL: escalating on the launcher
# alone leaves the proxy running and holding its listening ports, so the next
# run fails on a port that is already in use and leaves a second one behind.

set -euo pipefail

launcher="${1:?usage: stop-envoy.sh <launcher-pid> [grace-seconds]}"
grace_seconds="${2:-10}"

kill -0 "$launcher" 2>/dev/null || exit 0

# Resolved first: once the launcher is gone its child is reparented and can no
# longer be found from it.
children="$(pgrep -P "$launcher" 2>/dev/null || true)"

kill -INT "$launcher" 2>/dev/null || true
deadline=$((SECONDS + grace_seconds))
while ((SECONDS < deadline)); do
  kill -0 "$launcher" 2>/dev/null || break
  sleep 0.05
done

for pid in $launcher $children; do
  kill -0 "$pid" 2>/dev/null || continue
  kill -KILL "$pid" 2>/dev/null || true
done

for pid in $launcher $children; do
  for _ in $(seq 1 100); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.05
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "stop-envoy: process $pid survived SIGKILL" >&2
    exit 1
  fi
done
