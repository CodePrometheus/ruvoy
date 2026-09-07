#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
worker_sources=(
  "$repo_root/crates/ruvoy-envoy-sync/src/worker.rs"
  "$repo_root/crates/ruvoy-envoy-fiber/src/worker.rs"
)

forbidden='magnus|RubyRuntime|Ruby::|BoxValue|RArray|RHash|Value|funcall|eval|call_app'

if grep -nE "$forbidden" "${worker_sources[@]}"; then
  echo "FAIL: Envoy worker source contains a Ruby VM reference" >&2
  exit 1
fi

for worker_source in "${worker_sources[@]}"; do
  grep -qE 'RuntimeClient' "$worker_source"
  grep -qE 'Request' "$worker_source"
  grep -qE 'scheduler\.commit' "$worker_source"
done

echo "PASS: sync and Fiber worker sources use only owned bridge types and Envoy scheduler"
