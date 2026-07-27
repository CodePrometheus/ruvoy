#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
worker_sources=(
  "$repo_root/crates/ruvoy-envoy-sync/src/worker.rs"
  "$repo_root/crates/ruvoy-envoy-fiber/src/worker.rs"
)

forbidden='magnus|RubyRuntime|Ruby::|BoxValue|RArray|RHash|Value|funcall|eval|call_app'

if rg -n "$forbidden" "${worker_sources[@]}"; then
  echo "FAIL: Envoy worker source contains a Ruby VM reference" >&2
  exit 1
fi

for worker_source in "${worker_sources[@]}"; do
  rg -q 'RuntimeClient' "$worker_source"
  rg -q 'Request' "$worker_source"
  rg -q 'scheduler\.commit' "$worker_source"
done

echo "PASS: sync and Fiber worker sources use only owned bridge types and Envoy scheduler"
