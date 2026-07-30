#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_dir="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
profile="${RUVOY_BUILD_PROFILE:-release}"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_file="$result_dir/poc3-fiber-runtime-$profile-$run_id.log"
ruby_bin="${RUVOY_RUBY:-"$HOME/.rbenv/versions/4.0.5/bin/ruby"}"

case "$profile" in
  debug | release)
    ;;
  *)
    echo "RUVOY_BUILD_PROFILE must be debug or release" >&2
    exit 1
    ;;
esac

mkdir -p "$result_dir"

run_args=(run --package ruvoy --example fiber_runtime)
if [[ "$profile" == "release" ]]; then
  run_args+=(--release)
fi

if ! BUNDLE_GEMFILE="$repo_root/Gemfile" \
  BUNDLE_PATH="$repo_root/vendor/bundle" \
  BUNDLE_FROZEN=true \
  RUBY="$ruby_bin" \
  cargo "${run_args[@]}" >"$result_file" 2>&1; then
  echo "FAIL: independent Fiber runtime" >&2
  cat "$result_file" >&2
  echo "raw result: $result_file" >&2
  exit 1
fi

rg -q '^result=PASS$' "$result_file"
rg -q '^async_version=2.39.0$' "$result_file"
rg -q '^scheduler_aware_unique_fibers=10$' "$result_file"
rg -q '^scheduler_io_requests=10$' "$result_file"
rg -q '^scheduler_io_unique_fibers=10$' "$result_file"
rg -q '^shutdown=PASS$' "$result_file"

echo "PASS: independent Fiber runtime ($profile)"
echo "raw result: $result_file"
