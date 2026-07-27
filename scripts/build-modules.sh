#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
module_dir="$repo_root/build/modules"
profile="${RUVOY_BUILD_PROFILE:-release}"

case "$profile" in
  debug)
    ;;
  release)
    ;;
  *)
    echo "RUVOY_BUILD_PROFILE must be debug or release" >&2
    exit 1
    ;;
esac

cd "$repo_root"
if [[ "$profile" == "release" ]]; then
  cargo build --release --package ruvoy-envoy-baseline
else
  cargo build --package ruvoy-envoy-baseline
fi

mkdir -p "$module_dir"

case "$(uname -s)" in
  Darwin)
    source_module="$repo_root/target/$profile/libruvoy_baseline.dylib"
    target_module="$module_dir/libruvoy_baseline.so"
    cp "$source_module" "$target_module"
    file "$target_module"
    otool -L "$target_module"
    nm -gU "$target_module" | grep 'envoy_dynamic_module_on_program_init'
    ;;
  Linux)
    source_module="$repo_root/target/$profile/libruvoy_baseline.so"
    target_module="$module_dir/libruvoy_baseline.so"
    cp "$source_module" "$target_module"
    file "$target_module"
    ldd "$target_module"
    nm -D --defined-only "$target_module" | grep 'envoy_dynamic_module_on_program_init'
    ;;
  *)
    echo "unsupported operating system: $(uname -s)" >&2
    exit 1
    ;;
esac

echo "built module: $target_module"
