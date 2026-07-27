#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
module_dir="$repo_root/build/modules"
profile="${RUVOY_BUILD_PROFILE:-release}"
ruby_version="$(tr -d '[:space:]' <"$repo_root/.ruby-version")"
ruby_bin="${RUVOY_RUBY:-}"

case "$profile" in
  debug | release)
    ;;
  *)
    echo "RUVOY_BUILD_PROFILE must be debug or release" >&2
    exit 1
    ;;
esac

if [[ -z "$ruby_bin" ]]; then
  if command -v rbenv >/dev/null 2>&1; then
    ruby_bin="$(RBENV_VERSION="$ruby_version" rbenv which ruby)"
  elif [[ -x "$HOME/.rbenv/versions/$ruby_version/bin/ruby" ]]; then
    ruby_bin="$HOME/.rbenv/versions/$ruby_version/bin/ruby"
  else
    ruby_bin="$(command -v ruby)"
  fi
fi

actual_ruby_version="$("$ruby_bin" -e 'print RUBY_VERSION')"
if [[ "$actual_ruby_version" != "$ruby_version" ]]; then
  echo "expected Ruby $ruby_version, got $actual_ruby_version from $ruby_bin" >&2
  exit 1
fi

cd "$repo_root"
if [[ "$profile" == "release" ]]; then
  RUBY="$ruby_bin" cargo build --release --package ruvoy-envoy-sync
else
  RUBY="$ruby_bin" cargo build --package ruvoy-envoy-sync
fi

mkdir -p "$module_dir"

case "$(uname -s)" in
  Darwin)
    source_module="$repo_root/target/$profile/libruvoy_sync.dylib"
    target_module="$module_dir/libruvoy_sync.so"
    cp "$source_module" "$target_module"
    file "$target_module"
    otool -L "$target_module"
    otool -L "$target_module" | grep 'libruby.4.0'
    nm -gU "$target_module" | grep 'envoy_dynamic_module_on_program_init'
    ;;
  Linux)
    source_module="$repo_root/target/$profile/libruvoy_sync.so"
    target_module="$module_dir/libruvoy_sync.so"
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
