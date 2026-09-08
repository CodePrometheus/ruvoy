#!/usr/bin/env bash

# Builds the release gems.
#
#   ruvoy        the module and the command line, one build per Ruby ABI
#   ruvoy-envoy  the Envoy binary, versioned by the Envoy release it carries
#
# They are separate because their reasons to change are separate: a security
# fix in Envoy should reach users through `gem update ruvoy-envoy`, without
# waiting for a release of ruvoy. Installing the second one is optional.
#
# Pass a target to build only one: `build-gem.sh ruvoy`.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target="${1:-all}"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-gem.XXXXXX")"

cleanup() { rm -rf "$work_dir"; }
trap cleanup EXIT

fail() {
  printf 'build-gem: %s\n' "$1" >&2
  exit 1
}

[[ "$(uname -s)" == "Linux" ]] || fail "the release gems are built on Linux"

cd "$repo_root"
envoy_version="$(tr -d '[:space:]' <.envoy-version)"
[[ -n "$envoy_version" ]] || fail "no version in .envoy-version"

case "$(uname -m)" in
  x86_64)
    envoy_asset="envoy-$envoy_version-linux-x86_64"
    gem_platform="x86_64-linux"
    ;;
  aarch64 | arm64)
    envoy_asset="envoy-$envoy_version-linux-aarch_64"
    gem_platform="aarch64-linux"
    ;;
  *)
    fail "no Envoy release for $(uname -m)"
    ;;
esac

# Reads the file list out of a built gem, so a missing payload fails the build
# rather than reaching a user as a gem that installs and cannot run.
gem_contents() {
  tar -xOf "$1" data.tar.gz | tar -tzf -
}

require_in_gem() {
  local gem_file="$1"
  shift
  local contents
  contents="$(gem_contents "$gem_file")"
  local required
  for required in "$@"; do
    grep -Fqx "$required" <<<"$contents" || fail "$gem_file is missing $required"
  done
}

build_module_gem() {
  local ruby_abi
  ruby_abi="$(ruby -e 'print RbConfig::CONFIG["ruby_version"]')"
  [[ -n "$ruby_abi" ]] || fail "could not read the Ruby ABI version"

  printf 'Building the module for Ruby %s on %s\n' "$ruby_abi" "$gem_platform"
  cargo build --release -p ruvoy-envoy-fiber
  install -D -m 644 \
    "target/release/libruvoy_fiber.so" \
    "lib/ruvoy/$ruby_abi/libruvoy_fiber.so"

  # Envoy searches DT_RUNPATH only after LD_LIBRARY_PATH, which the command
  # line sets. DT_RPATH is searched first, so a stale build-machine path would
  # outrank the Ruby the gem is installed against.
  if command -v readelf >/dev/null; then
    readelf -d "lib/ruvoy/$ruby_abi/libruvoy_fiber.so" | grep -q 'RPATH' &&
      fail "the module carries DT_RPATH, which would outrank the installed Ruby"
  fi

  RUVOY_GEM_PLATFORM="$gem_platform" gem build ruvoy.gemspec
  local version gem_file
  version="$(ruby -Ilib -e 'require "ruvoy/version"; print Ruvoy::VERSION')"
  gem_file="ruvoy-$version-$gem_platform.gem"
  [[ -f "$gem_file" ]] || fail "expected $gem_file"
  require_in_gem "$gem_file" "lib/ruvoy/$ruby_abi/libruvoy_fiber.so" "exe/ruvoy"
  # Envoy belongs to the other gem; shipping it here would put a 100 MB
  # download in front of everyone who already has one.
  gem_contents "$gem_file" | grep -Fqx "exe/envoy" &&
    fail "$gem_file carries Envoy, which belongs to ruvoy-envoy"
  printf 'Built %s\n\n' "$gem_file"
}

build_envoy_gem() {
  printf 'Fetching %s\n' "$envoy_asset"
  curl -fsSL -o "$work_dir/envoy" \
    "https://github.com/envoyproxy/envoy/releases/download/v$envoy_version/$envoy_asset"
  curl -fsSL -o "$work_dir/checksums" \
    "https://github.com/envoyproxy/envoy/releases/download/v$envoy_version/checksums.txt.asc"

  local expected actual
  expected="$(awk -v asset="$envoy_asset" '$2 ~ ("/" asset "$") { print $1; exit }' "$work_dir/checksums")"
  [[ -n "$expected" ]] || fail "no checksum listed for $envoy_asset"
  actual="$(sha256sum "$work_dir/envoy" | cut -d' ' -f1)"
  [[ "$expected" == "$actual" ]] || fail "checksum mismatch for $envoy_asset"
  printf 'Checksum verified: %s\n' "$expected"

  install -D -m 755 "$work_dir/envoy" "exe/envoy"
  # Apache-2.0 carries the upstream attribution along with the binary.
  curl -fsSL -o "NOTICE" \
    "https://raw.githubusercontent.com/envoyproxy/envoy/v$envoy_version/NOTICE"

  RUVOY_GEM_PLATFORM="$gem_platform" gem build ruvoy-envoy.gemspec
  local gem_file="ruvoy-envoy-$envoy_version-$gem_platform.gem"
  [[ -f "$gem_file" ]] || fail "expected $gem_file"
  require_in_gem "$gem_file" "exe/envoy" "lib/ruvoy/envoy.rb" "NOTICE"
  printf 'Built %s\n\n' "$gem_file"
}

case "$target" in
  all)
    build_module_gem
    build_envoy_gem
    ;;
  ruvoy) build_module_gem ;;
  ruvoy-envoy) build_envoy_gem ;;
  *) fail "unknown target: $target (expected ruvoy, ruvoy-envoy or all)" ;;
esac
