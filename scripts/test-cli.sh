#!/usr/bin/env bash

# Checks the command line that is the whole installed interface: `ruvoy app.ru`.
#
# Nothing here starts Envoy. What it covers is the part that decides what Envoy
# would be told — the generated configuration, where the module and the Ruby
# library are looked for, and what a user sees when one of them is missing.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-cli.XXXXXX")"
failures=0

cleanup() { rm -rf "$work_dir"; }
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

ruvoy() { ruby -I"$repo_root/lib" "$repo_root/exe/ruvoy" "$@"; }

status_of() {
  local expected="$1"
  shift
  local actual=0
  "$@" >/dev/null 2>&1 || actual=$?
  [[ "$actual" -eq "$expected" ]]
}

printf 'run ->[200, {}, []]\n' >"$work_dir/app.ru"

# One release ships the gem and the module together, so two version numbers
# that can drift are one too many.
cargo_version="$(awk '/^\[workspace.package\]/ { found = 1; next }
                      found && /^version = / { gsub(/[",]/, "", $3); print $3; exit }' \
                 "$repo_root/Cargo.toml")"
check 'the gem version matches the Cargo workspace version' \
  test "$(ruvoy --version)" = "$cargo_version"

check 'help exits successfully' status_of 0 ruvoy --help
check 'a missing rackup is refused' status_of 1 ruvoy "$work_dir/absent.ru"
check 'a missing rackup is named in the message' \
  bash -c 'ruby -I"$1/lib" "$1/exe/ruvoy" "$2/absent.ru" 2>&1 | grep -Fq "absent.ru"' _ "$repo_root" "$work_dir"

config="$work_dir/config.yaml"
ruvoy "$work_dir/app.ru" --print-config --port 9100 --address 0.0.0.0 --admin-port 9101 >"$config"

check 'the requested port reaches the listener' grep -Fq 'port_value: 9100' "$config"
check 'the requested address reaches the listener' grep -Fq 'address: 0.0.0.0' "$config"
check 'the admin interface is exposed when asked for' grep -Fq 'port_value: 9101' "$config"
check 'the rackup path is absolute, since Envoy resolves it from its own cwd' \
  bash -c 'ruby -ryaml -e "
    filter = YAML.load_file(ARGV.fetch(0))
      .dig(\"static_resources\", \"listeners\", 0, \"filter_chains\", 0, \"filters\", 0,
           \"typed_config\", \"http_filters\", 0, \"typed_config\", \"filter_config\", \"value\")
    exit filter == File.expand_path(ARGV.fetch(1)) ? 0 : 1" "$1" "$2"' _ "$config" "$work_dir/app.ru"
check 'the Rack filter terminates the chain' grep -Fq 'terminal_filter: true' "$config"
check 'the stream timeout is set, since a stuck fiber writes nothing' \
  grep -Fq 'stream_idle_timeout: 15s' "$config"

# Omitting the admin interface has to mean omitting it: an admin port bound by
# default would expose Envoy's control surface to whoever can reach the host.
ruvoy "$work_dir/app.ru" --print-config >"$work_dir/plain.yaml"
check 'no admin interface is configured unless asked for' \
  bash -c '! grep -Fq "admin:" "$1"' _ "$work_dir/plain.yaml"

check 'a module directory without the module is refused by name' \
  bash -c 'RUVOY_MODULE_DIR="$2" ruby -I"$1/lib" -e "
    require \"ruvoy/paths\"
    begin
      Ruvoy::Paths.module_directory
      exit 1
    rescue Ruvoy::Paths::NotFound => error
      exit error.message.include?(\"RUVOY_MODULE_DIR\") ? 0 : 1
    end"' _ "$repo_root" "$work_dir"

check 'the Ruby library directory is reported, since Envoy cannot infer it' \
  bash -c 'ruby -I"$1/lib" -e "
    require \"ruvoy/paths\"
    exit File.directory?(Ruvoy::Paths.ruby_library_directory) ? 0 : 1"' _ "$repo_root"

check 'the gemspec loads and installs the executable' \
  bash -c 'cd "$1" && ruby -e "
    spec = Gem::Specification.load(\"ruvoy.gemspec\")
    exit spec.executables == [\"ruvoy\"] && spec.files.include?(\"exe/ruvoy\") ? 0 : 1"' _ "$repo_root"

printf '\n%s assertions failed\n' "$failures"
[[ "$failures" -eq 0 ]]
