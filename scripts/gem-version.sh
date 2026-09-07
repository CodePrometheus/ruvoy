#!/usr/bin/env bash

# Prints the locked version of one gem.
#
# Assertions and recorded provenance read the version from here rather than
# repeating a literal, so an upgrade cannot leave them disagreeing.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gem="${1:?usage: gem-version.sh <gem>}"

version="$(
  awk -v gem="$gem" '$1 == gem && $2 ~ /^\(.*\)$/ { gsub(/[()]/, "", $2); print $2; exit }' \
    "$repo_root/Gemfile.lock"
)"
[[ -n "$version" ]] || {
  printf 'gem %s is not in Gemfile.lock\n' "$gem" >&2
  exit 1
}
printf '%s\n' "$version"
