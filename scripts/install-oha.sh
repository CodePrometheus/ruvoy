#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="1.15.0"
platform=""
checksum=""

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)
    platform="macos-arm64"
    checksum="70d7cb7c15ed3d5eb4b7d9a7e76f0a8ee32ba1f18f560acef3b28e8670b89bb0"
    ;;
  Darwin-x86_64)
    platform="macos-amd64"
    checksum="fc8ccb4126737aae85cc9fbc6f95b161bf8bbb676bf02d4bb6196ec02c709c36"
    ;;
  Linux-aarch64 | Linux-arm64)
    platform="linux-arm64"
    checksum="72d5bf4575cede9f9277f93f097b904f893b0f0cd4d92f0869439b05e1403731"
    ;;
  Linux-x86_64)
    platform="linux-amd64"
    checksum="86ab7fa2c1df23b3bbc53b73561ffa44a7a38ca08f0e10351df9522a5c4c3c61"
    ;;
  *)
    echo "unsupported oha platform: $(uname -s)-$(uname -m)" >&2
    exit 1
    ;;
esac

target_dir="$repo_root/.tools/oha"
target="$target_dir/oha"
temporary="$(mktemp "${TMPDIR:-/tmp}/ruvoy-oha.XXXXXX")"
trap 'rm -f -- "$temporary"' EXIT INT TERM

mkdir -p "$target_dir"
curl \
  --fail \
  --show-error \
  --location \
  --output "$temporary" \
  "https://github.com/hatoo/oha/releases/download/v$version/oha-$platform"

if command -v shasum >/dev/null; then
  printf '%s  %s\n' "$checksum" "$temporary" | shasum -a 256 -c -
elif command -v sha256sum >/dev/null; then
  printf '%s  %s\n' "$checksum" "$temporary" | sha256sum -c -
else
  echo "shasum or sha256sum is required" >&2
  exit 1
fi

chmod 755 "$temporary"
mv "$temporary" "$target"
trap - EXIT INT TERM
"$target" --version
