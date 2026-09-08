#!/usr/bin/env bash

# Checks the summariser against a synthetic campaign whose answers are known in
# advance.
#
# This program turns raw measurements into the numbers the project publishes,
# so a silent mistake here is a mistake in every claim made from them. The
# fixture states each expected value directly rather than recomputing it the
# way the summariser does, which is the only way the assertion can disagree.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruvoy-summarize.XXXXXX")"
result_dir="$work_dir/campaign"
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

contains() { grep -Fq "$2" "$1"; }

# One oha document per measurement. Round-to-round variation stays tiny so the
# run is never rejected as unstable, which would mask what is under test.
write_measurement() {
  local path="$1" rps="$2" requests="$3"
  mkdir -p "$(dirname "$path")"
  cat >"$path" <<JSON
{
  "summary": { "successRate": 1.0, "requestsPerSec": $rps, "total": 30.0 },
  "errorDistribution": {},
  "statusCodeDistribution": { "200": $requests },
  "latencyPercentiles": { "p50": 0.001, "p95": 0.002, "p99": 0.003 }
}
JSON
  printf 'sample,cpu_percent,rss_kib\n0,50.0,102400\n' >"${path%.json}.resources.csv"
}

mkdir -p "$result_dir"
printf 'mode=full\narchitectures=fiber,falcon_direct\narchitecture_order_mode=rotate\n' \
  >"$result_dir/preflight.log"
printf 'run\tarchitecture\tscenario\ttls_mode\tround\tprotocol\tstatus\tdetail\n' \
  >"$result_dir/campaign.tsv"
printf 'scenario\tround\tarchitecture\tprotocol\treason\n' >"$result_dir/skipped.tsv"
printf 'sequence\tarchitecture\tscenario\ttls_mode\tround\tprotocol\tconcurrency\twait_ms\trequest_bytes\tresponse_bytes\tapp_params\tserved_requests\toha_json\tresources_csv\n' \
  >"$result_dir/control-manifest.tsv"
cat >"$result_dir/bridge-run1.json" <<'JSON'
{ "pure_rust": { "p50_us": 0.16, "p99_us": 0.20 },
  "ruby_bridge": { "p50_us": 23.7, "p99_us": 40.0 } }
JSON

manifest="$result_dir/manifest.tsv"
printf 'sequence\tarchitecture\tscenario\ttls_mode\tround\tprotocol\tconcurrency\twait_ms\trequest_bytes\tresponse_bytes\tapp_params\tserved_requests\toha_json\tresources_csv\n' \
  >"$manifest"

sequence=0
add_rows() {
  local architecture="$1" scenario="$2" wait_ms="$3" app_params="$4" rps="$5"
  local round
  for round in 1 2 3; do
    sequence=$((sequence + 1))
    local relative="$architecture/${scenario}-round${round}.json"
    write_measurement "$result_dir/$relative" "$rps" "$(printf '%.0f' "$(echo "$rps * 30" | bc)")"
    printf '%s\t%s\t%s\tplain\t%s\t1.1\t100\t%s\t0\t0\t%s\t3000\t%s\t%s\n' \
      "$sequence" "$architecture" "$scenario" "$round" "$wait_ms" "$app_params" \
      "$relative" "${relative%.json}.resources.csv" >>"$manifest"
  done
}

# 50 ms of latency, once yielding and once not. A fiber runtime keeps almost
# nothing when the call does not yield; a thread pool is barely affected.
add_rows fiber wait50_h1_c100 50 '' 1900
add_rows fiber block50_h1_c100 0 'block_ms=50' 19
add_rows falcon_direct wait50_h1_c100 50 '' 1850
add_rows falcon_direct block50_h1_c100 0 'block_ms=50' 20
# CPU-bound: both architectures are held by the same interpreter lock, so the
# gap the no-op scenario shows is expected to disappear here.
add_rows fiber cpu25k_h1_c100 0 'cpu_iterations=25000' 420
add_rows falcon_direct cpu25k_h1_c100 0 'cpu_iterations=25000' 415

ruby "$repo_root/bench/summarize.rb" "$result_dir" >/dev/null

summary_json="$result_dir/summary.json"
summary_md="$result_dir/summary.md"

check 'summary.json is written' test -s "$summary_json"
check 'the scenario parameters survive into the summary' \
  contains "$summary_json" '"app_params": "block_ms=50"'
check 'yield sensitivity is reported for the fiber runtime' \
  contains "$summary_json" '"block50_rps_median": 19.0'

# 19 / 1900 = 1%. Stated here rather than recomputed, so a summariser that
# divides the wrong way round fails instead of agreeing with itself.
retained="$(ruby -rjson -e '
  summary = JSON.parse(File.read(ARGV.fetch(0)))
  print format("%.4f", summary.fetch("yield_sensitivity").fetch("fiber").fetch("retained_fraction"))
' "$summary_json")"
check 'a fiber runtime keeps 1% of its throughput when the call does not yield' \
  test "$retained" = "0.0100"

check 'the report explains what the comparison isolates' \
  contains "$summary_md" 'native call that does not yield'
check 'the CPU-bound comparison reaches the report' \
  contains "$summary_md" 'CPU-bound work'
check 'the report carries no untranslated text' \
  ruby -e 'exit File.read(ARGV.fetch(0)).scan(/\p{Han}/).empty?' "$summary_md"

printf '\n%s assertions failed\n' "$failures"
[[ "$failures" -eq 0 ]]
