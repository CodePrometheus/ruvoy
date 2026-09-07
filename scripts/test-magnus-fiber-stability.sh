#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id="$(date -u '+%Y%m%dT%H%M%SZ')"
result_root="${RUVOY_RESULTS_DIR:-"$repo_root/.agents/results"}"
result_dir="$result_root/poc5-magnus-fiber-stability-$run_id"
preflight_log="$result_dir/preflight.log"
probe_log="$result_dir/probe.log"
rss_file="$result_dir/rss.tsv"
summary_file="$result_dir/summary.log"
diagnostic_dir="$HOME/Library/Logs/DiagnosticReports"
before_crashes="$result_dir/crashes-before.txt"
after_crashes="$result_dir/crashes-after.txt"
new_crashes="$result_dir/new-crashes.txt"
probe_binary="$repo_root/target/release/examples/fiber_stability_probe"

waves="${RUVOY_PROBE_WAVES:-3}"
requests_per_wave="${RUVOY_PROBE_REQUESTS_PER_WAVE:-1000}"
concurrency="${RUVOY_PROBE_CONCURRENCY:-100}"
body_bytes="${RUVOY_PROBE_BODY_BYTES:-0}"
host_threads="${RUVOY_PROBE_HOST_THREADS:-4}"

mkdir -p "$result_dir"
printf 'timestamp\tpid\trss_kib\n' >"$rss_file"

fail() {
  printf 'result=FAIL\nreason=%s\nraw_results=%s\n' "$*" "$result_dir" | tee -a "$summary_file" >&2
  exit 1
}

for tool in awk cargo comm find ps ruby sort; do
  command -v "$tool" >/dev/null || fail "$tool is required"
done
for value_name in waves requests_per_wave concurrency body_bytes host_threads; do
  value="${!value_name}"
  [[ "$value" =~ ^[0-9]+$ ]] || fail "$value_name must be a non-negative integer"
done
[[ "$waves" -gt 0 ]] || fail "waves must be positive"
[[ "$requests_per_wave" -gt 0 ]] || fail "requests_per_wave must be positive"
[[ "$concurrency" -gt 0 ]] || fail "concurrency must be positive"
[[ "$concurrency" -le 1024 ]] || fail "concurrency exceeds runtime limit 1024"
[[ "$body_bytes" -le 2097152 ]] || fail "body_bytes exceeds the 2 MiB PoC limit"

if [[ -d "$diagnostic_dir" ]]; then
  find "$diagnostic_dir" -maxdepth 1 -type f \
    -name 'fiber_stability_probe*.ips' -print | sort >"$before_crashes"
else
  : >"$before_crashes"
fi

{
  printf 'run_id=%s\n' "$run_id"
  printf 'ruby=%s\n' "$(ruby --version)"
  printf 'waves=%s\nrequests_per_wave=%s\nconcurrency=%s\n' \
    "$waves" "$requests_per_wave" "$concurrency"
  printf 'body_bytes=%s\nhost_threads=%s\n' "$body_bytes" "$host_threads"
  cargo tree -p ruvoy | grep -E 'magnus|rb-sys'
  cargo build --release --example fiber_stability_probe
} >"$preflight_log" 2>&1 || fail "probe release build failed"

BUNDLE_GEMFILE="$repo_root/Gemfile" \
  BUNDLE_PATH="$repo_root/vendor/bundle" \
  BUNDLE_FROZEN=true \
  RUVOY_PROBE_WAVES="$waves" \
  RUVOY_PROBE_REQUESTS_PER_WAVE="$requests_per_wave" \
  RUVOY_PROBE_CONCURRENCY="$concurrency" \
  RUVOY_PROBE_BODY_BYTES="$body_bytes" \
  RUVOY_PROBE_HOST_THREADS="$host_threads" \
  "$probe_binary" >"$probe_log" 2>&1 &
probe_pid=$!

(
  while kill -0 "$probe_pid" 2>/dev/null; do
    rss_kib="$(ps -o rss= -p "$probe_pid" 2>/dev/null | tr -d '[:space:]')"
    if [[ "$rss_kib" =~ ^[0-9]+$ ]]; then
      printf '%s\t%s\t%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$probe_pid" "$rss_kib"
    fi
    sleep 0.2
  done
) >>"$rss_file" &
monitor_pid=$!

set +e
wait "$probe_pid"
probe_status=$?
set -e
wait "$monitor_pid" 2>/dev/null || true

if [[ -d "$diagnostic_dir" ]]; then
  find "$diagnostic_dir" -maxdepth 1 -type f \
    -name 'fiber_stability_probe*.ips' -print | sort >"$after_crashes"
else
  : >"$after_crashes"
fi
comm -13 "$before_crashes" "$after_crashes" >"$new_crashes"

max_rss_kib="$(awk -F '\t' 'NR > 1 && $3 > max { max = $3 } END { print max + 0 }' "$rss_file")"
{
  printf 'probe_exit_status=%s\n' "$probe_status"
  printf 'max_rss_kib=%s\n' "$max_rss_kib"
  printf 'new_crash_reports=%s\n' "$(wc -l <"$new_crashes" | tr -d '[:space:]')"
  printf 'raw_results=%s\n' "$result_dir"
} >"$summary_file"

[[ "$probe_status" -eq 0 ]] || fail "probe exited with status $probe_status"
grep -Fq 'result=PASS' "$probe_log" || fail "probe did not report PASS"
[[ ! -s "$new_crashes" ]] || fail "probe produced a new crash report"

printf 'result=PASS\n' >>"$summary_file"
cat "$summary_file"
