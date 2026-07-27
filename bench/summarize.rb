# frozen_string_literal: true

require "csv"
require "json"

result_dir = File.expand_path(ARGV.fetch(0))

def median(values)
  sorted = values.sort
  middle = sorted.length / 2
  return sorted.fetch(middle) if sorted.length.odd?

  (sorted.fetch(middle - 1) + sorted.fetch(middle)) / 2.0
end

def relative_standard_deviation(values)
  return 0.0 if values.length < 2

  mean = values.sum / values.length.to_f
  return 0.0 if mean.zero?

  variance = values.sum { |value| (value - mean)**2 } / values.length.to_f
  Math.sqrt(variance) / mean
end

def load_json(result_dir, relative_path)
  JSON.parse(File.read(File.join(result_dir, relative_path)))
end

def resource_metrics(result_dir, relative_path)
  rows = CSV.read(File.join(result_dir, relative_path), headers: true)
  return { "cpu_average" => 0.0, "rss_max_mib" => 0.0 } if rows.empty?

  cpu = rows.map { |row| Float(row.fetch("cpu_percent")) }
  rss = rows.map { |row| Integer(row.fetch("rss_kib")) }
  {
    "cpu_average" => cpu.sum / cpu.length,
    "rss_max_mib" => rss.max / 1024.0
  }
end

manifest = CSV.read(
  File.join(result_dir, "manifest.tsv"),
  headers: true,
  col_sep: "\t"
)
groups = manifest.group_by { |row| [row.fetch("architecture"), row.fetch("scenario")] }

summary_rows = groups.map do |(architecture, scenario), rows|
  documents = rows.map { |row| load_json(result_dir, row.fetch("oha_json")) }
  resources = rows.map { |row| resource_metrics(result_dir, row.fetch("resources_csv")) }
  rps = documents.map { |document| Float(document.dig("summary", "requestsPerSec")) }
  p50 = documents.map { |document| Float(document.dig("latencyPercentiles", "p50")) }
  p95 = documents.map { |document| Float(document.dig("latencyPercentiles", "p95")) }
  p99 = documents.map { |document| Float(document.dig("latencyPercentiles", "p99")) }
  errors = documents.sum do |document|
    document.fetch("errorDistribution").values.sum { |count| Integer(count) }
  end

  {
    "architecture" => architecture,
    "scenario" => scenario,
    "runs" => rows.length,
    "protocol" => rows.first.fetch("protocol"),
    "concurrency" => Integer(rows.first.fetch("concurrency")),
    "wait_ms" => Integer(rows.first.fetch("wait_ms")),
    "request_bytes" => Integer(rows.first.fetch("request_bytes")),
    "response_bytes" => Integer(rows.first.fetch("response_bytes")),
    "rps_median" => median(rps),
    "rps_rsd" => relative_standard_deviation(rps),
    "p50_median_seconds" => median(p50),
    "p95_median_seconds" => median(p95),
    "p99_median_seconds" => median(p99),
    "p99_rsd" => relative_standard_deviation(p99),
    "errors" => errors,
    "cpu_percent_median" => median(resources.map { |resource| resource.fetch("cpu_average") }),
    "rss_mib_median" => median(resources.map { |resource| resource.fetch("rss_max_mib") })
  }
end.sort_by { |row| [row.fetch("scenario"), row.fetch("architecture")] }

control_manifest = CSV.read(
  File.join(result_dir, "control-manifest.tsv"),
  headers: true,
  col_sep: "\t"
)
control_rows = control_manifest
  .group_by { |row| [row.fetch("architecture"), row.fetch("state")] }
  .map do |(architecture, state), rows|
    documents = rows.map { |row| load_json(result_dir, row.fetch("oha_json")) }
    {
      "architecture" => architecture,
      "state" => state,
      "runs" => rows.length,
      "rps_median" => median(
        documents.map { |document| Float(document.dig("summary", "requestsPerSec")) }
      ),
      "p99_median_seconds" => median(
        documents.map { |document| Float(document.dig("latencyPercentiles", "p99")) }
      )
    }
  end
  .sort_by { |row| [row.fetch("architecture"), row.fetch("state")] }

bridge_documents = Dir.glob(File.join(result_dir, "bridge-run*.json"))
  .sort
  .map { |path| JSON.parse(File.read(path)) }
bridge_summary = {
  "runs" => bridge_documents.length,
  "pure_rust_p50_us_median" => median(
    bridge_documents.map { |document| Float(document.dig("pure_rust", "p50_us")) }
  ),
  "pure_rust_p99_us_median" => median(
    bridge_documents.map { |document| Float(document.dig("pure_rust", "p99_us")) }
  ),
  "ruby_bridge_p50_us_median" => median(
    bridge_documents.map { |document| Float(document.dig("ruby_bridge", "p50_us")) }
  ),
  "ruby_bridge_p99_us_median" => median(
    bridge_documents.map { |document| Float(document.dig("ruby_bridge", "p99_us")) }
  )
}

def row_for(rows, architecture, scenario)
  rows.find do |row|
    row.fetch("architecture") == architecture && row.fetch("scenario") == scenario
  end
end

def performance_comparison(candidate, reference)
  rps_improvement = (candidate.fetch("rps_median") / reference.fetch("rps_median") - 1.0) * 100
  p99_improvement =
    (1.0 - candidate.fetch("p99_median_seconds") / reference.fetch("p99_median_seconds")) * 100
  rps_threshold = [
    5.0,
    candidate.fetch("rps_rsd") * 100,
    reference.fetch("rps_rsd") * 100
  ].max
  p99_threshold = [
    5.0,
    candidate.fetch("p99_rsd") * 100,
    reference.fetch("p99_rsd") * 100
  ].max

  {
    "rps_improvement_percent" => rps_improvement,
    "rps_noise_threshold_percent" => rps_threshold,
    "rps_stable_advantage" => rps_improvement > rps_threshold,
    "p99_improvement_percent" => p99_improvement,
    "p99_noise_threshold_percent" => p99_threshold,
    "p99_stable_advantage" => p99_improvement > p99_threshold,
    "has_stable_advantage" =>
      rps_improvement > rps_threshold || p99_improvement > p99_threshold
  }
end

mode_line = File.readlines(File.join(result_dir, "preflight.log"))
  .find { |line| line.start_with?("mode=") }
mode = mode_line&.split("=", 2)&.last&.strip || "unknown"
performance_decision = {}
if mode == "full"
  puma_primary = row_for(summary_rows, "puma", "noop_h1_c100")
  %w[sync fiber].each do |architecture|
    candidate = row_for(summary_rows, architecture, "noop_h1_c100")
    next unless candidate && puma_primary

    performance_decision[architecture] = performance_comparison(
      candidate,
      puma_primary
    )
  end
end

fiber_wait_c10 = row_for(summary_rows, "fiber", "wait200_h1_c10")
fiber_wait_c100 = row_for(summary_rows, "fiber", "wait200_h1_c100")
fiber_wait_scaling =
  if fiber_wait_c10 && fiber_wait_c100
    fiber_wait_c100.fetch("rps_median") / fiber_wait_c10.fetch("rps_median")
  end

summary = {
  "mode" => mode,
  "measurements" => summary_rows,
  "control_route" => control_rows,
  "standalone_bridge" => bridge_summary,
  "performance_decision" => performance_decision,
  "fiber_wait_c100_over_c10_rps_ratio" => fiber_wait_scaling
}
File.write(File.join(result_dir, "summary.json"), JSON.pretty_generate(summary) << "\n")

markdown = []
markdown << "# ruvoy 性能 PoC"
markdown << ""
markdown << "- 模式：`#{mode}`"
markdown << "- 原始目录：`#{result_dir}`"
if mode != "full"
  markdown << "- 这是 smoke 校准结果，不能用于最终性能结论。"
end
markdown << ""
markdown << "## 网络结果"
markdown << ""
markdown << "| architecture | scenario | runs | RPS median | RPS RSD | p50 ms | p95 ms | p99 ms | CPU % | RSS MiB |"
markdown << "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"
summary_rows.each do |row|
  markdown << format(
    "| %s | %s | %d | %.2f | %.2f%% | %.3f | %.3f | %.3f | %.1f | %.1f |",
    row.fetch("architecture"),
    row.fetch("scenario"),
    row.fetch("runs"),
    row.fetch("rps_median"),
    row.fetch("rps_rsd") * 100,
    row.fetch("p50_median_seconds") * 1_000,
    row.fetch("p95_median_seconds") * 1_000,
    row.fetch("p99_median_seconds") * 1_000,
    row.fetch("cpu_percent_median"),
    row.fetch("rss_mib_median")
  )
end

markdown << ""
markdown << "## Envoy control route"
markdown << ""
markdown << "| architecture | state | runs | RPS median | p99 ms |"
markdown << "|---|---|---:|---:|---:|"
control_rows.each do |row|
  markdown << format(
    "| %s | %s | %d | %.2f | %.3f |",
    row.fetch("architecture"),
    row.fetch("state"),
    row.fetch("runs"),
    row.fetch("rps_median"),
    row.fetch("p99_median_seconds") * 1_000
  )
end

markdown << ""
markdown << "## Standalone bridge"
markdown << ""
markdown << format(
  "- pure Rust owned call：p50 `%.3f µs`，p99 `%.3f µs`。",
  bridge_summary.fetch("pure_rust_p50_us_median"),
  bridge_summary.fetch("pure_rust_p99_us_median")
)
markdown << format(
  "- CRuby bridge call：p50 `%.3f µs`，p99 `%.3f µs`。",
  bridge_summary.fetch("ruby_bridge_p50_us_median"),
  bridge_summary.fetch("ruby_bridge_p99_us_median")
)
if fiber_wait_scaling
  markdown << format(
    "- Fiber 200 ms wait，c100/c10 吞吐比：`%.2fx`。",
    fiber_wait_scaling
  )
end

if mode == "full"
  markdown << ""
  markdown << "## 预注册性能判定"
  markdown << ""
  performance_decision.each do |architecture, decision|
    markdown << format(
      "- %s vs Envoy→Puma（H1 no-op c100）：RPS `%+.2f%%`（阈值 `%.2f%%`），p99 `%+.2f%%`（阈值 `%.2f%%`），稳定优势：`%s`。",
      architecture,
      decision.fetch("rps_improvement_percent"),
      decision.fetch("rps_noise_threshold_percent"),
      decision.fetch("p99_improvement_percent"),
      decision.fetch("p99_noise_threshold_percent"),
      decision.fetch("has_stable_advantage")
    )
  end
  if performance_decision.empty?
    markdown << "- 当前结果不含 H1 no-op c100 的完整候选/对照，预注册总判定不适用。"
  elsif !performance_decision.values.any? { |decision| decision.fetch("has_stable_advantage") }
    markdown << ""
    markdown << "**同进程设计未兑现性能价值。**"
  end
end

File.write(File.join(result_dir, "summary.md"), markdown.join("\n") << "\n")
