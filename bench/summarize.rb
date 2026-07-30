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

def range_percent(values)
  middle = median(values)
  return 0.0 if middle.zero?

  (values.max - values.min) / middle
end

def load_json(result_dir, relative_path)
  JSON.parse(File.read(File.join(result_dir, relative_path)))
end

def validate_measurement!(document, relative_path)
  summary = document.fetch("summary")
  errors = document.fetch("errorDistribution")
  statuses = document.fetch("statusCodeDistribution")

  raise "#{relative_path}: success rate is not 100%" unless Float(summary.fetch("successRate")) == 1.0
  raise "#{relative_path}: requestsPerSec is not positive" unless Float(summary.fetch("requestsPerSec")) > 0
  raise "#{relative_path}: transport errors are present" unless errors.empty?
  raise "#{relative_path}: non-200 status is present" unless statuses.keys == ["200"]

  requests = Integer(statuses.fetch("200"))
  raise "#{relative_path}: no successful requests" unless requests.positive?

  requests
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

def preflight_values(result_dir)
  File.readlines(File.join(result_dir, "preflight.log"), chomp: true).each_with_object({}) do |line, values|
    key, value = line.split("=", 2)
    values[key] = value if value
  end
end

manifest = CSV.read(
  File.join(result_dir, "manifest.tsv"),
  headers: true,
  col_sep: "\t"
)
groups = manifest.group_by do |row|
  [row.fetch("architecture"), row.fetch("scenario"), row.fetch("tls_mode")]
end

summary_rows = groups.map do |(architecture, scenario, tls_mode), rows|
  documents = rows.map { |row| load_json(result_dir, row.fetch("oha_json")) }
  request_counts = documents.zip(rows).map do |document, row|
    validate_measurement!(document, row.fetch("oha_json"))
  end
  resources = rows.map { |row| resource_metrics(result_dir, row.fetch("resources_csv")) }
  rps = documents.map { |document| Float(document.dig("summary", "requestsPerSec")) }
  elapsed = documents.map { |document| Float(document.dig("summary", "total")) }
  p50 = documents.map { |document| Float(document.dig("latencyPercentiles", "p50")) }
  p95 = documents.map { |document| Float(document.dig("latencyPercentiles", "p95")) }
  p99 = documents.map { |document| Float(document.dig("latencyPercentiles", "p99")) }
  cpu = resources.map { |resource| resource.fetch("cpu_average") }
  rss = resources.map { |resource| resource.fetch("rss_max_mib") }

  {
    "architecture" => architecture,
    "scenario" => scenario,
    "tls_mode" => tls_mode,
    "rounds" => rows.length,
    "protocol" => rows.first.fetch("protocol"),
    "concurrency" => Integer(rows.first.fetch("concurrency")),
    "wait_ms" => Integer(rows.first.fetch("wait_ms")),
    "request_bytes" => Integer(rows.first.fetch("request_bytes")),
    "response_bytes" => Integer(rows.first.fetch("response_bytes")),
    "requests_total" => request_counts.sum,
    "requests_per_round_median" => median(request_counts),
    "elapsed_seconds_total" => elapsed.sum,
    "rps_median" => median(rps),
    "rps_rsd" => relative_standard_deviation(rps),
    "rps_range_percent" => range_percent(rps),
    "p50_median_seconds" => median(p50),
    "p50_rsd" => relative_standard_deviation(p50),
    "p95_median_seconds" => median(p95),
    "p95_rsd" => relative_standard_deviation(p95),
    "p99_median_seconds" => median(p99),
    "p99_rsd" => relative_standard_deviation(p99),
    "cpu_percent_median" => median(cpu),
    "cpu_percent_rsd" => relative_standard_deviation(cpu),
    "rss_mib_median" => median(rss),
    "rss_mib_max" => rss.max,
    "errors" => 0
  }
end.sort_by { |row| [row.fetch("tls_mode"), row.fetch("scenario"), row.fetch("architecture")] }

control_manifest = CSV.read(
  File.join(result_dir, "control-manifest.tsv"),
  headers: true,
  col_sep: "\t"
)
control_rows = control_manifest
  .group_by { |row| [row.fetch("architecture"), row.fetch("state")] }
  .map do |(architecture, state), rows|
    documents = rows.map { |row| load_json(result_dir, row.fetch("oha_json")) }
    documents.zip(rows).each { |document, row| validate_measurement!(document, row.fetch("oha_json")) }
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

def row_for(rows, architecture, scenario, tls_mode)
  rows.find do |row|
    row.fetch("architecture") == architecture &&
      row.fetch("scenario") == scenario &&
      row.fetch("tls_mode") == tls_mode
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
    # Throughput and latency are reported on their own. An overall advantage
    # requires both, so a single improved metric cannot carry the claim.
    "has_stable_advantage" =>
      rps_improvement > rps_threshold && p99_improvement > p99_threshold
  }
end

preflight = preflight_values(result_dir)
mode = preflight.fetch("mode", "unknown")
performance_decision = {}
topology_comparison = {}
# Beating Puma only shows that Fibers suit I/O waiting better than threads.
# Falcon is fiber-per-request too, so fiber vs falcon_direct is the comparison
# that isolates what running inside the Envoy process actually buys.
same_model_decision = {}
tls_modes_present = summary_rows.map { |row| row.fetch("tls_mode") }.uniq
if mode == "full"
  tls_modes_present.each do |tls_mode|
    envoy_puma_primary = row_for(summary_rows, "envoy_puma", "noop_h1_c100", tls_mode)
    %w[sync fiber].each do |architecture|
      candidate = row_for(summary_rows, architecture, "noop_h1_c100", tls_mode)
      next unless candidate && envoy_puma_primary

      performance_decision["#{architecture}/#{tls_mode}"] =
        performance_comparison(candidate, envoy_puma_primary)
    end

    fiber_primary = row_for(summary_rows, "fiber", "noop_h1_c100", tls_mode)
    falcon_primary = row_for(summary_rows, "falcon_direct", "noop_h1_c100", tls_mode)
    if fiber_primary && falcon_primary
      same_model_decision[tls_mode] = performance_comparison(fiber_primary, falcon_primary)
    end

    direct_puma_primary = row_for(summary_rows, "puma_direct", "noop_h1_c100", tls_mode)
    if envoy_puma_primary && direct_puma_primary
      topology_comparison[tls_mode] =
        performance_comparison(envoy_puma_primary, direct_puma_primary)
    end
  end
end

primary_tls_mode = tls_modes_present.include?("plain") ? "plain" : tls_modes_present.first
fiber_wait_c10 = row_for(summary_rows, "fiber", "wait200_h1_c10", primary_tls_mode)
fiber_wait_c100 = row_for(summary_rows, "fiber", "wait200_h1_c100", primary_tls_mode)
fiber_wait_scaling =
  if fiber_wait_c10 && fiber_wait_c100
    fiber_wait_c100.fetch("rps_median") / fiber_wait_c10.fetch("rps_median")
  end

campaign_path = File.join(result_dir, "campaign.tsv")
campaign_rows = CSV.read(campaign_path, headers: true, col_sep: "\t").map(&:to_h)
skipped_path = File.join(result_dir, "skipped.tsv")
skipped_rows = CSV.read(skipped_path, headers: true, col_sep: "\t").map(&:to_h)

# Rounds that disagree by more than this are noise, not a measurement.
UNSTABLE_RPS_RSD = 0.15
unstable_measurements = summary_rows
  .select { |row| row.fetch("rps_rsd") > UNSTABLE_RPS_RSD }
  .map do |row|
    {
      "architecture" => row.fetch("architecture"),
      "scenario" => row.fetch("scenario"),
      "tls_mode" => row.fetch("tls_mode"),
      "rps_rsd_percent" => (row.fetch("rps_rsd") * 100).round(2)
    }
  end

summary = {
  "mode" => mode,
  "unstable_measurements" => unstable_measurements,
  "architecture_order" => preflight.fetch("architectures", "unknown"),
  "architecture_order_mode" => preflight.fetch("architecture_order_mode", "unknown"),
  "campaign" => campaign_rows,
  "skipped_measurements" => skipped_rows,
  "measurements" => summary_rows,
  "control_route" => control_rows,
  "standalone_bridge" => bridge_summary,
  "performance_decision" => performance_decision,
  "fiber_over_falcon_direct" => same_model_decision,
  "envoy_puma_over_direct_puma" => topology_comparison,
  "fiber_wait_c100_over_c10_rps_ratio" => fiber_wait_scaling
}
File.write(File.join(result_dir, "summary.json"), JSON.pretty_generate(summary) << "\n")

markdown = []
markdown << "# ruvoy 性能基准"
markdown << ""
markdown << "- 模式：`#{mode}`"
markdown << "- 架构顺序：`#{preflight.fetch("architectures", "unknown")}`（`#{preflight.fetch("architecture_order_mode", "unknown")}`）"
markdown << "- 原始目录：`#{result_dir}`"
markdown << "- 所有纳入汇总的轮次均要求：100% success、仅 HTTP 200、无 transport error。"
if mode != "full"
  markdown << "- 这是低强度 smoke 校准结果，不能用于最终性能结论。"
end
unless unstable_measurements.empty?
  markdown << ""
  markdown << "## 不稳定场景（RPS RSD > 15%，数据作废需重跑）"
  markdown << ""
  unstable_measurements.each do |row|
    markdown << format(
      "- %s / %s / %s：RPS RSD `%.2f%%`。",
      row.fetch("architecture"),
      row.fetch("scenario"),
      row.fetch("tls_mode"),
      row.fetch("rps_rsd_percent")
    )
  end
end
markdown << ""
markdown << "## 网络结果"
markdown << ""
markdown << "| architecture | scenario | tls | rounds | requests | RPS median (RSD) | p50 ms (RSD) | p95 ms (RSD) | p99 ms (RSD) | CPU % (RSD) | RSS MiB median/max |"
markdown << "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"
summary_rows.each do |row|
  markdown << format(
    "| %s | %s | %s | %d | %d | %.2f (%.2f%%) | %.3f (%.2f%%) | %.3f (%.2f%%) | %.3f (%.2f%%) | %.1f (%.2f%%) | %.1f / %.1f |",
    row.fetch("architecture"),
    row.fetch("scenario"),
    row.fetch("tls_mode"),
    row.fetch("rounds"),
    row.fetch("requests_total"),
    row.fetch("rps_median"),
    row.fetch("rps_rsd") * 100,
    row.fetch("p50_median_seconds") * 1_000,
    row.fetch("p50_rsd") * 100,
    row.fetch("p95_median_seconds") * 1_000,
    row.fetch("p95_rsd") * 100,
    row.fetch("p99_median_seconds") * 1_000,
    row.fetch("p99_rsd") * 100,
    row.fetch("cpu_percent_median"),
    row.fetch("cpu_percent_rsd") * 100,
    row.fetch("rss_mib_median"),
    row.fetch("rss_mib_max")
  )
end

unless skipped_rows.empty?
  markdown << ""
  markdown << "## 明确跳过"
  markdown << ""
  skipped_rows.each do |row|
    markdown << "- #{row.fetch("architecture")} / #{row.fetch("scenario")} / round #{row.fetch("round")}：#{row.fetch("reason")}。"
  end
end

unless control_rows.empty?
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
      "- %s vs Envoy→Puma（H1 no-op c100）：吞吐 `%+.2f%%`（阈值 `%.2f%%`，稳定优势 `%s`），" \
      "延迟 p99 `%+.2f%%`（阈值 `%.2f%%`，稳定优势 `%s`）；两项同时成立才算总体优势：`%s`。",
      architecture,
      decision.fetch("rps_improvement_percent"),
      decision.fetch("rps_noise_threshold_percent"),
      decision.fetch("rps_stable_advantage"),
      decision.fetch("p99_improvement_percent"),
      decision.fetch("p99_noise_threshold_percent"),
      decision.fetch("p99_stable_advantage"),
      decision.fetch("has_stable_advantage")
    )
  end
  topology_comparison.each do |tls_mode, comparison|
    markdown << format(
      "- Envoy→Puma vs direct Puma（H1 no-op c100，%s）：RPS `%+.2f%%`，p99 `%+.2f%%`；" \
      "这是拓扑开销对照，不是 Ruvoy 性能结论。",
      tls_mode,
      comparison.fetch("rps_improvement_percent"),
      comparison.fetch("p99_improvement_percent")
    )
  end

  markdown << ""
  markdown << "### 同并发模型对照（Ruvoy vs Falcon）"
  markdown << ""
  markdown << "Falcon 同为 fiber-per-request，因此这一项的差值才是「嵌入 Envoy 同进程、" \
              "省去进程外 HTTP hop」的收益；赢 Puma 只能说明 Fiber 比线程更适合等待。"
  markdown << ""
  if same_model_decision.empty?
    markdown << "- 当前结果不含 H1 no-op c100 的 fiber/falcon_direct 对照，该判定不适用。"
  else
    same_model_decision.each do |tls_mode, decision|
      markdown << format(
        "- Ruvoy vs direct Falcon（H1 no-op c100，%s）：吞吐 `%+.2f%%`（阈值 `%.2f%%`，" \
        "稳定优势 `%s`），延迟 p99 `%+.2f%%`（阈值 `%.2f%%`，稳定优势 `%s`）；总体优势：`%s`。",
        tls_mode,
        decision.fetch("rps_improvement_percent"),
        decision.fetch("rps_noise_threshold_percent"),
        decision.fetch("rps_stable_advantage"),
        decision.fetch("p99_improvement_percent"),
        decision.fetch("p99_noise_threshold_percent"),
        decision.fetch("p99_stable_advantage"),
        decision.fetch("has_stable_advantage")
      )
    end
    unless same_model_decision.values.any? { |decision| decision.fetch("has_stable_advantage") }
      markdown << ""
      markdown << "**相对同并发模型的 Falcon 没有稳定优势：Ruvoy 的性能主张不成立，" \
                  "仅剩运维形态收益（单进程部署、Envoy 原生 HTTP/TLS、无 upstream hop）。**"
    end
  end

  if performance_decision.empty?
    markdown << ""
    markdown << "- 当前结果不含 H1 no-op c100 的完整候选/Envoy→Puma 对照，预注册总判定不适用。"
  elsif !performance_decision.values.any? { |decision| decision.fetch("has_stable_advantage") }
    markdown << ""
    markdown << "**同进程设计相对 Envoy→Puma 未兑现性能价值。**"
  end
end

File.write(File.join(result_dir, "summary.md"), markdown.join("\n") << "\n")
