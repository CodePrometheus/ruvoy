# frozen_string_literal: true

# Renders benchmark charts as dependency-free SVG.
#
# Throughput and p99 are always drawn as a pair: a server can buy throughput
# with latency, so publishing only the throughput bars would let that trade
# disappear from the report.

require "json"

result_dir = File.expand_path(ARGV.fetch(0))
summary = JSON.parse(File.read(File.join(result_dir, "summary.json")))
charts_dir = File.join(result_dir, "charts")
Dir.mkdir(charts_dir) unless Dir.exist?(charts_dir)

WIDTH = 900
HEIGHT = 420
MARGIN_LEFT = 90
MARGIN_RIGHT = 30
MARGIN_TOP = 60
MARGIN_BOTTOM = 90
PALETTE = %w[#4c78a8 #f58518 #54a24b #b279a2 #e45756 #72b7b2].freeze

def escape(text)
  text.to_s.gsub("&", "&amp;").gsub("<", "&lt;").gsub(">", "&gt;")
end

def axis_maximum(values)
  highest = values.max
  return 1.0 if highest.nil? || highest <= 0

  magnitude = 10**Math.log10(highest).floor
  (highest / magnitude).ceil * magnitude.to_f
end

def format_value(value)
  return format("%.0f", value) if value >= 100
  return format("%.1f", value) if value >= 10

  format("%.2f", value)
end

def bar_chart(title, subtitle, series, unit)
  plot_width = WIDTH - MARGIN_LEFT - MARGIN_RIGHT
  plot_height = HEIGHT - MARGIN_TOP - MARGIN_BOTTOM
  maximum = axis_maximum(series.map { |entry| entry.fetch(:value) })
  slot = plot_width.to_f / series.length
  bar_width = [ slot * 0.6, 90 ].min

  svg = []
  svg << %(<svg xmlns="http://www.w3.org/2000/svg" width="#{WIDTH}" height="#{HEIGHT}" ) +
         %(viewBox="0 0 #{WIDTH} #{HEIGHT}" font-family="system-ui, sans-serif">)
  svg << %(<rect width="#{WIDTH}" height="#{HEIGHT}" fill="#ffffff"/>)
  svg << %(<text x="#{MARGIN_LEFT}" y="28" font-size="17" font-weight="600">#{escape(title)}</text>)
  svg << %(<text x="#{MARGIN_LEFT}" y="48" font-size="12" fill="#666">#{escape(subtitle)}</text>)

  5.downto(0) do |step|
    value = maximum * step / 5.0
    y = MARGIN_TOP + plot_height - (plot_height * step / 5.0)
    svg << %(<line x1="#{MARGIN_LEFT}" y1="#{y.round(1)}" x2="#{MARGIN_LEFT + plot_width}" ) +
           %(y2="#{y.round(1)}" stroke="#e5e5e5" stroke-width="1"/>)
    svg << %(<text x="#{MARGIN_LEFT - 10}" y="#{(y + 4).round(1)}" font-size="11" ) +
           %(fill="#666" text-anchor="end">#{format_value(value)}</text>)
  end

  series.each_with_index do |entry, index|
    value = entry.fetch(:value)
    height = maximum.positive? ? plot_height * value / maximum : 0
    x = MARGIN_LEFT + (slot * index) + ((slot - bar_width) / 2.0)
    y = MARGIN_TOP + plot_height - height
    colour = PALETTE[index % PALETTE.length]

    svg << %(<rect x="#{x.round(1)}" y="#{y.round(1)}" width="#{bar_width.round(1)}" ) +
           %(height="#{height.round(1)}" fill="#{colour}" rx="2"/>)
    svg << %(<text x="#{(x + bar_width / 2.0).round(1)}" y="#{(y - 6).round(1)}" ) +
           %(font-size="11" text-anchor="middle" fill="#333">#{format_value(value)}</text>)
    svg << %(<text x="#{(x + bar_width / 2.0).round(1)}" ) +
           %(y="#{MARGIN_TOP + plot_height + 18}" font-size="11" text-anchor="middle" ) +
           %(fill="#333">#{escape(entry.fetch(:label))}</text>)
  end

  svg << %(<line x1="#{MARGIN_LEFT}" y1="#{MARGIN_TOP + plot_height}" ) +
         %(x2="#{MARGIN_LEFT + plot_width}" y2="#{MARGIN_TOP + plot_height}" ) +
         %(stroke="#333" stroke-width="1"/>)
  svg << %(<text x="#{MARGIN_LEFT}" y="#{HEIGHT - 20}" font-size="11" fill="#666">#{escape(unit)}</text>)
  svg << "</svg>"
  svg.join("\n") << "\n"
end

preflight_path = File.join(result_dir, "preflight.log")
provenance = File.readlines(preflight_path, chomp: true).each_with_object({}) do |line, values|
  key, value = line.split("=", 2)
  values[key] = value if value
end
commit = provenance.fetch("git_commit", "unknown")[0, 12]
host = provenance.fetch("host", "unknown")
rounds = provenance.fetch("rounds", "?")
duration = provenance.fetch("bench_duration", "?")

written = []
summary.fetch("measurements").group_by { |row| [ row.fetch("scenario"), row.fetch("tls_mode") ] }
  .each do |(scenario, tls_mode), rows|
    ordered = rows.sort_by { |row| row.fetch("architecture") }
    subtitle = "commit #{commit} · #{host} · #{rounds} rounds x #{duration} · #{tls_mode}"

    throughput = ordered.map do |row|
      { label: row.fetch("architecture"), value: row.fetch("rps_median") }
    end
    latency = ordered.map do |row|
      { label: row.fetch("architecture"), value: row.fetch("p99_median_seconds") * 1_000 }
    end

    throughput_path = File.join(charts_dir, "#{scenario}_#{tls_mode}_throughput.svg")
    File.write(
      throughput_path,
      bar_chart("Throughput — #{scenario} (#{tls_mode})", subtitle, throughput, "requests/sec (median)")
    )
    latency_path = File.join(charts_dir, "#{scenario}_#{tls_mode}_p99.svg")
    File.write(
      latency_path,
      bar_chart("Latency p99 — #{scenario} (#{tls_mode})", subtitle, latency, "milliseconds (median of rounds, lower is better)")
    )
    written << throughput_path << latency_path
  end

puts "charts=#{written.length}"
