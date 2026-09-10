# frozen_string_literal: true

require "json"
require_relative "test_helper"

# The summariser against a synthetic campaign whose answers are known in
# advance. It turns raw measurements into the numbers this project publishes,
# so each expected value is stated here rather than recomputed the way the
# summariser does it — the only way an assertion can disagree.
class SummarizeTest < E2ETestCase
  MANIFEST_COLUMNS = %w[sequence architecture scenario tls_mode round protocol concurrency wait_ms
                        request_bytes response_bytes app_params served_requests oha_json resources_csv].freeze
  ROUNDS = 3
  # 50 ms of latency, once yielding and once not: a fiber runtime keeps almost
  # nothing when the call does not yield, a thread pool is barely affected.
  # Then CPU-bound work, where the interpreter lock holds both alike.
  MEASUREMENTS = [
    [ "fiber", "wait50_h1_c100", 50, "", 1900 ],
    [ "fiber", "block50_h1_c100", 0, "block_ms=50", 19 ],
    [ "falcon_direct", "wait50_h1_c100", 50, "", 1850 ],
    [ "falcon_direct", "block50_h1_c100", 0, "block_ms=50", 20 ],
    [ "fiber", "cpu25k_h1_c100", 0, "cpu_iterations=25000", 420 ],
    [ "falcon_direct", "cpu25k_h1_c100", 0, "cpu_iterations=25000", 415 ]
  ].freeze

  class << self
    def summary
      @summary ||= summarize(write_campaign(File.join(scratch_dir, "campaign")))
    end

    private

    def write_campaign(dir)
      FileUtils.mkdir_p(dir)
      File.write(File.join(dir, "preflight.log"),
                 "mode=full\narchitectures=fiber,falcon_direct\narchitecture_order_mode=rotate\n")
      File.write(File.join(dir, "campaign.tsv"),
                 "run\tarchitecture\tscenario\ttls_mode\tround\tprotocol\tstatus\tdetail\n")
      File.write(File.join(dir, "skipped.tsv"), "scenario\tround\tarchitecture\tprotocol\treason\n")
      File.write(File.join(dir, "control-manifest.tsv"), "#{MANIFEST_COLUMNS.join("\t")}\n")
      File.write(File.join(dir, "bridge-run1.json"), JSON.generate(
        "pure_rust" => { "p50_us" => 0.16, "p99_us" => 0.20 },
        "ruby_bridge" => { "p50_us" => 23.7, "p99_us" => 40.0 }
      ))
      File.open(File.join(dir, "manifest.tsv"), "w") do |manifest|
        manifest.puts MANIFEST_COLUMNS.join("\t")
        sequence = 0
        MEASUREMENTS.each do |architecture, scenario, wait_ms, app_params, rps|
          1.upto(ROUNDS) do |round|
            sequence += 1
            relative = "#{architecture}/#{scenario}-round#{round}.json"
            write_measurement(File.join(dir, relative), rps)
            manifest.puts [ sequence, architecture, scenario, "plain", round, "1.1", 100, wait_ms, 0, 0,
                            app_params, 3000, relative, "#{relative.delete_suffix(".json")}.resources.csv" ].join("\t")
          end
        end
      end
      dir
    end

    # Round-to-round variation stays tiny so the run is never rejected as
    # unstable, which would mask what is under test.
    def write_measurement(path, rps)
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, JSON.generate(
        "summary" => { "successRate" => 1.0, "requestsPerSec" => rps, "total" => 30.0 },
        "errorDistribution" => {},
        "statusCodeDistribution" => { "200" => (rps * 30).round },
        "latencyPercentiles" => { "p50" => 0.001, "p95" => 0.002, "p99" => 0.003 }
      ))
      File.write("#{path.delete_suffix(".json")}.resources.csv", "sample,cpu_percent,rss_kib\n0,50.0,102400\n")
    end

    def summarize(dir)
      system(RbConfig.ruby, File.join(root, "bench", "summarize.rb"), dir, out: File::NULL) or
        raise "summarize.rb failed"
      { json: File.read(File.join(dir, "summary.json")), md: File.read(File.join(dir, "summary.md")) }
    end
  end

  def test_summary_json_is_written
    refute_empty self.class.summary[:json]
  end

  def test_the_scenario_parameters_survive_into_the_summary
    assert_includes self.class.summary[:json], '"app_params": "block_ms=50"'
  end

  def test_yield_sensitivity_is_reported_for_the_fiber_runtime
    assert_includes self.class.summary[:json], '"block50_rps_median": 19.0'
  end

  # 19 / 1900 = 1%, stated rather than recomputed, so a summariser that divides
  # the wrong way round fails instead of agreeing with itself.
  def test_a_fiber_runtime_keeps_one_percent_when_the_call_does_not_yield
    retained = JSON.parse(self.class.summary[:json]).fetch("yield_sensitivity").fetch("fiber").fetch("retained_fraction")
    assert_equal "0.0100", format("%.4f", retained)
  end

  def test_the_report_explains_what_the_comparison_isolates
    assert_includes self.class.summary[:md], "native call that does not yield"
  end

  def test_the_cpu_bound_comparison_reaches_the_report
    assert_includes self.class.summary[:md], "CPU-bound work"
  end

  def test_the_report_carries_no_untranslated_text
    assert_empty self.class.summary[:md].scan(/\p{Han}/)
  end
end
