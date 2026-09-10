# frozen_string_literal: true

require_relative "test_helper"

# The embedding under sustained concurrent load, watched for crash reports
# and resident-memory growth.
class MagnusFiberStabilityTest < E2ETestCase
  PROBE = "fiber_stability_probe"
  CRASH_REPORTS = File.expand_path("~/Library/Logs/DiagnosticReports")
  DEFAULTS = { waves: 3, requests_per_wave: 1000, concurrency: 100, body_bytes: 0, host_threads: 4 }.freeze
  LIMITS = { concurrency: 1024, body_bytes: 2 * 1024 * 1024 }.freeze
  POSITIVE = %i[waves requests_per_wave concurrency].freeze

  def test_probe_survives_the_waves
    dir = File.join(result_dir, "poc5-magnus-fiber-stability-#{run_id}")
    FileUtils.mkdir_p(dir)
    settings = probe_settings
    preflight(File.join(dir, "preflight.log"), settings)

    reports_before = crash_reports
    probe_log = File.join(dir, "probe.log")
    rss_file = File.join(dir, "rss.tsv")
    pid = Bundler.with_unbundled_env do
      spawn(bundle_env.merge(settings.to_h { |key, value| [ "RUVOY_PROBE_#{key.upcase}", value.to_s ] }),
            File.join(root, "target", "release", "examples", PROBE),
            out: probe_log, err: [ :child, :out ])
    end
    sampler = sample_rss(pid, rss_file)
    _, status = Process.wait2(pid)
    sampler.join
    new_reports = crash_reports - reports_before

    summary = File.join(dir, "summary.log")
    File.write(summary, { "probe_exit_status" => status.exitstatus, "max_rss_kib" => max_rss(rss_file),
                          "new_crash_reports" => new_reports.size, "raw_results" => dir }
                          .map { |key, value| "#{key}=#{value}\n" }.join)
    begin
      assert_predicate status, :success?, "probe exited with status #{status.exitstatus}"
      assert_includes File.readlines(probe_log, chomp: true), "result=PASS", "probe did not report PASS"
      assert_empty new_reports, "probe produced a new crash report"
    rescue Minitest::Assertion => failure
      File.write(summary, "result=FAIL\nreason=#{failure.message}\n", mode: "a")
      raise
    end
    File.write(summary, "result=PASS\n", mode: "a")
    puts File.read(summary)
  end

  private

  def probe_settings
    DEFAULTS.to_h do |key, default|
      value = Integer(ENV.fetch("RUVOY_PROBE_#{key.upcase}", default.to_s), 10)
      raise ArgumentError, "#{key} must be non-negative" if value.negative?
      raise ArgumentError, "#{key} must be positive" if POSITIVE.include?(key) && value.zero?
      raise ArgumentError, "#{key} exceeds #{LIMITS[key]}" if LIMITS[key] && value > LIMITS[key]

      [ key, value ]
    end
  end

  def preflight(log, settings)
    File.open(log, "w") do |file|
      file.puts "run_id=#{run_id}", "ruby=#{RUBY_DESCRIPTION}"
      settings.each { |key, value| file.puts "#{key}=#{value}" }
      file.puts IO.popen([ "cargo", "tree", "-p", "ruvoy" ], chdir: root, &:read).lines.grep(/magnus|rb-sys/)
    end
    built = system("cargo", "build", "--release", "--example", PROBE,
                   out: [ log, "a" ], err: [ :child, :out ], chdir: root)
    raise "probe release build failed; see #{log}" unless built
  end

  def sample_rss(pid, path)
    Thread.new do
      File.open(path, "w") do |file|
        file.puts "timestamp\tpid\trss_kib"
        while running?(pid)
          rss = IO.popen([ "ps", "-o", "rss=", "-p", pid.to_s ], &:read).strip
          file.puts "#{Time.now.utc.strftime("%Y-%m-%dT%H:%M:%SZ")}\t#{pid}\t#{rss}" if rss.match?(/\A\d+\z/)
          sleep 0.2
        end
      end
    end
  end

  def running?(pid)
    Process.kill(0, pid)
    true
  rescue Errno::ESRCH
    false
  end

  def max_rss(path)
    File.readlines(path, chomp: true).drop(1).map { |line| line.split("\t").last.to_i }.max || 0
  end

  def crash_reports
    Dir.glob(File.join(CRASH_REPORTS, "#{PROBE}*.ips")).sort
  end
end
