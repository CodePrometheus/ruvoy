# frozen_string_literal: true

require_relative "test_helper"

# The fiber runtime as its own process, with no Envoy in front of it.
class FiberRuntimeTest < E2ETestCase
  EXPECTED = {
    "result" => "PASS",
    "scheduler_aware_unique_fibers" => "10",
    "scheduler_io_requests" => "10",
    "scheduler_io_unique_fibers" => "10",
    "shutdown" => "PASS"
  }.freeze

  def test_runtime_stands_alone
    log = File.join(result_dir, "poc3-fiber-runtime-#{build_profile}-#{run_id}.log")
    args = [ "run", "--package", "ruvoy", "--example", "fiber_runtime" ]
    args << "--release" if build_profile == "release"

    ran = system(bundle_env.merge("RUBY" => RbConfig.ruby), "cargo", *args,
                 out: log, err: [ :child, :out ], chdir: root)
    assert ran, "cargo run failed; raw result: #{log}\n#{File.read(log)}"

    lines = File.readlines(log, chomp: true)
    EXPECTED.merge("async_version" => locked_version("async")).each do |key, value|
      assert_includes lines, "#{key}=#{value}"
    end
    puts "raw result: #{log}"
  end
end
