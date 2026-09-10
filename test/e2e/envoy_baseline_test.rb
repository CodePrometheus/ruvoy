# frozen_string_literal: true

require_relative "test_helper"

# The pure-Rust module against a real Envoy: the ABI handshake, a response
# from a foreign thread, a body large enough to arrive in pieces, and a
# clean exit on SIGINT.
class EnvoyBaselineTest < E2ETestCase
  PORT = 19180
  TWO_MIB = 2 * 1024 * 1024
  SCHEDULER_ROUNDS = 50
  SDK_COMMIT = "8eea3285d6bdb89f8ea34632cfe7ce1608a8f374"

  def test_module_serves_through_a_real_envoy
    assert_port_free(PORT)
    log = File.join(result_dir, "poc1-envoy-baseline-#{run_id}.log")
    File.write(log, [ "run_id=#{run_id}", "envoy_package=#{Envoy::PACKAGE}", "envoy_sdk_commit=#{SDK_COMMIT}",
                      "rustc=#{IO.popen(%w[rustc --version], &:read).strip}",
                      "host=#{IO.popen(%w[uname -srm], &:read).strip}", "" ].join("\n"))
    build_module("baseline", log: log)
    system("uvx", "--from", Envoy::PACKAGE, "envoy", "--version", out: [ log, "a" ], err: [ :child, :out ])

    envoy_log = File.join(scratch_dir, "envoy.log")
    envoy = Envoy.start(config: File.join(root, "config", "envoy-baseline.yaml"), modules: module_dir,
                        log: envoy_log, concurrency: 2, log_level: "info")
    assert envoy.serving?(url("/direct")), "Envoy did not become ready:\n#{tail(envoy_log)}"

    assert_mode "/direct", "direct", "rust-baseline"
    assert_mode "/scheduler", "scheduler", "rust-scheduler"

    response = http_post(url("/benchmark?wait_ms=0&response_bytes=0&expected_request_bytes=#{TWO_MIB}"),
                         "\0" * TWO_MIB)
    assert_equal "200", response.code, "2 MiB POST /benchmark returned HTTP #{response.code}"
    assert_empty response.body.to_s, "2 MiB POST /benchmark returned an unexpected body"
    assert_equal TWO_MIB.to_s, response["x-request-bytes"], "2 MiB POST /benchmark was not read completely"

    SCHEDULER_ROUNDS.times { assert_mode "/scheduler", "scheduler", "rust-scheduler" }

    status = envoy.stop
    assert status&.success?, "Envoy did not exit cleanly after SIGINT"
    text = File.read(envoy_log)
    assert_includes text, "Dynamic module ABI version v0.1.0 matched"
    refute_match(/panic|fatal|segmentation fault/i, text, "Envoy log contains a fatal runtime error")

    File.write(log, [ "direct_response=PASS", "two_mib_request=PASS",
                      "foreign_thread_scheduler_responses=#{SCHEDULER_ROUNDS + 1}", "sigint_cleanup=PASS", "" ]
                      .join("\n"), mode: "a")
    puts "raw result: #{log}"
  ensure
    envoy&.stop
    File.write(log, "\n===== envoy log =====\n#{File.read(envoy_log)}", mode: "a") if envoy_log && File.exist?(envoy_log)
  end

  private

  def url(path)
    "http://127.0.0.1:#{PORT}#{path}"
  end

  def assert_mode(path, mode, body)
    response = http_get(url(path))
    assert_equal "200", response.code, "#{path} returned HTTP #{response.code}"
    assert_equal mode, response["x-ruvoy-mode"], "#{path} did not return x-ruvoy-mode: #{mode}"
    assert_equal body, response.body.delete("\n"), "#{path} returned an unexpected body"
  end
end
