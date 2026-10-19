# frozen_string_literal: true

# What the bootstrap suite watches: a boot that takes as long as it is told,
# hooks that leave marks in a file, and a request that waits.
marks = ENV.fetch("RUVOY_TEST_MARKS")
boot_delay = Float(ENV.fetch("RUVOY_TEST_BOOT_DELAY", "0"))

Ruvoy::Lifecycle.before_ready do
  sleep boot_delay
  File.write(marks, "ready\n", mode: "a")
end
Ruvoy::Lifecycle.on_drain { File.write(marks, "drain\n", mode: "a") }

run lambda { |env|
  if env["PATH_INFO"] == "/wait"
    sleep Float(Rack::Utils.parse_query(env["QUERY_STRING"])["s"] || "1")
    [ 200, { "content-type" => "text/plain" }, [ "waited\n" ] ]
  else
    [ 200, { "content-type" => "text/plain" }, [ "ok\n" ] ]
  end
}
