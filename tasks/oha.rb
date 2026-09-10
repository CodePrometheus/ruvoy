# frozen_string_literal: true

require "digest"
require "fileutils"
require "rbconfig"
require "tmpdir"
require_relative "build"

# The load generator, pinned by version and checksum and installed into the
# checkout, so every measurement is taken with the same one.
module Oha
  VERSION = "1.15.0"
  PATH = File.join(Build::ROOT, ".tools", "oha", "oha")
  RELEASES = {
    [ "darwin", "arm64" ] => [ "macos-arm64", "70d7cb7c15ed3d5eb4b7d9a7e76f0a8ee32ba1f18f560acef3b28e8670b89bb0" ],
    [ "darwin", "x86_64" ] => [ "macos-amd64", "fc8ccb4126737aae85cc9fbc6f95b161bf8bbb676bf02d4bb6196ec02c709c36" ],
    [ "linux", "aarch64" ] => [ "linux-arm64", "72d5bf4575cede9f9277f93f097b904f893b0f0cd4d92f0869439b05e1403731" ],
    [ "linux", "arm64" ] => [ "linux-arm64", "72d5bf4575cede9f9277f93f097b904f893b0f0cd4d92f0869439b05e1403731" ],
    [ "linux", "x86_64" ] => [ "linux-amd64", "86ab7fa2c1df23b3bbc53b73561ffa44a7a38ca08f0e10351df9522a5c4c3c61" ]
  }.freeze

  def self.install
    os = RbConfig::CONFIG.fetch("host_os")[/darwin|linux/]
    cpu = RbConfig::CONFIG.fetch("host_cpu")
    platform, checksum = RELEASES.fetch([ os, cpu ]) { raise Build::Failed, "unsupported oha platform: #{os}-#{cpu}" }
    FileUtils.mkdir_p(File.dirname(PATH))
    Dir.mktmpdir("ruvoy-oha-") do |work|
      binary = Build.download("https://github.com/hatoo/oha/releases/download/v#{VERSION}/oha-#{platform}",
                              File.join(work, "oha"))
      actual = Digest::SHA256.file(binary).hexdigest
      raise Build::Failed, "oha-#{platform} checksum is #{actual}, expected #{checksum}" unless actual == checksum

      File.chmod(0o755, binary)
      FileUtils.mv(binary, PATH)
    end
    puts IO.popen([ PATH, "--version" ], &:read)
  end
end
