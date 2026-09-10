# frozen_string_literal: true

require "bundler"

# The versions the checkout pins, read from its lockfiles rather than repeated
# as literals, so an upgrade cannot leave a check disagreeing with it.
module Lockfile
  GEMS = File.expand_path("../Gemfile.lock", __dir__)
  CRATES = File.expand_path("../Cargo.lock", __dir__)

  class << self
    def version(name)
      spec = gems.specs.find { |candidate| candidate.name == name }
      raise ArgumentError, "gem #{name} is not in Gemfile.lock" unless spec

      spec.version.to_s
    end

    def bundler_version
      gems.bundler_version.to_s
    end

    def envoy_sdk_commit
      File.read(CRATES)[%r{envoyproxy/envoy\?tag=[^#"]+#(\h{40})}, 1] or
        raise ArgumentError, "the Envoy SDK is not in Cargo.lock"
    end

    private

    def gems
      @gems ||= Bundler::LockfileParser.new(File.read(GEMS))
    end
  end
end
