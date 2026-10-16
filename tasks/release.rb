# frozen_string_literal: true

require_relative "build"

# The text of a GitHub release. A ruvoy release carries its section of
# CHANGELOG.md; a ruvoy-envoy release names the Envoy it packages.
module Release
  CHANGELOG = File.join(Build::ROOT, "CHANGELOG.md")
  ENVOY_RELEASE_PAGES = "https://github.com/envoyproxy/envoy/releases/tag"

  class << self
    def notes(tag)
      case tag
      when %r{\Aenvoy/v(.+)\z}
        platforms = Build::RELEASE_PLATFORMS.values.map { |platform| platform[:gem] }.uniq.join(" and ")
        "Envoy #{$1}, packaged for #{platforms}.\n\nWhat changed in Envoy: #{ENVOY_RELEASE_PAGES}/v#{$1}\n"
      when /\Av(.+)\z/
        section($1)
      else
        raise Build::Failed, "#{tag} is neither vX.Y.Z nor envoy/vX.Y.Z"
      end
    end

    def check_changelog
      require_relative "../lib/ruvoy/version"
      section(Ruvoy::VERSION)
      puts "PASS: CHANGELOG.md has a section for #{Ruvoy::VERSION}"
    end

    private

    def section(version)
      part = File.read(CHANGELOG).split(/^## /).drop(1).find { |text| text[/\A\S+/] == version }
      raise Build::Failed, "CHANGELOG.md has no section for #{version}" unless part

      text = part.lines.drop(1).join.strip
      raise Build::Failed, "CHANGELOG.md's section for #{version} is empty" if text.empty?

      "#{text}\n"
    end
  end
end
