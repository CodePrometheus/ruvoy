# frozen_string_literal: true

require "digest"
require "fileutils"
require "net/http"
require "open3"
require "rbconfig"
require "rubygems/package"
require "tmpdir"

# Builds the Envoy modules and the release gems, and holds the checkout to the
# one source rule the modules depend on.
module Build
  Failed = Class.new(StandardError)

  ROOT = File.expand_path("..", __dir__)
  MODULE_DIR = File.join(ROOT, "build", "modules")
  PROFILES = %w[debug release].freeze
  ENTRY_SYMBOL = "envoy_dynamic_module_on_program_init"

  Target = Data.define(:package, :library, :embeds_ruby)
  TARGETS = {
    "fiber" => Target.new("ruvoy-envoy-fiber", "libruvoy_fiber", true),
    "sync" => Target.new("ruvoy-envoy-sync", "libruvoy_sync", true),
    "baseline" => Target.new("ruvoy-envoy-baseline", "libruvoy_baseline", false)
  }.freeze

  # Envoy workers hand the runtime owned data and wake it through the scheduler;
  # a Ruby value on a worker thread is a VM access from a thread that may not
  # make one.
  WORKER_SOURCES = %w[crates/ruvoy-envoy-sync/src/worker.rs crates/ruvoy-envoy-fiber/src/worker.rs].freeze
  FORBIDDEN_IN_WORKERS = /magnus|RubyRuntime|Ruby::|BoxValue|RArray|RHash|Value|funcall|eval|call_app/
  REQUIRED_IN_WORKERS = [ /RuntimeClient/, /Request/, /scheduler\.commit/ ].freeze

  RELEASE_PLATFORMS = {
    "x86_64" => { envoy: "linux-x86_64", gem: "x86_64-linux" },
    "aarch64" => { envoy: "linux-aarch_64", gem: "aarch64-linux" },
    "arm64" => { envoy: "linux-aarch_64", gem: "aarch64-linux" }
  }.freeze
  ENVOY_RELEASES = "https://github.com/envoyproxy/envoy/releases/download"

  class << self
    def envoy_module(kind, profile: ENV.fetch("RUVOY_BUILD_PROFILE", "release"),
                     ruby: ENV.fetch("RUVOY_RUBY", RbConfig.ruby), log: nil)
      target = TARGETS.fetch(kind.to_s) { raise Failed, "unknown module #{kind}; expected #{TARGETS.keys.join(", ")}" }
      raise Failed, "RUVOY_BUILD_PROFILE must be debug or release" unless PROFILES.include?(profile)

      env = {}
      if target.embeds_ruby
        require_pinned_ruby(ruby)
        env["RUBY"] = ruby
      end
      command = [ "cargo", "build", "--package", target.package ]
      command << "--release" if profile == "release"
      run!(*command, env: env, log: log)
      install_module(target, profile, log)
    end

    def check_worker_boundary(log: nil)
      WORKER_SOURCES.each do |source|
        lines = File.readlines(File.join(ROOT, source), chomp: true)
        leaks = lines.each_with_index.filter_map do |line, index|
          "#{source}:#{index + 1}: #{line.strip}" if line.match?(FORBIDDEN_IN_WORKERS)
        end
        raise Failed, "Envoy worker source contains a Ruby VM reference:\n#{leaks.join("\n")}" if leaks.any?

        missing = REQUIRED_IN_WORKERS.reject { |pattern| lines.any? { |line| line.match?(pattern) } }
        raise Failed, "#{source} no longer matches #{missing.map(&:source).join(", ")}" if missing.any?
      end
      say("PASS: sync and Fiber worker sources use only owned bridge types and Envoy scheduler", log)
    end

    # The two gems change for separate reasons: a security fix in Envoy should
    # reach users through `gem update ruvoy-envoy` without a release of ruvoy.
    def gems(target = "all")
      builders = { "ruvoy" => :module_gem, "ruvoy-envoy" => :envoy_gem }
      selected = if target == "all"
        builders.values
      else
        [ builders.fetch(target) { raise Failed, "unknown target: #{target} (expected ruvoy, ruvoy-envoy or all)" } ]
      end
      raise Failed, "the release gems are built on Linux" unless linux?

      cpu = RbConfig::CONFIG.fetch("host_cpu")
      platform = RELEASE_PLATFORMS.fetch(cpu) { raise Failed, "no Envoy release for #{cpu}" }
      selected.each { |builder| send(builder, platform) }
    end

    def download(url, path, redirects: 5)
      uri = URI(url)
      Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https") do |http|
        http.request_get(uri.request_uri) do |response|
          case response
          when Net::HTTPSuccess
            File.open(path, "wb") { |file| response.read_body { |chunk| file.write(chunk) } }
          when Net::HTTPRedirection
            raise Failed, "too many redirects fetching #{url}" if redirects.zero?

            return download(URI.join(url, response["location"]).to_s, path, redirects: redirects - 1)
          else
            raise Failed, "#{url} answered #{response.code}"
          end
        end
      end
      path
    end

    private

    def install_module(target, profile, log)
      source = File.join(ROOT, "target", profile, "#{target.library}.#{darwin? ? "dylib" : "so"}")
      installed = File.join(MODULE_DIR, "#{target.library}.so")
      FileUtils.mkdir_p(MODULE_DIR)
      # Installed through a rename so the module always arrives at a fresh inode:
      # overwriting in place leaves the kernel checking the new content against
      # the code signature it cached for the old file.
      FileUtils.cp(source, "#{installed}.staged")
      File.rename("#{installed}.staged", installed)
      inspect_module(target, installed, log)
      say("built module: #{installed}", log)
      installed
    end

    def inspect_module(target, path, log)
      capture("file", path, log: log)
      if darwin?
        links = capture("otool", "-L", path, log: log)
        series = "libruby.#{pinned_ruby_version.split(".").first(2).join(".")}"
        raise Failed, "#{path} does not link #{series}" if target.embeds_ruby && !links.include?(series)

        symbols = capture("nm", "-gU", path, quiet: true)
      elsif linux?
        capture("ldd", path, log: log)
        symbols = capture("nm", "-D", "--defined-only", path, quiet: true)
      else
        raise Failed, "unsupported operating system: #{RbConfig::CONFIG["host_os"]}"
      end
      raise Failed, "#{path} does not export #{ENTRY_SYMBOL}" unless symbols.include?(ENTRY_SYMBOL)
    end

    def module_gem(platform)
      abi = RbConfig::CONFIG.fetch("ruby_version")
      say("Building the module for Ruby #{abi} on #{platform[:gem]}")
      run!("cargo", "build", "--release", "--package", TARGETS.fetch("fiber").package)
      installed = File.join("lib", "ruvoy", abi, "libruvoy_fiber.so")
      FileUtils.mkdir_p(File.join(ROOT, File.dirname(installed)))
      FileUtils.install(File.join(ROOT, "target", "release", "libruvoy_fiber.so"), File.join(ROOT, installed), mode: 0o644)
      # Envoy searches DT_RUNPATH only after LD_LIBRARY_PATH, which the command
      # line sets, while DT_RPATH comes first: a stale build-machine path would
      # outrank the Ruby the gem is installed against.
      if executable?("readelf") && capture("readelf", "-d", File.join(ROOT, installed), quiet: true).include?("RPATH")
        raise Failed, "the module carries DT_RPATH, which would outrank the installed Ruby"
      end

      require File.join(ROOT, "lib", "ruvoy", "version")
      gem_file = build_gem("ruvoy.gemspec", platform, "ruvoy-#{Ruvoy::VERSION}-#{platform[:gem]}.gem")
      contents = require_in_gem(gem_file, installed, "exe/ruvoy")
      # Envoy belongs to the other gem; shipping it here would put a 100 MB
      # download in front of everyone who already has one.
      raise Failed, "#{gem_file} carries Envoy, which belongs to ruvoy-envoy" if contents.include?("exe/envoy")

      say("Built #{gem_file}\n\n")
    end

    def envoy_gem(platform)
      version = File.read(File.join(ROOT, ".envoy-version")).strip
      raise Failed, "no version in .envoy-version" if version.empty?

      asset = "envoy-#{version}-#{platform[:envoy]}"
      Dir.mktmpdir("ruvoy-gem-") do |work|
        say("Fetching #{asset}")
        binary = download("#{ENVOY_RELEASES}/v#{version}/#{asset}", File.join(work, "envoy"))
        checksums = download("#{ENVOY_RELEASES}/v#{version}/checksums.txt.asc", File.join(work, "checksums"))
        expected = File.readlines(checksums).map(&:split).find { |(_, path)| path.to_s.end_with?("/#{asset}") }&.first
        raise Failed, "no checksum listed for #{asset}" unless expected
        raise Failed, "checksum mismatch for #{asset}" unless Digest::SHA256.file(binary).hexdigest == expected

        say("Checksum verified: #{expected}")
        FileUtils.install(binary, File.join(ROOT, "exe", "envoy"), mode: 0o755)
      end
      # Apache-2.0 carries the upstream attribution along with the binary.
      download("https://raw.githubusercontent.com/envoyproxy/envoy/v#{version}/NOTICE", File.join(ROOT, "NOTICE"))
      gem_file = build_gem("ruvoy-envoy.gemspec", platform, "ruvoy-envoy-#{version}-#{platform[:gem]}.gem")
      require_in_gem(gem_file, "exe/envoy", "lib/ruvoy/envoy.rb", "NOTICE")
      say("Built #{gem_file}\n\n")
    end

    def build_gem(gemspec, platform, expected)
      run!(File.join(RbConfig::CONFIG.fetch("bindir"), "gem"), "build", gemspec,
           env: { "RUVOY_GEM_PLATFORM" => platform[:gem] })
      raise Failed, "expected #{expected}" unless File.file?(File.join(ROOT, expected))

      expected
    end

    # Reads the file list out of a built gem, so a missing payload fails the
    # build rather than reaching a user as a gem that installs and cannot run.
    def require_in_gem(gem_file, *required)
      contents = Gem::Package.new(File.join(ROOT, gem_file)).contents
      missing = required - contents
      raise Failed, "#{gem_file} is missing #{missing.join(", ")}" if missing.any?

      contents
    end

    def require_pinned_ruby(ruby)
      actual = capture(ruby, "-e", "print RUBY_VERSION", quiet: true)
      raise Failed, "expected Ruby #{pinned_ruby_version}, got #{actual} from #{ruby}" unless actual == pinned_ruby_version
    end

    def pinned_ruby_version
      File.read(File.join(ROOT, ".ruby-version")).strip
    end

    def run!(*command, env: {}, log: nil)
      streams = log ? { out: [ log, "a" ], err: %i[child out] } : {}
      return if unbundled { system(env, *command, chdir: ROOT, **streams) }

      raise Failed, "#{command.join(" ")} failed#{" (see #{log})" if log}"
    end

    def capture(*command, env: {}, log: nil, quiet: false)
      output, status = unbundled { Open3.capture2e(env, *command, chdir: ROOT) }
      say(output, log) unless quiet
      raise Failed, "#{command.join(" ")} failed:\n#{output}" unless status.success?

      output
    rescue SystemCallError => error
      raise Failed, "#{command.first}: #{error.message}"
    end

    def say(message, log = nil)
      log ? File.write(log, "#{message.chomp}\n", mode: "a") : puts(message)
    end

    # Builds start from the environment the task itself started with, so a
    # bundle the caller happens to be inside never reaches cargo or a gem.
    def unbundled(&block)
      defined?(Bundler) ? Bundler.with_unbundled_env(&block) : yield
    end

    def darwin? = RbConfig::CONFIG["host_os"].include?("darwin")

    def linux? = RbConfig::CONFIG["host_os"].include?("linux")

    def executable?(name)
      ENV.fetch("PATH", "").split(File::PATH_SEPARATOR).any? { |directory| File.executable?(File.join(directory, name)) }
    end
  end
end
