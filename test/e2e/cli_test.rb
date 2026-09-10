# frozen_string_literal: true

require "open3"
require "yaml"
require "ruvoy/paths"
require_relative "test_helper"

# The command line that is the whole installed interface: `ruvoy app.ru`.
#
# Nothing here starts Envoy. What it covers is the part that decides what
# Envoy would be told — the generated configuration, where the module and the
# Ruby library are looked for, and what a user sees when one is missing.
class CLITest < E2ETestCase
  class << self
    def rackup
      @rackup ||= File.join(scratch_dir, "app.ru").tap { |path| File.write(path, "run ->[200, {}, []]\n") }
    end

    def printed_config
      @printed_config ||= ruvoy(rackup, "--print-config", "--port", "9100",
                                "--address", "0.0.0.0", "--admin-port", "9101").first
    end

    def ruvoy(*args)
      Open3.capture3(RbConfig.ruby, "-I#{File.join(root, "lib")}", File.join(root, "exe", "ruvoy"), *args)
    end

    # One release ships the gem and the module together, so two version
    # numbers that can drift are one too many.
    def cargo_version
      File.read(File.join(root, "Cargo.toml"))[/^\[workspace\.package\]\n(?:.*\n)*?version = "([^"]+)"/, 1]
    end
  end

  def test_version_matches_the_cargo_workspace
    stdout, = self.class.ruvoy("--version")
    assert_equal self.class.cargo_version, stdout.strip
  end

  def test_help_exits_successfully
    _, _, status = self.class.ruvoy("--help")
    assert_predicate status, :success?
  end

  def test_a_missing_rackup_is_refused_by_name
    _, stderr, status = self.class.ruvoy(File.join(scratch_dir, "absent.ru"))
    assert_equal 1, status.exitstatus
    assert_includes stderr, "absent.ru"
  end

  def test_listener_options_reach_the_configuration
    config = self.class.printed_config
    assert_includes config, "port_value: 9100"
    assert_includes config, "address: 0.0.0.0"
    assert_includes config, "port_value: 9101"
    assert_includes config, "terminal_filter: true"
    # A stuck fiber writes nothing, so the stream has to time out on its own.
    assert_includes config, "stream_idle_timeout: 15s"
  end

  # Envoy resolves the rackup from its own working directory.
  def test_the_rackup_path_is_absolute
    filter = YAML.safe_load(self.class.printed_config)
      .dig("static_resources", "listeners", 0, "filter_chains", 0, "filters", 0,
           "typed_config", "http_filters", 0, "typed_config", "filter_config", "value")
    assert_equal File.expand_path(self.class.rackup), filter
  end

  # An admin port bound by default would expose Envoy's control surface to
  # whoever can reach the host.
  def test_no_admin_interface_unless_asked_for
    stdout, = self.class.ruvoy(self.class.rackup, "--print-config")
    refute_includes stdout, "admin:"
  end

  def test_a_module_directory_without_the_module_is_refused_by_name
    error = assert_raises(Ruvoy::Paths::NotFound) do
      with_env("RUVOY_MODULE_DIR" => scratch_dir) { Ruvoy::Paths.module_directory }
    end
    assert_includes error.message, "RUVOY_MODULE_DIR"
  end

  # Envoy cannot infer where libruby lives.
  def test_the_ruby_library_directory_is_reported
    assert File.directory?(Ruvoy::Paths.ruby_library_directory)
  end

  def test_the_gemspec_installs_the_executable
    spec = Dir.chdir(root) { Gem::Specification.load("ruvoy.gemspec") }
    assert_equal [ "ruvoy" ], spec.executables
    assert_includes spec.files, "exe/ruvoy"
  end
end
