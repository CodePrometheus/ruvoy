# frozen_string_literal: true

require "rbconfig"

module Ruvoy
  # Locates the three things a run needs: the Ruby the module was built
  # against, the module itself, and Envoy.
  #
  # An installed gem carries the module and Envoy; a source checkout has
  # neither in those places, so each lookup falls back to where a build leaves
  # them. Both paths matter — the checkout is how this project tests itself.
  module Paths
    class NotFound < StandardError; end

    ROOT = File.expand_path("../..", __dir__)
    MODULE_BASENAME = "libruvoy_fiber.so"

    class << self
      # Envoy resolves a module by name against this directory.
      def module_directory
        from_env = ENV["RUVOY_MODULE_DIR"]
        return require_directory(from_env, "RUVOY_MODULE_DIR") if from_env

        candidates = [ packaged_module_directory, File.join(ROOT, "build", "modules") ]
        found = candidates.find { |directory| File.exist?(File.join(directory, MODULE_BASENAME)) }
        return found if found

        raise NotFound, "no #{MODULE_BASENAME} in: #{candidates.join(", ")}"
      end

      def envoy_binary
        from_env = ENV["RUVOY_ENVOY"]
        return require_executable(from_env, "RUVOY_ENVOY") if from_env

        packaged = packaged_envoy
        return packaged if packaged

        on_path = which("envoy")
        return on_path if on_path

        raise NotFound,
              "no Envoy found: install the ruvoy-envoy gem, put one on PATH, " \
              "or point RUVOY_ENVOY at it"
      end

      # Present only where the ruvoy-envoy gem is installed, which is optional:
      # an Envoy the machine already has serves just as well.
      def packaged_envoy
        require "ruvoy/envoy"
        Envoy::BINARY if File.executable?(Envoy::BINARY)
      rescue LoadError
        nil
      end

      # The module links against libruby, and Envoy loads it with no Ruby
      # process to inherit a search path from.
      def ruby_library_directory
        RbConfig::CONFIG.fetch("libdir")
      end

      # Precompiled gems carry one module per Ruby ABI, since the module links
      # against the libruby of the Ruby it was built for.
      def packaged_module_directory
        File.join(ROOT, "lib", "ruvoy", RbConfig::CONFIG.fetch("ruby_version"))
      end

      private

      def require_directory(path, source)
        return path if File.exist?(File.join(path, MODULE_BASENAME))

        raise NotFound, "#{source} is #{path}, which holds no #{MODULE_BASENAME}"
      end

      def require_executable(path, source)
        return path if File.executable?(path)

        raise NotFound, "#{source} is #{path}, which is not executable"
      end

      def which(command)
        ENV.fetch("PATH", "").split(File::PATH_SEPARATOR).filter_map do |directory|
          candidate = File.join(directory, command)
          candidate if File.executable?(candidate) && !File.directory?(candidate)
        end.first
      end
    end
  end
end
