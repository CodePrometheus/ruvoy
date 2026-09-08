# frozen_string_literal: true

require "optparse"
require "tempfile"
require "ruvoy/envoy_config"
require "ruvoy/paths"
require "ruvoy/version"

module Ruvoy
  # Turns `ruvoy config.ru` into a running Envoy that serves it.
  class CLI
    DEFAULT_RACKUP = "config.ru"
    DEFAULT_ADDRESS = "127.0.0.1"
    DEFAULT_PORT = 8080

    Options = Struct.new(:rackup, :address, :port, :admin_port, :print_config, keyword_init: true)

    def initialize(argv, output: $stdout, error: $stderr)
      @argv = argv
      @output = output
      @error = error
    end

    def run
      options = catch(:finished) { parse }
      return 0 unless options.is_a?(Options)

      rackup = File.expand_path(options.rackup)
      unless File.file?(rackup)
        @error.puts("ruvoy: no such rackup: #{options.rackup}")
        return 1
      end

      yaml = EnvoyConfig.to_yaml(
        rackup: rackup,
        address: options.address,
        port: options.port,
        admin_port: options.admin_port
      )
      if options.print_config
        @output.puts(yaml)
        return 0
      end

      launch(yaml)
    rescue Paths::NotFound => error
      @error.puts("ruvoy: #{error.message}")
      1
    end

    private

    def launch(yaml)
      config = Tempfile.new([ "ruvoy", ".yaml" ])
      config.write(yaml)
      config.close
      # Replacing this process rather than supervising a child keeps signals,
      # exit status and container lifecycle in Envoy's hands.
      Kernel.exec(environment, Paths.envoy_binary, "-c", config.path)
    end

    def environment
      {
        "ENVOY_DYNAMIC_MODULES_SEARCH_PATH" => Paths.module_directory,
        # Envoy dlopens the module with no Ruby process to inherit a search
        # path from, so libruby has to be findable here.
        "LD_LIBRARY_PATH" => [ Paths.ruby_library_directory, ENV["LD_LIBRARY_PATH"] ]
          .compact.reject(&:empty?).join(File::PATH_SEPARATOR)
      }
    end

    def parse
      options = Options.new(
        rackup: DEFAULT_RACKUP,
        address: DEFAULT_ADDRESS,
        port: DEFAULT_PORT,
        admin_port: nil,
        print_config: false
      )
      parser = build_parser(options)
      parser.parse!(@argv)
      options.rackup = @argv.shift unless @argv.empty?
      options
    end

    def build_parser(options)
      OptionParser.new do |parser|
        parser.banner = "Usage: ruvoy [options] [#{DEFAULT_RACKUP}]"
        parser.on("-p", "--port PORT", Integer, "Listen on PORT (default #{DEFAULT_PORT})") do |port|
          options.port = port
        end
        parser.on("-a", "--address ADDRESS", "Bind to ADDRESS (default #{DEFAULT_ADDRESS})") do |address|
          options.address = address
        end
        parser.on("--admin-port PORT", Integer, "Expose Envoy's admin interface on PORT") do |port|
          options.admin_port = port
        end
        parser.on("--print-config", "Print the generated Envoy configuration and exit") do
          options.print_config = true
        end
        parser.on("-v", "--version", "Print the version and exit") do
          @output.puts(VERSION)
          throw :finished
        end
        parser.on("-h", "--help", "Print this message and exit") do
          @output.puts(parser)
          throw :finished
        end
      end
    end
  end
end
