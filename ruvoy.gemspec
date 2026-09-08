# frozen_string_literal: true

require_relative "lib/ruvoy/version"

Gem::Specification.new do |spec|
  spec.name = "ruvoy"
  spec.version = Ruvoy::VERSION
  spec.authors = [ "Zixin Zhou" ]
  spec.email = [ "zhouzixin@apache.org" ]
  spec.license = "Apache-2.0"

  spec.summary = "A Rack application server that runs inside Envoy as a dynamic module"
  spec.description = <<~TEXT
    Ruvoy is a Ruby application runtime inside Envoy. It loads an ordinary
    rackup into a CRuby VM embedded in a dynamic module and answers requests as
    a terminal HTTP filter, so a request reaches Rack as owned data rather than
    over a second HTTP connection to a separate application server. HTTP/1.1,
    HTTP/2, TLS, timeouts and the downstream connection lifecycle stay with
    Envoy. Scheduler-aware Ruby I/O overlaps on fibers; CPU-bound Ruby does not
    run in parallel, since the process holds one VM.
  TEXT
  spec.homepage = "https://github.com/CodePrometheus/ruvoy"
  spec.metadata = {
    "source_code_uri" => spec.homepage,
    "bug_tracker_uri" => "#{spec.homepage}/issues",
    "rubygems_mfa_required" => "true"
  }

  # The module embeds the Ruby it was built against, so a release supports the
  # ABIs it was actually built and tested for and no others.
  spec.required_ruby_version = ">= 4.0.0"

  # lib/ruvoy/envoy.rb belongs to the ruvoy-envoy gem, which shares this
  # namespace directory but never this file list.
  spec.files = Dir["lib/**/*.rb"].grep_v(%r{\Alib/ruvoy/envoy\.rb\z}) +
               Dir["exe/ruvoy", "LICENSE", "README.md"]
  spec.bindir = "exe"
  spec.executables = [ "ruvoy" ]
  spec.require_paths = [ "lib" ]

  # Required by the Ruby VM embedded in the module, not by the command line.
  # Envoy loads the module with no bundle activated, so these have to arrive
  # through the gem rather than through the application's Gemfile.
  spec.add_dependency "async", "~> 2.45"
  spec.add_dependency "rack", "~> 3.2"

  # A platform build carries the module for the ABIs it was built against; the
  # plain gem carries only the command line, which then looks for the module in
  # a source checkout. Built by scripts/build-gem.sh.
  #
  # Envoy is not here. It ships as ruvoy-envoy, versioned by the Envoy release
  # it carries, so a security fix reaches users without waiting for a release
  # of this gem. Installing it is optional: the command line also accepts an
  # Envoy already on the machine.
  platform = ENV["RUVOY_GEM_PLATFORM"]
  if platform
    spec.platform = platform
    spec.files += Dir["lib/ruvoy/*/libruvoy_fiber.so"]
  end
end
