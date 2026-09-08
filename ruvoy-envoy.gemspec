# frozen_string_literal: true

envoy_version = File.read(File.join(__dir__, ".envoy-version")).strip

Gem::Specification.new do |spec|
  spec.name = "ruvoy-envoy"
  # The Envoy release this carries, not the version of ruvoy. A security fix
  # in Envoy ships as a new version here and reaches users through a plain
  # `gem update`, without waiting for a release of ruvoy.
  spec.version = envoy_version
  spec.authors = [ "Zixin Zhou" ]
  spec.email = [ "zhouzixin@apache.org" ]
  spec.license = "Apache-2.0"

  spec.summary = "The Envoy binary, packaged for ruvoy"
  spec.description = <<~TEXT
    Carries an Envoy binary from the project's own release, verified against
    its published checksums. Install it alongside ruvoy when the machine has no
    Envoy of its own; ruvoy also accepts one already on the PATH, so this gem
    is optional.
  TEXT
  spec.homepage = "https://github.com/CodePrometheus/ruvoy"
  spec.metadata = {
    "source_code_uri" => spec.homepage,
    "bug_tracker_uri" => "#{spec.homepage}/issues",
    "rubygems_mfa_required" => "true"
  }

  # It carries a binary, not compiled Ruby, so it installs anywhere ruvoy does.
  spec.required_ruby_version = ">= 3.0.0"

  # Only a binary and the path to it, so no Ruby ABI is involved.
  spec.files = [ "lib/ruvoy/envoy.rb", "exe/envoy", "LICENSE", "NOTICE" ].select do |path|
    File.exist?(File.join(__dir__, path))
  end
  spec.require_paths = [ "lib" ]

  platform = ENV["RUVOY_GEM_PLATFORM"]
  spec.platform = platform if platform
end
