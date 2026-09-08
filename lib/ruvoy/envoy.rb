# frozen_string_literal: true

module Ruvoy
  # Where the ruvoy-envoy gem put the Envoy it carries.
  #
  # This file ships in that gem, not in ruvoy itself, so the constant existing
  # is what tells the command line the binary is installed.
  module Envoy
    BINARY = File.expand_path("../../exe/envoy", __dir__)
  end
end
