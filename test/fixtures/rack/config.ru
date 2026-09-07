# frozen_string_literal: true

# Non-ASCII on purpose: a rackup is read through Encoding.default_external. Café ✓

require_relative "app"

use Rack::Lint
use RackCompatibilityMiddleware
run RackCompatibilityApp.new
