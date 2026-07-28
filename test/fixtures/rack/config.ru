# frozen_string_literal: true

require_relative "app"

use Rack::Lint
use RackCompatibilityMiddleware
run RackCompatibilityApp.new
