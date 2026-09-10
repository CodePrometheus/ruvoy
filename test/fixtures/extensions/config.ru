# frozen_string_literal: true

require_relative "app"

use Rack::Lint
run ExtensionsApp.new
