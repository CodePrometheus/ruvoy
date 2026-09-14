# frozen_string_literal: true

require "rails"
require "active_record/railtie"
require "action_controller/railtie"

module RailsApp
  # A small production-mode application the suites run on Ruvoy and on a
  # reference server. It writes nothing under its own directory.
  class Application < Rails::Application
    config.load_defaults 8.1
    config.root = File.expand_path("..", __dir__)
    config.eager_load = true
    config.enable_reloading = false
    config.consider_all_requests_local = false
    config.secret_key_base = ENV.fetch("SECRET_KEY_BASE")
    config.logger = ActiveSupport::Logger.new($stdout)
    config.log_level = :warn
    config.cache_store = :null_store
    # Each request's fiber checks out its own connection; under the thread
    # default every fiber on the runtime thread would share one.
    config.active_support.isolation_level = ENV.fetch("RAILS_ISOLATION_LEVEL", "fiber").to_sym
  end
end
