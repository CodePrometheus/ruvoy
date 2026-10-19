# frozen_string_literal: true

require "json"

module Ruvoy
  # What the application registers for the runtime's lifecycle, and what the
  # runtime tells it.
  module Lifecycle
    @before_ready = []
    @on_drain = []
    @settings = {}.freeze
    @started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    class << self
      # Runtime settings as Envoy configured them, set once at boot.
      attr_reader :settings

      def load_settings(json)
        @settings = JSON.parse(json).freeze
      end

      # Runs after the application loaded and before Envoy accepts traffic; a
      # block that raises keeps Envoy from becoming ready.
      def before_ready(&block)
        @before_ready << block
      end

      # Runs when Envoy begins draining. Requests are still served; this is
      # for closing persistent connections and stopping background work.
      def on_drain(&block)
        @on_drain << block
      end

      def run_before_ready
        hooks = @before_ready.dup
        Sync { hooks.each(&:call) }
      end

      def drain
        hooks = @on_drain.dup
        hooks.each do |hook|
          hook.call
        rescue StandardError => error
          warn "[ruvoy] drain hook failed: #{error.class}: #{error.message}"
        end
      end

      def uptime
        Process.clock_gettime(Process::CLOCK_MONOTONIC) - @started_at
      end
    end
  end

  # Envoy's admin interface, the part of it that asks about Ruby.
  module Admin
    ROUTES = {
      "/ruvoy" => :overview,
      "/ruvoy/gc" => :gc,
      "/ruvoy/fibers" => :fibers,
      "/ruvoy/config" => :config
    }.freeze

    class << self
      def handle(envelope)
        path, query = envelope.path.split("?", 2)
        route = ROUTES[path.chomp("/")] or return envelope.reply(404, "no such ruvoy endpoint\n")
        envelope.reply(200, "#{JSON.pretty_generate(send(route, envelope, query.to_s))}\n")
      rescue StandardError => error
        envelope.reply(500, "#{error.class}: #{error.message}\n")
      end

      private

      def overview(envelope, _query)
        {
          "version" => Lifecycle.settings["version"],
          "ruby" => RUBY_DESCRIPTION,
          "rackup" => Lifecycle.settings["rackup"],
          "uptime_seconds" => Lifecycle.uptime.round(1),
          "inflight_requests" => envelope.inflight,
          "fibers" => live_fibers.size,
          "threads" => Thread.list.size,
          "heap_live_slots" => GC.stat(:heap_live_slots)
        }
      end

      def gc(envelope, query)
        if envelope.http_method == "POST"
          GC.start
          GC.compact if query.include?("compact=1")
        end
        GC.stat
      end

      def fibers(_envelope, _query)
        live_fibers.map { |fiber| fiber.backtrace(0, 16) || [] }
      end

      def config(_envelope, _query)
        Lifecycle.settings
      end

      def live_fibers
        ObjectSpace.each_object(Fiber).select { |fiber| fiber.alive? && fiber != Fiber.current }
      end
    end
  end
end
