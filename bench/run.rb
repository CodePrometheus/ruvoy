#!/usr/bin/env ruby
# frozen_string_literal: true

# Runs the benchmark campaign. Every knob is an RUVOY_BENCH_* variable; the
# defaults and the guards full mode adds are in bench/campaign/settings.rb.

require_relative "campaign"

exit Bench::Campaign.new(ENV).run
