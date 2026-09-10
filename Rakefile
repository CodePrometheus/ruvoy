# frozen_string_literal: true

require "rake/testtask"
require_relative "tasks/build"
require_relative "tasks/oha"

namespace :build do
  desc "Build an Envoy module into build/modules: fiber, sync or baseline"
  task :module, [ :kind ] do |_task, args|
    Build.envoy_module(args[:kind] || "fiber")
  end

  desc "Build the release gems: ruvoy, ruvoy-envoy or all"
  task :gem, [ :target ] do |_task, args|
    Build.gems(args[:target] || "all")
  end
end

namespace :check do
  desc "Check that Envoy worker sources never touch the Ruby VM"
  task :worker_boundary do
    Build.check_worker_boundary
  end
end

namespace :tools do
  desc "Install the pinned load generator into .tools/oha"
  task :oha do
    Oha.install
  end
end

desc "Run the benchmark campaign, configured through RUVOY_BENCH_* variables"
task :bench do
  ruby File.join(__dir__, "bench", "run.rb")
end

# One task per suite, so a failing suite reruns alone.
namespace :e2e do
  FileList["test/e2e/*_test.rb"].each do |file|
    Rake::TestTask.new(File.basename(file, "_test.rb")) do |task|
      task.test_files = [ file ]
    end
  end
end

Rake::TestTask.new(:e2e) do |task|
  task.test_files = FileList["test/e2e/*_test.rb"]
end
