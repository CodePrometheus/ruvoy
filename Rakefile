# frozen_string_literal: true

require "rake/testtask"

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
