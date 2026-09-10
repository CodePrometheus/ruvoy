source "https://rubygems.org"

gem "async", "= 2.45.1"
# A bundled gem since Ruby 3.4, so the benchmark summariser can load it under bundler.
gem "csv", "= 3.3.5"
gem "falcon", "= 0.57.0"
# A bundled gem since Ruby 3.5, so the benchmark app's nanosleep call needs it declared.
gem "fiddle", "= 1.1.8"
gem "puma", "= 8.0.2"
gem "rack", "= 3.2.7"

# End-to-end suites only; kept out of the default install for the same reason.
group :test, optional: true do
  gem "minitest", "= 6.0.6"
  gem "rake", "= 13.3.1"
end

# Linting only; kept out of the default install so runtime images stay lean.
group :lint, optional: true do
  gem "rubocop-rails-omakase", require: false
end
