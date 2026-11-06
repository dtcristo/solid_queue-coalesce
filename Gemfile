source "https://rubygems.org"
git_source(:github) { |repo| "https://github.com/#{repo}.git" }

# Specify your gem's dependencies in solid_queue-coalesce.gemspec.
gemspec

group :development do
  gem "appraisal"
  gem "debug", "~> 1.9"
  gem "minitest", "~> 5.0"
  gem "mocha"
  gem "mysql2"
  gem "pg"
  gem "sqlite3"
  gem "rubocop-rails-omakase"
  gem "rdoc"
  gem "logger"

  # Its latest release, so CI checks the two gems still work together
  gem "solid_queue-rate_limit"
end
