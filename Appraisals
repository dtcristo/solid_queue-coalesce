# frozen_string_literal: true

appraise "rails-7-1" do
  gem "railties", "~> 7.1.0"
  # Active Support passes json 3 a quirks_mode option it no longer accepts
  gem "json", "< 3"
end

appraise "rails-7-2" do
  gem "railties", "~> 7.2.0"
end

appraise "rails-8-0" do
  gem "railties", "~> 8.0.0"
  # Active Support passes json 3 a quirks_mode option it no longer accepts
  gem "json", "< 3"
end

appraise "rails-8-1" do
  gem "railties", "~> 8.1.0"
end

appraise "rails-main" do
  gem "railties", github: "rails/rails", branch: "main"
end

appraise "solid-queue-1-7-0" do
  gem "railties", "~> 8.1.0"
  gem "solid_queue", "1.7.0"
end

appraise "solid-queue-1-7-1" do
  gem "railties", "~> 8.1.0"
  gem "solid_queue", "1.7.1"
end

appraise "solid-queue-1-7-2" do
  gem "railties", "~> 8.1.0"
  gem "solid_queue", "1.7.2"
end

appraise "solid-queue-main" do
  gem "railties", "~> 8.1.0"
  gem "solid_queue", github: "rails/solid_queue", branch: "main"
end
