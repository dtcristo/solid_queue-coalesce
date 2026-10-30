require_relative "lib/solid_queue/coalesce/version"

Gem::Specification.new do |spec|
  spec.name        = "solid_queue-coalesce"
  spec.version     = SolidQueue::Coalesce::VERSION
  spec.authors     = [ "David Cristofaro" ]
  spec.email       = [ "david@dtcristo.com" ]
  spec.homepage    = "https://github.com/dtcristo/solid_queue-coalesce"
  spec.summary     = "Coalescing for Solid Queue."
  spec.description = "Lets many perform_later calls with the same key share one pending Solid Queue job."
  spec.license     = "MIT"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/dtcristo/solid_queue-coalesce"

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    Dir["{app,lib}/**/*", "MIT-LICENSE", "README.md"]
  end

  spec.required_ruby_version = ">= 3.2"

  # Patches private Solid Queue methods, so each new minor has to be tested first
  spec.add_dependency "solid_queue", "~> 1.7.0"

  spec.add_development_dependency "appraisal"
  spec.add_development_dependency "debug", "~> 1.9"
  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "mocha"
  spec.add_development_dependency "mysql2"
  spec.add_development_dependency "pg"
  spec.add_development_dependency "sqlite3"
  spec.add_development_dependency "rubocop-rails-omakase"
  spec.add_development_dependency "rdoc"
  spec.add_development_dependency "logger"
end
