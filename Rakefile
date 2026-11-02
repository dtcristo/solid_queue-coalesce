# frozen_string_literal: true

require "bundler/setup"

APP_RAKEFILE = File.expand_path("test/dummy/Rakefile", __dir__)
load "rails/tasks/engine.rake"

if Rails::VERSION::MAJOR < 8
  load "rails/tasks/statistics.rake"
end

require "bundler/gem_tasks"
require "rake/tasklib"

class ReleaseHelpers < Rake::TaskLib
  def initialize(gemspec)
    @gemspec = gemspec
    define
  end

  def define
    Rake::Task["release"].clear

    desc "Pick a version from CHANGELOG.md, then tag it, push its gem and create or update its GitHub release"
    task :release do
      release pick_version
    end
  end

  private

  def release(version)
    tag = "v#{version}"
    notes = notes_for(version)

    sh "git", "fetch", "--quiet", "origin", "main", "--tags"
    tag_pushed = !git("ls-remote", "--tags", "origin", "refs/tags/#{tag}").empty?
    gem_pushed = gem_pushed?(version)

    guard_can_publish(version, tag) unless tag_pushed && gem_pushed

    unless tag_pushed
      sh "git", "tag", "--sign", "--message", tag, tag if tag_commit(tag).empty?
      sh "git", "push", "origin", "refs/tags/#{tag}"
    end

    unless gem_pushed
      Rake::Task["build"].invoke
      sh "gem", "push", "pkg/#{@gemspec.name}-#{version}.gem"
    end

    if system("gh", "release", "view", tag, out: File::NULL, err: File::NULL)
      sh "gh", "release", "edit", tag, "--title", version, "--notes", notes, verbose: false
    else
      sh "gh", "release", "create", tag, "--verify-tag", "--title", version, "--notes", notes, verbose: false
    end
  end

  def pick_version
    versions = changelog.scan(/^## \[([^\]]+)\] - \d{4}-\d{2}-\d{2}$/).flatten
    abort "CHANGELOG.md has no dated versions" if versions.empty?

    print "Version to release (#{versions.join(", ")}) [#{versions.first}]: "
    $stdout.flush
    picked = $stdin.gets&.strip || abort("No version picked")
    picked = versions.first if picked.empty?

    versions.include?(picked) ? picked : abort("CHANGELOG.md has no dated section for #{picked}")
  end

  def notes_for(version)
    changelog[/^## \[#{Regexp.escape(version)}\] - .+?\n(.+?)^(?:## \[|\[Unreleased\]:)/m, 1].gsub(/^### /, "## ").strip
  end

  def changelog
    File.read("CHANGELOG.md")
  end

  def gem_pushed?(version)
    versions = `gem list --remote --exact --all #{@gemspec.name}`
    abort "Couldn't list the versions on RubyGems" unless $?.success?

    versions[/^#{Regexp.escape(@gemspec.name)} \((.*)\)$/, 1].to_s.split(", ").include?(version)
  end

  # Building reads the working tree, so it has to hold exactly what the tag points at.
  def guard_can_publish(version, tag)
    abort "Only #{@gemspec.version} can be published from this commit" unless version == @gemspec.version.to_s

    expected = tag_commit(tag)
    expected = git("rev-parse", "origin/main") if expected.empty?
    abort "HEAD must be at #{expected} to publish #{tag}" unless git("rev-parse", "HEAD") == expected

    Rake::Task["release:guard_clean"].invoke
  end

  def tag_commit(tag)
    git("rev-parse", "--verify", "--quiet", "refs/tags/#{tag}^{commit}")
  end

  def git(*args)
    IO.popen([ "git", *args ], &:read).strip
  end
end

ReleaseHelpers.new(Bundler::GemHelper.instance.gemspec)

class TestHelpers < Rake::TaskLib
  def initialize(databases)
    @databases = databases
    define
  end

  def define
    desc "Run tests for all databases (mysql, postgres, sqlite)"
    task :test do
      @databases.each { |database| run_test_for_database(database) }
    end

    namespace :test do
      @databases.each do |database|
        desc "Run tests for #{database} database"
        task database do
          run_test_for_database(database)
        end
      end
    end
  end

  private

  def run_test_for_database(database)
    sh("TARGET_DB=#{database} bin/setup")
    sh("TARGET_DB=#{database} bin/rails test")
  end
end

TestHelpers.new(%w[ mysql postgres sqlite ])
