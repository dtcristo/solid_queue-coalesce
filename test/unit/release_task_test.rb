# frozen_string_literal: true

require "test_helper"
require "open3"
require "tmpdir"

class ReleaseTaskTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  VERSION = SolidQueue::Coalesce::VERSION
  TAG = "v#{VERSION}"
  ROOT = File.expand_path("../..", __dir__)

  setup do
    @tmp = Dir.mktmpdir("release_task_test")
    @repo = File.join(@tmp, "repo")
    @origin = File.join(@tmp, "origin.git")
    @stubs = File.join(@tmp, "stubs")

    FileUtils.mkdir_p([ @repo, @stubs ])
    File.write(File.join(@stubs, "published"), "0.0.1")
    write_stubs

    git_in(@tmp, "init", "--quiet", "--bare", @origin)
    assert system("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", File.join(@tmp, "signing_key"))
    build_repo
  end

  teardown do
    FileUtils.rm_rf(@tmp)
  end

  test "pressing enter tags the newest version, pushes its gem and creates its release" do
    output, status = run_release("\n")

    assert status.success?, output
    assert_equal [ "gem build", "gem push pkg/solid_queue-coalesce-#{VERSION}.gem", "gh release view #{TAG}", "gh release create #{TAG}" ], log
    assert_match(%r{refs/tags/#{Regexp.escape(TAG)}$}, git("ls-remote", "--tags", @origin))
    assert_includes output, "Version to release (#{VERSION}, 0.0.2, 0.0.1) [#{VERSION}]:"
  end

  test "signs the tag, with the tag as its message" do
    run_release("\n")

    tag = git_in(@origin, "cat-file", "tag", TAG)
    assert_match(/^#{Regexp.escape(TAG)}\n-----BEGIN SSH SIGNATURE-----$/, tag)

    File.write(File.join(@tmp, "allowed_signers"), "test@example.com #{File.read(File.join(@tmp, "signing_key.pub"))}")
    git("-c", "gpg.ssh.allowedSignersFile=#{File.join(@tmp, "allowed_signers")}", "tag", "--verify", TAG)
  end

  test "publishes nothing when it can't sign the tag" do
    git("config", "user.signingkey", File.join(@tmp, "missing_key"))

    _output, status = run_release("\n")

    assert_not status.success?
    assert_nothing_published
  end

  test "the release notes are the version's section with its headings raised" do
    run_release("#{VERSION}\n")

    assert_equal "Newest summary, see [the guide](https://example.com/guide).\n\n## Added\n\n- A new thing.\n\n## Fixed\n\n- A bug.", notes(TAG)
  end

  test "running it again updates the release from the latest changelog and publishes nothing" do
    run_release("\n")
    File.write(File.join(@stubs, "log"), "")

    File.write(File.join(@repo, "CHANGELOG.md"), changelog.sub("A new thing.", "A better thing."))
    git("commit", "--quiet", "--all", "--message", "Correct the changelog")
    git("push", "--quiet", "origin", "main")

    output, status = run_release("\n")

    assert status.success?, output
    assert_equal [ "gh release view #{TAG}", "gh release edit #{TAG}" ], log
    assert_match(/A better thing\./, notes(TAG))
  end

  test "an older version that's already published only gets its release" do
    git("tag", "--annotate", "--message", "v0.0.1", "v0.0.1")
    git("push", "--quiet", "origin", "refs/tags/v0.0.1")

    output, status = run_release("0.0.1\n")

    assert status.success?, output
    assert_equal [ "gh release view v0.0.1", "gh release create v0.0.1" ], log
    assert_equal "Older summary.\n\n## Fixed\n\n- An old bug.", notes("v0.0.1")
  end

  test "picks up where a failed run stopped" do
    git("tag", "--annotate", "--message", TAG, TAG)
    git("push", "--quiet", "origin", "refs/tags/#{TAG}")

    output, status = run_release("\n")

    assert status.success?, output
    assert_equal [ "gem build", "gem push pkg/solid_queue-coalesce-#{VERSION}.gem", "gh release view #{TAG}", "gh release create #{TAG}" ], log
  end

  test "refuses a version the changelog doesn't have" do
    output, status = run_release("9.9.9\n")

    assert_not status.success?
    assert_match(/no dated section for 9\.9\.9/, output)
    assert_nothing_published
  end

  test "refuses to publish a commit that isn't on origin/main" do
    File.write(File.join(@repo, "README.md"), "Unpushed\n")
    git("add", "README.md")
    git("commit", "--quiet", "--message", "Unpushed")

    output, status = run_release("\n")

    assert_not status.success?
    assert_match(/HEAD must be at/, output)
    assert_nothing_published
  end

  test "refuses to publish a dirty working tree" do
    File.write(File.join(@repo, "CHANGELOG.md"), changelog + "\nUncommitted\n")

    output, status = run_release("\n")

    assert_not status.success?
    assert_match(/files that need to be committed/, output)
    assert_nothing_published
  end

  test "refuses to publish an unpublished version the working tree isn't at" do
    output, status = run_release("0.0.2\n")

    assert_not status.success?
    assert_match(/Only #{Regexp.escape(VERSION)} can be published/, output)
    assert_nothing_published
  end

  private

  def build_repo
    %w[ app lib test ].each { |path| File.symlink(File.join(ROOT, path), File.join(@repo, path)) }
    %w[ Rakefile solid_queue-coalesce.gemspec ].each { |path| FileUtils.cp(File.join(ROOT, path), @repo) }
    File.write(File.join(@repo, ".gitignore"), "/pkg\n")
    File.write(File.join(@repo, "CHANGELOG.md"), <<~MARKDOWN)
      # Changelog

      ## [Unreleased]

      ## [#{VERSION}] - 2026-01-03

      Newest summary, see [the guide](https://example.com/guide).

      ### Added

      - A new thing.

      ### Fixed

      - A bug.

      ## [0.0.2] - 2026-01-02

      Unpublished summary.

      ## [0.0.1] - 2026-01-01

      Older summary.

      ### Fixed

      - An old bug.

      [Unreleased]: https://example.com/compare/#{TAG}...HEAD
      [#{VERSION}]: https://example.com/compare/v0.0.1...#{TAG}
      [0.0.1]: https://example.com/releases/tag/v0.0.1
    MARKDOWN

    git("init", "--quiet", "--initial-branch", "main")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.com")
    git("config", "core.hooksPath", File::NULL)
    git("config", "commit.gpgsign", "false")
    git("config", "gpg.format", "ssh")
    git("config", "user.signingkey", File.join(@tmp, "signing_key"))
    git("remote", "add", "origin", @origin)
    git("add", "--all")
    git("commit", "--quiet", "--message", "Initial")
    git("push", "--quiet", "--set-upstream", "origin", "main")
  end

  def write_stubs
    write_stub "gem", <<~SH
      case "$1" in
        list) echo "*** REMOTE GEMS ***"; echo; echo "solid_queue-coalesce ($(cat "$STUBS/published"))" ;;
        build) echo "gem build" >> "$STUBS/log"; touch "solid_queue-coalesce-#{VERSION}.gem" ;;
        push) echo "gem push $2" >> "$STUBS/log"; printf ', %s' "#{VERSION}" >> "$STUBS/published" ;;
      esac
    SH

    write_stub "gh", <<~SH
      echo "gh $1 $2 $3" >> "$STUBS/log"
      [ "$2" = view ] && { [ -e "$STUBS/release-$3" ]; exit $?; }
      for notes; do :; done
      printf '%s' "$notes" > "$STUBS/notes-$3"
      touch "$STUBS/release-$3"
    SH
  end

  def write_stub(name, body)
    path = File.join(@stubs, name)
    File.write(path, "#!/bin/sh\n#{body}")
    File.chmod(0o755, path)
  end

  def run_release(input)
    env = { "PATH" => "#{@stubs}:#{ENV["PATH"]}", "STUBS" => @stubs, "RAILS_ENV" => "test" }
    Open3.capture2e(env, "bundle", "exec", "rake", "release", chdir: @repo, stdin_data: input)
  end

  def assert_nothing_published
    assert_empty git("ls-remote", "--tags", @origin)
    assert_empty log.grep(/gem push|gh release (create|edit)/)
  end

  def log
    path = File.join(@stubs, "log")
    File.exist?(path) ? File.readlines(path, chomp: true) : []
  end

  def notes(tag)
    File.read(File.join(@stubs, "notes-#{tag}"))
  end

  def changelog
    File.read(File.join(@repo, "CHANGELOG.md"))
  end

  def git(*args)
    git_in(@repo, *args)
  end

  def git_in(dir, *args)
    output, status = Open3.capture2e("git", *args, chdir: dir)
    assert status.success?, output
    output
  end
end
