# frozen_string_literal: true

require "test_helper"

# These run on separate connections and threads on purpose: what they test is
# the locking in admission and start, and any error raised by a thread,
# deadlocks included, fails the test when the thread is joined.
class SolidQueue::CoalesceConcurrencyTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class DropJob < ApplicationJob
    coalesce key: ->(group, *) { group }

    def perform(*)
      RACED_RUNS << provider_job_id
    end
  end

  RACED_RUNS = Concurrent::Array.new

  setup do
    RACED_RUNS.clear
    @process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "race-worker")
  end

  test "concurrent first calls create one job" do
    5.times do |round|
      results = race(8) { DropJob.perform_later("g#{round}") }

      assert_equal 1, results.count { |active_job| !active_job.coalesced? }
      assert_equal [ results.first.provider_job_id ], results.map(&:provider_job_id).uniq
    end

    assert_equal 5, SolidQueue::Job.count
  end

  test "bulk enqueues racing on overlapping keys create one job per key" do
    race(6) do
      SolidQueue::Job.enqueue_all(%w[ a b c d ].shuffle.flat_map { |group| 3.times.map { DropJob.new(group) } })
    end

    assert_equal 4, SolidQueue::Job.count
    assert_equal 4, SolidQueue::CoalesceKey.count
  end

  test "calls racing starts never leave a key without a job" do
    done = Concurrent::AtomicBoolean.new(false)
    worker = Thread.new { work_until(done) }

    results = begin
      race(6) { 40.times.map { DropJob.perform_later("g") } }.flatten
    ensure
      done.make_true
      worker.join rescue nil
    end
    worker.value
    drain

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal results.count { |active_job| !active_job.coalesced? }, RACED_RUNS.size
  end

  test "a call waiting on a start that deletes the key opens a new job" do
    skip_on_sqlite

    first = DropJob.perform_later("g")
    job = SolidQueue::Job.find(first.provider_job_id)
    locked = Concurrent::Event.new

    starter = in_thread do
      SolidQueue::Record.transaction do
        SolidQueue::CoalesceKey.lock_for_release(job.id)
        locked.set
        sleep 0.3
        SolidQueue::Job.find(job.id).start_coalesced_run
      end
    end

    locked.wait(5)
    second = in_thread { DropJob.perform_later("g") }.value
    starter.value

    assert_not second.coalesced?
    assert_not_equal job.id, second.provider_job_id
    assert_equal second.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
  end

  test "calls racing discards of the open job don't fail" do
    race(5) do |thread|
      20.times do
        if thread.zero?
          SolidQueue::Job.where(class_name: DropJob.name).find_each(&:discard)
        else
          DropJob.perform_later("g")
        end
      end
    end

    if SolidQueue::CoalesceKey.any?
      assert_equal SolidQueue::CoalesceKey.sole.job_id, SolidQueue::Job.sole.id
    else
      assert_equal 0, SolidQueue::Job.count
    end
  end

  test "a deadlock retries the admission up to its limit, then raises" do
    SolidQueue::Job.expects(:create!).times(6).raises(ActiveRecord::Deadlocked)

    assert_raises(SolidQueue::Job::EnqueueError) { DropJob.perform_later("g") }

    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a deadlock inside the app's transaction isn't retried" do
    SolidQueue::Job.expects(:create!).once.raises(ActiveRecord::Deadlocked)

    assert_raises(SolidQueue::Job::EnqueueError) do
      # Rails 7.2 holds a perform_later back until the transaction commits
      SolidQueue::Record.transaction { SolidQueue::Job.enqueue(DropJob.new("g")) }
    end

    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  private
    def work_until(done)
      SolidQueue::Record.connection_pool.with_connection do
        until done.true?
          perform_claimed
          sleep 0.005
        end
      end
    end

    def drain
      nil until perform_claimed.empty? && SolidQueue::ReadyExecution.none?
    end

    def perform_claimed
      SolidQueue::ReadyExecution.claim("*", 2, @process.id).each(&:perform)
    end

    # Runs the block in that many threads with a connection each, all starting
    # at once, and returns their results. Thread#value re-raises their errors.
    def race(count)
      barrier = Concurrent::CyclicBarrier.new(count)

      threads = count.times.map do |i|
        in_thread do
          barrier.wait(10)
          yield i
        end
      end

      # Let all of them finish, so a failure can't leak writes into the next test
      threads.each { |thread| thread.join rescue nil }
      threads.map(&:value)
    end

    def in_thread(&block)
      Thread.new { SolidQueue::Record.connection_pool.with_connection(&block) }
    end

    def skip_on_sqlite
      skip "Row locks don't exist on SQLite" if SolidQueue::Record.connection_pool.db_config.adapter == "sqlite3"
    end
end
