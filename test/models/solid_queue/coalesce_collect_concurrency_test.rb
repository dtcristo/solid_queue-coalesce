# frozen_string_literal: true

require "test_helper"

# These run on separate connections and threads on purpose: what they test is
# the locking between appends, starts and early flush, and any error raised by
# a thread, deadlocks included, fails the test when the thread is joined.
class SolidQueue::CoalesceCollectConcurrencyTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class CollectAllJob < ApplicationJob
    coalesce key: ->(group, *) { group }, collect: true, limit: 10_000

    def perform(calls)
      RACED_BATCHES << [ provider_job_id, calls ]
    end
  end

  class CollectFiveJob < ApplicationJob
    coalesce key: ->(group, *) { group }, collect: true, limit: 5

    def perform(calls)
      RACED_BATCHES << [ provider_job_id, calls ]
    end
  end

  class CollectFiveLimitedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, collect: true, limit: 5
    limits_concurrency key: "shared", to: 1

    def perform(calls)
      RACED_BATCHES << [ provider_job_id, calls ]
    end
  end

  RACED_BATCHES = Concurrent::Array.new

  setup do
    RACED_BATCHES.clear
    @process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "race-worker")
  end

  test "concurrent first calls produce one open job and keep every collected row" do
    5.times do |round|
      results = race(8) { |thread| 5.times.map { |i| CollectAllJob.perform_later("g#{round}", thread, i) } }.flatten

      assert_equal [ results.first.provider_job_id ], results.map(&:provider_job_id).uniq
      assert_equal 1, results.count { |active_job| !active_job.coalesced? }
      assert results.all?(&:successfully_enqueued?)
      assert_equal 40, SolidQueue::CoalesceKey.find_by(job_id: results.first.provider_job_id).coalesced_arguments.count
    end

    assert_equal 5, SolidQueue::Job.count
    assert_equal 5, SolidQueue::CoalesceKey.count
  end

  test "bulk enqueues racing on overlapping keys lose nothing" do
    race(6) do |thread|
      SolidQueue::Job.enqueue_all(%w[ a b c d ].shuffle.flat_map { |group| 3.times.map { |i| CollectAllJob.new(group, thread, i) } })
    end

    assert_equal 4, SolidQueue::Job.count
    assert_equal 6 * 4 * 3, SolidQueue::CoalescedArgument.waiting.count
  end

  test "appends racing starts lose nothing, strand nothing and claim every row once" do
    calls = race_appends_against_workers(CollectFiveJob, appenders: 6, calls_per_appender: 40, workers: 3)

    assert_every_call_ran_once calls
    assert RACED_BATCHES.all? { |_, batch| batch.size <= 5 }
    assert_equal 0, SolidQueue::CoalescedArgument.waiting.count
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_batches_match_claimed_rows
  end

  test "appends racing starts with several keys and a blocking concurrency limit" do
    calls = race_appends_against_workers(CollectFiveLimitedJob, appenders: 4, calls_per_appender: 30, workers: 3, groups: %w[ a b ])

    assert_every_call_ran_once calls
    assert RACED_BATCHES.all? { |_, batch| batch.size <= 5 }
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a row appended while a start waits for the key's lock isn't deleted with the key" do
    skip_on_sqlite

    first = CollectFiveJob.perform_later("g", 0)
    job = SolidQueue::Job.find(first.provider_job_id)
    appending = Concurrent::Event.new
    starting = Concurrent::Event.new

    appender = in_thread do
      SolidQueue::Record.transaction do
        SolidQueue::CoalesceKey.admit("#{CollectFiveJob.name}/g").collect([ CollectFiveJob.new("g", 1) ])
        appending.set
        starting.wait(5)
        sleep 0.3
      end
    end

    appending.wait(5)
    starter = in_thread do
      starting.set
      SolidQueue::Job.find(job.id).start_coalesced_run
    end

    run = starter.value
    appender.value

    assert_equal 2, run["calls"].size
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "an append waiting on a start that deletes the key opens a new job" do
    skip_on_sqlite

    first = CollectFiveJob.perform_later("g", 0)
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
    second = in_thread { CollectFiveJob.perform_later("g", 1) }.value
    starter.value

    assert_not second.coalesced?
    assert_not_equal job.id, second.provider_job_id
    assert_equal 1, SolidQueue::CoalescedArgument.waiting.count
    assert_equal second.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
  end

  test "appends racing discards of the open job don't fail, and every append ends up on a job" do
    rounds = 20
    results = race(5) do |thread|
      rounds.times.map do |i|
        if thread.zero?
          SolidQueue::Job.where(class_name: CollectAllJob.name).find_each(&:discard)
          nil
        else
          CollectAllJob.perform_later("g", thread, i)
        end
      end
    end

    assert results.flatten.compact.all?(&:successfully_enqueued?)

    # Whatever wasn't discarded is on the last open job
    if SolidQueue::CoalesceKey.any?
      assert_equal SolidQueue::CoalesceKey.sole.job_id, SolidQueue::Job.sole.id
    else
      assert_equal 0, SolidQueue::CoalescedArgument.count
    end
  end

  test "concurrent early flushes and the dispatcher dispatch a job once" do
    5.times do |round|
      group = "g#{round}"
      first = CollectFiveLimitedJob.set(wait: 1.minute).perform_later(group, 0)
      3.times { |i| CollectFiveLimitedJob.set(wait: 1.minute).perform_later(group, i + 1) }
      job_id = first.provider_job_id

      # Due now, so the dispatcher competes with the flushes
      SolidQueue::ScheduledExecution.where(job_id: job_id).update_all(scheduled_at: 1.second.ago)

      race(6) do |thread|
        if thread.zero?
          SolidQueue::ScheduledExecution.dispatch_next_batch(10)
        else
          CollectFiveLimitedJob.set(wait: 1.minute).perform_later(group, 10 + thread)
        end
      end

      assert_equal 0, SolidQueue::ScheduledExecution.where(job_id: job_id).count
      assert_equal 1, SolidQueue::ReadyExecution.where(job_id: job_id).count + SolidQueue::BlockedExecution.where(job_id: job_id).count
    end

    # Each of the five took its turn at the only semaphore, once
    assert_equal 1, SolidQueue::ReadyExecution.count
    assert_equal 4, SolidQueue::BlockedExecution.count
    assert_equal 0, SolidQueue::Semaphore.sole.value
  end

  private
    def race_appends_against_workers(job_class, appenders:, calls_per_appender:, workers:, groups: %w[ g ])
      done = Concurrent::AtomicBoolean.new(false)
      worker_threads = workers.times.map { Thread.new { work_until(done) } }

      appended = begin
        race(appenders) do |thread|
          calls_per_appender.times.map do |i|
            call = [ groups.sample, thread, i ]
            job_class.perform_later(*call)
            call
          end
        end.flatten(1)
      ensure
        done.make_true
        worker_threads.each { |thread| thread.join rescue nil }
      end

      worker_threads.each(&:value)
      drain

      appended
    end

    def work_until(done)
      SolidQueue::Record.connection_pool.with_connection do
        until done.true?
          perform_claimed
          sleep 0.005
        end
      end
    end

    def drain
      loop do
        SolidQueue::BlockedExecution.unblock(100)
        SolidQueue::ScheduledExecution.update_all(scheduled_at: 1.second.ago)
        SolidQueue::ScheduledExecution.dispatch_next_batch(100)
        break if perform_claimed.empty? && SolidQueue::ScheduledExecution.none? && SolidQueue::BlockedExecution.none? && SolidQueue::ReadyExecution.none?
      end
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

    def assert_every_call_ran_once(calls)
      assert_equal calls.sort_by(&:to_s), RACED_BATCHES.flat_map(&:last).sort_by(&:to_s)
    end

    def assert_batches_match_claimed_rows
      RACED_BATCHES.each do |job_id, batch|
        rows = SolidQueue::CoalescedArgument.where(job_id: job_id).order(:id).map { |row| ActiveJob::Arguments.deserialize(row.arguments) }
        assert_equal rows, batch
      end
    end

    def skip_on_sqlite
      skip "Row locks don't exist on SQLite" if SolidQueue::Record.connection_pool.db_config.adapter == "sqlite3"
    end
end
