# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceDebounceTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class DebouncedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, debounce: 30.seconds

    def perform(*arguments)
      JobBuffer.add(arguments)
    end
  end

  class CappedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, debounce: 30.seconds, max_wait: 1.minute

    def perform(*); end
  end

  class CollectingJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, debounce: 30.seconds

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class CappedCollectingJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 2, debounce: 30.seconds, max_wait: 1.minute

    def perform_calls(_calls); end
  end

  class UndebouncedJob < ApplicationJob
    coalesce key: ->(group, *) { group }

    def perform(*); end
  end

  class DispatchingJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 2, on_full: :dispatch, debounce: 30.seconds

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class LimitedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, debounce: 30.seconds
    limits_concurrency key: "shared", to: 1

    def perform(*arguments)
      JobBuffer.add(arguments)
    end
  end

  class FailingJob < ApplicationJob
    coalesce key: "failing", debounce: 30.seconds

    def perform
      raise ExpectedTestError
    end
  end

  class RetriedLaterJob < ApplicationJob
    coalesce key: "retried", debounce: 30.seconds, max_wait: 1.minute
    retry_on ExpectedTestError, wait: 1.hour, jitter: 0

    def perform
      raise ExpectedTestError
    end
  end

  class RetriedRightAwayJob < ApplicationJob
    coalesce key: "retried", debounce: 30.seconds
    rescue_from(ExpectedTestError) { retry_job }

    def perform
      raise ExpectedTestError if executions == 1
    end
  end

  test "a plain call waits for the window" do
    active_job = DebouncedJob.perform_later("g")

    job = SolidQueue::Job.sole
    assert job.scheduled?
    assert_scheduled_around 30.seconds.from_now, job
    assert_in_delta job.scheduled_at.to_f, active_job.scheduled_at.to_f, 1
  end

  test "set wait and wait_until replace the window for that call" do
    DebouncedJob.set(wait: 10.minutes).perform_later("wait")
    DebouncedJob.set(wait_until: 20.minutes.from_now).perform_later("wait_until")

    jobs = SolidQueue::Job.order(:id).to_a
    assert_scheduled_around 10.minutes.from_now, jobs.first
    assert_scheduled_around 20.minutes.from_now, jobs.last
  end

  test "the cap applies to the call that creates a job, without raising" do
    CappedJob.set(wait: 1.hour).perform_later("g")

    assert_scheduled_around 1.minute.from_now, SolidQueue::Job.sole
  end

  test "the window and the time reach the job's own payload" do
    active_job = CappedJob.set(wait: 1.hour).perform_later("g")

    assert_in_delta 1.minute.from_now.to_f, Time.iso8601(SolidQueue::Job.sole.arguments["scheduled_at"]).to_f, 2
    assert_in_delta 1.minute.from_now.to_f, active_job.scheduled_at.to_f, 2
  end

  test "a call that joins pushes the job back to its own time" do
    DebouncedJob.perform_later("g", 1)
    travel 20.seconds
    active_job = DebouncedJob.perform_later("g", 2)

    assert_equal :discarded, active_job.coalesce_status
    assert_scheduled_around 30.seconds.from_now, SolidQueue::Job.sole
  end

  test "a call that collects pushes the job back to its own time" do
    CollectingJob.perform_later("g", 1)
    travel 20.seconds
    active_job = CollectingJob.perform_later("g", 2)

    assert_equal :collected, active_job.coalesce_status
    assert_scheduled_around 30.seconds.from_now, SolidQueue::Job.sole
    assert_equal 2, SolidQueue::Job.sole.coalesced_calls.count
  end

  test "a call with a longer wait pushes the job back to that time" do
    DebouncedJob.perform_later("g", 1)
    DebouncedJob.set(wait: 1.hour).perform_later("g", 2)

    assert_scheduled_around 1.hour.from_now, SolidQueue::Job.sole
  end

  test "a job never moves earlier" do
    DebouncedJob.set(wait: 1.hour).perform_later("g", 1)
    DebouncedJob.perform_later("g", 2)
    DebouncedJob.set(wait: 1.minute).perform_later("g", 3)
    DebouncedJob.set(wait_until: 1.hour.ago).perform_later("g", 4)

    assert_scheduled_around 1.hour.from_now, SolidQueue::Job.sole
  end

  test "a move within a second is skipped, and a longer one isn't" do
    freeze_time

    DebouncedJob.set(wait: 1.minute).perform_later("g", 1)
    job = SolidQueue::Job.sole
    scheduled_at = job.scheduled_at

    DebouncedJob.set(wait: 1.minute + 0.5.seconds).perform_later("g", 2)
    assert_equal scheduled_at, job.reload.scheduled_at
    assert_equal scheduled_at, job.scheduled_execution.scheduled_at

    DebouncedJob.set(wait: 1.minute + 2.seconds).perform_later("g", 3)
    assert_in_delta 2, job.reload.scheduled_at - scheduled_at, 0.01
    assert_in_delta 2, job.scheduled_execution.scheduled_at - scheduled_at, 0.01
  end

  test "calls on a hot key don't write" do
    freeze_time
    DebouncedJob.perform_later("g", 1)

    writes = []
    callback = ->(*, payload) { writes << payload[:sql] if payload[:sql].match?(/\AUPDATE/i) }
    ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
      5.times { |i| DebouncedJob.perform_later("g", i) }
    end

    assert_empty writes
  end

  test "the cap stops a job moving past max_wait from when it opened" do
    opened_at = Time.current

    CappedJob.perform_later("g", 1)
    travel 20.seconds
    CappedJob.perform_later("g", 2)
    assert_scheduled_around opened_at + 50.seconds, SolidQueue::Job.sole

    travel 20.seconds
    CappedJob.perform_later("g", 3)
    assert_scheduled_around opened_at + 1.minute, SolidQueue::Job.sole
  end

  test "the cap's clock is the app's, the same as the job's time, not the database's" do
    travel 1.hour

    CappedJob.perform_later("g", 1)
    travel 20.seconds
    CappedJob.perform_later("g", 2)

    assert_scheduled_around 30.seconds.from_now, SolidQueue::Job.sole
  end

  test "the cap applies to an explicit wait on a call that joins" do
    opened_at = Time.current

    CappedJob.perform_later("g", 1)
    travel 40.seconds
    CappedJob.set(wait: 1.hour).perform_later("g", 2)

    assert_scheduled_around opened_at + 1.minute, SolidQueue::Job.sole
  end

  test "a job that's due but hasn't been dispatched still moves" do
    DebouncedJob.perform_later("g", 1)
    travel 40.seconds
    DebouncedJob.perform_later("g", 2)

    job = SolidQueue::Job.sole
    assert job.scheduled?
    assert_scheduled_around 30.seconds.from_now, job
  end

  test "a job comes due, is dispatched and runs once, after the last move" do
    DebouncedJob.perform_later("g", 1)
    travel 20.seconds
    DebouncedJob.perform_later("g", 2)

    travel 20.seconds
    assert_equal 0, SolidQueue::ScheduledExecution.dispatch_next_batch(10)

    travel 15.seconds
    assert_equal 1, SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    perform_ready_jobs

    assert_equal [ [ "g", 1 ] ], JobBuffer.values
  end

  test "a job from before the class debounced only moves later" do
    UndebouncedJob.set(wait: 1.minute).perform_later("g", 1)
    job = SolidQueue::Job.sole
    scheduled_at = job.scheduled_at

    UndebouncedJob.set(wait: 1.hour).perform_later("g", 2)
    assert_equal scheduled_at, job.reload.scheduled_at

    UndebouncedJob.coalesce_debounce = 2.hours
    UndebouncedJob.perform_later("g", 3)
    assert_scheduled_around 2.hours.from_now, job
  ensure
    UndebouncedJob.coalesce_debounce = nil
  end

  test "a job that has moved keeps its time when the class stops debouncing" do
    DebouncedJob.set(wait: 1.hour).perform_later("g", 1)
    DebouncedJob.coalesce_debounce = nil

    DebouncedJob.set(wait: 2.hours).perform_later("g", 2)

    assert_scheduled_around 1.hour.from_now, SolidQueue::Job.sole
  ensure
    DebouncedJob.coalesce_debounce = 30.seconds
  end

  test "a bulk enqueue uses the window, and the cap, like single calls" do
    ActiveJob.perform_all_later([ DebouncedJob.new("a"), CappedJob.new("b").set(wait: 1.hour) ])

    jobs = SolidQueue::Job.order(:class_name).to_a.index_by(&:class_name)
    assert_scheduled_around 30.seconds.from_now, jobs[DebouncedJob.name]
    assert_scheduled_around 1.minute.from_now, jobs[CappedJob.name]
  end

  test "a bulk enqueue that creates a job moves it to the latest of its calls" do
    ActiveJob.perform_all_later([ DebouncedJob.new("g", 1), DebouncedJob.new("g", 2).set(wait: 1.hour), DebouncedJob.new("g", 3).set(wait: 10.minutes) ])

    assert_scheduled_around 1.hour.from_now, SolidQueue::Job.sole
  end

  test "a bulk enqueue that joins moves the job once" do
    DebouncedJob.perform_later("g", 0)
    moves = []
    callback = ->(*, payload) { moves << payload[:sql] if payload[:sql].match?(/\AUPDATE.*solid_queue_scheduled_executions/i) }

    ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
      ActiveJob.perform_all_later(3.times.map { |i| DebouncedJob.new("g", i).set(wait: (i + 1).hours) })
    end

    assert_equal 1, moves.size
    assert_scheduled_around 3.hours.from_now, SolidQueue::Job.sole
  end

  test "a bulk enqueue of collecting calls moves each job to the latest of its own calls" do
    calls = [ 1.hour, 3.hours, 2.hours, 10.minutes ].map { |wait| CollectingJob.new("g", wait.to_i).set(wait: wait) }
    ActiveJob.perform_all_later(calls)

    job = SolidQueue::Job.sole
    assert_equal 4, job.coalesced_calls.count
    assert_scheduled_around 3.hours.from_now, job
  end

  test "filling a job with on_full dispatch dispatches it, and doesn't move it" do
    DispatchingJob.perform_later("g", 1)
    DispatchingJob.set(wait: 1.hour).perform_later("g", 2)

    job = SolidQueue::Job.sole
    assert job.ready?
    assert_in_delta Time.current.to_f, job.scheduled_at.to_f, 3

    perform_ready_jobs
    assert_equal [ [ [ "g", 1 ], [ "g", 2 ] ] ], JobBuffer.values
  end

  test "a batch that doesn't fill runs once it's quiet, without on_full getting involved" do
    DispatchingJob.perform_later("g", 1)

    job = SolidQueue::Job.sole
    assert job.scheduled?
    assert_scheduled_around 30.seconds.from_now, job
  end

  test "filling a job that waits moves it, seals it, and it keeps that time" do
    CappedCollectingJob.perform_later("g", 1)
    travel 20.seconds
    CappedCollectingJob.perform_later("g", 2)

    sealed = SolidQueue::Job.sole
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert sealed.scheduled?
    assert_scheduled_around 30.seconds.from_now, sealed
    scheduled_at = sealed.scheduled_at

    travel 20.seconds
    next_call = CappedCollectingJob.perform_later("g", 3)

    assert_equal :created, next_call.coalesce_status
    assert_equal scheduled_at, sealed.reload.scheduled_at
    assert_not_equal sealed.id, next_call.provider_job_id
  end

  test "each batch has its own clock" do
    CappedCollectingJob.perform_later("g", 1)
    SolidQueue::CoalesceKey.update_all(created_at: 1.hour.ago)
    CappedCollectingJob.perform_later("g", 2)

    first_call = CappedCollectingJob.perform_later("g", 3)
    travel 20.seconds
    CappedCollectingJob.perform_later("g", 4)

    assert_scheduled_around 30.seconds.from_now, SolidQueue::Job.find(first_call.provider_job_id)
  end

  test "a job that has been open longer than max_wait doesn't move" do
    CappedJob.perform_later("g", 1)
    job = SolidQueue::Job.sole
    scheduled_at = job.scheduled_at
    SolidQueue::CoalesceKey.update_all(created_at: 1.hour.ago)

    CappedJob.perform_later("g", 2)

    assert_equal scheduled_at, job.reload.scheduled_at
  end

  test "a new job after a start has its own window and clock" do
    DebouncedJob.perform_later("g", 1)
    travel 31.seconds
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    perform_ready_jobs

    active_job = DebouncedJob.perform_later("g", 2)

    assert_equal :created, active_job.coalesce_status
    assert_scheduled_around 30.seconds.from_now, SolidQueue::Job.find(active_job.provider_job_id)
  end

  test "a job that's ready stays as it is" do
    DebouncedJob.set(wait: 0.seconds).perform_later("g", 1)
    job = SolidQueue::Job.sole
    scheduled_at = job.scheduled_at

    active_job = DebouncedJob.perform_later("g", 2)

    assert_equal :discarded, active_job.coalesce_status
    assert job.reload.ready?
    assert_equal scheduled_at, job.scheduled_at
  end

  test "a job that's blocked stays as it is" do
    LimitedJob.set(wait: 0.seconds).perform_later("a")
    LimitedJob.set(wait: 0.seconds).perform_later("b", 1)
    blocked = SolidQueue::Job.order(:id).last
    scheduled_at = blocked.scheduled_at
    assert blocked.blocked?

    active_job = LimitedJob.perform_later("b", 2)

    assert_equal :discarded, active_job.coalesce_status
    assert blocked.reload.blocked?
    assert_equal scheduled_at, blocked.scheduled_at
  end

  test "a job that's claimed but hasn't started stays as it is" do
    DebouncedJob.set(wait: 0.seconds).perform_later("g", 1)
    job = SolidQueue::Job.sole
    scheduled_at = job.scheduled_at
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "test-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 10, process.id)

    active_job = DebouncedJob.perform_later("g", 2)

    assert_equal :discarded, active_job.coalesce_status
    assert job.reload.claimed?
    assert_equal scheduled_at, job.scheduled_at

    claimed.each(&:perform)
    assert_equal [ [ "g", 1 ] ], JobBuffer.values
  end

  test "a sliding job takes no semaphore until it comes due" do
    LimitedJob.perform_later("a", 1)
    travel 20.seconds
    LimitedJob.perform_later("a", 2)
    assert_equal 0, SolidQueue::Semaphore.count

    other = LimitedJob.set(wait: 0.seconds).perform_later("b")
    assert SolidQueue::Job.find(other.provider_job_id).ready?

    perform_ready_jobs
    travel 31.seconds
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    assert_equal 0, SolidQueue::Semaphore.sole.value
    perform_ready_jobs

    assert_equal [ [ "b" ], [ "a", 1 ] ], JobBuffer.values
  end

  test "discarding a job that moved frees its key" do
    DebouncedJob.perform_later("g", 1)
    DebouncedJob.set(wait: 1.hour).perform_later("g", 2)

    SolidQueue::Job.sole.discard

    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal :created, DebouncedJob.perform_later("g", 3).coalesce_status
  end

  test "retrying a failed job that moved runs it now" do
    FailingJob.perform_later
    travel 20.seconds
    FailingJob.perform_later

    travel 31.seconds
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    assert_raises(ExpectedTestError) { perform_ready_jobs }

    job = SolidQueue::Job.sole
    job.failed_execution.retry

    assert job.reload.ready?
    assert_not job.scheduled?
  end

  test "a retry's own wait isn't capped" do
    RetriedLaterJob.set(wait: 0.seconds).perform_later
    perform_ready_jobs

    retried = SolidQueue::Job.where(finished_at: nil).sole
    assert retried.scheduled?
    assert_in_delta 1.hour.from_now.to_f, retried.scheduled_at.to_f, 3
  end

  test "a retry with no wait of its own doesn't get the window" do
    RetriedRightAwayJob.set(wait: 0.seconds).perform_later
    perform_ready_jobs

    assert SolidQueue::Job.where(finished_at: nil).sole.ready?
  end

  test "a call that joins doesn't move a retry" do
    RetriedLaterJob.set(wait: 0.seconds).perform_later
    perform_ready_jobs
    retried = SolidQueue::Job.where(finished_at: nil).sole
    scheduled_at = retried.scheduled_at

    active_job = RetriedLaterJob.perform_later

    assert_equal :created, active_job.coalesce_status
    assert_equal scheduled_at, retried.reload.scheduled_at
  end

  test "a call that joins doesn't wait for a dispatcher that's dispatching the job, and it's ready once" do
    skip_on_sqlite

    DebouncedJob.perform_later("g", 1)
    SolidQueue::Job.update_all(scheduled_at: 1.second.ago)
    SolidQueue::ScheduledExecution.update_all(scheduled_at: 1.second.ago)
    dispatched = Concurrent::Event.new
    joined = Concurrent::Event.new

    dispatcher = in_thread do
      SolidQueue::Record.transaction do
        SolidQueue::ScheduledExecution.dispatch_next_batch(10)
        dispatched.set
        joined.wait(5)
      end
    end

    dispatched.wait(5)
    active_job = Timeout.timeout(3) { in_thread { DebouncedJob.perform_later("g", 2) }.value }
    joined.set
    dispatcher.value

    assert_equal :discarded, active_job.coalesce_status
    assert_equal 1, SolidQueue::ReadyExecution.count
    assert_equal 0, SolidQueue::ScheduledExecution.count
    assert_operator SolidQueue::Job.sole.scheduled_at, :<, Time.current
  end

  test "a call that joins while a discard that gets rolled back holds the job leaves it at its time" do
    skip_on_sqlite

    DebouncedJob.perform_later("g", 1)
    job = SolidQueue::Job.sole
    scheduled_at = job.scheduled_at
    holding = Concurrent::Event.new
    joined = Concurrent::Event.new

    discard = in_thread do
      SolidQueue::Record.transaction do
        job.scheduled_execution.lock!
        holding.set
        joined.wait(5)
        raise ActiveRecord::Rollback
      end
    end

    holding.wait(5)
    active_job = Timeout.timeout(3) { in_thread { DebouncedJob.set(wait: 1.hour).perform_later("g", 2) }.value }
    joined.set
    discard.value

    assert_equal :discarded, active_job.coalesce_status
    assert_equal scheduled_at, job.reload.scheduled_at
    assert_equal job.id, SolidQueue::CoalesceKey.sole.job_id

    DebouncedJob.set(wait: 1.hour).perform_later("g", 3)
    assert_scheduled_around 1.hour.from_now, job
  end

  # A discard whose job moved or got dispatched after it loaded the execution raises,
  # as it does when the dispatcher gets there first.
  [ DebouncedJob, CollectingJob ].each do |job_class|
    test "#{job_class.name.demodulize} calls racing discards never fail, or leave a key without its job" do
      race(5) do |thread|
        30.times do |i|
          if thread.zero?
            begin
              SolidQueue::Job.where(class_name: job_class.name).find_each(&:discard)
            rescue ActiveRecord::RecordNotFound
            end
          else
            job_class.set(wait: 1.hour + i.minutes).perform_later("g", thread, i)
          end
        end
      end

      assert_equal 0, SolidQueue::CoalesceKey.where.not(job_id: SolidQueue::Job.select(:id)).count
      assert_coalesce_invariants
      assert_equal SolidQueue::Job.where(class_name: job_class.name).count, SolidQueue::ScheduledExecution.count
      SolidQueue::ScheduledExecution.includes(:job).each { |execution| assert_equal execution.job.scheduled_at, execution.scheduled_at }
    end

    # The move and a discard can really deadlock here. Admission retries, and a single
    # discard doesn't, so that one's error is what its caller would get.
    test "without SKIP LOCKED, #{job_class.name.demodulize} calls racing discards still lose nothing they kept" do
      SolidQueue.with(use_skip_locked: false) do
        race(5) do |thread|
          10.times do |i|
            if thread.zero?
              begin
                SolidQueue::Job.where(class_name: job_class.name).find_each(&:discard)
              rescue ActiveRecord::Deadlocked, ActiveRecord::RecordNotFound
              end
            else
              job_class.set(wait: 1.hour + i.minutes).perform_later("g", thread, i)
            end
          end
        end
      end

      assert_equal 0, SolidQueue::CoalesceKey.where.not(job_id: SolidQueue::Job.select(:id)).count
      assert_coalesce_invariants
    end
  end

  private
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

    def assert_scheduled_around(time, job, delta: 3)
      job.reload
      assert_in_delta time.to_f, job.scheduled_at.to_f, delta
      assert_in_delta job.scheduled_at.to_f, job.scheduled_execution.scheduled_at.to_f, 0.001
    end
end

class SolidQueue::CoalesceDebounceTestAdapterTest < ActiveSupport::TestCase
  include ActiveJob::TestHelper

  test "a plain call runs right away on other adapters, and an explicit wait applies uncapped" do
    assert_no_difference -> { SolidQueue::CoalesceKey.count + SolidQueue::Job.count } do
      assert_enqueued_with(job: SolidQueue::CoalesceDebounceTest::CappedJob, at: nil) { SolidQueue::CoalesceDebounceTest::CappedJob.perform_later("g") }
      assert_nil enqueued_jobs.last["scheduled_at"]

      assert_enqueued_with(job: SolidQueue::CoalesceDebounceTest::CappedJob, at: 1.hour.from_now) { SolidQueue::CoalesceDebounceTest::CappedJob.set(wait: 1.hour).perform_later("g") }
    end
  end

  private
    # The dummy app configures Solid Queue for tests, which Active Job respects otherwise
    def queue_adapter_for_test
      ActiveJob::QueueAdapters::TestAdapter.new
    end
end
