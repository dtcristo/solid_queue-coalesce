# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceOnFullTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class DispatchedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 2, on_full: :dispatch

    def perform_batch(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class WaitingJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 2

    def perform_batch(_calls); end
  end

  class SingleCallJob < ApplicationJob
    coalesce key: "single", on_conflict: :collect, batch_size: 1, on_full: :dispatch

    def perform_batch(_calls); end
  end

  class HundredsJob < ApplicationJob
    coalesce key: "hundreds", on_conflict: :collect, batch_size: 100, on_full: :dispatch

    def perform_batch(_calls); end
  end

  class LimitedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 2, on_full: :dispatch
    limits_concurrency key: "shared", to: 1

    def perform_batch(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class FailingJob < ApplicationJob
    coalesce key: "failing", on_conflict: :collect, batch_size: 2, on_full: :dispatch

    def perform_batch(_calls)
      raise ExpectedTestError
    end
  end

  test "a delayed job that fills is ready with all its calls" do
    DispatchedJob.set(wait: 1.hour).perform_later("g", 1)
    DispatchedJob.set(wait: 1.hour).perform_later("g", 2)

    job = SolidQueue::Job.sole
    assert job.ready?
    assert_not job.scheduled?
    assert_equal 0, SolidQueue::CoalesceKey.count

    perform_ready_jobs

    assert_equal [ [ [ "g", 1 ], [ "g", 2 ] ] ], JobBuffer.values
  end

  test "a delayed job that doesn't fill keeps its time" do
    active_job = DispatchedJob.set(wait: 1.hour).perform_later("g", 1)

    job = SolidQueue::Job.sole
    assert job.scheduled?
    assert_in_delta active_job.scheduled_at.to_f, job.scheduled_at.to_f, 1
  end

  test "a job that fills keeps its time by default" do
    2.times { |i| WaitingJob.set(wait: 1.hour).perform_later("g", i) }

    job = SolidQueue::Job.sole
    assert job.scheduled?
    assert_operator job.scheduled_at, :>, 50.minutes.from_now
  end

  test "a job that fills with its first call is ready" do
    SingleCallJob.set(wait: 1.hour).perform_later

    assert SolidQueue::Job.sole.ready?
  end

  test "a bulk enqueue dispatches each batch it fills, and nothing else" do
    ActiveJob.perform_all_later(250.times.map { |i| HundredsJob.new(i).set(wait: 1.hour) })

    jobs = SolidQueue::Job.order(:id).to_a
    assert_equal [ true, true, false ], jobs.map(&:ready?)
    assert_equal [ false, false, true ], jobs.map(&:scheduled?)
    assert_equal [ 100, 100, 50 ], jobs.map { |job| job.coalesced_calls.count }
  end

  test "the next batch takes its own first call's schedule, queue and priority" do
    2.times { |i| DispatchedJob.set(wait: 1.hour).perform_later("g", i) }
    next_call = DispatchedJob.set(wait: 2.hours, queue: "later", priority: 7).perform_later("g", 2)

    next_job = SolidQueue::Job.find(next_call.provider_job_id)
    assert next_job.scheduled?
    assert_operator next_job.scheduled_at, :>, 1.hour.from_now
    assert_equal "later", next_job.queue_name
    assert_equal 7, next_job.priority
  end

  test "a job that's ready already stays as it is when it fills" do
    DispatchedJob.perform_later("g", 1)
    job = SolidQueue::Job.sole
    scheduled_at = job.scheduled_at

    DispatchedJob.perform_later("g", 2)

    assert job.reload.ready?
    assert_equal 1, SolidQueue::ReadyExecution.count
    assert_equal scheduled_at, job.scheduled_at
  end

  test "a job that's blocked already stays blocked when it fills" do
    LimitedJob.perform_later("a", 1)
    LimitedJob.perform_later("b", 1)
    blocked = SolidQueue::Job.order(:id).last
    assert blocked.blocked?

    LimitedJob.perform_later("b", 2)

    assert blocked.reload.blocked?
    assert_equal 1, SolidQueue::BlockedExecution.count
    assert_equal 0, SolidQueue::Semaphore.sole.value
  end

  test "dispatching takes a slot of the concurrency limit" do
    2.times { |i| LimitedJob.set(wait: 1.hour).perform_later("g", i) }

    assert SolidQueue::Job.sole.ready?
    assert_equal 0, SolidQueue::Semaphore.sole.value
  end

  test "a concurrency conflict blocks the job that fills, and it runs when the limit allows" do
    LimitedJob.perform_later("a", 1)
    2.times { |i| LimitedJob.set(wait: 1.hour).perform_later("b", i) }

    full = SolidQueue::Job.order(:id).last
    assert full.blocked?
    assert_not full.scheduled?
    assert_equal 0, SolidQueue::Semaphore.sole.value

    perform_ready_jobs
    perform_ready_jobs

    assert_equal [ [ [ "a", 1 ] ], [ [ "b", 0 ], [ "b", 1 ] ] ], JobBuffer.values
  end

  test "a conflict never destroys a collecting job, whatever its class says about conflicts" do
    LimitedJob.perform_later("a", 1)
    LimitedJob.concurrency_on_conflict = :discard

    2.times { |i| LimitedJob.set(wait: 1.hour).perform_later("b", i) }

    full = SolidQueue::Job.order(:id).last
    assert full.blocked?
    assert_equal 2, full.coalesced_calls.count
  ensure
    LimitedJob.concurrency_on_conflict = :block
  end

  test "rolling back the call that fills leaves the job waiting for its time" do
    DispatchedJob.set(wait: 1.hour).perform_later("g", 1)

    SolidQueue::Record.transaction do
      DispatchedJob.set(wait: 1.hour).perform_later("g", 2)
      assert SolidQueue::Job.sole.ready?
      raise ActiveRecord::Rollback
    end

    job = SolidQueue::Job.sole
    assert job.scheduled?
    assert_operator job.scheduled_at, :>, 50.minutes.from_now
    assert_equal 1, job.coalesced_calls.count
    assert_equal 1, SolidQueue::CoalesceKey.sole.calls_count
  end

  test "retrying a failed job that was dispatched early runs it now, not at its original time" do
    2.times { |i| FailingJob.set(wait: 1.hour).perform_later(i) }
    assert_raises(ExpectedTestError) { perform_ready_jobs }

    job = SolidQueue::Job.sole
    job.failed_execution.retry

    assert job.reload.ready?
    assert_not job.scheduled?
    assert_equal 2, job.coalesced_calls.count
  end
end
