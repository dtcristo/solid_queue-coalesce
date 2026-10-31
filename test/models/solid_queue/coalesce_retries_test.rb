# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceRetriesTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class FlakyCollectedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 3
    retry_on ExpectedTestError, wait: 0.seconds, attempts: 3

    def perform_batch(calls)
      JobBuffer.add([ executions, calls.map(&:arguments) ])
      raise ExpectedTestError if executions < self.class.succeed_on
    end

    cattr_accessor :succeed_on, default: 2
  end

  class ManuallyRetriedJob < ApplicationJob
    coalesce key: "manual", on_conflict: :collect
    rescue_from(ExpectedTestError) { retry_job }

    def perform_batch(calls)
      JobBuffer.add([ executions, calls.map(&:arguments) ])
      raise ExpectedTestError if executions == 1
    end
  end

  class FailingCollectedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 2

    def perform_batch(calls)
      JobBuffer.add(calls.map(&:arguments))
      raise ExpectedTestError unless self.class.fixed
    end

    cattr_accessor :fixed, default: false
  end

  class AbortedRetryJob < ApplicationJob
    coalesce key: "aborted", on_conflict: :collect
    retry_on ExpectedTestError, wait: 0.seconds
    before_enqueue { throw :abort if executions > 0 }

    def perform_batch(calls)
      raise ExpectedTestError
    end
  end

  class RetriedLimitedJob < ApplicationJob
    coalesce key: "limited", on_conflict: :collect
    limits_concurrency key: ->(result) { result }
    retry_on ExpectedTestError, wait: 0.seconds

    def perform_batch(calls)
      JobBuffer.add([ executions, calls.map(&:arguments) ])
      raise ExpectedTestError
    end
  end

  setup do
    FlakyCollectedJob.succeed_on = 2
    FailingCollectedJob.fixed = false
  end

  test "retry_on keeps the exact claimed calls on the new job and the old job's finish doesn't delete it" do
    FlakyCollectedJob.perform_later("g", 1)
    FlakyCollectedJob.perform_later("g", 2)
    old_job = SolidQueue::Job.sole
    rows = SolidQueue::CoalescedCall.waiting.order(:id).pluck(:id)

    # A call that arrives during the failed run can't get into the set
    perform_ready_jobs_with_callback { FlakyCollectedJob.perform_later("g", 3) }

    assert old_job.reload.finished?
    assert_empty old_job.coalesced_calls

    retry_job = SolidQueue::Job.where(active_job_id: old_job.active_job_id, finished_at: nil).sole
    assert_equal rows, retry_job.coalesced_calls.order(:id).pluck(:id)
    assert_not SolidQueue::CoalesceKey.exists?(job_id: retry_job.id)

    # The batch stays out of what a retry serializes
    assert_equal [ "g", 1 ], ActiveJob::Arguments.deserialize(retry_job.arguments["arguments"])
    assert_not retry_job.arguments.key?("coalesced_run")

    # With the old job gone too, nothing cascades to the rows that moved
    SolidQueue::Job.clear_finished_in_batches(finished_before: 1.minute.from_now)
    assert_equal rows, retry_job.coalesced_calls.order(:id).pluck(:id)

    SolidQueue::ReadyExecution.where.not(job_id: retry_job.id).delete_all
    perform_ready_jobs

    assert_equal [ [ 1, [ [ "g", 1 ], [ "g", 2 ] ] ], [ 2, [ [ "g", 1 ], [ "g", 2 ] ] ] ], JobBuffer.values
  end

  test "rescue_from with retry_job keeps the claimed calls" do
    ManuallyRetriedJob.perform_later("a")
    ManuallyRetriedJob.perform_later("b")

    perform_ready_jobs
    perform_ready_jobs

    assert_equal [ [ 1, [ [ "a" ], [ "b" ] ] ], [ 2, [ [ "a" ], [ "b" ] ] ] ], JobBuffer.values
  end

  test "a retry that can't be enqueued fails the run, which keeps its rows" do
    FlakyCollectedJob.perform_later("g", 1)
    job = SolidQueue::Job.sole

    SolidQueue::Job.stubs(:create!).raises(ActiveRecord::StatementInvalid, "database went away")
    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }
    SolidQueue::Job.unstub(:create!)

    assert job.reload.failed?
    assert_equal [ [ "g", 1 ] ], job.coalesced_calls.map { |row| row.arguments["arguments"] }
    assert_equal SolidQueue::Job::EnqueueError.name, job.failed_execution.exception_class
  end

  test "a retry whose payload record is gone keeps the concurrency key the job was enqueued with" do
    result, other = JobResult.create!, JobResult.create!
    RetriedLimitedJob.perform_later(result)
    RetriedLimitedJob.perform_later(other)
    job = SolidQueue::Job.sole
    result.destroy!

    perform_ready_jobs

    retried = SolidQueue::Job.where(active_job_id: job.active_job_id, finished_at: nil).sole
    assert_equal job.concurrency_key, retried.concurrency_key
    assert_equal 1, retried.coalesced_calls.count
    assert_equal [ [ 1, [ [ other ] ] ] ], JobBuffer.values
  end

  test "a retry stopped by a callback fails the run too" do
    AbortedRetryJob.perform_later("a")
    job = SolidQueue::Job.sole

    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }

    assert job.reload.failed?
    assert_equal 1, job.coalesced_calls.count
  end

  test "exhausted retries leave a failed execution and manual retry reprocesses the same rows" do
    FlakyCollectedJob.succeed_on = 100
    FlakyCollectedJob.perform_later("g", 1)
    FlakyCollectedJob.perform_later("g", 2)

    perform_ready_jobs
    perform_ready_jobs
    assert_raises(ExpectedTestError) { perform_ready_jobs }

    failed = SolidQueue::FailedExecution.sole.job
    rows = failed.coalesced_calls.order(:id).pluck(:id)
    assert_equal 2, rows.size

    FlakyCollectedJob.succeed_on = 1
    failed.retry
    JobBuffer.clear
    perform_ready_jobs

    assert_equal [ [ 1, [ [ "g", 1 ], [ "g", 2 ] ] ] ], JobBuffer.values
    assert_equal rows, failed.reload.coalesced_calls.order(:id).pluck(:id)
    assert failed.finished?
  end

  test "discarding a failed job deletes its rows" do
    FailingCollectedJob.perform_later("g", 1)
    assert_raises(ExpectedTestError) { perform_ready_jobs }

    assert_equal 1, SolidQueue::CoalescedCall.count
    SolidQueue::FailedExecution.sole.discard
    assert_equal 0, SolidQueue::CoalescedCall.count
  end

  test "a released job re-runs with the rows it claimed, even with new ones waiting" do
    FailingCollectedJob.fixed = true
    FailingCollectedJob.perform_later("g", 1)
    FailingCollectedJob.perform_later("g", 2)
    FailingCollectedJob.perform_later("g", 3)
    job = SolidQueue::Job.sole

    # The worker's claim and start happened, and then it died
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "crashed-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 1, process.id).sole
    job.start_coalesced_run
    assert_equal 2, job.coalesced_calls.count

    claimed.release
    assert job.reload.ready?

    SolidQueue::ReadyExecution.where.not(job_id: job.id).delete_all
    perform_ready_jobs

    assert_equal [ [ [ "g", 1 ], [ "g", 2 ] ] ], JobBuffer.values
  end

  test "a job of a pruned process that had started fails with its rows" do
    FailingCollectedJob.perform_later("g", 1)
    job = SolidQueue::Job.sole

    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "pruned-worker")
    SolidQueue::ReadyExecution.claim("*", 1, process.id)
    job.start_coalesced_run

    process.update_column(:last_heartbeat_at, 1.day.ago)
    SolidQueue::Process.prune

    assert job.reload.failed?
    assert_equal 1, job.coalesced_calls.count
  end

  test "a job of a pruned process that hadn't started claims its rows on failure, overflow included" do
    FailingCollectedJob.perform_later("g", 1)
    FailingCollectedJob.perform_later("g", 2)
    FailingCollectedJob.perform_later("g", 3)
    job = SolidQueue::Job.sole

    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "pruned-worker")
    SolidQueue::ReadyExecution.claim("*", 1, process.id)
    process.update_column(:last_heartbeat_at, 1.day.ago)
    SolidQueue::Process.prune

    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count

    next_job = SolidQueue::CoalesceKey.sole.job
    assert_not_equal job.id, next_job.id
    assert_equal 1, SolidQueue::CoalescedCall.waiting.count
  end

  test "a failure in the start step of a collecting job becomes a failed execution and perform doesn't run" do
    FailingCollectedJob.perform_later("g", 1)
    job = SolidQueue::Job.sole
    SolidQueue::Job.any_instance.stubs(:start_coalesced_run).raises(ActiveRecord::StatementInvalid, "boom")

    assert_raises(ActiveRecord::StatementInvalid) { perform_ready_jobs }

    assert_empty JobBuffer.values
    assert job.reload.failed?
  end

  test "failing a job with a missing class keeps every row on it, and retrying after the class is back works" do
    FailingCollectedJob.fixed = true
    5.times { |i| FailingCollectedJob.perform_later("g", i) }
    job = SolidQueue::Job.sole
    rename_job_class(job, "RemovedCollectedJob")

    assert_raises(NameError) { perform_ready_jobs }

    assert job.reload.failed?
    assert_equal 5, job.coalesced_calls.count
    assert_equal 0, SolidQueue::CoalescedCall.waiting.count
    assert_equal 0, SolidQueue::CoalesceKey.count

    rename_job_class(job, "SolidQueue::CoalesceRetriesTest::FailingCollectedJob")
    SolidQueue::Job.find(job.id).retry
    perform_ready_jobs

    assert_equal [ 5 ], JobBuffer.values.map(&:size)
  end

  test "the sweeper hands over the key of a failed job" do
    FailingCollectedJob.perform_later("g", 1)
    FailingCollectedJob.perform_later("g", 2)
    FailingCollectedJob.perform_later("g", 3)
    job = SolidQueue::Job.sole
    SolidQueue::ReadyExecution.delete_all
    SolidQueue::FailedExecution.create!(job: job, exception: ExpectedTestError.new("boom"))

    SolidQueue::CoalesceKey.release_stranded(10)

    assert_equal 2, job.coalesced_calls.count
    assert_not_equal job.id, SolidQueue::CoalesceKey.sole.job_id
    assert_equal 1, SolidQueue::CoalescedCall.waiting.count
  end

  test "the sweeper never loses waiting calls of a finished job" do
    FailingCollectedJob.fixed = true
    FailingCollectedJob.perform_later("g", 1)
    FailingCollectedJob.perform_later("g", 2)
    job = SolidQueue::Job.sole
    SolidQueue::ReadyExecution.delete_all
    job.update!(finished_at: Time.current)

    SolidQueue::CoalesceKey.release_stranded(10)

    next_job = SolidQueue::CoalesceKey.sole.job
    assert_not_equal job.id, next_job.id
    assert_equal 2, SolidQueue::CoalescedCall.waiting.count

    perform_ready_jobs
    assert_equal [ [ [ "g", 1 ], [ "g", 2 ] ] ], JobBuffer.values
  end

  test "the sweeper moves waiting calls onto a finished job when the class stopped collecting" do
    job = SolidQueue::Job.create!(queue_name: "default", class_name: "AddToBufferJob", active_job_id: SecureRandom.uuid, scheduled_at: Time.current, arguments: AddToBufferJob.new("a").serialize)
    SolidQueue::ReadyExecution.delete_all
    key = SolidQueue::CoalesceKey.create!(key: "stopped", job: job)
    SolidQueue::CoalescedCall.create!(coalesce_key: key, arguments: AddToBufferJob.new("a").serialize)
    job.update!(finished_at: Time.current)

    SolidQueue::CoalesceKey.release_stranded(10)

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 1, job.coalesced_calls.count
    assert_equal 1, SolidQueue::Job.count
  end
end
