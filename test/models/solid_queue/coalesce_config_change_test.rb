# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceConfigChangeTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class ChangingJob < ApplicationJob
    retry_on StandardError, wait: 0.seconds, attempts: 3

    def perform(*arguments)
      JobBuffer.add([ :perform, arguments ])
    end

    def perform_batch(calls)
      JobBuffer.add([ :perform_batch, calls.map(&:arguments) ])

      if self.class.fail_next_batch
        self.class.fail_next_batch = false
        raise ExpectedTestError
      end
    end

    cattr_accessor :fail_next_batch, default: false
  end

  class NoBatchJob < ApplicationJob
    coalesce key: "no-batch", on_conflict: :collect
  end

  setup { collect }
  teardown { collect }

  test "a collecting job whose class stopped coalescing fails and keeps its calls" do
    job = enqueue_calls
    stop_coalescing

    error = assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }

    assert_match ChangingJob.name, error.message
    assert_match(/retry|discard/, error.message)
    assert_empty JobBuffer.values
    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count
  end

  test "a collecting job whose class switched to discarding fails and keeps its calls" do
    job = enqueue_calls
    discard

    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }

    assert_empty JobBuffer.values
    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count
  end

  test "a job failed for a changed class runs all of its calls when it's restored and retried" do
    job = enqueue_calls
    stop_coalescing
    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }

    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) do
      SolidQueue::Job.find(job.id).retry
      perform_ready_jobs
    end
    assert job.reload.failed?
    assert_empty JobBuffer.values

    collect
    SolidQueue::Job.find(job.id).retry
    perform_ready_jobs

    assert_equal [ [ :perform_batch, [ [ 1 ], [ 2 ] ] ] ], JobBuffer.values
    assert job.reload.finished?
  end

  test "a job waiting to retry when its class changes fails, and a restored retry still has its calls" do
    ChangingJob.fail_next_batch = true
    enqueue_calls
    perform_ready_jobs
    retrying_job = SolidQueue::Job.where(finished_at: nil).sole
    assert_equal 2, retrying_job.coalesced_calls.count
    stop_coalescing

    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }
    assert retrying_job.reload.failed?

    collect
    retrying_job.retry
    perform_ready_jobs

    assert_equal [ [ :perform_batch, [ [ 1 ], [ 2 ] ] ] ] * 2, JobBuffer.values
  end

  test "a collecting job whose class lost perform_batch fails and keeps its calls until it's restored" do
    job = enqueue_calls
    rename_job_class(job, NoBatchJob.name)

    assert_raises(NotImplementedError) { perform_ready_jobs }

    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count

    rename_job_class(job, ChangingJob.name)
    SolidQueue::Job.find(job.id).retry
    perform_ready_jobs

    assert_equal [ [ :perform_batch, [ [ 1 ], [ 2 ] ] ] ], JobBuffer.values
  end

  test "a collecting job that lost all of its calls fails instead of running its own arguments" do
    job = enqueue_calls
    SolidQueue::CoalescedCall.where(job_id: job.id).delete_all

    assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }

    assert_empty JobBuffer.values
    assert job.reload.failed?
  end

  test "a collecting job that lost some of its calls runs the rest and warns" do
    job = enqueue_calls
    job.coalesced_calls.order(:id).first.delete
    events = []

    ActiveSupport::Notifications.subscribed(->(event) { events << event.payload }, "coalesced_calls_mismatch.solid_queue") do
      perform_ready_jobs
    end

    assert_equal [ [ :perform_batch, [ [ 2 ] ] ] ], JobBuffer.values
    assert_equal [ { job_id: job.id, expected: 2, actual: 1 } ], events.map { |event| event.slice(:job_id, :expected, :actual) }
  end

  test "a call to a class switched to collecting doesn't join a discarding job, and both calls run" do
    discard
    first = ChangingJob.perform_later(1)
    collect

    second = ChangingJob.perform_later(2)

    assert_equal :created, second.coalesce_status
    assert_not_equal first.provider_job_id, second.provider_job_id
    assert_coalesce_invariants

    perform_ready_jobs

    assert_equal [ [ :perform_batch, [ [ 1 ] ] ], [ :perform_batch, [ [ 2 ] ] ] ], JobBuffer.values
  end

  test "a call to a class switched to discarding isn't suppressed by a collecting job" do
    enqueue_calls
    discard

    call = ChangingJob.perform_later(3)

    assert_equal :created, call.coalesce_status
    assert_equal call.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
    assert_coalesce_invariants
    assert_equal :discarded, ChangingJob.perform_later(4).coalesce_status
  end

  test "a batch size or key changed while calls wait doesn't touch them" do
    job = enqueue_calls
    ChangingJob.coalesce key: "changing", on_conflict: :collect, batch_size: 50
    ChangingJob.perform_later(3)
    ChangingJob.coalesce key: "another", on_conflict: :collect, batch_size: 1
    ChangingJob.perform_later(4)

    assert_equal 3, job.coalesced_calls.count
    assert_coalesce_invariants

    perform_ready_jobs

    assert_equal [ [ :perform_batch, [ [ 1 ], [ 2 ], [ 3 ] ] ], [ :perform_batch, [ [ 4 ] ] ] ], JobBuffer.values
  end

  test "only the job's own payload is marked, not its calls, and ordinary jobs aren't" do
    job = enqueue_calls

    assert job.arguments["coalesce_collecting"]
    assert_not job.coalesced_calls.any? { |call| call.arguments.key?("coalesce_collecting") }

    assert_not AddToBufferJob.new("plain").serialize.key?("coalesce_collecting")
    assert_not ChangingJob.new(1).serialize.key?("coalesce_collecting")
  end

  test "running an ordinary job never reads the calls table" do
    AddToBufferJob.perform_later("plain")
    queries = []

    ActiveSupport::Notifications.subscribed(->(event) { queries << event.payload[:sql] }, "sql.active_record") do
      perform_ready_jobs
    end

    assert_equal [ "plain" ], JobBuffer.values
    assert_not queries.any? { |sql| sql.include?(SolidQueue::CoalescedCall.table_name) }
  end

  test "the key, the job and the calls stay consistent through a job's life" do
    job = enqueue_calls
    assert_coalesce_invariants

    ChangingJob.perform_later(3)
    assert_equal 0, SolidQueue::CoalesceKey.count, "a full job is sealed"
    assert_coalesce_invariants

    open_job = ChangingJob.perform_later(4).provider_job_id
    assert_equal open_job, SolidQueue::CoalesceKey.sole.job_id
    assert_coalesce_invariants

    ChangingJob.fail_next_batch = true
    perform_ready_jobs
    assert_equal 0, SolidQueue::CoalesceKey.count, "the open job released its key when it started"

    retrying_job = SolidQueue::Job.where(finished_at: nil).sole
    assert_equal 3, retrying_job.coalesced_calls.count
    assert_equal job.active_job_id, retrying_job.active_job_id
    assert_equal 0, job.reload.coalesced_calls.count, "the retry took the calls with it"
    assert_coalesce_invariants

    retrying_job.discard
    assert_equal 0, SolidQueue::CoalescedCall.where(job_id: retrying_job.id).count
    assert_coalesce_invariants
  end

  test "a rolled back call leaves the open job's count and calls as they were" do
    job = enqueue_calls

    SolidQueue::Job.transaction do
      ChangingJob.perform_later(3)
      raise ActiveRecord::Rollback
    end

    assert_equal 2, job.coalesced_calls.count
    assert_equal 2, SolidQueue::CoalesceKey.sole.calls_count
    assert_coalesce_invariants
  end

  test "a job that fails with an open key, then is discarded, leaves nothing behind" do
    job = enqueue_calls
    job.failed_with(ExpectedTestError.new("boom"))
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 2, job.coalesced_calls.count

    job.failed_execution.discard

    assert_equal 0, SolidQueue::CoalescedCall.count
    assert_coalesce_invariants
  end

  test "deleting jobs in bulk or directly leaves no key or call behind" do
    bulk_job = enqueue_calls
    SolidQueue::ReadyExecution.discard_all_from_jobs([ bulk_job ])
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 0, SolidQueue::CoalescedCall.count

    direct_job = enqueue_calls
    SolidQueue::Job.where(id: direct_job.id).delete_all
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 0, SolidQueue::CoalescedCall.count
  end

  private
    def collect
      ChangingJob.coalesce key: "changing", on_conflict: :collect, batch_size: 3
    end

    def discard
      ChangingJob.coalesce key: "changing", on_conflict: :discard
    end

    def stop_coalescing
      ChangingJob.coalesce_key = nil
      ChangingJob.coalesce_on_conflict = nil
      ChangingJob.coalesce_batch_size = nil
    end

    def enqueue_calls
      ChangingJob.perform_later(1)
      ChangingJob.perform_later(2)
      SolidQueue::Job.sole
    end
end
