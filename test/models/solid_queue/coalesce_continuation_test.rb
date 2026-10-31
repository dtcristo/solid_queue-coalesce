# frozen_string_literal: true

require "test_helper"

begin
  require "active_job/continuation"
rescue LoadError
  return
end

class SolidQueue::CoalesceContinuationTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  setup do
    @results = 3.times.map { JobResult.create! }
    ResumableCollectedJob.stop_after = @results.first.id
    @results.each { |result| ResumableCollectedJob.perform_later(result) }
  end

  teardown do
    ResumableCollectedJob.stop_after = nil
  end

  test "a resumed job whose class stopped coalescing fails, and runs its remaining steps once restored" do
    perform_ready_jobs
    resumed = SolidQueue::Job.where(finished_at: nil).sole
    key, on_conflict, batch_size = ResumableCollectedJob.coalesce_key, ResumableCollectedJob.coalesce_on_conflict, ResumableCollectedJob.coalesce_batch_size

    begin
      stop_coalescing(ResumableCollectedJob)
      ActiveJob::QueueAdapters::SolidQueueAdapter.stopping = false

      assert_raises(ActiveJob::Coalescing::CollectedJobMismatch) { perform_ready_jobs }
      assert resumed.reload.failed?
      assert_equal 3, resumed.coalesced_calls.count
    ensure
      ResumableCollectedJob.coalesce_key, ResumableCollectedJob.coalesce_on_conflict, ResumableCollectedJob.coalesce_batch_size = key, on_conflict, batch_size
    end

    ResumableCollectedJob.stop_after = nil
    resumed.retry
    perform_ready_jobs

    assert resumed.reload.finished?
    assert_equal [
      [ :run, @results.map(&:id) ],
      [ :processed, @results.first.id ],
      [ :run, @results.map(&:id) ],
      [ :processed, @results.second.id ],
      [ :processed, @results.third.id ]
    ], JobBuffer.values
  end

  test "a resume keeps the calls on the new job and carries on with the standard step behaviour" do
    old_job = SolidQueue::Job.sole
    rows = SolidQueue::CoalescedCall.waiting.order(:id).pluck(:id)
    late = nil

    perform_ready_jobs_with_callback { late = ResumableCollectedJob.perform_later(@results.first) }

    assert old_job.reload.finished?
    assert_empty old_job.coalesced_calls

    resumed = SolidQueue::Job.where(active_job_id: old_job.active_job_id, finished_at: nil).sole
    assert_equal rows, resumed.coalesced_calls.order(:id).pluck(:id)

    # The call that arrived during the first run opened its own job
    assert_not late.coalesced?
    assert_not_equal resumed.id, late.provider_job_id

    ActiveJob::QueueAdapters::SolidQueueAdapter.stopping = false
    ResumableCollectedJob.stop_after = nil
    SolidQueue::ReadyExecution.where(job_id: late.provider_job_id).delete_all
    perform_ready_jobs

    assert_equal [
      [ :run, @results.map(&:id) ],
      [ :processed, @results.first.id ],
      [ :run, @results.map(&:id) ],
      [ :processed, @results.second.id ],
      [ :processed, @results.third.id ]
    ], JobBuffer.values
  end

  test "a failed enqueue of a resume leaves a failed execution with the calls" do
    old_job = SolidQueue::Job.sole
    SolidQueue::Job.stubs(:create!).raises(ActiveRecord::StatementInvalid, "database went away")

    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }

    SolidQueue::Job.unstub(:create!)
    assert old_job.reload.failed?
    assert_equal 3, old_job.coalesced_calls.count
  end
end
