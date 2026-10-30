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

  test "a resume keeps the frozen set on the new job and carries on from the cursor" do
    old_job = SolidQueue::Job.sole
    rows = SolidQueue::CoalescedArgument.waiting.order(:id).pluck(:id)
    late = nil

    perform_ready_jobs_with_callback { late = ResumableCollectedJob.perform_later(@results.first) }

    assert old_job.reload.finished?
    assert_empty old_job.coalesced_arguments

    resumed = SolidQueue::Job.where(active_job_id: old_job.active_job_id, finished_at: nil).sole
    assert_equal rows, resumed.coalesced_arguments.order(:id).pluck(:id)

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

  test "a cursor that's a record id survives a call being dropped from the set" do
    perform_ready_jobs

    ActiveJob::QueueAdapters::SolidQueueAdapter.stopping = false
    ResumableCollectedJob.stop_after = nil
    @results.second.destroy!
    JobBuffer.clear

    perform_ready_jobs

    # An array index of 1 would have skipped the third result
    assert_equal [ [ :run, [ @results.first.id, @results.third.id ] ], [ :processed, @results.third.id ] ], JobBuffer.values
  end

  test "a failed enqueue of a resume leaves a failed execution with the rows" do
    old_job = SolidQueue::Job.sole
    SolidQueue::Job.stubs(:create!).raises(ActiveRecord::StatementInvalid, "database went away")

    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }

    SolidQueue::Job.unstub(:create!)
    assert old_job.reload.failed?
    assert_equal 3, old_job.coalesced_arguments.count
  end
end
