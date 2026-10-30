# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceSimpleTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class AnotherRefreshJob < ApplicationJob
    coalesce key: ->(name) { name }

    def perform(name)
      JobBuffer.add(name)
    end
  end

  class RetriedRefreshJob < ApplicationJob
    coalesce key: ->(name) { name }
    retry_on ExpectedTestError, wait: 0.seconds

    def perform(name)
      JobBuffer.add(name)
      raise ExpectedTestError if executions == 1
    end
  end

  class AbortedRetryRefreshJob < ApplicationJob
    coalesce key: ->(name) { name }
    retry_on ExpectedTestError, wait: 0.seconds
    before_enqueue { throw :abort if executions > 0 }

    def perform(name)
      raise ExpectedTestError
    end
  end

  class RetriedDiscardingRefreshJob < ApplicationJob
    coalesce key: ->(name) { name }
    limits_concurrency key: ->(name) { name }, on_conflict: :discard
    retry_on ExpectedTestError, wait: 0.seconds

    def perform(name)
      raise ExpectedTestError
    end
  end

  class LimitedRefreshJob < ApplicationJob
    coalesce key: ->(name, *) { name }
    limits_concurrency key: ->(_name, group) { group }

    def perform(name, _group)
      JobBuffer.add(name)
    end
  end

  class DiscardingRefreshJob < ApplicationJob
    coalesce key: ->(name, *) { name }
    limits_concurrency key: ->(_name, group) { group }, on_conflict: :discard

    def perform(name, _group)
      JobBuffer.add(name)
    end
  end

  class RecordKeyedRefreshJob < ApplicationJob
    coalesce key: ->(result) { result }

    def perform(result)
      JobBuffer.add(result)
    end
  end

  class StringKeyedRefreshJob < ApplicationJob
    coalesce key: "everything"

    def perform(*)
    end
  end

  test "the first call creates a job" do
    assert_difference -> { SolidQueue::Job.count }, +1 do
      active_job = CoalescedRefreshJob.perform_later("a")

      assert active_job.successfully_enqueued?
      assert_not active_job.coalesced?
      assert_equal SolidQueue::Job.last.id, active_job.provider_job_id
    end
  end

  test "another call with the same key is dropped and reports the open job" do
    first = CoalescedRefreshJob.perform_later("a")

    events = capture_events("coalesce.solid_queue") do
      assert_no_difference -> { SolidQueue::Job.count } do
        second = CoalescedRefreshJob.perform_later("a")

        assert second.coalesced?
        assert_equal first.provider_job_id, second.provider_job_id
        assert_not_equal first.job_id, second.job_id
      end
    end

    assert_equal 1, events.size
    assert_equal "CoalescedRefreshJob/a", events.first.payload[:key]
    assert_equal first.provider_job_id, events.first.payload[:job_id]
    assert_equal :dropped, events.first.payload[:action]
    assert events.first.payload[:active_job_id].present?
  end

  test "enqueueing directly reports a dropped call as not enqueued" do
    CoalescedRefreshJob.perform_later("a")

    dropped = CoalescedRefreshJob.new("a")
    SolidQueue::Job.enqueue(dropped)

    assert_not dropped.successfully_enqueued?
    assert dropped.coalesced?
  end

  test "different keys and different classes are independent" do
    assert_difference -> { SolidQueue::Job.count }, +3 do
      CoalescedRefreshJob.perform_later("a")
      CoalescedRefreshJob.perform_later("b")
      AnotherRefreshJob.perform_later("a")
    end
  end

  test "keys that differ only by case or accent are independent" do
    %w[ a A á ].each do |name|
      assert_not CoalescedRefreshJob.perform_later(name).coalesced?, name
    end

    assert_equal 3, SolidQueue::Job.count
  end

  test "keys are built from a record's class and id, or a string" do
    result = JobResult.create!

    active_job = RecordKeyedRefreshJob.perform_later(result)
    assert_equal "SolidQueue::CoalesceSimpleTest::RecordKeyedRefreshJob/JobResult/#{result.id}", active_job.coalesce_key
    assert RecordKeyedRefreshJob.perform_later(JobResult.find(result.id)).coalesced?

    assert_equal "SolidQueue::CoalesceSimpleTest::StringKeyedRefreshJob/everything", StringKeyedRefreshJob.new.coalesce_key
  end

  test "the first schedule wins and a later wait doesn't move the open job" do
    first = CoalescedRefreshJob.set(wait: 1.hour).perform_later("a")
    job = SolidQueue::Job.find(first.provider_job_id)
    scheduled_at = job.scheduled_at

    assert_no_changes -> { job.reload.scheduled_at } do
      assert CoalescedRefreshJob.set(wait: 1.minute).perform_later("a").coalesced?
      assert CoalescedRefreshJob.perform_later("a").coalesced?
    end

    assert job.scheduled?
    assert_equal scheduled_at, job.scheduled_execution.scheduled_at
  end

  test "the key is released when the job starts and a new call opens a new job" do
    CoalescedRefreshJob.perform_later("a")

    assert_equal 1, SolidQueue::CoalesceKey.count
    keys_while_performing = perform_ready_jobs_with_callback { SolidQueue::CoalesceKey.count }

    assert_equal [ 0 ], keys_while_performing
    assert_equal [ "a" ], JobBuffer.values

    assert_not CoalescedRefreshJob.perform_later("a").coalesced?
    assert_equal 1, SolidQueue::CoalesceKey.count
  end

  test "a call while the job is performing opens a new job" do
    CoalescedRefreshJob.perform_later("a")
    joined = nil

    perform_ready_jobs_with_callback { joined = CoalescedRefreshJob.perform_later("a").coalesced? }

    assert_equal false, joined
    assert_equal 1, SolidQueue::ReadyExecution.count
  end

  test "there's no time window to configure" do
    assert_raises(ArgumentError) { Class.new(ApplicationJob) { coalesce key: "a", duration: 5.minutes } }
  end

  test "discarding the open job frees the key" do
    active_job = CoalescedRefreshJob.perform_later("a")

    SolidQueue::Job.find(active_job.provider_job_id).discard

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_not CoalescedRefreshJob.perform_later("a").coalesced?
  end

  test "deleting the open job frees the key" do
    CoalescedRefreshJob.perform_later("a")

    SolidQueue::Job.where(class_name: "CoalescedRefreshJob").delete_all

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_not CoalescedRefreshJob.perform_later("a").coalesced?
  end

  test "a job discarded by a concurrency conflict on enqueue leaves no key" do
    DiscardingRefreshJob.perform_later("a", "group")
    discarded = DiscardingRefreshJob.perform_later("b", "group")

    assert_not discarded.coalesced?
    assert_nil discarded.provider_job_id
    assert_equal [ "SolidQueue::CoalesceSimpleTest::DiscardingRefreshJob/a" ], SolidQueue::CoalesceKey.pluck(:key)
    assert_not DiscardingRefreshJob.perform_later("b", "another_group").coalesced?
  end

  test "a scheduled job discarded by a concurrency conflict when it's due frees the key" do
    DiscardingRefreshJob.perform_later("a", "group")
    DiscardingRefreshJob.set(wait: 1.second).perform_later("b", "group")

    travel_to 2.seconds.from_now do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    end

    assert_not DiscardingRefreshJob.perform_later("b", "another_group").coalesced?
    assert DiscardingRefreshJob.perform_later("a", "another_group").coalesced?
  end

  test "a retry doesn't get suppressed by a pending job or absorb later calls" do
    RetriedRefreshJob.perform_later("a")

    pending = nil
    perform_ready_jobs_with_callback { pending ||= RetriedRefreshJob.perform_later("a") }

    assert_not pending.coalesced?
    assert_equal 2, SolidQueue::ReadyExecution.count
    assert_equal pending.provider_job_id, SolidQueue::CoalesceKey.sole.job_id

    # Later calls join the pending job, not the retry
    assert_equal pending.provider_job_id, RetriedRefreshJob.perform_later("a").provider_job_id
  end

  test "a retry after the key is free doesn't take it" do
    RetriedRefreshJob.perform_later("a")

    perform_ready_jobs

    assert_equal 1, SolidQueue::ReadyExecution.count
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_not RetriedRefreshJob.perform_later("a").coalesced?
  end

  test "a retry that can't be enqueued fails the run" do
    RetriedRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole

    SolidQueue::Job.stubs(:create!).raises(ActiveRecord::StatementInvalid, "database went away")
    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }
    SolidQueue::Job.unstub(:create!)

    assert job.reload.failed?
    assert_equal SolidQueue::Job::EnqueueError.name, job.failed_execution.exception_class
  end

  test "a retry stopped by a callback fails the run too" do
    AbortedRetryRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole

    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }

    assert job.reload.failed?
  end

  test "a retry discarded by its own concurrency limit fails the run" do
    RetriedDiscardingRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole

    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }

    assert job.reload.failed?
    assert_equal SolidQueue::Job::EnqueueError.name, job.failed_execution.exception_class
  end

  test "a job that isn't coalesced doesn't touch the coalescing tables" do
    AddToBufferJob.perform_later("a")
    queries = []
    callback = ->(*, payload) { queries << payload[:sql] if payload[:sql].include?("solid_queue_coalesce") }

    ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
      AddToBufferJob.perform_later("b")
      perform_ready_jobs
    end

    assert_empty queries
  end

  test "a failure in the start step becomes a failed execution and perform doesn't run" do
    CoalescedRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole
    SolidQueue::CoalesceKey.stubs(:lock_for_release).raises(ActiveRecord::StatementInvalid, "boom").then.returns(nil)

    assert_raises(ActiveRecord::StatementInvalid) { perform_ready_jobs }

    assert_empty JobBuffer.values
    assert job.reload.failed?
  end

  test "a job of a pruned process that hadn't started frees the key when it fails" do
    CoalescedRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole

    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "pruned-worker")
    SolidQueue::ReadyExecution.claim("*", 1, process.id)
    process.update_column(:last_heartbeat_at, 1.day.ago)
    SolidQueue::Process.prune

    assert job.reload.failed?
    assert_not CoalescedRefreshJob.perform_later("a").coalesced?
  end

  test "a job whose class is gone frees the key when it fails" do
    CoalescedRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole
    rename_job_class(job, "RemovedRefreshJob")

    assert_raises(NameError) { perform_ready_jobs }

    assert job.reload.failed?
    assert_not CoalescedRefreshJob.perform_later("a").coalesced?
  end

  test "the sweeper deletes the key of a finished job and respects the batch size" do
    2.times { |i| CoalescedRefreshJob.perform_later("a#{i}") }
    SolidQueue::ReadyExecution.delete_all
    SolidQueue::Job.update_all(finished_at: Time.current)

    SolidQueue::CoalesceKey.release_stranded(1)
    assert_equal 1, SolidQueue::CoalesceKey.count

    SolidQueue::CoalesceKey.release_stranded(1)
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "the sweeper deletes the key of a failed job" do
    CoalescedRefreshJob.perform_later("a")
    job = SolidQueue::Job.sole
    SolidQueue::ReadyExecution.delete_all
    SolidQueue::FailedExecution.create!(job: job, exception: ExpectedTestError.new("boom"))

    SolidQueue::CoalesceKey.release_stranded(10)

    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "the sweeper leaves keys of pending jobs alone" do
    CoalescedRefreshJob.perform_later("a")

    SolidQueue::CoalesceKey.release_stranded(10)

    assert_equal 1, SolidQueue::CoalesceKey.count
  end

  test "the dispatcher's maintenance runs the sweeper" do
    CoalescedRefreshJob.perform_later("a")
    SolidQueue::ReadyExecution.delete_all
    SolidQueue::Job.update_all(finished_at: Time.current)

    maintenance = SolidQueue::Dispatcher::Maintenance.new(600, 10, concurrency: true, batches: false)
    maintenance.start
    wait_for(timeout: 2.seconds) { SolidQueue::CoalesceKey.none? }
    maintenance.stop

    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "concurrency limits stay independent and a blocked job holds its key" do
    LimitedRefreshJob.perform_later("a", "group")

    blocked = LimitedRefreshJob.perform_later("b", "group")
    job = SolidQueue::Job.find(blocked.provider_job_id)
    assert job.blocked?
    assert_equal "#{LimitedRefreshJob.name}/group", job.concurrency_key

    # Joining the blocked job doesn't change it, and its concurrency key stays
    # the one computed for the call that created it
    joined = LimitedRefreshJob.perform_later("b", "another_group")
    assert joined.coalesced?
    assert_equal blocked.provider_job_id, joined.provider_job_id
    assert_equal "#{LimitedRefreshJob.name}/group", job.reload.concurrency_key

    perform_ready_jobs
    assert_equal [ "a" ], JobBuffer.values
    assert_equal [ job.id ], SolidQueue::ReadyExecution.pluck(:job_id)

    perform_ready_jobs
    assert_equal [ "a", "b" ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "bulk enqueue with duplicates creates one job per key and counts only what was enqueued" do
    CoalescedRefreshJob.perform_later("existing")

    jobs = [ "a", "b", "a", "existing", "a" ].map { |name| CoalescedRefreshJob.new(name) } + [ AddToBufferJob.new("normal") ]

    assert_difference -> { SolidQueue::Job.count }, +3 do
      assert_equal 3, enqueued_count_for(jobs)
    end

    assert_equal [ true, true, false, false, false, true ], jobs.map(&:successfully_enqueued?)
    assert_equal [ false, false, true, true, true, false ], jobs.map(&:coalesced?)
    assert_equal jobs[0].provider_job_id, jobs[2].provider_job_id
    assert_equal jobs[0].provider_job_id, jobs[4].provider_job_id
    assert jobs[5].provider_job_id.present?
  end

  test "a duplicate nested in a transaction on the same database doesn't abort it" do
    CoalescedRefreshJob.perform_later("a")

    SolidQueue::Record.transaction do
      assert SolidQueue::Job.enqueue(CoalescedRefreshJob.new("c")).present?
      SolidQueue::Job.enqueue(CoalescedRefreshJob.new("a"))
      SolidQueue::Job.enqueue_all([ CoalescedRefreshJob.new("a"), CoalescedRefreshJob.new("c") ])

      assert_equal 2, SolidQueue::Job.count
    end

    assert_equal 2, SolidQueue::Job.count
    assert_equal 2, SolidQueue::CoalesceKey.count
  end

  test "rolling back the outer transaction leaves no key behind" do
    SolidQueue::Record.transaction do
      SolidQueue::Job.enqueue(CoalescedRefreshJob.new("a"))
      raise ActiveRecord::Rollback
    end

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_not CoalescedRefreshJob.perform_later("a").coalesced?
  end

  private
    def enqueued_count_for(jobs)
      SolidQueue::Job.enqueue_all(jobs)
    end

    def capture_events(name)
      events = []
      subscriber = ActiveSupport::Notifications.subscribe(name) { |*args| events << ActiveSupport::Notifications::Event.new(*args) }
      yield
      events
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end
end
