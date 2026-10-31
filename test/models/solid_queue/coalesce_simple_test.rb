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

  class InlineRefreshJob < ApplicationJob
    self.queue_adapter = :inline

    coalesce key: ->(name) { name }

    def perform(name)
      JobBuffer.add(name)
    end
  end

  class StatusInCallbackJob < ApplicationJob
    coalesce key: ->(name) { name }

    after_enqueue { JobBuffer.add(coalesce_status) }

    def perform(name)
    end
  end

  class KeywordRefreshJob < ApplicationJob
    coalesce key: ->(name, **) { name }

    def perform(name, force:)
      JobBuffer.add([ name, force, coalesced_calls ])
    end
  end

  class StringKeyedRefreshJob < ApplicationJob
    coalesce key: "everything"

    def perform(*)
    end
  end

  test "the first call creates a job, and says so" do
    events = capture_events("coalesce.solid_queue") do
      assert_difference -> { SolidQueue::Job.count }, +1 do
        active_job = CoalescedRefreshJob.perform_later("a")

        assert active_job.successfully_enqueued?
        assert_not active_job.coalesced?
        assert_equal :created, active_job.coalesce_status
        assert_equal SolidQueue::Job.last.id, active_job.provider_job_id
      end
    end

    assert_equal({ action: :created, key: "CoalescedRefreshJob/a", job_id: SolidQueue::Job.last.id, active_job_id: SolidQueue::Job.last.active_job_id, class_name: "CoalescedRefreshJob" }, events.sole.payload)
  end

  test "coalesce_status is nil until an enqueue, and for perform_now" do
    assert_nil CoalescedRefreshJob.new("a").coalesce_status

    active_job = CoalescedRefreshJob.new("a")
    active_job.perform_now
    assert_nil active_job.coalesce_status
    assert_not active_job.coalesced?
  end

  test "coalesce_status is nil on another adapter" do
    active_job = InlineRefreshJob.perform_later("a")

    assert_nil active_job.coalesce_status
    assert_not active_job.coalesced?
    assert_equal [ "a" ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "after_enqueue sees the outcome of a single enqueue" do
    StatusInCallbackJob.perform_later("a")
    StatusInCallbackJob.perform_later("a")

    assert_equal [ :created, :discarded ], JobBuffer.values
  end

  test "a discarding job performs with its own arguments and has no coalesced calls" do
    KeywordRefreshJob.perform_later("a", force: true)

    perform_ready_jobs

    assert_equal [ [ "a", true, nil ] ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalescedCall.count
  end

  test "the first call's queue, priority and arguments win" do
    CoalescedRefreshJob.set(queue: "first", priority: 7).perform_later("a", "first")
    CoalescedRefreshJob.set(queue: "second", priority: 1).perform_later("a", "second")

    job = SolidQueue::Job.sole
    assert_equal "first", job.queue_name
    assert_equal 7, job.priority
    assert_equal [ "a", "first" ], job.arguments["arguments"]
  end

  test "perform_all_later counts discarded calls as not enqueued and reports them with events, skipping enqueue callbacks" do
    jobs = [ StatusInCallbackJob.new("bulk"), StatusInCallbackJob.new("bulk") ]

    events = capture_events("coalesce.solid_queue") { ActiveJob.perform_all_later(jobs) }

    assert_equal [ true, false ], jobs.map(&:successfully_enqueued?)
    assert_equal [ :created, :discarded ], jobs.map(&:coalesce_status)
    assert_equal [ :created, :discarded ], events.map { |event| event.payload[:action] }
    assert_equal jobs.map(&:job_id), events.map { |event| event.payload[:active_job_id] }
    assert_empty JobBuffer.values
  end

  test "no event and no status when a concurrency conflict destroys the new job" do
    DiscardingRefreshJob.perform_later("a", "group")

    discarded = nil
    events = capture_events("coalesce.solid_queue") { discarded = DiscardingRefreshJob.perform_later("b", "group") }

    assert_nil discarded.coalesce_status
    assert_empty events
  end

  test "a bulk enqueue destroyed by a concurrency conflict has no outcome for any of its calls" do
    DiscardingRefreshJob.perform_later("a", "group")
    jobs = 2.times.map { DiscardingRefreshJob.new("b", "group") }

    events = capture_events("coalesce.solid_queue") { assert_equal 0, SolidQueue::Job.enqueue_all(jobs) }

    assert_equal [ nil, nil ], jobs.map(&:coalesce_status)
    assert_empty events
  end

  test "an enqueue that fails has no outcome and no event" do
    active_job = CoalescedRefreshJob.new("a")
    SolidQueue::Job.stubs(:create!).raises(ActiveRecord::StatementInvalid, "database went away")

    events = capture_events("coalesce.solid_queue") do
      assert_raises(SolidQueue::Job::EnqueueError) { SolidQueue::Job.enqueue(active_job) }
    end

    assert_nil active_job.coalesce_status
    assert_empty events
  end

  test "another call with the same key is discarded and reports the open job" do
    first = CoalescedRefreshJob.perform_later("a")
    second = nil

    events = capture_events("coalesce.solid_queue") do
      assert_no_difference -> { SolidQueue::Job.count } do
        second = CoalescedRefreshJob.perform_later("a")

        assert second.coalesced?
        assert_equal :discarded, second.coalesce_status
        assert_equal first.provider_job_id, second.provider_job_id
        assert_not_equal first.job_id, second.job_id
      end
    end

    assert_equal 1, events.size
    assert_equal "CoalescedRefreshJob/a", events.first.payload[:key]
    assert_equal first.provider_job_id, events.first.payload[:job_id]
    assert_equal :discarded, events.first.payload[:action]
    assert_equal "CoalescedRefreshJob", events.first.payload[:class_name]
    assert_equal second.job_id, events.first.payload[:active_job_id]
  end

  test "enqueueing directly reports a discarded call as not enqueued" do
    CoalescedRefreshJob.perform_later("a")

    discarded = CoalescedRefreshJob.new("a")
    SolidQueue::Job.enqueue(discarded)

    assert_not discarded.successfully_enqueued?
    assert discarded.coalesced?
  end

  test "different keys and different classes are independent" do
    assert_difference -> { SolidQueue::Job.count }, +3 do
      CoalescedRefreshJob.perform_later("a")
      CoalescedRefreshJob.perform_later("b")
      AnotherRefreshJob.perform_later("a")
    end
  end

  test "keys are built the way limits_concurrency builds its key, and stored in the same kind of column" do
    result = JobResult.create!

    [ [ "literal", [] ], [ :literal, [] ], [ ->(name) { name }, [ "a" ] ], [ ->(record) { record }, [ result ] ], [ ->(*) { nil }, [] ] ].each do |key, arguments|
      job_class = Class.new(ApplicationJob) do
        def self.name = "ParityJob"

        coalesce key: key
        limits_concurrency key: key
      end

      assert_equal job_class.new(*arguments).concurrency_key, job_class.new(*arguments).coalesce_key
    end

    coalesce_key = SolidQueue::CoalesceKey.columns_hash["key"]
    semaphore_key = SolidQueue::Semaphore.columns_hash["key"]
    assert_equal [ semaphore_key.sql_type, semaphore_key.null, semaphore_key.collation ], [ coalesce_key.sql_type, coalesce_key.null, coalesce_key.collation ]

    SolidQueue::Record.connection_pool.with_connection do |connection|
      assert connection.index_exists?(:solid_queue_coalesce_keys, :key, unique: true)
    end
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

  test "a job that isn't coalesced only reads the coalescing tables, once, when it finishes" do
    AddToBufferJob.perform_later("a")
    queries = []
    callback = ->(*, payload) { queries << payload[:sql] if payload[:sql].include?("solid_queue_coalesce") }

    ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
      AddToBufferJob.perform_later("b")
    end
    assert_empty queries

    ActiveSupport::Notifications.subscribed(callback, "sql.active_record") { perform_ready_jobs }

    assert_equal 2, queries.size
    assert queries.all? { |sql| sql.start_with?("SELECT") && !sql.include?("FOR UPDATE") }, queries.to_s
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

  test "a key left on a job that finishes without starting is released, so calls can't join a job that won't run them" do
    [ true, false ].each do |preserve|
      SolidQueue.with(preserve_finished_jobs: preserve) do
        job = SolidQueue::Job.find_by!(active_job_id: AddToBufferJob.perform_later("no longer coalesced").job_id)
        SolidQueue::CoalesceKey.create!(key: "#{CoalescedRefreshJob.name}/a", job: job)

        perform_ready_jobs

        assert_equal 0, SolidQueue::CoalesceKey.count
        assert_equal :created, CoalescedRefreshJob.perform_later("a").coalesce_status
        perform_ready_jobs
      end
    end

    assert_equal [ "no longer coalesced", "a", "no longer coalesced", "a" ], JobBuffer.values
  end

  test "finishing a job that was never coalesced leaves other keys alone" do
    CoalescedRefreshJob.set(wait: 1.hour).perform_later("a")
    AddToBufferJob.perform_later("other")

    perform_ready_jobs

    assert_equal 1, SolidQueue::CoalesceKey.count
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
    assert_equal [ :created, :created, :discarded, :discarded, :discarded, nil ], jobs.map(&:coalesce_status)
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
