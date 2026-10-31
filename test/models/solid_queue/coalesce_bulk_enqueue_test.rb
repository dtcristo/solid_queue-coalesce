# frozen_string_literal: true

require "test_helper"

# The contract of a bulk enqueue that has coalesced calls in it, as the README
# states it: what's written, in what order, and what each call reports.
class SolidQueue::CoalesceBulkEnqueueTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class OrdinaryJob < ApplicationJob
    def perform(*); end
  end

  class DiscardJob < ApplicationJob
    coalesce key: ->(group, *) { group }

    def perform(*); end
  end

  class CollectJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 2

    def perform_batch(_calls); end
  end

  class RaisingKeyJob < ApplicationJob
    coalesce key: ->(*) { raise ExpectedTestError, "no key" }

    def perform(*); end
  end

  class LimitedCollectJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 2
    limits_concurrency key: ->(_group, i) { i == :poison ? raise(ExpectedTestError, "no concurrency key") : i }

    def perform_batch(_calls); end
  end

  class ConflictDiscardJob < ApplicationJob
    coalesce key: ->(group, *) { group }
    limits_concurrency key: "shared", on_conflict: :discard

    def perform(*); end
  end

  test "mixed jobs are stored ordinary first, then by key, and each call reports its own outcome" do
    first_ordinary = OrdinaryJob.new(1)
    second_ordinary = OrdinaryJob.new(2)
    collect_a = 3.times.map { |i| CollectJob.new("a", i) }
    collect_b = CollectJob.new("b", 0)
    discard_w = DiscardJob.new("w")
    discard_x = 2.times.map { DiscardJob.new("x") }
    jobs = [ discard_x.first, collect_b, collect_a[0], first_ordinary, discard_w, collect_a[1], second_ordinary, discard_x.last, collect_a[2] ]

    events = capture_events { assert_equal 8, SolidQueue::Job.enqueue_all(jobs) }

    assert_equal [ first_ordinary, second_ordinary, collect_a[0], collect_a[2], collect_b, discard_w, discard_x.first ].map(&:job_id),
      SolidQueue::Job.order(:id).pluck(:active_job_id)
    assert_equal collect_a.map { |call| call.arguments } + [ collect_b.arguments ],
      SolidQueue::CoalescedCall.order(:id).map { |row| row.arguments["arguments"] }

    assert_equal [ nil, nil ], [ first_ordinary, second_ordinary ].map(&:coalesce_status)
    assert_equal [ :created, :collected, :created ], collect_a.map(&:coalesce_status)
    assert_equal :created, collect_b.coalesce_status
    assert_equal :created, discard_w.coalesce_status
    assert_equal [ :created, :discarded ], discard_x.map(&:coalesce_status)

    assert_equal jobs - [ discard_x.last ], jobs.select(&:successfully_enqueued?)

    assert_equal collect_a[0].provider_job_id, collect_a[1].provider_job_id
    assert_not_equal collect_a[1].provider_job_id, collect_a[2].provider_job_id
    assert_equal SolidQueue::Job.find_by!(active_job_id: discard_x.first.job_id).id, discard_x.last.provider_job_id
    (jobs - [ discard_x.last ]).select { |active_job| active_job.class != CollectJob }.each do |active_job|
      assert_equal active_job.job_id, SolidQueue::Job.find(active_job.provider_job_id).active_job_id
    end
    [ collect_a, [ collect_b ] ].flatten.each do |call|
      assert_includes SolidQueue::CoalescedCall.where(job_id: call.provider_job_id).map { |row| row.arguments["job_id"] }, call.job_id
    end

    assert_equal [ collect_a, collect_b, discard_w, discard_x ].flatten.map(&:job_id), events.map { |event| event.payload[:active_job_id] }
  end

  test "a coalesce key that raises fails the call with nothing written" do
    jobs = [ OrdinaryJob.new(1), CollectJob.new("a", 0), DiscardJob.new("b"), RaisingKeyJob.new ]

    assert_raises(ExpectedTestError) { SolidQueue::Job.enqueue_all(jobs) }

    assert_nothing_written jobs
  end

  test "an unserializable argument on a collect call fails the call with nothing written" do
    jobs = [ OrdinaryJob.new(1), DiscardJob.new("a"), CollectJob.new("b", 0), CollectJob.new("b", Object.new) ]

    assert_raises(ActiveJob::SerializationError) { SolidQueue::Job.enqueue_all(jobs) }

    assert_nothing_written jobs
  end

  test "an unserializable argument on a key's first discard call fails the call with nothing written" do
    jobs = [ OrdinaryJob.new(1), CollectJob.new("a", 0), DiscardJob.new("b", Object.new) ]

    assert_raises(ActiveJob::SerializationError) { SolidQueue::Job.enqueue_all(jobs) }

    assert_nothing_written jobs
  end

  test "an unserializable argument on a repeated discard call stays silently discarded" do
    jobs = [ DiscardJob.new("a", 0), DiscardJob.new("a", Object.new) ]

    assert_equal 1, SolidQueue::Job.enqueue_all(jobs)

    assert_equal [ :created, :discarded ], jobs.map(&:coalesce_status)
    assert_equal [ jobs.first.provider_job_id ] * 2, jobs.map(&:provider_job_id)
    assert_equal [ "a", 0 ], SolidQueue::Job.sole.arguments["arguments"]
  end

  test "a raising subscriber leaves every call in a collecting batch with its outcome, and later calls with none" do
    batch = 2.times.map { |i| CollectJob.new("a", i) }
    later = [ CollectJob.new("a", 2), DiscardJob.new("b") ]

    with_raising_subscriber(on_call: 1) do
      assert_raises(ExpectedTestError) { SolidQueue::Job.enqueue_all(batch + later) }
    end

    assert_equal [ :created, :collected ], batch.map(&:coalesce_status)
    assert_equal [ batch.first.provider_job_id ] * 2, batch.map(&:provider_job_id)
    assert_equal [ true, true ], batch.map(&:successfully_enqueued?)
    assert_equal [ nil, nil ], later.map(&:provider_job_id)
    assert_equal 2, SolidQueue::CoalescedCall.count
  end

  test "a raising subscriber leaves every discarding call with its outcome" do
    calls = 2.times.map { DiscardJob.new("a") }

    with_raising_subscriber(on_call: 1) do
      assert_raises(ExpectedTestError) { SolidQueue::Job.enqueue_all(calls) }
    end

    assert_equal [ :created, :discarded ], calls.map(&:coalesce_status)
    assert_equal [ calls.first.provider_job_id ] * 2, calls.map(&:provider_job_id)
    assert_equal [ true, false ], calls.map { |call| call.successfully_enqueued? || false }
  end

  test "an ordinary job that can't serialize fails the call with nothing written" do
    jobs = [ OrdinaryJob.new(Object.new), DiscardJob.new("a"), CollectJob.new("b", 0) ]

    assert_raises(ActiveJob::SerializationError) { SolidQueue::Job.enqueue_all(jobs) }

    assert_nothing_written jobs
  end

  test "a database failure on the ordinary jobs raises as is, with nothing written" do
    jobs = mixed_jobs
    SolidQueue::Job.stubs(:insert_all).raises(ActiveRecord::StatementInvalid, "boom")

    assert_raises(ActiveRecord::StatementInvalid) { SolidQueue::Job.enqueue_all(jobs) }

    assert_nothing_written jobs
  end

  test "a failure partway through sealed batches keeps earlier work, and later calls have no outcome" do
    ordinary, collect_a, discard_b, collect_c = mixed_jobs_by_kind

    failing(SolidQueue::CoalescedCall, :insert_all, on_attempt: 2) do
      assert_raises(SolidQueue::Job::EnqueueError) { SolidQueue::Job.enqueue_all([ ordinary, *collect_a, *discard_b, collect_c ]) }
    end

    assert_equal [ ordinary, collect_a.first ].map(&:job_id), SolidQueue::Job.order(:id).pluck(:active_job_id)
    assert_equal collect_a.first(2).map(&:job_id), SolidQueue::CoalescedCall.order(:id).map { |row| row.arguments["job_id"] }
    assert_equal [ :created, :collected, nil, nil, nil ], collect_a.map(&:coalesce_status)
    assert_equal [ true, true, nil, nil, nil ], collect_a.map { |call| call.provider_job_id && true }
    assert_equal [ nil, nil, nil ], [ *discard_b, collect_c ].map(&:provider_job_id)
    assert ordinary.successfully_enqueued?
  end

  test "a failure on a discard admission keeps earlier work, and later calls have no outcome" do
    ordinary, collect_a, discard_b, collect_c = mixed_jobs_by_kind
    discard_c = DiscardJob.new("c")

    # Sorted by key: CollectJob/a takes three jobs and CollectJob/c one, then DiscardJob/b
    failing(SolidQueue::Job, :create!, on_attempt: 5) do
      assert_raises(SolidQueue::Job::EnqueueError) { SolidQueue::Job.enqueue_all([ ordinary, *collect_a, *discard_b, collect_c, discard_c ]) }
    end

    assert collect_a.all?(&:provider_job_id)
    assert collect_c.provider_job_id
    assert_equal [ nil, nil, nil ], [ *discard_b, discard_c ].map(&:provider_job_id)
    assert_equal [ collect_a.first.coalesce_key, collect_c.coalesce_key ], SolidQueue::CoalesceKey.pluck(:key).sort
    assert_equal [ ordinary, collect_a[0], collect_a[2], collect_a[4], collect_c ].map(&:job_id), SolidQueue::Job.order(:id).pluck(:active_job_id)
  end

  test "a concurrency key that raises on a later batch's first call leaves exact outcomes" do
    jobs = [ LimitedCollectJob.new("g", 0), LimitedCollectJob.new("g", 1), LimitedCollectJob.new("g", :poison), LimitedCollectJob.new("g", 3) ]

    assert_raises(ExpectedTestError) { SolidQueue::Job.enqueue_all(jobs) }

    assert_equal [ :created, :collected, nil, nil ], jobs.map(&:coalesce_status)
    assert_equal [ jobs.first.job_id ], SolidQueue::Job.pluck(:active_job_id)
    assert_equal jobs.first(2).map(&:job_id), SolidQueue::CoalescedCall.order(:id).map { |row| row.arguments["job_id"] }
  end

  test "re-enqueuing the calls without a provider job ID stores each one once, in order" do
    ordinary, collect_a, discard_b, collect_c = mixed_jobs_by_kind
    jobs = [ ordinary, *collect_a, *discard_b, collect_c ]

    failing(SolidQueue::CoalescedCall, :insert_all, on_attempt: 2) do
      assert_raises(SolidQueue::Job::EnqueueError) { SolidQueue::Job.enqueue_all(jobs) }
    end

    SolidQueue::Job.enqueue_all(jobs.reject(&:provider_job_id))

    assert_equal [ *collect_a, collect_c ].map(&:job_id), SolidQueue::CoalescedCall.order(:id).map { |row| row.arguments["job_id"] }
    assert_equal [ ordinary, collect_a[0], collect_a[2], collect_a[4], collect_c, discard_b.first ].map(&:job_id), SolidQueue::Job.order(:id).pluck(:active_job_id)
    assert_equal [ :created, :discarded ], discard_b.map(&:coalesce_status)
    assert_equal [ 2, 2, 1 ], collect_a.map(&:provider_job_id).uniq.map { |job_id| SolidQueue::CoalescedCall.where(job_id: job_id).count }
  end

  test "re-enqueuing after a raising subscriber stores nothing twice" do
    ordinary, collect_a, discard_b, collect_c = mixed_jobs_by_kind
    jobs = [ ordinary, *collect_a, *discard_b, collect_c ]

    with_raising_subscriber(on_call: 2) do
      assert_raises(ExpectedTestError) { SolidQueue::Job.enqueue_all(jobs) }
    end

    SolidQueue::Job.enqueue_all(jobs.reject(&:provider_job_id))

    assert_equal [ *collect_a, collect_c ].map(&:job_id), SolidQueue::CoalescedCall.order(:id).map { |row| row.arguments["job_id"] }
    assert_equal 1, SolidQueue::Job.where(active_job_id: ordinary.job_id).count
    assert_equal 1, SolidQueue::Job.where(class_name: DiscardJob.name).count
  end

  test "re-enqueuing a call a concurrency conflict destroyed has no effect while the conflict lasts" do
    ConflictDiscardJob.perform_later("a")
    destroyed = ConflictDiscardJob.new("b")

    SolidQueue::Job.enqueue_all([ destroyed ])
    assert_nil destroyed.provider_job_id

    assert_no_difference -> { SolidQueue::Job.count + SolidQueue::CoalesceKey.count } do
      SolidQueue::Job.enqueue_all([ destroyed ].reject(&:provider_job_id))
    end
  end

  test "a failed re-enqueue of the same instance keeps showing its old outcome" do
    call = DiscardJob.new("a")
    SolidQueue::Job.enqueue_all([ call ])
    job_id = call.provider_job_id
    SolidQueue::Job.stubs(:create!).raises(ActiveRecord::StatementInvalid, "boom")
    SolidQueue::Job.find(job_id).destroy!

    assert_raises(SolidQueue::Job::EnqueueError) { SolidQueue::Job.enqueue_all([ call ]) }

    assert_equal job_id, call.provider_job_id
    assert_equal :created, call.coalesce_status
  end

  test "deadlocks in a mixed bulk enqueue retry each admission without repeating earlier work" do
    ordinary, collect_a, discard_b, collect_c = mixed_jobs_by_kind
    jobs = [ ordinary, *collect_a, *discard_b, collect_c ]

    failing(SolidQueue::CoalescedCall, :insert_all, on_attempt: 2, error: ActiveRecord::Deadlocked) do
      failing(SolidQueue::Job, :create!, on_attempt: 6, error: ActiveRecord::Deadlocked) do
        assert_equal 8, SolidQueue::Job.enqueue_all(jobs)
      end
    end

    assert_equal [ ordinary, collect_a[0], collect_a[2], collect_a[4], collect_c, discard_b.first ].map(&:job_id), SolidQueue::Job.order(:id).pluck(:active_job_id)
    assert_equal [ *collect_a, collect_c ].map(&:job_id), SolidQueue::CoalescedCall.order(:id).map { |row| row.arguments["job_id"] }
    assert_equal [ :created, :discarded ], discard_b.map(&:coalesce_status)
    assert_equal [ :created, :collected, :created, :collected, :created ], collect_a.map(&:coalesce_status)
  end

  test "rolling back an enclosing transaction leaves no rows, and outcomes and events as they were" do
    ordinary, collect_a, discard_b, collect_c = mixed_jobs_by_kind
    jobs = [ ordinary, *collect_a, *discard_b, collect_c ]

    events = capture_events do
      SolidQueue::Record.transaction do
        SolidQueue::Job.enqueue_all(jobs)
        raise ActiveRecord::Rollback
      end
    end

    assert_nothing_stored
    assert jobs.all?(&:provider_job_id)
    assert ordinary.successfully_enqueued?
    assert_equal [ :created, :collected, :created, :collected, :created, :created, :created, :discarded ], [ *collect_a, collect_c, *discard_b ].map(&:coalesce_status)
    assert_equal [ *collect_a, collect_c, *discard_b ].map(&:job_id), events.map { |event| event.payload[:active_job_id] }
  end

  test "a failure inside an enclosing transaction rolls back everything" do
    jobs = mixed_jobs

    failing(SolidQueue::CoalescedCall, :insert_all, on_attempt: 2) do
      assert_raises(SolidQueue::Job::EnqueueError) do
        SolidQueue::Record.transaction { SolidQueue::Job.enqueue_all(jobs) }
      end
    end

    assert_nothing_stored
  end

  private
    def mixed_jobs_by_kind
      [ OrdinaryJob.new(1), 5.times.map { |i| CollectJob.new("a", i) }, 2.times.map { DiscardJob.new("b") }, CollectJob.new("c", 0) ]
    end

    def mixed_jobs
      mixed_jobs_by_kind.flatten
    end

    # Runs the method, then raises as if it had failed, so its transaction has
    # something to roll back
    def failing(receiver, method_name, on_attempt:, error: ActiveRecord::StatementInvalid)
      original = receiver.method(method_name)
      attempts = 0

      receiver.define_singleton_method(method_name) do |*args, **options, &block|
        original.call(*args, **options, &block).tap { raise error, "boom" if (attempts += 1) == on_attempt }
      end

      yield
    ensure
      receiver.singleton_class.remove_method(method_name)
    end

    def capture_events
      events = []
      subscriber = ActiveSupport::Notifications.subscribe("coalesce.solid_queue") { |*args| events << ActiveSupport::Notifications::Event.new(*args) }
      yield
      events
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    def with_raising_subscriber(on_call:)
      calls = 0
      subscriber = ActiveSupport::Notifications.subscribe("coalesce.solid_queue") do
        raise ExpectedTestError, "subscriber" if (calls += 1) == on_call
      end

      yield
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    def assert_nothing_stored
      assert_equal 0, SolidQueue::Job.count
      assert_equal 0, SolidQueue::CoalesceKey.count
      assert_equal 0, SolidQueue::CoalescedCall.count
    end

    def assert_nothing_written(jobs)
      assert_nothing_stored
      assert_equal [ nil ] * jobs.size, jobs.map(&:provider_job_id)
      assert_equal [ nil ] * jobs.size, jobs.map(&:coalesce_status)
    end
end
