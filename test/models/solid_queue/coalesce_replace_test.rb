# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceReplaceTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class ReplacingJob < ApplicationJob
    coalesce key: ->(name, *) { name }, on_conflict: :replace

    def perform(name, value = nil, **options)
      JobBuffer.add([ name, value, options ])
    end
  end

  class DebouncedReplacingJob < ApplicationJob
    coalesce key: ->(name, *) { name }, on_conflict: :replace, debounce: 30.seconds, max_wait: 5.minutes

    def perform(name, value)
      JobBuffer.add(value)
    end
  end

  class LimitedReplacingJob < ApplicationJob
    coalesce key: ->(name, *) { name }, on_conflict: :replace
    limits_concurrency key: ->(*) { "limited" }, to: 1

    def perform(name, value)
      JobBuffer.add(value)
    end
  end

  # A custom field, and a deserialize that fails on a payload it's been told is stale
  class CustomFieldJob < ApplicationJob
    coalesce key: ->(name, *) { name }, on_conflict: :replace

    attr_accessor :revision, :note

    def serialize
      super.merge("revision" => revision, **(note ? { "note" => note } : {}))
    end

    def deserialize(job_data)
      raise ExpectedTestError, "stale payload" if job_data["revision"] == "stale"

      super
      self.revision = job_data["revision"]
      self.note = job_data["note"]
    end

    def perform(name)
      JobBuffer.add([ name, revision, note ])
    end
  end

  class RetriedReplacingJob < ApplicationJob
    coalesce key: ->(name, *) { name }, on_conflict: :replace
    retry_on ExpectedTestError, wait: 0.seconds, attempts: 2

    def perform(name, value)
      JobBuffer.add([ value, executions ])
      raise ExpectedTestError
    end
  end

  class RecordReplacingJob < ApplicationJob
    coalesce key: "records", on_conflict: :replace

    def perform(result)
      JobBuffer.add(result.id)
    end
  end

  class CountingReplacingJob < ReplacingJob
    attr_reader :serializations

    def serialize
      @serializations = serializations.to_i + 1
      super
    end
  end

  class InlineReplacingJob < ApplicationJob
    self.queue_adapter = :inline
    coalesce key: ->(name, *) { name }, on_conflict: :replace

    def perform(name, value)
      JobBuffer.add(value)
    end
  end

  class ChangingJob < ApplicationJob
    def perform(*arguments)
      JobBuffer.add([ :perform, arguments ])
    end

    def perform_calls(calls)
      JobBuffer.add([ :perform_calls, calls.map(&:arguments) ])
    end
  end

  teardown { ChangingJob.coalesce_key = nil }

  test "the latest call's arguments run, on the job the first call created" do
    first = ReplacingJob.perform_later("a", 1)
    second = ReplacingJob.perform_later("a", 2, force: true)

    events = capture_events("coalesce.solid_queue") do
      assert_no_difference -> { SolidQueue::Job.count } do
        @third = ReplacingJob.perform_later("a", 3, force: false)
      end
    end

    assert_equal [ :created, :replaced, :replaced ], [ first, second, @third ].map(&:coalesce_status)
    assert [ first, second, @third ].all?(&:successfully_enqueued?)
    assert_equal [ first.provider_job_id ], [ first, second, @third ].map(&:provider_job_id).uniq
    assert_equal({ status: :replaced, key: "#{ReplacingJob.name}/a", job_id: first.provider_job_id, active_job_id: @third.job_id, class_name: ReplacingJob.name }, events.sole.payload)
    assert_equal 0, SolidQueue::CoalescedCall.count
    assert_equal 0, SolidQueue::CoalesceKey.sole.calls_count

    perform_ready_jobs

    assert_equal [ [ "a", 3, { force: false } ] ], JobBuffer.values
    assert SolidQueue::Job.find(first.provider_job_id).finished?
  end

  test "the job keeps its identity, queue, priority, time and marker" do
    first = ReplacingJob.set(queue: "first", priority: 7, wait: 1.hour).perform_later("a", 1)
    job = SolidQueue::Job.sole
    original = job.arguments

    ReplacingJob.set(queue: "second", priority: 1, wait: 2.hours).perform_later("a", 2)

    job.reload
    assert_equal [ "first", 7, first.job_id ], [ job.queue_name, job.priority, job.active_job_id ]
    assert_in_delta 1.hour.from_now, job.scheduled_at, 2.seconds
    kept = %w[ job_class job_id queue_name priority scheduled_at enqueued_at solid_queue_coalesce_on_conflict ]
    assert_equal original.slice(*kept), job.arguments.slice(*kept)
    assert_equal "replace", job.arguments["solid_queue_coalesce_on_conflict"]
    assert_equal [ "a", 2 ], job.arguments["arguments"]
  end

  test "different keys are independent" do
    ReplacingJob.perform_later("a", 1)
    ReplacingJob.perform_later("b", 2)
    ReplacingJob.perform_later("a", 3)

    perform_ready_jobs

    assert_equal [ [ "a", 3, {} ], [ "b", 2, {} ] ], JobBuffer.values.sort_by(&:first)
  end

  test "custom fields come from the latest payload, and a superseded one is never deserialized" do
    first = CustomFieldJob.new("a").tap { |job| job.revision = "stale"; job.note = "old" }
    SolidQueue::Job.enqueue(first)
    second = CustomFieldJob.new("a").tap { |job| job.revision = "fresh" }
    SolidQueue::Job.enqueue(second)

    perform_ready_jobs

    assert_equal [ [ "a", "fresh", nil ] ], JobBuffer.values
    assert_not SolidQueue::Job.sole.arguments.key?("note")
  end

  test "a replacement after the job is claimed, but before it starts, is in that run" do
    ReplacingJob.perform_later("a", 1)
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "test-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 10, process.id)
    claimed.each { |execution| execution.job.arguments }

    replaced = ReplacingJob.perform_later("a", 2)
    claimed.each(&:perform)

    assert_equal :replaced, replaced.coalesce_status
    assert_equal [ [ "a", 2, {} ] ], JobBuffer.values
  end

  test "a call after the job starts opens a new job" do
    first = ReplacingJob.perform_later("a", 1)

    later = perform_ready_jobs_with_callback { ReplacingJob.perform_later("a", 2) }.sole

    assert_equal :created, later.coalesce_status
    assert_not_equal first.provider_job_id, later.provider_job_id

    perform_ready_jobs
    assert_equal [ [ "a", 1, {} ], [ "a", 2, {} ] ], JobBuffer.values
  end

  test "the superseded call's deleted record doesn't matter, and the winner's follows Active Job" do
    gone, kept = JobResult.create!, JobResult.create!
    RecordReplacingJob.perform_later(gone)
    RecordReplacingJob.perform_later(kept)
    gone.destroy!

    perform_ready_jobs
    assert_equal [ kept.id ], JobBuffer.values

    RecordReplacingJob.perform_later(kept)
    kept.destroy!

    assert_raises(ActiveJob::DeserializationError) { perform_ready_jobs }
    assert SolidQueue::Job.where(finished_at: nil).sole.failed?
  end

  test "a debounced job moves later with each call, up to max_wait, and runs the latest arguments" do
    DebouncedReplacingJob.perform_later("a", 1)
    job = SolidQueue::Job.sole

    travel 20.seconds do
      DebouncedReplacingJob.perform_later("a", 2)
      assert_in_delta 30.seconds.from_now, job.reload.scheduled_at, 2.seconds
    end

    travel 10.minutes do
      DebouncedReplacingJob.perform_later("a", 3)
      assert_in_delta job.created_at + 5.minutes, job.reload.scheduled_at, 2.seconds

      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      perform_ready_jobs
    end

    assert_equal [ 3 ], JobBuffer.values
  end

  test "a blocked job keeps its place, and runs the latest arguments once the slot is free" do
    LimitedReplacingJob.perform_later("a", 1)
    LimitedReplacingJob.perform_later("b", 1)
    blocked = SolidQueue::Job.find_by!(active_job_id: SolidQueue::BlockedExecution.sole.job.active_job_id)
    LimitedReplacingJob.perform_later("b", 2)

    assert blocked.reload.blocked?
    assert_equal [ "b", 2 ], blocked.arguments["arguments"]

    perform_ready_jobs
    perform_ready_jobs

    assert_equal [ 1, 2 ], JobBuffer.values
    assert_equal 1, SolidQueue::Semaphore.sole.value
  end

  test "a replacement keeps the semaphore a job holds, and a stale writer keeps the replacement" do
    LimitedReplacingJob.perform_later("a", 1)
    LimitedReplacingJob.perform_later("b", 1)
    held, blocked = SolidQueue::Job.order(:id).to_a

    LimitedReplacingJob.perform_later("a", 2)
    assert_equal held.arguments["solid_queue_concurrency_lock"], held.reload.arguments["solid_queue_concurrency_lock"]

    stale = SolidQueue::Job.find(blocked.id)
    LimitedReplacingJob.perform_later("b", 2)
    SolidQueue::Semaphore.delete_all
    SolidQueue::Record.transaction { stale.acquire_concurrency_lock }

    blocked.reload
    assert_equal [ "b", 2 ], blocked.arguments["arguments"]
    assert blocked.arguments["solid_queue_concurrency_lock"]["semaphore_id"]
  end

  test "a retry runs the arguments the job ran with, and the retries run out as usual" do
    RetriedReplacingJob.perform_later("a", 1)
    RetriedReplacingJob.perform_later("a", 2)

    perform_ready_jobs
    assert_raises(ExpectedTestError) { perform_ready_jobs }

    assert_equal [ [ 2, 1 ], [ 2, 2 ] ], JobBuffer.values
    failed = SolidQueue::FailedExecution.sole.job
    assert_equal [ "a", 2 ], failed.arguments["arguments"]

    failed.retry
    perform_ready_jobs
    assert_raises(ExpectedTestError) { perform_ready_jobs }

    assert_equal [ [ 2, 1 ], [ 2, 2 ] ] * 2, JobBuffer.values
  end

  test "a retry waiting to run isn't replaced, and a new call opens a job of its own" do
    RetriedReplacingJob.retry_on ExpectedTestError, wait: 1.hour, attempts: 2
    RetriedReplacingJob.perform_later("a", 1)
    perform_ready_jobs

    later = RetriedReplacingJob.perform_later("a", 2)

    assert_equal :created, later.coalesce_status
    assert_equal [ [ "a", 1 ], [ "a", 2 ] ], SolidQueue::Job.where(finished_at: nil).order(:id).map { |job| job.arguments["arguments"] }
  ensure
    RetriedReplacingJob.retry_on ExpectedTestError, wait: 0.seconds, attempts: 2
  end

  test "perform_all_later serializes only each key's last call, which creates the job" do
    jobs = [ ReplacingJob.new("a", Object.new), ReplacingJob.new("b", 1), ReplacingJob.new("a", 2) ]
    jobs.last.queue_name = "last"
    jobs.last.priority = 3

    events = capture_events("coalesce.solid_queue") { assert_equal 2, SolidQueue::Job.enqueue_all(jobs) }

    assert_equal [ :discarded, :created, :created ], jobs.map(&:coalesce_status)
    assert_equal [ false, true, true ], jobs.map(&:successfully_enqueued?)
    assert_equal jobs.last.provider_job_id, jobs.first.provider_job_id
    assert_equal %i[ discarded created ], events.select { |event| event.payload[:key].end_with?("/a") }.map { |event| event.payload[:status] }

    job = SolidQueue::Job.find(jobs.last.provider_job_id)
    assert_equal [ "last", 3, jobs.last.job_id ], [ job.queue_name, job.priority, job.active_job_id ]
    assert_equal [ "a", 2 ], job.arguments["arguments"]
  end

  test "perform_all_later replaces an existing job's payload and keeps the rest of it" do
    first = ReplacingJob.set(queue: "first").perform_later("a", 1)
    jobs = [ ReplacingJob.new("a", 2), ReplacingJob.new("a", 3) ]
    jobs.each { |job| job.queue_name = "other" }

    ActiveJob.perform_all_later(jobs)

    assert_equal [ :discarded, :replaced ], jobs.map(&:coalesce_status)
    job = SolidQueue::Job.sole
    assert_equal [ "first", first.job_id, [ "a", 3 ] ], [ job.queue_name, job.active_job_id, job.arguments["arguments"] ]
  end

  test "an unserializable winner fails the call with nothing written" do
    assert_raises(ActiveJob::SerializationError) { ActiveJob.perform_all_later([ ReplacingJob.new("a", 1), ReplacingJob.new("a", Object.new) ]) }
    assert_raises(SolidQueue::Job::EnqueueError, ActiveJob::SerializationError) { ReplacingJob.perform_later("a", Object.new) }

    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a raising subscriber leaves the replacement committed, with its outcome" do
    ReplacingJob.perform_later("a", 1)
    replaced = ReplacingJob.new("a", 2)
    callback = ->(*) { raise ExpectedTestError }

    ActiveSupport::Notifications.subscribed(callback, "coalesce.solid_queue") do
      assert_raises(ExpectedTestError) { SolidQueue::Job.enqueue(replaced) }
    end

    assert_equal :replaced, replaced.coalesce_status
    assert_equal [ "a", 2 ], SolidQueue::Job.sole.arguments["arguments"]
  end

  test "a call serializes once, through deadlock retries, and raises with nothing replaced once they run out" do
    CountingReplacingJob.perform_later("a", 1)
    replaced = CountingReplacingJob.new("a", 2)

    deadlocks = deadlocking_on(/UPDATE .*solid_queue_jobs/) { assert_raises(SolidQueue::Job::EnqueueError) { SolidQueue::Job.enqueue(replaced) } }

    assert_equal SolidQueue::Job::MAX_COALESCE_DEADLOCK_RETRIES + 1, deadlocks
    assert_equal 1, replaced.serializations
    assert_nil replaced.coalesce_status
    assert_equal [ "a", 1 ], SolidQueue::Job.sole.arguments["arguments"]
  end

  test "on another adapter every call runs" do
    InlineReplacingJob.perform_later("a", 1)
    InlineReplacingJob.perform_later("a", 2)
    ReplacingJob.perform_now("a", 3)

    assert_equal [ 1, 2, [ "a", 3, {} ] ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a class that starts replacing seals a discarding job, which runs its own arguments" do
    ChangingJob.coalesce key: "changing"
    ChangingJob.perform_later(1)
    ChangingJob.coalesce key: "changing", on_conflict: :replace

    assert_equal :created, ChangingJob.perform_later(2).coalesce_status
    assert_equal :replaced, ChangingJob.perform_later(3).coalesce_status
    assert_coalesce_invariants

    perform_ready_jobs
    assert_equal [ [ :perform, [ 1 ] ], [ :perform, [ 3 ] ] ], JobBuffer.values
  end

  test "a class that starts replacing seals a collecting job, which runs its calls" do
    ChangingJob.coalesce key: "changing", on_conflict: :collect
    ChangingJob.perform_later(1)
    ChangingJob.perform_later(2)
    ChangingJob.coalesce key: "changing", on_conflict: :replace

    assert_equal :created, ChangingJob.perform_later(3).coalesce_status
    assert_coalesce_invariants

    ChangingJob.coalesce key: "changing", on_conflict: :collect
    perform_ready_jobs
    assert_includes JobBuffer.values, [ :perform_calls, [ [ 1 ], [ 2 ] ] ]
  end

  test "a class that starts collecting seals a replacing job, which runs its latest payload as one call" do
    ChangingJob.coalesce key: "changing", on_conflict: :replace
    ChangingJob.perform_later(1)
    ChangingJob.perform_later(2)
    ChangingJob.coalesce key: "changing", on_conflict: :collect

    assert_equal :created, ChangingJob.perform_later(3).coalesce_status
    assert_coalesce_invariants

    perform_ready_jobs
    assert_equal [ [ :perform_calls, [ [ 2 ] ] ], [ :perform_calls, [ [ 3 ] ] ] ], JobBuffer.values
  end

  test "a class that starts discarding discards calls to a replacing job, which runs its latest payload" do
    ChangingJob.coalesce key: "changing", on_conflict: :replace
    ChangingJob.perform_later(1)
    ChangingJob.perform_later(2)
    ChangingJob.coalesce key: "changing"

    assert_equal :discarded, ChangingJob.perform_later(3).coalesce_status

    perform_ready_jobs
    assert_equal [ [ :perform, [ 2 ] ] ], JobBuffer.values
  end

  test "a replacing job whose class stopped coalescing runs its latest payload and releases its key" do
    ChangingJob.coalesce key: "changing", on_conflict: :replace
    ChangingJob.perform_later(1)
    ChangingJob.perform_later(2)
    stop_coalescing(ChangingJob)

    perform_ready_jobs

    assert_equal [ [ :perform, [ 2 ] ] ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  private
    def capture_events(name)
      events = []
      subscriber = ActiveSupport::Notifications.subscribe(name) { |*args| events << ActiveSupport::Notifications::Event.new(*args) }
      yield
      events
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end
end
