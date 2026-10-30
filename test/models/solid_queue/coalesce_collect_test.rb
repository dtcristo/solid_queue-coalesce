# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceCollectTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class RecordsCollectedJob < ApplicationJob
    coalesce key: "records", collect: true

    def perform(calls)
      JobBuffer.add(calls)
    end
  end

  class InlineCollectedJob < ApplicationJob
    self.queue_adapter = :inline

    coalesce key: "inline", collect: true

    def perform(calls)
      JobBuffer.add(calls)
    end
  end

  class LimitOfTwoJob < ApplicationJob
    coalesce key: ->(group, *) { group }, collect: true, limit: 2

    def perform(calls)
      JobBuffer.add(calls)
    end
  end

  class SubclassedPerformJob < CollectedCallsJob
    def perform(calls)
      JobBuffer.add([ :subclass, calls ])
    end
  end

  class ExposingArgumentsJob < ApplicationJob
    coalesce key: ->(group, *) { group }, collect: true, limit: 5

    before_perform { JobBuffer.add([ :before, arguments ]) }

    def perform(calls)
      JobBuffer.add([ :perform, arguments, calls ])
    end
  end

  class LimitedCollectedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, collect: true, limit: 2
    limits_concurrency key: ->(_group, concurrency_group) { concurrency_group }

    def perform(calls)
      JobBuffer.add(calls)
    end
  end

  class WaitsForItsTimeJob < ApplicationJob
    coalesce key: ->(group, *) { group }, collect: true, limit: 2, flush_when_full: false

    def perform(calls)
      JobBuffer.add(calls)
    end
  end

  class LimitOfTwoRecordsJob < ApplicationJob
    coalesce key: "records", collect: true, limit: 2

    def perform(calls)
      JobBuffer.add(calls)
    end
  end

  setup do
    @a, @b = JobResult.create!, JobResult.create!
  end

  test "every call is stored, including the first, and perform gets them once in row order" do
    first = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 1)
    second = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 2)

    assert first.successfully_enqueued?
    assert_not first.coalesced?
    assert second.successfully_enqueued?
    assert second.coalesced?
    assert_equal first.provider_job_id, second.provider_job_id

    assert_equal 1, SolidQueue::Job.count
    assert_equal 2, SolidQueue::CoalescedArgument.waiting.count

    travel_to 2.hours.from_now do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      perform_ready_jobs
    end

    assert_equal [ [ [ "g", 1 ], [ "g", 2 ] ] ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 0, SolidQueue::CoalescedArgument.waiting.count
    assert_equal 2, SolidQueue::Job.find(first.provider_job_id).coalesced_arguments.count
  end

  test "joining is reported with an event" do
    first = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 1)

    events = capture_events("coalesce.solid_queue") { CollectedCallsJob.perform_later("g", 2) }

    assert_equal [ :collected ], events.map { |event| event.payload[:action] }
    assert_equal first.provider_job_id, events.first.payload[:job_id]
    assert_equal "CollectedCallsJob/g", events.first.payload[:key]
  end

  test "call shapes: one argument is the value, several are a tuple, keywords a trailing hash, records come back" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@a, 1)
    RecordsCollectedJob.perform_later(@b, force: true)
    RecordsCollectedJob.perform_later(only: "keywords")
    RecordsCollectedJob.perform_later
    RecordsCollectedJob.perform_later(nil)

    perform_ready_jobs

    assert_equal [ [ @a, [ @a, 1 ], [ @b, { force: true } ], { only: "keywords" }, [], nil ] ], JobBuffer.values
  end

  test "perform in a subclass gets the batch too" do
    SubclassedPerformJob.perform_later("g", 1)

    perform_ready_jobs

    assert_equal [ [ :subclass, [ [ "g", 1 ] ] ] ], JobBuffer.values
  end

  test "arguments stay the arguments of the call that created the job" do
    ExposingArgumentsJob.perform_later("g", 1)
    ExposingArgumentsJob.perform_later("g", 2)

    perform_ready_jobs

    assert_equal [ [ :before, [ "g", 1 ] ], [ :perform, [ "g", 1 ], [ [ "g", 1 ], [ "g", 2 ] ] ] ], JobBuffer.values
  end

  test "the limit is enforced exactly per execution" do
    5.times { |i| CollectedCallsJob.set(wait: 1.hour).perform_later("g", i) }
    SolidQueue::ScheduledExecution.update_all(scheduled_at: 1.minute.ago)
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)

    perform_ready_jobs
    assert_equal [ [ [ "g", 0 ], [ "g", 1 ], [ "g", 2 ] ] ], JobBuffer.values
    assert_equal 2, SolidQueue::CoalescedArgument.waiting.count
  end

  test "start claims only as many rows as the limit, however big the backlog" do
    7.times { |i| LimitOfTwoJob.set(wait: 1.hour).perform_later("g", i) }
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    job = SolidQueue::Job.last

    assert_equal 7, SolidQueue::CoalescedArgument.waiting.count
    perform_ready_jobs

    assert_equal 2, job.coalesced_arguments.count
    assert_equal 5, SolidQueue::CoalescedArgument.waiting.count
  end

  test "overflow creates a next job due at the oldest remaining row's schedule" do
    in_an_hour = 1.hour.from_now
    in_two_hours = 2.hours.from_now

    LimitOfTwoJob.perform_later("g", 0)
    LimitOfTwoJob.set(wait_until: in_an_hour).perform_later("g", 1)
    LimitOfTwoJob.set(wait_until: in_an_hour).perform_later("g", 2)
    first = SolidQueue::Job.sole

    perform_ready_jobs
    assert_equal [ [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values
    assert first.reload.finished?

    next_job = SolidQueue::Job.where(finished_at: nil).sole
    assert next_job.scheduled?
    assert_in_delta in_an_hour.to_f, next_job.scheduled_at.to_f, 0.001
    assert_equal next_job.id, SolidQueue::CoalesceKey.sole.job_id
    assert_equal first.class_name, next_job.class_name
    assert_equal first.queue_name, next_job.queue_name
    assert_equal first.priority, next_job.priority
    assert_not_equal first.active_job_id, next_job.active_job_id
    assert_equal next_job.active_job_id, next_job.arguments["job_id"]
    assert_equal 0, next_job.arguments["executions"]
    assert_equal [ "g", 2 ], ActiveJob::Arguments.deserialize(next_job.arguments["arguments"])
    assert_equal 1, SolidQueue::CoalescedArgument.waiting.count

    # A later call joins the next job
    assert LimitOfTwoJob.set(wait_until: in_two_hours).perform_later("g", 3).coalesced?
    assert_equal 1, SolidQueue::Job.where(finished_at: nil).count
  end

  test "overflow with a full batch left is due now" do
    5.times { |i| LimitOfTwoJob.set(wait: 1.hour).perform_later("g", i) }
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)

    perform_ready_jobs
    assert_equal [ [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values

    next_job = SolidQueue::Job.where(finished_at: nil).sole
    assert next_job.ready?

    perform_ready_jobs
    assert_equal [ "g", 2 ], JobBuffer.values.last.first
    assert_equal 1, SolidQueue::CoalescedArgument.waiting.count
    assert SolidQueue::Job.where(finished_at: nil).sole.scheduled?
  end

  test "next job copies the starting job's concurrency key" do
    LimitedCollectedJob.perform_later("g", "first_group")
    LimitedCollectedJob.perform_later("g", "second_group")
    LimitedCollectedJob.perform_later("g", "third_group")

    perform_ready_jobs

    next_job = SolidQueue::Job.where(finished_at: nil).sole
    assert_equal "#{LimitedCollectedJob.name}/first_group", next_job.concurrency_key
    assert_equal [ "g", "third_group" ], ActiveJob::Arguments.deserialize(next_job.arguments["arguments"])
  end

  test "reaching the limit moves a scheduled job to ready and later calls get their own delay" do
    first = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 0)
    job = SolidQueue::Job.find(first.provider_job_id)
    assert job.scheduled?

    CollectedCallsJob.set(wait: 1.hour).perform_later("g", 1)
    assert job.reload.scheduled?

    CollectedCallsJob.set(wait: 1.hour).perform_later("g", 2)
    job = SolidQueue::Job.find(job.id)
    assert job.ready?
    assert_not job.scheduled?

    # Hasn't started yet, so this one still joins, but it keeps its own delay
    later = 3.hours.from_now
    CollectedCallsJob.set(wait_until: later).perform_later("g", 3)

    perform_ready_jobs
    assert_equal 3, JobBuffer.values.first.size

    next_job = SolidQueue::Job.where(finished_at: nil).sole
    assert next_job.scheduled?
    assert_in_delta later.to_f, next_job.scheduled_at.to_f, 0.001
  end

  test "with flush_when_full false, a scheduled job at the limit stays scheduled without reading its rows" do
    in_an_hour = 1.hour.from_now
    reads = []
    callback = ->(*, payload) { reads << payload[:sql] if payload[:sql].match?(/\ASELECT .*solid_queue_coalesced_arguments/) }

    ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
      3.times { |i| WaitsForItsTimeJob.set(wait_until: in_an_hour).perform_later("g", i) }
    end

    job = SolidQueue::Job.sole
    assert job.scheduled?
    assert_not job.ready?
    assert_empty reads
    assert_equal 3, SolidQueue::CoalescedArgument.waiting.count

    perform_ready_jobs
    assert_empty JobBuffer.values

    # The default still flushes
    2.times { |i| LimitOfTwoJob.set(wait_until: in_an_hour).perform_later("flushed", i) }
    assert SolidQueue::Job.find_by(class_name: LimitOfTwoJob.name).ready?
  end

  test "with flush_when_full false, the next job is due at the oldest remaining row's time, even with a full batch left" do
    in_an_hour = 1.hour.from_now

    WaitsForItsTimeJob.perform_later("g", 0)
    3.times { |i| WaitsForItsTimeJob.set(wait_until: in_an_hour).perform_later("g", i + 1) }

    perform_ready_jobs
    assert_equal [ [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values

    next_job = SolidQueue::Job.where(finished_at: nil).sole
    assert next_job.scheduled?
    assert_in_delta in_an_hour.to_f, next_job.scheduled_at.to_f, 0.001

    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    perform_ready_jobs
    assert_equal 1, JobBuffer.values.size, "the remainder ran before its time"

    travel_to in_an_hour + 1.second do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      perform_ready_jobs
    end
    assert_equal [ [ "g", 2 ], [ "g", 3 ] ], JobBuffer.values.last
  end

  test "with flush_when_full false, a backlog that's already due drains in batches of the limit" do
    in_an_hour = 1.hour.from_now
    5.times { |i| WaitsForItsTimeJob.set(wait_until: in_an_hour).perform_later("g", i) }

    travel_to in_an_hour + 1.minute do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)

      3.times do
        assert_equal 1, SolidQueue::ReadyExecution.count
        perform_ready_jobs
      end
    end

    assert_equal [ 2, 2, 1 ], JobBuffer.values.map(&:size)
    assert_equal (0..4).map { |i| [ "g", i ] }, JobBuffer.values.flatten(1)
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 0, SolidQueue::Job.where(finished_at: nil).count
  end

  test "with flush_when_full false, the first call sets the schedule and later ones join or open their own job" do
    in_an_hour = 1.hour.from_now
    in_three_hours = 3.hours.from_now

    WaitsForItsTimeJob.set(wait_until: in_an_hour).perform_later("g", 0)
    job = SolidQueue::Job.sole
    assert WaitsForItsTimeJob.set(wait_until: in_three_hours).perform_later("g", 1).coalesced?
    assert_in_delta in_an_hour.to_f, job.reload.scheduled_at.to_f, 0.001

    travel_to in_an_hour + 1.second do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)

      after_start = perform_ready_jobs_with_callback do
        WaitsForItsTimeJob.set(wait_until: in_three_hours).perform_later("g", 2)
      end

      assert_not after_start.sole.coalesced?
    end
    assert_equal [ [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values

    assert_in_delta in_three_hours.to_f, SolidQueue::Job.where(finished_at: nil).sole.scheduled_at.to_f, 0.001
  end

  test "early flush respects concurrency limits" do
    LimitedCollectedJob.perform_later("other", "shared")
    first = LimitedCollectedJob.set(wait: 1.hour).perform_later("g", "shared")
    LimitedCollectedJob.set(wait: 1.hour).perform_later("g", "shared")

    job = SolidQueue::Job.find(first.provider_job_id)
    assert job.blocked?
  end

  test "reaching the limit does nothing to a job that's ready or blocked" do
    first = CollectedCallsJob.perform_later("g", 0)
    job = SolidQueue::Job.find(first.provider_job_id)

    2.times { |i| CollectedCallsJob.perform_later("g", i + 1) }

    assert SolidQueue::Job.find(job.id).ready?
    assert_equal 1, SolidQueue::ReadyExecution.count
    assert_equal 0, SolidQueue::ScheduledExecution.count

    LimitedCollectedJob.perform_later("holder", "shared")
    blocked = SolidQueue::Job.find(LimitedCollectedJob.perform_later("g2", "shared").provider_job_id)
    2.times { LimitedCollectedJob.perform_later("g2", "shared") }

    assert SolidQueue::Job.find(blocked.id).blocked?
  end

  test "calls during execution go to a new job and never join the running batch" do
    CollectedCallsJob.perform_later("g", 0)
    CollectedCallsJob.perform_later("g", 1)

    joined = perform_ready_jobs_with_callback { CollectedCallsJob.perform_later("g", 2).coalesced? }

    assert_equal [ false ], joined
    assert_equal [ [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values
    assert_equal 1, SolidQueue::CoalescedArgument.waiting.count
    assert_equal 1, SolidQueue::ReadyExecution.count

    perform_ready_jobs
    assert_equal [ [ "g", 2 ] ], JobBuffer.last_value
  end

  test "bulk enqueue stores every call, counts them all and flushes the key that reached its limit" do
    jobs = 4.times.map { |i| CollectedCallsJob.new("g", i).set(wait: 1.hour) } + [ CollectedCallsJob.new("h", 9).set(wait: 1.hour) ]

    assert_difference -> { SolidQueue::Job.count }, +2 do
      assert_equal 5, SolidQueue::Job.enqueue_all(jobs)
    end

    assert jobs.all?(&:successfully_enqueued?)
    assert_equal [ false, true, true, true, false ], jobs.map(&:coalesced?)
    assert_equal 5, SolidQueue::CoalescedArgument.waiting.count
    assert SolidQueue::Job.find(jobs.first.provider_job_id).ready?
    assert SolidQueue::Job.find(jobs.last.provider_job_id).scheduled?
  end

  test "a deleted record's row is dropped with an event and the others run" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@b)
    @a.destroy!

    events = capture_events("drop_coalesced_argument.solid_queue") { perform_ready_jobs }

    assert_equal [ [ @b ] ], JobBuffer.values
    assert_equal 1, events.size
    assert_empty SolidQueue::FailedExecution.all
  end

  test "perform is skipped when no calls remain and the job finishes" do
    RecordsCollectedJob.perform_later(@a)
    @a.destroy!

    perform_ready_jobs

    assert_empty JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all
    assert_equal 0, SolidQueue::Job.where(finished_at: nil).count
  end

  test "a job whose calls are all gone doesn't run a call on the re-run after a crash" do
    RecordsCollectedJob.perform_later(@a)
    job = SolidQueue::Job.sole
    @a.destroy!
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "crashing-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 10, process.id).sole

    # The run drops the call, and the worker dies before recording the job as finished
    claimed.send(:execute)
    claimed.release
    perform_ready_jobs

    assert_empty JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all
    assert job.reload.finished?
  end

  test "a job with no rows from before it collected still runs its own arguments" do
    active_job = RecordsCollectedJob.new(@a)
    SolidQueue::Job.send(:create_from_active_job, active_job.tap { |j| j.scheduled_at = Time.current })

    perform_ready_jobs

    assert_equal [ [ @a ] ], JobBuffer.values
  end

  test "a deserialization error that isn't a deleted record fails the run and keeps the rows" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@b)
    job = SolidQueue::Job.sole
    @a.destroy!
    # Rails main looks records up with fetch instead of locate
    GlobalID::Locator.stubs(:locate).raises(ActiveRecord::ConnectionNotEstablished, "gone")
    GlobalID::Locator.stubs(:fetch).raises(ActiveRecord::ConnectionNotEstablished, "gone")

    assert_raises(ActiveJob::DeserializationError) { perform_ready_jobs }
    GlobalID::Locator.unstub(:locate, :fetch)

    assert job.reload.failed?
    assert_equal 2, job.coalesced_arguments.count
    assert_empty JobBuffer.values
  end

  test "next job isn't broken by a deleted record in its payload arguments" do
    LimitOfTwoRecordsJob.perform_later(@a)
    LimitOfTwoRecordsJob.perform_later(@a)
    LimitOfTwoRecordsJob.perform_later(@b)
    LimitOfTwoRecordsJob.perform_later(@a)
    @a.destroy!

    # The first run loses both its rows, and the handover doesn't need to load them
    perform_ready_jobs
    assert_empty JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all

    next_job = SolidQueue::Job.where(finished_at: nil).sole
    assert next_job.ready?, "a full batch remained"

    # Its payload arguments are @b's. The ones after are @a's, dropped on its own run
    perform_ready_jobs
    assert_equal [ [ @b ] ], JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all
  end

  test "perform_now gets a batch of one and writes nothing" do
    assert_no_difference -> { SolidQueue::CoalescedArgument.count + SolidQueue::CoalesceKey.count + SolidQueue::Job.count } do
      CollectedCallsJob.perform_now("g", 1)
      RecordsCollectedJob.perform_now(@a)
    end

    assert_equal [ [ [ "g", 1 ] ], [ @a ] ], JobBuffer.values
  end

  test "the inline adapter gets a batch of one" do
    InlineCollectedJob.perform_later(@a)

    assert_equal [ [ @a ] ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalescedArgument.count
  end

  test "a job enqueued before the class started collecting gets a batch of one" do
    job = SolidQueue::Job.create!(
      queue_name: "default", class_name: "CollectedCallsJob", active_job_id: SecureRandom.uuid, scheduled_at: Time.current,
      arguments: CollectedCallsJob.new("g", 1).serialize
    )

    perform_ready_jobs

    assert_equal [ [ [ "g", 1 ] ] ], JobBuffer.values
    assert job.reload.finished?
  end

  test "discarding an open job deletes its waiting rows" do
    first = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 0)
    CollectedCallsJob.set(wait: 1.hour).perform_later("g", 1)
    assert_equal 2, SolidQueue::CoalescedArgument.waiting.count

    SolidQueue::Job.find(first.provider_job_id).discard

    assert_equal 0, SolidQueue::CoalescedArgument.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "clearing a finished job deletes its claimed rows" do
    SolidQueue.with(preserve_finished_jobs: true) do
      first = CollectedCallsJob.perform_later("g", 0)
      perform_ready_jobs

      job = SolidQueue::Job.find(first.provider_job_id)
      assert job.finished?
      assert_equal 1, job.coalesced_arguments.count

      SolidQueue::Job.clear_finished_in_batches(finished_before: 1.minute.from_now)

      assert_equal 0, SolidQueue::CoalescedArgument.count
    end
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

class SolidQueue::CoalesceTestAdapterTest < ActiveSupport::TestCase
  include ActiveJob::TestHelper

  test "the test adapter gets a batch of one, sees the call's own arguments and writes nothing" do
    a, b = JobResult.create!, JobResult.create!

    assert_no_difference -> { SolidQueue::CoalescedArgument.count + SolidQueue::CoalesceKey.count + SolidQueue::Job.count } do
      assert_enqueued_with(job: SolidQueue::CoalesceCollectTest::RecordsCollectedJob, args: [ a ]) { SolidQueue::CoalesceCollectTest::RecordsCollectedJob.perform_later(a) }
      assert_enqueued_with(job: SolidQueue::CoalesceCollectTest::RecordsCollectedJob, args: [ b ]) { SolidQueue::CoalesceCollectTest::RecordsCollectedJob.perform_later(b) }
      perform_enqueued_jobs
    end

    assert_equal [ [ a ], [ b ] ], JobBuffer.values
  end

  private
    # The dummy app configures Solid Queue for tests, which Active Job respects otherwise
    def queue_adapter_for_test
      ActiveJob::QueueAdapters::TestAdapter.new
    end
end
