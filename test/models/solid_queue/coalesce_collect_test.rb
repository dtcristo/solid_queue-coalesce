# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceCollectTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class Money
    attr_reader :cents

    def initialize(cents)
      @cents = cents
    end
  end

  class MoneySerializer < ActiveJob::Serializers::ObjectSerializer
    def serialize(money)
      super("cents" => money.cents)
    end

    def deserialize(hash)
      Money.new(hash["cents"])
    end

    def klass
      Money
    end
  end

  ActiveJob::Serializers.add_serializers MoneySerializer

  class RecordsCollectedJob < ApplicationJob
    coalesce key: "records", on_conflict: :collect

    def perform_batch(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class InlineCollectedJob < ApplicationJob
    self.queue_adapter = :inline

    coalesce key: "inline", on_conflict: :collect

    def perform_batch(calls)
      JobBuffer.add(calls)
    end
  end

  class BatchOfTwoJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 2

    def perform_batch(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class BatchOfTwoRecordsJob < ApplicationJob
    coalesce key: "records", on_conflict: :collect, batch_size: 2

    def perform_batch(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class DefaultBatchSizeJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect

    def perform_batch(calls)
      JobBuffer.add(calls.map { |call| call.arguments.last })
    end
  end

  class SubclassedBatchJob < CollectedCallsJob
    def perform_batch(calls)
      JobBuffer.add([ :subclass, calls.map(&:arguments) ])
    end
  end

  class ConcretePerformJob < ApplicationJob
    coalesce key: "concrete", on_conflict: :collect

    def perform(*)
      JobBuffer.add(:perform)
    end

    def perform_batch(calls)
      JobBuffer.add(:perform_batch)
    end
  end

  class NoPerformBatchJob < ApplicationJob
    coalesce key: "missing", on_conflict: :collect

    def perform(*)
      JobBuffer.add(:perform)
    end
  end

  class ExposingCallbacksJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 5

    before_perform { JobBuffer.add([ :before, arguments, coalesced_calls.map(&:arguments) ]) }
    around_perform do |job, block|
      JobBuffer.add([ :around, job.coalesced_calls.size ])
      block.call
    end
    after_perform { JobBuffer.add([ :after, coalesced_calls.size ]) }

    def perform_batch(calls)
      JobBuffer.add([ :perform_batch, arguments, calls.equal?(coalesced_calls) ])
    end
  end

  class LimitedCollectedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 2
    limits_concurrency key: ->(_group, concurrency_group) { concurrency_group }

    def perform_batch(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class TenantJob < ApplicationJob
    coalesce key: "tenant", on_conflict: :collect

    cattr_accessor :current_tenant

    def serialize
      super.merge("tenant" => self.class.current_tenant)
    end

    def perform_batch(calls)
      JobBuffer.add(calls)
    end
  end

  class BulkIndexJob < ApplicationJob
    coalesce key: ->(status, *) { status }, on_conflict: :collect, batch_size: 100

    def perform_batch(calls)
      status = calls.first.arguments.first
      ids = calls.map { |call| call.arguments.last }.uniq

      JobBuffer.add(JobResult.where(status: status, id: ids).to_a)
    end
  end

  setup do
    @a, @b = JobResult.create!, JobResult.create!
  end

  test "every call stores its whole payload, including the first, and perform_batch gets them once in row order" do
    first = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 1)
    second = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 2)

    assert first.successfully_enqueued?
    assert_not first.coalesced?
    assert second.successfully_enqueued?
    assert second.coalesced?
    assert_equal first.provider_job_id, second.provider_job_id

    assert_equal 1, SolidQueue::Job.count
    rows = SolidQueue::CoalescedCall.waiting.order(:id).to_a
    assert_equal [ first.job_id, second.job_id ], rows.map { |row| row.arguments["job_id"] }
    assert_equal [ [ "g", 1 ], [ "g", 2 ] ], rows.map { |row| row.arguments["arguments"] }
    assert_empty SolidQueue::CoalescedCall.claimed

    travel_to 2.hours.from_now do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      perform_ready_jobs
    end

    assert_equal [ [ [ "g", 1 ], [ "g", 2 ] ] ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_empty SolidQueue::CoalescedCall.waiting
    assert_equal rows.map(&:id), SolidQueue::CoalescedCall.claimed.where(job_id: first.provider_job_id).order(:id).pluck(:id)
  end

  test "the calls table has only the columns it needs" do
    assert_equal %w[ arguments coalesce_key_id created_at id job_id ], SolidQueue::CoalescedCall.column_names.sort
  end

  test "job_data is the raw payload of each call, custom fields included, and read-only" do
    TenantJob.current_tenant = "first"
    first = TenantJob.set(wait: 1.hour, queue: "tenants", priority: 3).perform_later("a")
    TenantJob.current_tenant = "second"
    second = TenantJob.set(wait: 2.hours).perform_later("b")

    SolidQueue::ScheduledExecution.update_all(scheduled_at: 1.minute.ago)
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    perform_ready_jobs

    calls = JobBuffer.values.sole
    assert_equal SolidQueue::CoalescedCall.order(:id).pluck(:id), calls.map(&:id)

    assert_equal first.job_id, calls.first.job_data["job_id"]
    assert_equal TenantJob.name, calls.first.job_data["job_class"]
    assert_equal "tenants", calls.first.job_data["queue_name"]
    assert_equal 3, calls.first.job_data["priority"]
    assert_equal [ "a" ], calls.first.job_data["arguments"]
    assert_equal 0, calls.first.job_data["executions"]
    assert_equal "en", calls.first.job_data["locale"]
    assert_equal "first", calls.first.job_data["tenant"]
    assert_nil calls.first.job_data["provider_job_id"], "the snapshot comes from before the job row existed"
    assert_in_delta first.scheduled_at.to_f, Time.iso8601(calls.first.job_data["scheduled_at"]).to_f, 0.001
    assert calls.first.job_data["enqueued_at"].present?
    assert calls.first.job_data.key?("timezone")

    assert_equal second.job_id, calls.last.job_data["job_id"]
    assert_equal "second", calls.last.job_data["tenant"]
    assert_in_delta second.scheduled_at.to_f, Time.iso8601(calls.last.job_data["scheduled_at"]).to_f, 0.001
    assert_equal calls.first.job_data.keys.sort, calls.last.job_data.keys.sort

    assert calls.first.job_data.frozen?
    assert_raises(FrozenError) { calls.first.job_data["job_id"] = "x" }
  ensure
    TenantJob.current_tenant = nil
  end

  test "a call exposes id, arguments and job_data, and nothing else" do
    assert_equal %i[ arguments id job_data ], ActiveJob::Coalescing::Call.public_instance_methods(false).sort
  end

  test "call arguments come back as the positional array, with records, serializers and the keyword marker" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@a, 1)
    RecordsCollectedJob.perform_later(@b, force: true)
    RecordsCollectedJob.perform_later(only: "keywords")
    RecordsCollectedJob.perform_later
    RecordsCollectedJob.perform_later(nil)
    RecordsCollectedJob.perform_later(Money.new(250))

    perform_ready_jobs

    arguments = JobBuffer.values.sole
    assert_equal [ [ @a ], [ @a, 1 ], [ @b, { force: true } ], [ { only: "keywords" } ], [], [ nil ] ], arguments.first(6)

    assert Hash.ruby2_keywords_hash?(arguments[2].last)
    assert Hash.ruby2_keywords_hash?(arguments[3].last)
    assert_equal({ force: true }, ->(_record, force:) { { force: force } }.call(*arguments[2]))

    assert_kind_of Money, arguments[6].first
    assert_equal 250, arguments[6].first.cents
  end

  test "a trailing hash that wasn't keywords stays a plain hash" do
    RecordsCollectedJob.perform_later(@a, { "force" => true })

    perform_ready_jobs

    assert_not Hash.ruby2_keywords_hash?(JobBuffer.values.sole.first.last)
  end

  test "ids from a batch feed one bounded query" do
    records = 3.times.map { JobResult.create!(status: "account") }
    records.each { |record| BulkIndexJob.perform_later("account", record.id) }
    BulkIndexJob.perform_later("account", records.first.id)
    queries = []
    callback = ->(*, payload) { queries << payload[:sql] if payload[:sql].match?(/\ASELECT .*job_results/) }

    ActiveSupport::Notifications.subscribed(callback, "sql.active_record") { perform_ready_jobs }

    assert_equal records.sort_by(&:id), JobBuffer.values.sole.sort_by(&:id)
    assert_equal 1, queries.size
  end

  test "perform_batch in a subclass gets the batch too" do
    SubclassedBatchJob.perform_later("g", 1)

    perform_ready_jobs

    assert_equal [ [ :subclass, [ [ "g", 1 ] ] ] ], JobBuffer.values
  end

  test "a collecting job runs perform_batch, even when it has a perform" do
    ConcretePerformJob.perform_later("a")

    perform_ready_jobs

    assert_equal [ :perform_batch ], JobBuffer.values
  end

  test "a collecting job without perform_batch fails its run, keeps its calls and says what to do" do
    NoPerformBatchJob.perform_later("a")
    job = SolidQueue::Job.sole

    error = assert_raises(NotImplementedError) { perform_ready_jobs }

    assert_match(/#{NoPerformBatchJob.name}.*perform_batch/, error.message)
    assert_empty JobBuffer.values
    assert job.reload.failed?
    assert_equal 1, job.coalesced_calls.count
  end

  test "perform_now of a collecting job without perform_batch raises too" do
    assert_raises(NotImplementedError) { NoPerformBatchJob.perform_now("a") }
  end

  test "perform callbacks see coalesced_calls, and arguments stay those of the job's first call" do
    ExposingCallbacksJob.perform_later("g", 1)
    ExposingCallbacksJob.perform_later("g", 2)

    perform_ready_jobs

    assert_equal [
      [ :before, [ "g", 1 ], [ [ "g", 1 ], [ "g", 2 ] ] ],
      [ :around, 2 ],
      [ :perform_batch, [ "g", 1 ], true ],
      [ :after, 2 ]
    ], JobBuffer.values
  end

  test "backlogs of 1, 100 and 101 run in batches of the default size" do
    { "one" => 1, "hundred" => 100, "hundred_and_one" => 101 }.each do |group, count|
      JobBuffer.clear
      count.times { |i| DefaultBatchSizeJob.perform_later(group, i) }

      perform_ready_jobs
      perform_ready_jobs

      assert_equal (0...count).each_slice(100).to_a, JobBuffer.values, group
      assert_equal 0, SolidQueue::CoalesceKey.count
    end
  end

  test "a start claims only a batch, the oldest calls first, however big the backlog" do
    7.times { |i| BatchOfTwoJob.set(wait: 1.hour).perform_later("g", i) }
    SolidQueue::ScheduledExecution.update_all(scheduled_at: 1.minute.ago)
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    job = SolidQueue::Job.sole
    oldest = SolidQueue::CoalescedCall.order(:id).limit(2).pluck(:id)

    assert_equal 7, SolidQueue::CoalescedCall.waiting.count
    perform_ready_jobs

    assert_equal oldest, job.coalesced_calls.order(:id).pluck(:id)
    assert_equal 5, SolidQueue::CoalescedCall.waiting.count
    assert_equal [ [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values
  end

  test "a large backlog drains in order, in bounded batches" do
    7.times { |i| BatchOfTwoJob.perform_later("g", i) }

    4.times { perform_ready_jobs }

    assert_equal [ 2, 2, 2, 1 ], JobBuffer.values.map(&:size)
    assert_equal (0..6).map { |i| [ "g", i ] }, JobBuffer.values.flatten(1)
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 0, SolidQueue::Job.where(finished_at: nil).count
  end

  test "overflow creates a next job due when the oldest waiting call asked to run" do
    in_an_hour = 1.hour.from_now
    in_two_hours = 2.hours.from_now

    BatchOfTwoJob.perform_later("g", 0)
    BatchOfTwoJob.set(wait_until: in_an_hour).perform_later("g", 1)
    BatchOfTwoJob.set(wait_until: in_an_hour).perform_later("g", 2)
    BatchOfTwoJob.set(wait_until: in_two_hours).perform_later("g", 3)
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
    assert_equal [ "g", 2 ], next_job.arguments["arguments"]
    assert_equal 2, SolidQueue::CoalescedCall.waiting.count

    # A later call joins the next job and doesn't move it
    assert BatchOfTwoJob.set(wait_until: in_two_hours).perform_later("g", 4).coalesced?
    assert_equal 1, SolidQueue::Job.where(finished_at: nil).count
    assert_in_delta in_an_hour.to_f, next_job.reload.scheduled_at.to_f, 0.001

    # It doesn't run before its time, full batch or not
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    perform_ready_jobs
    assert_equal 1, JobBuffer.size

    travel_to in_an_hour + 1.second do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      perform_ready_jobs
    end
    assert_equal [ [ "g", 2 ], [ "g", 3 ] ], JobBuffer.last_value
  end

  test "overflow whose oldest call is already due runs right away" do
    5.times { |i| BatchOfTwoJob.perform_later("g", i) }

    perform_ready_jobs

    assert SolidQueue::Job.where(finished_at: nil).sole.ready?
  end

  test "overflow from a bulk enqueue uses the schedule of the oldest waiting call too" do
    in_an_hour = 1.hour.from_now
    jobs = [ BatchOfTwoJob.new("g", 0), BatchOfTwoJob.new("g", 1), BatchOfTwoJob.new("g", 2).set(wait_until: in_an_hour) ]

    SolidQueue::Job.enqueue_all(jobs)
    perform_ready_jobs

    assert_in_delta in_an_hour.to_f, SolidQueue::Job.where(finished_at: nil).sole.scheduled_at.to_f, 0.001
  end

  test "a bulk enqueue stores the schedule of an immediate call in its payload" do
    SolidQueue::Job.enqueue_all([ BatchOfTwoJob.new("g", 0) ])

    assert SolidQueue::CoalescedCall.sole.arguments["scheduled_at"].present?
  end

  test "next job copies the starting job's concurrency key" do
    LimitedCollectedJob.perform_later("g", "first_group")
    LimitedCollectedJob.perform_later("g", "second_group")
    LimitedCollectedJob.perform_later("g", "third_group")

    perform_ready_jobs

    next_job = SolidQueue::Job.where(finished_at: nil).sole
    assert_equal "#{LimitedCollectedJob.name}/first_group", next_job.concurrency_key
    assert_equal [ "g", "third_group" ], next_job.arguments["arguments"]
  end

  test "reaching the batch size never dispatches a scheduled job, and doesn't read the calls" do
    in_an_hour = 1.hour.from_now
    reads = []
    callback = ->(*, payload) { reads << payload[:sql] if payload[:sql].match?(/\ASELECT .*solid_queue_coalesced_calls/) }

    ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
      5.times { |i| BatchOfTwoJob.set(wait_until: in_an_hour).perform_later("g", i) }
    end

    job = SolidQueue::Job.sole
    assert job.scheduled?
    assert_not job.ready?
    assert_empty reads
    assert_equal 5, SolidQueue::CoalescedCall.waiting.count

    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    perform_ready_jobs
    assert_empty JobBuffer.values
  end

  test "a bulk enqueue past the batch size doesn't dispatch a scheduled job either" do
    jobs = 5.times.map { |i| BatchOfTwoJob.new("g", i).set(wait: 1.hour) }

    SolidQueue::Job.enqueue_all(jobs)

    assert SolidQueue::Job.sole.scheduled?
  end

  test "the first pending schedule wins, and a call that joins runs with that job even if it asked for later" do
    in_an_hour = 1.hour.from_now
    in_three_hours = 3.hours.from_now

    DefaultBatchSizeJob.set(wait_until: in_an_hour).perform_later("g", 0)
    job = SolidQueue::Job.sole
    assert DefaultBatchSizeJob.set(wait_until: in_three_hours).perform_later("g", 1).coalesced?
    assert_in_delta in_an_hour.to_f, job.reload.scheduled_at.to_f, 0.001

    travel_to in_an_hour + 1.second do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)

      after_start = perform_ready_jobs_with_callback do
        DefaultBatchSizeJob.set(wait_until: in_three_hours).perform_later("g", 2)
      end

      assert_not after_start.sole.coalesced?
    end
    assert_equal [ [ 0, 1 ] ], JobBuffer.values

    assert_in_delta in_three_hours.to_f, SolidQueue::Job.where(finished_at: nil).sole.scheduled_at.to_f, 0.001
  end

  test "a blocked job stays blocked when calls join it" do
    LimitedCollectedJob.perform_later("holder", "shared")
    blocked = SolidQueue::Job.find(LimitedCollectedJob.perform_later("g", "shared").provider_job_id)
    assert blocked.blocked?

    3.times { LimitedCollectedJob.perform_later("g", "shared") }

    assert SolidQueue::Job.find(blocked.id).blocked?
    assert_equal 4, SolidQueue::CoalescedCall.waiting.where(coalesce_key: SolidQueue::CoalesceKey.find_by(job_id: blocked.id)).count
  end

  test "calls during execution go to a new job and never join the running batch" do
    CollectedCallsJob.perform_later("g", 0)
    CollectedCallsJob.perform_later("g", 1)

    joined = perform_ready_jobs_with_callback { CollectedCallsJob.perform_later("g", 2).coalesced? }

    assert_equal [ false ], joined
    assert_equal [ [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values
    assert_equal 1, SolidQueue::CoalescedCall.waiting.count
    assert_equal 1, SolidQueue::ReadyExecution.count

    perform_ready_jobs
    assert_equal [ [ "g", 2 ] ], JobBuffer.last_value
  end

  test "bulk enqueue stores every call and counts them all" do
    jobs = 4.times.map { |i| CollectedCallsJob.new("g", i).set(wait: 1.hour) } + [ CollectedCallsJob.new("h", 9).set(wait: 1.hour) ]

    assert_difference -> { SolidQueue::Job.count }, +2 do
      assert_equal 5, SolidQueue::Job.enqueue_all(jobs)
    end

    assert jobs.all?(&:successfully_enqueued?)
    assert_equal [ :created, :collected, :collected, :collected, :created ], jobs.map(&:coalesce_status)
    assert_equal jobs.map(&:job_id).sort, SolidQueue::CoalescedCall.waiting.map { |row| row.arguments["job_id"] }.sort
  end

  test "a deleted record's call is dropped with an event and the others run" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@b)
    job = SolidQueue::Job.sole
    dropped_id = SolidQueue::CoalescedCall.order(:id).first.id
    @a.destroy!

    events = capture_events("drop_coalesced_call.solid_queue") { perform_ready_jobs }

    assert_equal [ [ [ @b ] ] ], JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all

    payload = events.sole.payload
    assert_equal dropped_id, payload[:call_id]
    assert_equal job.id, payload[:job_id]
    assert_equal job.active_job_id, payload[:active_job_id]
    assert_operator payload[:exception_class].constantize, :<=, ActiveJob::DeserializationError
    assert_equal %i[ active_job_id call_id exception_class job_id ], payload.keys.sort
    assert_not SolidQueue::CoalescedCall.exists?(dropped_id)
  end

  test "perform_batch and its callbacks are skipped when no calls remain, and the job finishes" do
    ExposingCallbacksJob.perform_later(@a)
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

  test "a call dropped from a batch stays out of the re-run after a crash" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@b)
    job = SolidQueue::Job.sole
    @a.destroy!
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "crashing-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 10, process.id).sole

    claimed.send(:execute)
    claimed.release
    events = capture_events("drop_coalesced_call.solid_queue") { perform_ready_jobs }

    assert_equal [ [ [ @b ] ], [ [ @b ] ] ], JobBuffer.values
    assert_empty events
    assert job.reload.finished?
  end

  test "a job with no calls from before it collected runs a batch of one from its own arguments" do
    job = SolidQueue::Job.create!(
      queue_name: "default", class_name: "CollectedCallsJob", active_job_id: SecureRandom.uuid, scheduled_at: Time.current,
      arguments: CollectedCallsJob.new("g", 1).serialize
    )

    perform_ready_jobs

    assert_equal [ [ [ "g", 1 ] ] ], JobBuffer.values
    assert job.reload.finished?
  end

  test "a deserialization error that isn't a deleted record fails the run and keeps the calls" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@b)
    job = SolidQueue::Job.sole
    @a.destroy!
    # Rails main looks records up with fetch instead of locate
    GlobalID::Locator.stubs(:locate).raises(ActiveRecord::ConnectionNotEstablished, "gone")
    GlobalID::Locator.stubs(:fetch).raises(ActiveRecord::ConnectionNotEstablished, "gone")

    events = capture_events("drop_coalesced_call.solid_queue") do
      assert_raises(ActiveJob::DeserializationError) { perform_ready_jobs }
    end
    GlobalID::Locator.unstub(:locate, :fetch)

    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count
    assert_empty JobBuffer.values
    assert_empty events
  end

  test "next job isn't broken by a deleted record in its payload arguments" do
    BatchOfTwoRecordsJob.perform_later(@a)
    BatchOfTwoRecordsJob.perform_later(@a)
    BatchOfTwoRecordsJob.perform_later(@b)
    BatchOfTwoRecordsJob.perform_later(@a)
    @a.destroy!

    # The first run loses both its calls, and the handover doesn't need to load them
    perform_ready_jobs
    assert_empty JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all

    next_job = SolidQueue::Job.where(finished_at: nil).sole
    assert next_job.ready?, "the oldest waiting call was due"

    # Its payload arguments are @b's. The ones after are @a's, dropped on its own run
    perform_ready_jobs
    assert_equal [ [ [ @b ] ] ], JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all
  end

  test "perform_now gets a batch of one with no id and writes nothing" do
    assert_no_difference -> { SolidQueue::CoalescedCall.count + SolidQueue::CoalesceKey.count + SolidQueue::Job.count } do
      InlineCollectedJob.perform_now(@a, force: true)
    end

    call = JobBuffer.values.sole.sole
    assert_nil call.id
    assert_equal [ @a, { force: true } ], call.arguments
    assert Hash.ruby2_keywords_hash?(call.arguments.last)
    assert_equal InlineCollectedJob.name, call.job_data["job_class"]
    assert call.job_data.frozen?
    assert_same call.job_data, call.job_data
  end

  test "perform_now with arguments that can't be serialized still runs" do
    unserializable = Object.new

    InlineCollectedJob.perform_now(unserializable)
    CoalescedRefreshJob.perform_now(unserializable)

    call = JobBuffer.values.first.sole
    assert_same unserializable, call.arguments.first
    assert_raises(ActiveJob::SerializationError) { call.job_data }
    assert_same unserializable, JobBuffer.values.last
  end

  test "the inline adapter gets a batch of one" do
    InlineCollectedJob.perform_later(@a)

    call = JobBuffer.values.sole.sole
    assert_nil call.id
    assert_equal [ @a ], call.arguments
    assert_equal 0, SolidQueue::CoalescedCall.count
  end

  test "discarding an open job deletes its waiting calls" do
    first = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 0)
    CollectedCallsJob.set(wait: 1.hour).perform_later("g", 1)
    assert_equal 2, SolidQueue::CoalescedCall.waiting.count

    SolidQueue::Job.find(first.provider_job_id).discard

    assert_equal 0, SolidQueue::CoalescedCall.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "clearing a finished job deletes its claimed calls" do
    SolidQueue.with(preserve_finished_jobs: true) do
      first = CollectedCallsJob.perform_later("g", 0)
      perform_ready_jobs

      job = SolidQueue::Job.find(first.provider_job_id)
      assert job.finished?
      assert_equal 1, job.coalesced_calls.count

      SolidQueue::Job.clear_finished_in_batches(finished_before: 1.minute.from_now)

      assert_equal 0, SolidQueue::CoalescedCall.count
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

    assert_no_difference -> { SolidQueue::CoalescedCall.count + SolidQueue::CoalesceKey.count + SolidQueue::Job.count } do
      assert_enqueued_with(job: SolidQueue::CoalesceCollectTest::RecordsCollectedJob, args: [ a ]) { SolidQueue::CoalesceCollectTest::RecordsCollectedJob.perform_later(a) }
      assert_enqueued_with(job: SolidQueue::CoalesceCollectTest::RecordsCollectedJob, args: [ b ]) { SolidQueue::CoalesceCollectTest::RecordsCollectedJob.perform_later(b) }
      perform_enqueued_jobs
    end

    assert_equal [ [ [ a ] ], [ [ b ] ] ], JobBuffer.values
  end

  private
    # The dummy app configures Solid Queue for tests, which Active Job respects otherwise
    def queue_adapter_for_test
      ActiveJob::QueueAdapters::TestAdapter.new
    end
end
