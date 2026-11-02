# frozen_string_literal: true

require "test_helper"

# What each event and monitoring query means, as the README states it
class SolidQueue::CoalesceInstrumentationTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  # Rails 7.2 and main can defer enqueues to the commit, and these are about what happens without that
  class DiscardJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :never : false if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: ->(group, *) { group }

    def perform(*); end
  end

  class CollectJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :never : false if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 3

    def perform_calls(_calls); end
  end

  class RecordsJob < ApplicationJob
    coalesce key: "records", on_conflict: :collect

    def perform_calls(_calls); end
  end

  class FlakyJob < ApplicationJob
    coalesce key: "flaky", on_conflict: :collect
    retry_on ExpectedTestError, wait: 1.hour, attempts: 2

    def perform_calls(_calls)
      raise ExpectedTestError
    end
  end

  class FailingJob < ApplicationJob
    coalesce key: "failing", on_conflict: :collect

    def perform_calls(_calls)
      raise ExpectedTestError
    end
  end

  setup do
    @a = JobResult.create!(queue_name: "default", status: "a")
    @b = JobResult.create!(queue_name: "default", status: "b")
  end

  test "an admission's event comes after its commit" do
    open_transaction = nil
    subscriber = ActiveSupport::Notifications.subscribe("coalesce.solid_queue") { open_transaction = SolidQueue::Record.connection_pool.with_connection(&:transaction_open?) }

    CollectJob.perform_later("g", 1)

    assert_equal false, open_transaction
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber)
  end

  test "inside an outer queue transaction the event comes before the commit, and stays fired after a rollback" do
    events = capture_events("coalesce.solid_queue") do
      SolidQueue::Record.transaction do
        CollectJob.perform_later("g", 1)
        raise ActiveRecord::Rollback
      end
    end

    assert_equal [ :created ], events.map { |event| event.payload[:status] }
    assert_equal 0, SolidQueue::Job.count
  end

  test "an app database transaction doesn't cover the queue's own commit" do
    events = capture_events("coalesce.solid_queue") do
      JobResult.transaction do
        CollectJob.perform_later("g", 1)
        raise ActiveRecord::Rollback
      end
    end

    assert_equal 1, events.size
    assert_equal 1, SolidQueue::Job.count
  end

  if SolidQueue::Record.respond_to?(:current_transaction)
    test "counting from after_commit counts what committed, once" do
      assert_equal [ :created ], counted_after_commit { CollectJob.perform_later("g", 1) }

      assert_equal [ :collected ], counted_after_commit {
        SolidQueue::Record.transaction { CollectJob.perform_later("g", 2) }
      }

      assert_equal [], counted_after_commit {
        SolidQueue::Record.transaction do
          CollectJob.perform_later("g", 3)
          raise ActiveRecord::Rollback
        end
      }
    end

    test "counting from after_commit drops what a savepoint rolled back" do
      counted = counted_after_commit do
        SolidQueue::Record.transaction do
          SolidQueue::Record.transaction(requires_new: true) do
            DiscardJob.perform_later("rolled-back")
            raise ActiveRecord::Rollback
          end

          DiscardJob.perform_later("kept")
          DiscardJob.perform_later("kept")
        end
      end

      assert_equal [ :created, :discarded ], counted
    end

    test "counting from after_commit ignores an app database transaction" do
      counted = counted_after_commit do
        JobResult.transaction do
          CollectJob.perform_later("g", 1)
          raise ActiveRecord::Rollback
        end
      end

      assert_equal [ :created ], counted
    end
  end

  test "a discard call's event names the call and the job it joined" do
    first = DiscardJob.perform_later("g")
    second = nil

    events = capture_events("coalesce.solid_queue") { second = DiscardJob.perform_later("g") }

    assert_equal({ status: :discarded, key: "#{DiscardJob.name}/g", job_id: first.provider_job_id, active_job_id: second.job_id, class_name: DiscardJob.name }, events.sole.payload)
  end

  test "a worker starting a job emits no coalesce event" do
    CollectJob.perform_later("g", 1)

    assert_empty capture_events("coalesce.solid_queue") { perform_ready_jobs }
  end

  test "a retry emits no coalesce event and keeps the active job id" do
    FlakyJob.perform_later(1)
    job = SolidQueue::Job.sole

    events = capture_events("coalesce.solid_queue") { perform_ready_jobs }

    assert_empty events
    assert_equal job.active_job_id, SolidQueue::Job.where(finished_at: nil).sole.active_job_id
  end

  test "a mismatch names the job's class" do
    CollectJob.perform_later("g", 1)
    CollectJob.perform_later("g", 2)
    job = SolidQueue::Job.sole
    job.coalesced_calls.order(:id).first.delete

    events = capture_events("coalesced_calls_mismatch.solid_queue") { perform_ready_jobs }

    assert_equal({ job_id: job.id, active_job_id: job.active_job_id, class_name: CollectJob.name, expected: 2, actual: 1 }, events.sole.payload)
  end

  test "a dropped call is reported as deleted when other calls run" do
    RecordsJob.perform_later(@a)
    RecordsJob.perform_later(@b)
    job = SolidQueue::Job.sole
    dropped_id = job.coalesced_calls.order(:id).first.id
    @a.destroy!

    payload = capture_events("drop_coalesced_call.solid_queue") { perform_ready_jobs }.sole.payload

    assert_equal dropped_id, payload[:call_id]
    assert_equal RecordsJob.name, payload[:class_name]
    assert_equal true, payload[:call_deleted]
  end

  test "calls are reported as kept when every record is gone, and a re-run reports them again" do
    RecordsJob.perform_later(@a)
    job = SolidQueue::Job.sole
    @a.destroy!
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "crashing-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 10, process.id).sole

    # The worker dies before recording the job as finished
    first_run = capture_events("drop_coalesced_call.solid_queue") { claimed.send(:execute) }
    assert_equal 1, job.coalesced_calls.count
    claimed.release
    re_run = capture_events("drop_coalesced_call.solid_queue") { perform_ready_jobs }

    assert_equal [ false ], first_run.map { |event| event.payload[:call_deleted] }
    assert_equal [ false ], re_run.map { |event| event.payload[:call_deleted] }
  end

  test "a call whose row is already gone isn't reported as deleted" do
    RecordsJob.perform_later(@a)
    RecordsJob.perform_later(@b)
    job = SolidQueue::Job.sole
    dropped_id = job.coalesced_calls.order(:id).first.id
    @a.destroy!

    # Something else deletes the row after the worker read it
    events = capture_events("drop_coalesced_call.solid_queue") do
      at_start_of_perform(-> { SolidQueue::CoalescedCall.where(id: dropped_id).delete_all }) { perform_ready_jobs }
    end

    assert_equal false, events.sole.payload[:call_deleted]
  end

  test "a call that can't be deleted reports the error and isn't reported as deleted" do
    RecordsJob.perform_later(@a)
    RecordsJob.perform_later(@b)
    @a.destroy!

    SolidQueue::CoalescedCall.define_singleton_method(:where) { |*| raise ActiveRecord::StatementInvalid, "boom" }

    events = capture_events("drop_coalesced_call.solid_queue") do
      assert_raises(ActiveRecord::StatementInvalid) { perform_ready_jobs }
    end

    assert_equal "ActiveRecord::StatementInvalid", events.sole.payload[:exception].first
    assert_not events.sole.payload.key?(:call_deleted)
  ensure
    SolidQueue::CoalescedCall.singleton_class.remove_method(:where)
  end

  test "perform.active_job reports a job's call count on each attempt" do
    CollectJob.perform_later("g", 1)
    CollectJob.perform_later("g", 2)
    DiscardJob.perform_later("g")

    sizes = call_counts { perform_ready_jobs }

    assert_equal [ 2 ], sizes
  end

  test "perform.active_job reports a size of 0 when every record is gone" do
    RecordsJob.perform_later(@a)
    @a.destroy!

    assert_equal [ 0 ], call_counts { perform_ready_jobs }
  end

  test "a job waiting to retry counts in waiting, with all of its calls" do
    FlakyJob.perform_later(1)
    FlakyJob.perform_later(2)

    perform_ready_jobs

    assert_equal 2, SolidQueue::CoalescedCall.waiting.count
  end

  test "the oldest waiting call's age, and nil when nothing is waiting" do
    assert_nil SolidQueue::CoalescedCall.waiting.minimum(:created_at)

    CollectJob.perform_later("g", 1)
    oldest = SolidQueue::CoalescedCall.sole.created_at
    travel 1.minute
    CollectJob.perform_later("g", 2)

    assert_equal oldest, SolidQueue::CoalescedCall.waiting.minimum(:created_at)
  ensure
    travel_back
  end

  test "failed collected jobs and the calls they hold are separate numbers" do
    FailingJob.perform_later(1)
    FailingJob.perform_later(2)
    DiscardJob.perform_later("failed-too")
    SolidQueue::Job.find_by!(class_name: DiscardJob.name).failed_with(ExpectedTestError.new("boom"))

    assert_raises(ExpectedTestError) { perform_ready_jobs }

    assert_equal 2, SolidQueue::FailedExecution.count
    assert_equal 1, SolidQueue::FailedExecution.where(job_id: SolidQueue::CoalescedCall.select(:job_id)).count
    assert_equal 2, SolidQueue::CoalescedCall.where(job_id: SolidQueue::FailedExecution.select(:job_id)).count
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

    # A worker has read the job's calls by the time perform.active_job starts
    def at_start_of_perform(callback)
      listener = Class.new do
        define_method(:start) { |*| callback.call }
        define_method(:finish) { |*| }
      end.new
      subscriber = ActiveSupport::Notifications.subscribe("perform.active_job", listener)
      yield
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    def counted_after_commit
      counted = []
      subscriber = ActiveSupport::Notifications.subscribe("coalesce.solid_queue") do |*, payload|
        SolidQueue::Record.current_transaction.after_commit { counted << payload[:status] }
      end
      yield
      counted
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    def call_counts
      sizes = []
      subscriber = ActiveSupport::Notifications.subscribe("perform.active_job") do |*, payload|
        calls = payload[:job].coalesced_calls
        sizes << calls.size if calls
      end
      yield
      sizes
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end
end
