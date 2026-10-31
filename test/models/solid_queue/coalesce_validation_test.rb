# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceValidationTest < ActiveSupport::TestCase
  class CollectingRecurringJob < ApplicationJob
    coalesce key: "recurring", on_conflict: :collect

    def perform_batch(calls)
    end
  end

  class SimpleRecurringJob < ApplicationJob
    coalesce key: "recurring"

    def perform
      JobBuffer.add("ran")
    end
  end

  test "on_conflict defaults to discard, with no batch size" do
    job_class = new_job_class { coalesce key: "a" }

    assert_equal :discard, job_class.coalesce_on_conflict
    assert_nil job_class.coalesce_batch_size
  end

  test "on_conflict is discard or collect, and anything else raises" do
    assert_equal :discard, new_job_class { coalesce key: "a", on_conflict: :discard }.coalesce_on_conflict
    assert_equal :collect, new_job_class { coalesce key: "a", on_conflict: :collect }.coalesce_on_conflict

    [ :replace, :block, "collect", nil ].each do |value|
      error = assert_raises(ArgumentError, value.inspect) { new_job_class { coalesce key: "a", on_conflict: value } }
      assert_match(/on_conflict/, error.message)
    end
  end

  test "batch_size needs on_conflict collect" do
    error = assert_raises(ArgumentError) { new_job_class { coalesce key: "a", batch_size: 5 } }
    assert_match(/on_conflict: :collect/, error.message)
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", on_conflict: :discard, batch_size: 5 } }
  end

  test "batch_size has to be a positive integer" do
    [ 0, -1, "5", 1.5, false ].each do |value|
      assert_raises(ArgumentError, value.inspect) { new_job_class { coalesce key: "a", on_conflict: :collect, batch_size: value } }
    end

    assert_equal 5, new_job_class { coalesce key: "a", on_conflict: :collect, batch_size: 5 }.coalesce_batch_size
  end

  test "batch_size defaults to 100 when collecting" do
    assert_equal 100, new_job_class { coalesce key: "a", on_conflict: :collect }.coalesce_batch_size
  end

  test "the options from before don't exist" do
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", collect: true } }
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", on_conflict: :collect, limit: 5 } }
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", on_conflict: :collect, flush_when_full: false } }
  end

  test "key is required and has to be a string, a symbol or a proc" do
    assert_raises(ArgumentError) { new_job_class { coalesce } }
    assert_raises(ArgumentError) { new_job_class { coalesce key: 1 } }
    assert_raises(ArgumentError) { new_job_class { coalesce key: nil } }
    assert_raises(ArgumentError) { new_job_class { coalesce key: "" } }
    assert_raises(ArgumentError) { new_job_class { coalesce key: :"" } }
    assert new_job_class { coalesce key: :a }
    assert new_job_class { coalesce key: "a" }
    assert new_job_class { coalesce key: -> { "a" } }
  end

  test "unknown options raise" do
    %i[ group duration arguments ].each do |option|
      assert_raises(ArgumentError) { new_job_class { coalesce key: "a", option => 1 } }
    end
  end

  test "collect can't be combined with on_conflict discard, whichever comes first" do
    assert_raises(ArgumentError) do
      new_job_class do
        coalesce key: "a", on_conflict: :collect
        limits_concurrency key: "b", on_conflict: :discard
      end
    end

    assert_raises(ArgumentError) do
      new_job_class do
        limits_concurrency key: "b", on_conflict: :discard
        coalesce key: "a", on_conflict: :collect
      end
    end
  end

  test "collect composes with blocking concurrency limits, and simple mode with discarding ones" do
    assert new_job_class {
      coalesce key: "a", on_conflict: :collect
      limits_concurrency key: "b"
    }

    assert new_job_class {
      coalesce key: "a"
      limits_concurrency key: "b", on_conflict: :discard
    }
  end

  test "coalesced jobs can't be enqueued in a batch, single or bulk" do
    assert_no_difference -> { SolidQueue::Job.count } do
      assert_raises(SolidQueue::Job::CoalescedInBatchError) do
        SolidQueue::Batch.enqueue { SolidQueue::Job.enqueue(CoalescedRefreshJob.new("a")) }
      end

      assert_raises(SolidQueue::Job::CoalescedInBatchError) do
        SolidQueue::Batch.enqueue { ActiveJob.perform_all_later([ CoalescedRefreshJob.new("b") ]) }
      end

      assert_raises(SolidQueue::Job::CoalescedInBatchError) do
        SolidQueue::Batch.enqueue { CoalescedRefreshJob.perform_later("a") }
      end
    end
  end

  test "collecting jobs can't be enqueued in a batch either" do
    assert_raises(SolidQueue::Job::CoalescedInBatchError) do
      SolidQueue::Batch.enqueue { SolidQueue::Job.enqueue(CollectedCallsJob.new("g")) }
    end

    assert_raises(SolidQueue::Job::CoalescedInBatchError) do
      SolidQueue::Batch.enqueue { ActiveJob.perform_all_later([ CollectedCallsJob.new("g") ]) }
    end
  end

  test "a job built in a batch and enqueued in bulk afterwards can't either" do
    active_job = nil
    SolidQueue::Batch.enqueue { active_job = CoalescedRefreshJob.new("a") }

    assert_raises(SolidQueue::Job::CoalescedInBatchError) { SolidQueue::Job.enqueue_all([ active_job ]) }
  end

  test "normal jobs in a bulk enqueue aren't affected by the batch rule" do
    assert_difference -> { SolidQueue::Job.count }, +1 do
      SolidQueue::Batch.enqueue { ActiveJob.perform_all_later([ AddToBufferJob.new("a") ]) }
    end
  end

  test "a recurring task for a collect job is invalid" do
    task = SolidQueue::RecurringTask.new(key: "collecting", class_name: CollectingRecurringJob.name, schedule: "every hour")

    assert_not task.valid?
    assert_match(/collect/, task.errors.full_messages.to_sentence)
  end

  test "configuration validation rejects it too" do
    configuration = SolidQueue::Configuration.new(recurring_schedule_file: config_file_path(:recurring_with_collecting_job))

    assert_not configuration.valid?
    assert_match(/collect/, configuration.errors.full_messages.to_sentence)
  end

  test "a recurring task for a discarding job is valid and a suppressed tick records nothing" do
    task = SolidQueue::RecurringTask.new(key: "simple", class_name: SimpleRecurringJob.name, schedule: "every hour")
    assert task.valid?
    task.save!

    first = task.enqueue(at: 2.hours.ago)
    assert first.successfully_enqueued?
    assert_equal :created, first.coalesce_status
    assert_equal 1, SolidQueue::RecurringExecution.count

    second = task.enqueue(at: 1.hour.ago)
    assert_equal :discarded, second.coalesce_status
    assert_equal first.provider_job_id, second.provider_job_id
    assert_not second.successfully_enqueued?
    assert_equal 1, SolidQueue::RecurringExecution.count
  end

  test "a tick that collides with the open job doesn't raise or roll anything back" do
    task = SolidQueue::RecurringTask.create!(key: "simple", class_name: SimpleRecurringJob.name, schedule: "every hour")
    run_at = 1.hour.ago

    first = task.enqueue(at: run_at)

    # Another scheduler taking the same tick
    assert_nothing_raised { assert_not task.enqueue(at: run_at).successfully_enqueued? }

    assert_equal [ first.provider_job_id ], SolidQueue::RecurringExecution.where(task_key: "simple", run_at: run_at).pluck(:job_id)
    assert_equal [ first.provider_job_id ], SolidQueue::CoalesceKey.pluck(:job_id)
    assert_equal [ first.provider_job_id ], SolidQueue::Job.pluck(:id)

    perform_ready_jobs
    assert_equal [ "ran" ], JobBuffer.values
  end

  test "a tick after the open job has started enqueues and records again" do
    task = SolidQueue::RecurringTask.create!(key: "simple", class_name: SimpleRecurringJob.name, schedule: "every hour")

    task.enqueue(at: 2.hours.ago)
    perform_ready_jobs

    assert task.enqueue(at: 1.hour.ago).successfully_enqueued?
    assert_equal 2, SolidQueue::RecurringExecution.count
  end

  private
    def new_job_class(&block)
      Class.new(ApplicationJob, &block)
    end
end
