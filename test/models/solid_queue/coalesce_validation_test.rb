# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceValidationTest < ActiveSupport::TestCase
  class CollectingRecurringJob < ApplicationJob
    coalesce key: "recurring", collect: true

    def perform(calls)
    end
  end

  class SimpleRecurringJob < ApplicationJob
    coalesce key: "recurring"

    def perform
      JobBuffer.add("ran")
    end
  end

  test "limit needs collect" do
    error = assert_raises(ArgumentError) { new_job_class { coalesce key: "a", limit: 5 } }
    assert_match(/collect/, error.message)
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", collect: false, limit: 5 } }
  end

  test "flush_when_full needs collect" do
    [ true, false ].each do |value|
      error = assert_raises(ArgumentError) { new_job_class { coalesce key: "a", flush_when_full: value } }
      assert_match(/collect/, error.message)
      assert_raises(ArgumentError) { new_job_class { coalesce key: "a", collect: false, flush_when_full: value } }
    end
  end

  test "flush_when_full is true by default in collect mode and has to be a boolean" do
    assert new_job_class { coalesce key: "a", collect: true }.coalesce_flush_when_full
    assert_equal false, new_job_class { coalesce key: "a", collect: true, flush_when_full: false }.coalesce_flush_when_full
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", collect: true, flush_when_full: "no" } }
  end

  test "limit has to be a positive integer" do
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", collect: true, limit: 0 } }
    assert_raises(ArgumentError) { new_job_class { coalesce key: "a", collect: true, limit: "5" } }
  end

  test "limit defaults to 100 in collect mode" do
    assert_equal 100, new_job_class { coalesce key: "a", collect: true }.coalesce_limit
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
        coalesce key: "a", collect: true
        limits_concurrency key: "b", on_conflict: :discard
      end
    end

    assert_raises(ArgumentError) do
      new_job_class do
        limits_concurrency key: "b", on_conflict: :discard
        coalesce key: "a", collect: true
      end
    end
  end

  test "collect composes with blocking concurrency limits, and simple mode with discarding ones" do
    assert new_job_class {
      coalesce key: "a", collect: true
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

  test "a recurring task for a simple job is valid and a suppressed tick records nothing" do
    task = SolidQueue::RecurringTask.new(key: "simple", class_name: SimpleRecurringJob.name, schedule: "every hour")
    assert task.valid?
    task.save!

    first = task.enqueue(at: 2.hours.ago)
    assert first.successfully_enqueued?
    assert_equal 1, SolidQueue::RecurringExecution.count

    second = task.enqueue(at: 1.hour.ago)
    assert second.coalesced?
    assert_equal first.provider_job_id, second.provider_job_id
    assert_not second.successfully_enqueued?
    assert_equal 1, SolidQueue::RecurringExecution.count
  end

  private
    def new_job_class(&block)
      Class.new(ApplicationJob, &block)
    end
end
