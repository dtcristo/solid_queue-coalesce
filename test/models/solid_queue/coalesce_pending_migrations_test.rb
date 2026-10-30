# frozen_string_literal: true

require "test_helper"
require_relative "../../../lib/generators/solid_queue/coalesce/install/templates/db/create_solid_queue_coalesce_tables"

class SolidQueue::CoalescePendingMigrationsTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  # Recreate an app that hasn't run the optional migration by reverting the
  # actual migration that ships with the install generator.
  setup do
    migrate(:down)
  end

  teardown do
    migrate(:up)
    destroy_records
  end

  test "the coalescing schema counts as pending migrations" do
    assert_not SolidQueue::CoalesceKey.migrated?
  end

  test "enqueueing a coalesced job raises a clear error, single or bulk" do
    error = assert_raises(SolidQueue::CoalesceKey::PendingMigrations) { CoalescedRefreshJob.perform_later("a") }
    assert_match(/solid_queue:coalesce:install/, error.message)

    assert_raises(SolidQueue::CoalesceKey::PendingMigrations) { SolidQueue::Job.enqueue_all([ CollectedCallsJob.new("g") ]) }
    assert_raises(SolidQueue::CoalesceKey::PendingMigrations) { SolidQueue::Job.enqueue_all([ CoalescedRefreshJob.new("a") ]) }
    assert_equal 0, SolidQueue::Job.count
  end

  test "normal jobs enqueue, run, fail and retry as before" do
    active_job = AddToBufferJob.perform_later("hey")
    ActiveJob.perform_all_later([ AddToBufferJob.new("ho") ])
    assert_equal 2, SolidQueue::Job.where(class_name: "AddToBufferJob").count

    perform_ready_jobs
    assert_equal %w[ hey ho ], JobBuffer.values.sort

    SolidQueue::Job.find_by!(active_job_id: active_job.job_id).failed_with(ExpectedTestError.new("boom"))
    assert_equal 1, SolidQueue::FailedExecution.count
  end

  test "maintenance skips the sweeper" do
    maintenance = SolidQueue::Dispatcher::Maintenance.new(600, 10, concurrency: true, batches: false)

    assert_nothing_raised { maintenance.send(:release_stranded_coalesce_keys) }
  end

  test "perform_now of a coalesced job doesn't need the tables" do
    CoalescedRefreshJob.perform_now("a")

    CollectedCallsJob.perform_now("g", 1)

    assert_equal [ "a", [ [ "g", 1 ] ] ], JobBuffer.values
  end

  private
    def migrate(direction)
      ActiveRecord::Migration.suppress_messages do
        SolidQueue::Record.connection_pool.with_connection do |connection|
          CreateSolidQueueCoalesceTables.new.exec_migration(connection, direction)
        end
      end

      SolidQueue::CoalesceKey.instance_variable_set(:@migrated, nil)
      SolidQueue::Record.connection_pool.disconnect!
    end
end
