# frozen_string_literal: true

require "test_helper"
require "open3"

class ReloadingTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  # Reloading can't be turned on in this process, since it's fixed at boot.
  test "the patches survive a code reload" do
    output, status = Open3.capture2e({ "RAILS_ENV" => "test", "ENABLE_RELOADING" => "1", "TARGET_DB" => ENV["TARGET_DB"] }, RbConfig.ruby, "-e", <<~RUBY)
      require "#{Rails.root.join("config/environment")}"

      before = SolidQueue::Job
      Rails.application.reloader.reload!
      raise "SolidQueue::Job wasn't reloaded" if before.equal?(SolidQueue::Job)

      active_job = CoalescedRefreshJob.perform_later("reloaded")
      joined = CoalescedRefreshJob.perform_later("reloaded")
      raise "Job.enqueue lost its patch" unless joined.coalesced?

      job = SolidQueue::Job.find(active_job.provider_job_id)
      raise "Job lost its coalesced_arguments" unless job.coalesced_arguments.none?

      job.failed_with(RuntimeError.new("boom"))
      raise "Job#failed_with lost its patch" if SolidQueue::CoalesceKey.exists?(key: active_job.coalesce_key)

      task = SolidQueue::RecurringTask.new(key: "collecting", class_name: "CollectedCallsJob", schedule: "every hour")
      raise "RecurringTask lost its validation" if task.valid?

      job.destroy!
      puts "reloaded"
    RUBY

    assert status.success?, output
    assert_match(/reloaded/, output)
  end
end
