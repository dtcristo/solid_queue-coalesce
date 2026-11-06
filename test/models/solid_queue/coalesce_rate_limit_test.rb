# frozen_string_literal: true

require "test_helper"

# With solid_queue-rate_limit, calls coalesce at enqueue and the rate is checked at
# claim. A job waiting on the rate hasn't started, so it keeps its coalesce key.
class SolidQueue::CoalesceRateLimitTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class DiscardingJob < ApplicationJob
    coalesce key: ->(account) { account }
    limits_rate to: 1, per: 10.seconds, group: "CoalesceRateLimitTest", key: :global

    def perform(account)
      JobBuffer.add(account)
    end
  end

  class CollectingJob < ApplicationJob
    coalesce key: ->(account, _id) { account }, on_conflict: :collect, max_calls: 3
    limits_rate to: 1, per: 10.seconds, group: "CoalesceRateLimitTest", key: :global

    def perform_calls(calls)
      JobBuffer.add(calls.map { |call| call.arguments.last })
    end
  end

  class ReplacingJob < ApplicationJob
    coalesce key: ->(account, _value) { account }, on_conflict: :replace
    limits_rate to: 1, per: 10.seconds, group: "CoalesceRateLimitTest", key: :global

    def perform(account, value)
      JobBuffer.add([ account, value ])
    end
  end

  class DebouncedReplacingJob < ApplicationJob
    coalesce key: ->(account, _value) { account }, on_conflict: :replace, debounce: 5.seconds
    limits_rate to: 1, per: 30.seconds, group: "CoalesceRateLimitTestSlow", key: :global

    def perform(account, value)
      JobBuffer.add([ account, value ])
    end
  end

  class SlowLimitedJob < ApplicationJob
    limits_rate to: 1, per: 30.seconds, group: "CoalesceRateLimitTestSlow", key: :global

    def perform; end
  end

  # Coalesces by account, and takes the rate of the region each call names
  class RegionalJob < ApplicationJob
    coalesce key: ->(account, _region) { account }, on_conflict: :replace
    limits_rate to: 1, per: 10.seconds, group: "CoalesceRateLimitTestRegions", key: ->(_account, region) { region }
    retry_on ExpectedTestError, wait: 0.seconds, attempts: 2

    cattr_accessor :fail_once, default: false

    def perform(account, region)
      if fail_once
        self.class.fail_once = false
        raise ExpectedTestError
      end

      JobBuffer.add([ account, region ])
    end
  end

  class RegionalSpendJob < ApplicationJob
    limits_rate to: 1, per: 10.seconds, group: "CoalesceRateLimitTestRegions", key: ->(region) { region }

    def perform(region); end
  end

  class SyncContactJob < ApplicationJob
    coalesce key: ->(user, _attributes) { user }, on_conflict: :replace
    limits_concurrency key: ->(user, _attributes) { user }
    limits_rate to: 1, per: 10.seconds, group: "CoalesceRateLimitTest", key: :global

    cattr_accessor :during_perform

    def perform(user, attributes)
      JobBuffer.add([ user, attributes ])
      during_perform&.call(user)
    end
  end

  setup do
    @start = Time.utc(2026, 1, 1, 12)
    travel_to @start
    @process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "coalesce-rate-limit-worker")
  end

  teardown do
    SyncContactJob.during_perform = nil
    RegionalJob.fail_once = false
    travel_back
  end

  test "a job waiting on the rate keeps its coalesce key, so later calls join it and it runs once" do
    DiscardingJob.perform_later("a")
    DiscardingJob.perform_later("b")
    perform_claimed
    assert_equal [ "a" ], JobBuffer.values

    later = 3.times.map { DiscardingJob.perform_later("b") }
    assert_equal [ :discarded ] * 3, later.map(&:coalesce_status)

    pass_window
    assert_equal [ "a", "b" ], JobBuffer.values
    assert_equal 2, SolidQueue::Job.count
  end

  test "a collecting job keeps collecting while it waits on the rate, and the next call after it's full opens another" do
    CollectingJob.perform_later("a", 1)
    perform_claimed
    CollectingJob.perform_later("a", 2)
    perform_claimed

    statuses = [ 3, 4, 5 ].map { |id| CollectingJob.perform_later("a", id).coalesce_status }
    assert_equal [ :collected, :collected, :created ], statuses

    pass_window
    pass_window
    assert_equal [ [ 1 ], [ 2, 3, 4 ], [ 5 ] ], JobBuffer.values
  end

  test "a replacing job waiting on the rate takes later calls' arguments, and runs once with the latest" do
    ReplacingJob.perform_later("a", 1)
    ReplacingJob.perform_later("b", 1)
    perform_claimed

    later = [ 2, 3 ].map { |value| ReplacingJob.perform_later("b", value) }
    assert_equal [ :replaced, :replaced ], later.map(&:coalesce_status)

    pass_window
    assert_equal [ [ "a", 1 ], [ "b", 3 ] ], JobBuffer.values
    assert_equal 2, SolidQueue::Job.count
  end

  test "debounce moves a replacing job waiting on the rate only to a later time" do
    SlowLimitedJob.perform_later
    claim
    job = DebouncedReplacingJob.perform_later("a", 1)

    travel 5.seconds
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    assert_empty claim
    assert_equal @start + 30.seconds, scheduled_at(job)

    travel 5.seconds
    DebouncedReplacingJob.perform_later("a", 2)
    assert_equal @start + 30.seconds, scheduled_at(job)

    travel 18.seconds
    DebouncedReplacingJob.perform_later("a", 3)
    assert_equal @start + 33.seconds, scheduled_at(job)

    travel_to @start + 33.seconds
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    perform_claimed
    assert_equal [ [ "a", 3 ] ], JobBuffer.values
  end

  test "a replaced job is charged to the first call's rate limit key, and runs with the latest arguments" do
    RegionalSpendJob.perform_later("us")
    perform_claimed

    RegionalJob.perform_later("a", "eu")
    assert_equal :replaced, RegionalJob.perform_later("a", "us").coalesce_status
    perform_claimed

    assert_equal [ [ "a", "us" ] ], JobBuffer.values
  end

  test "a bulk enqueue that replaces a pending job keeps its rate limit key" do
    RegionalSpendJob.perform_later("us")
    perform_claimed

    RegionalJob.perform_later("a", "eu")
    ActiveJob.perform_all_later(RegionalJob.new("a", "us"))
    perform_claimed

    assert_equal [ [ "a", "us" ] ], JobBuffer.values
  end

  test "a replacement after the claim keeps the key the claim charged" do
    RegionalSpendJob.perform_later("us")
    perform_claimed

    RegionalJob.perform_later("a", "eu")
    claimed = claim
    RegionalJob.perform_later("a", "us")
    claimed.each(&:perform)

    assert_equal [ [ "a", "us" ] ], JobBuffer.values
    assert_equal "CoalesceRateLimitTestRegions/eu", SolidQueue::Job.find_by!(class_name: RegionalJob.name).arguments[ActiveJob::RateControls::RATE_LIMIT_KEY]
  end

  test "a retry of a replaced job keeps the first call's rate limit key" do
    RegionalJob.fail_once = true
    RegionalJob.perform_later("a", "eu")
    RegionalJob.perform_later("a", "us")
    perform_claimed

    retry_job = SolidQueue::Job.where(finished_at: nil).sole
    assert_equal "CoalesceRateLimitTestRegions/eu", retry_job.arguments[ActiveJob::RateControls::RATE_LIMIT_KEY]
  end

  test "with a concurrency limit, a call during a run waits behind it and takes later arguments, and waits on the rate without holding the slot" do
    successor = nil
    SyncContactJob.perform_later("a", { "name" => "Ann" })
    SyncContactJob.perform_later("b", { "name" => "Bob" })

    SyncContactJob.during_perform = ->(user) do
      next unless user == "a" && JobBuffer.values.one?

      successor = SyncContactJob.perform_later("a", { "name" => "Anna" })
      assert_equal :created, successor.coalesce_status
      assert_equal :replaced, SyncContactJob.perform_later("a", { "name" => "Annie" }).coalesce_status
      assert SolidQueue::BlockedExecution.exists?(job_id: successor.provider_job_id)
    end

    perform_claimed
    assert_equal [ [ "a", { "name" => "Ann" } ] ], JobBuffer.values

    assert_empty claim
    assert SolidQueue::ScheduledExecution.exists?(job_id: successor.provider_job_id)
    assert_equal 0, SolidQueue::Semaphore.where("value < 1").count

    pass_window
    assert_equal [ "b", { "name" => "Bob" } ], JobBuffer.values.last

    pass_window
    assert_equal [ "a", { "name" => "Annie" } ], JobBuffer.values.last
    assert_equal 3, JobBuffer.values.size
  end

  private
    def claim
      SolidQueue::ReadyExecution.claim("*", 10, @process.id)
    end

    def perform_claimed
      claim.each(&:perform)
    end

    def pass_window
      travel 10.seconds
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      perform_claimed
    end

    def scheduled_at(active_job)
      SolidQueue::ScheduledExecution.find_by!(job_id: active_job.provider_job_id).scheduled_at
    end
end
