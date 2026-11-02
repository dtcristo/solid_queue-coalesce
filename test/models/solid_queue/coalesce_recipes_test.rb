# frozen_string_literal: true

require "test_helper"

# What the README's recipes and diagrams promise. Each test replays a diagram's calls.
class SolidQueue::CoalesceRecipesTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class FixedWaitJob < ApplicationJob
    coalesce key: "fixed", on_conflict: :collect

    def perform_calls(_calls); end
  end

  class FullOrTimedJob < ApplicationJob
    coalesce key: "full", on_conflict: :collect, max_calls: 2, on_full: :dispatch

    def perform_calls(_calls); end
  end

  class DebouncedJob < ApplicationJob
    coalesce key: "debounced", debounce: 30.seconds

    def perform; end
  end

  class CappedJob < ApplicationJob
    coalesce key: "capped", debounce: 30.seconds, max_wait: 40.seconds

    def perform; end
  end

  class SealingJob < ApplicationJob
    coalesce key: "sealing", on_conflict: :collect, max_calls: 2

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
      CALLS_DURING_RUN << self.class.perform_later("D") if calls.first.arguments == [ "A" ]
    end
  end

  class FollowUpJob < ApplicationJob
    coalesce key: "follow_up"
    limits_concurrency to: 1, key: "follow_up", on_conflict: :block, duration: 5.minutes

    def perform(call)
      JobBuffer.add(call)

      if call == "A"
        CALLS_DURING_RUN << self.class.perform_later("B")
        CALLS_DURING_RUN << self.class.perform_later("C")
      end
    end
  end

  class DailyBatchJob < ApplicationJob
    coalesce key: "daily", on_conflict: :collect, max_calls: 2
    limits_concurrency to: 1, key: "daily", on_conflict: :block, duration: 1.hour

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class EveryCallInTurnJob < ApplicationJob
    limits_concurrency to: 1, key: "in_turn", on_conflict: :block, duration: 5.minutes

    def perform(call)
      JobBuffer.add(call)
    end
  end

  # The next jobs pin Solid Queue's own behaviour, which the README warns about. If
  # one starts failing, that warning needs another look.
  class DiscardingRetryJob < ApplicationJob
    limits_concurrency to: 1, key: "discarding_retry", on_conflict: :discard, duration: 5.minutes
    retry_on ExpectedTestError, wait: 0.seconds

    def perform
      JobBuffer.add(executions)
      raise ExpectedTestError if executions == 1
    end
  end

  class DiscardingDelayedRetryJob < ApplicationJob
    limits_concurrency to: 1, key: "discarding_delayed", on_conflict: :discard, duration: 5.minutes
    retry_on ExpectedTestError, wait: 1.minute

    def perform(call)
      JobBuffer.add(call)
      raise ExpectedTestError if call == "first"
    end
  end

  class DiscardingFailingJob < ApplicationJob
    limits_concurrency to: 1, key: "discarding_failing", on_conflict: :discard, duration: 5.minutes

    def perform(call)
      raise ExpectedTestError if call == "failing"
    end
  end

  class CoalescingRetryJob < ApplicationJob
    coalesce key: "coalescing_retry"
    retry_on ExpectedTestError, wait: 0.seconds

    def perform
      JobBuffer.add(executions)
      raise ExpectedTestError if executions == 1
    end
  end

  class BlockingRetryJob < ApplicationJob
    coalesce key: "blocking_retry"
    limits_concurrency to: 1, key: "blocking_retry", on_conflict: :block, duration: 5.minutes
    retry_on ExpectedTestError, wait: 0.seconds

    def perform
      JobBuffer.add(executions)
      raise ExpectedTestError if executions == 1
    end
  end

  class CoalesceDiscardingRetryJob < ApplicationJob
    coalesce key: "coalesce_discarding"
    limits_concurrency to: 1, key: "coalesce_discarding", on_conflict: :discard, duration: 5.minutes
    retry_on ExpectedTestError, wait: 0.seconds

    def perform
      raise ExpectedTestError
    end
  end

  class DiscardingDelayedJob < ApplicationJob
    limits_concurrency to: 1, key: "discarding_duplicates", on_conflict: :discard, duration: 5.minutes

    def perform; end
  end

  class DiscardingDuringRunJob < ApplicationJob
    limits_concurrency to: 1, key: "discarding_during_run", on_conflict: :discard, duration: 5.minutes

    def perform(call)
      JobBuffer.add(call)
      CALLS_DURING_RUN << self.class.perform_later("during") if call == "first"
    end
  end

  class DiscardingAboveLimitJob < ApplicationJob
    limits_concurrency to: 2, key: "discarding_above_limit", on_conflict: :discard, duration: 5.minutes

    def perform; end
  end

  class DiscardingGroupedJob < ApplicationJob
    limits_concurrency key: "shared", group: "discarding_group", on_conflict: :discard, duration: 5.minutes

    def perform; end
  end

  class OtherDiscardingGroupedJob < ApplicationJob
    limits_concurrency key: "shared", group: "discarding_group", on_conflict: :discard, duration: 5.minutes

    def perform; end
  end

  class CoalescingDelayedJob < ApplicationJob
    coalesce key: "coalescing_duplicates"

    def perform; end
  end

  CALLS_DURING_RUN = Concurrent::Array.new

  setup { @start = Time.current.change(usec: 0) }
  teardown { CALLS_DURING_RUN.clear }

  test "every job class in the recipes loads with the options it shows" do
    classes = File.read(File.expand_path("../../../README.md", __dir__))[/^## Recipes$.*?(?=^## )/m].scan(/^class \w+ < ApplicationJob$.*?^end$/m)

    assert_operator classes.size, :>=, 9
    classes.each { |definition| Module.new.module_eval(definition) }
  end

  test "a fixed wait runs 30 seconds after the first call, with every call" do
    calls_at(0, 10, 20) { FixedWaitJob.set(wait: 30.seconds).perform_later }

    job = SolidQueue::Job.sole
    assert_equal @start + 30.seconds, job.scheduled_at
    assert_equal 3, job.coalesced_calls.count
  end

  test "a job that fills runs straight away, and the next call opens a job with its own wait" do
    calls_at(0, 10, 20) { FullOrTimedJob.set(wait: 30.seconds).perform_later }

    first, second = SolidQueue::Job.order(:id).to_a
    assert first.ready?
    assert_equal 2, first.coalesced_calls.count
    assert_equal @start + 10.seconds, first.scheduled_at
    assert second.scheduled?
    assert_equal 1, second.coalesced_calls.count
    assert_equal @start + 50.seconds, second.scheduled_at
  end

  test "a debounced job runs 30 seconds after the last call" do
    calls_at(0, 10, 20) { DebouncedJob.perform_later }

    assert_equal @start + 50.seconds, SolidQueue::Job.sole.scheduled_at
  end

  test "max_wait caps a debounced job at 40 seconds after the first call" do
    calls_at(0, 10, 20) { CappedJob.perform_later }

    assert_equal @start + 40.seconds, SolidQueue::Job.sole.scheduled_at
  end

  test "a key opens, seals and starts as the lifecycle diagram draws it" do
    a, b, c = [ "A", "B", "C" ].map { |call| SealingJob.set(wait: 1.minute).perform_later(call) }

    assert_equal [ :created, :collected, :created ], [ a, b, c ].map(&:coalesce_status)
    assert_equal a.provider_job_id, b.provider_job_id
    assert_not_equal a.provider_job_id, c.provider_job_id

    travel_to 2.minutes.from_now do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      perform_ready_jobs
    end

    during_first_run = CALLS_DURING_RUN.sole
    assert_equal :collected, during_first_run.coalesce_status, "job 2 still holds the key while job 1 runs"
    assert_equal c.provider_job_id, during_first_run.provider_job_id
    assert_equal [ [ [ "A" ], [ "B" ] ], [ [ "C" ], [ "D" ] ] ], JobBuffer.values

    assert_equal :created, SealingJob.perform_later("E").coalesce_status
  end

  test "calls during a run merge into one follow-up, and the next call after it runs straight away" do
    first = FollowUpJob.perform_later("A")
    assert SolidQueue::Job.find(first.provider_job_id).ready?

    perform_ready_jobs

    created, discarded = CALLS_DURING_RUN.to_a
    assert_equal [ :created, :discarded ], [ created.coalesce_status, discarded.coalesce_status ]
    assert_equal created.provider_job_id, discarded.provider_job_id

    follow_up = SolidQueue::Job.find(created.provider_job_id)
    assert follow_up.ready?, "the blocked job is released when the first run finishes"

    perform_ready_jobs
    assert_equal [ "A", "B" ], JobBuffer.values

    next_call = FollowUpJob.perform_later("D")
    assert SolidQueue::Job.find(next_call.provider_job_id).ready?
  end

  test "a batch at midnight collects calls until then, and runs its jobs one at a time" do
    midnight = Date.tomorrow.midnight
    calls = 3.times.map { |call| DailyBatchJob.set(wait_until: midnight).perform_later(call) }

    assert_equal [ :created, :collected, :created ], calls.map(&:coalesce_status)
    assert_equal [ midnight ], SolidQueue::Job.pluck(:scheduled_at).uniq

    travel_to midnight + 1.second do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      assert_equal [ :ready, :blocked ], SolidQueue::Job.order(:id).map(&:status)

      2.times { perform_ready_jobs }
      assert_equal [ [ [ 0 ], [ 1 ] ], [ [ 2 ] ] ], JobBuffer.values

      next_call = DailyBatchJob.set(wait_until: Date.tomorrow.midnight).perform_later(3)
      assert_equal :created, next_call.coalesce_status
      assert_equal midnight + 1.day, SolidQueue::Job.find(next_call.provider_job_id).scheduled_at
    end
  end

  test "without coalesce, every call runs in turn" do
    first, second = [ "A", "B" ].map { |call| EveryCallInTurnJob.perform_later(call) }

    assert SolidQueue::Job.find(first.provider_job_id).ready?
    assert SolidQueue::Job.find(second.provider_job_id).blocked?

    2.times { perform_ready_jobs }
    assert_equal [ "A", "B" ], JobBuffer.values
  end

  test "a concurrency discard loses an immediate retry, and records no failure" do
    DiscardingRetryJob.perform_later

    2.times { perform_ready_jobs }

    assert_equal [ 1 ], JobBuffer.values
    assert_no_unfinished_jobs
    assert_empty SolidQueue::FailedExecution.all
  end

  test "a concurrency discard loses a delayed retry when another job holds the semaphore" do
    DiscardingDelayedRetryJob.perform_later("first")
    perform_ready_jobs
    DiscardingDelayedRetryJob.perform_later("second")

    travel_to 2.minutes.from_now do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      perform_ready_jobs
    end

    assert_equal [ "first", "second" ], JobBuffer.values
    assert_no_unfinished_jobs
  end

  test "a concurrency discard deletes a failed job on a manual retry when another job holds the semaphore" do
    DiscardingFailingJob.perform_later("failing")
    assert_raises(ExpectedTestError) { perform_ready_jobs }
    other = DiscardingFailingJob.perform_later("other")

    SolidQueue::FailedExecution.sole.retry

    assert_empty SolidQueue::FailedExecution.all
    assert_equal [ other.provider_job_id ], SolidQueue::Job.pluck(:id)
  end

  test "a coalesced retry always runs" do
    CoalescingRetryJob.perform_later
    2.times { perform_ready_jobs }

    assert_equal [ 1, 2 ], JobBuffer.values
  end

  test "a coalesced retry with a blocking limit runs when the semaphore frees" do
    BlockingRetryJob.perform_later
    2.times { perform_ready_jobs }

    assert_equal [ 1, 2 ], JobBuffer.values
    assert_no_unfinished_jobs
  end

  test "a coalesced retry that a concurrency discard rejects fails the job and keeps it" do
    job = CoalesceDiscardingRetryJob.perform_later

    assert_raises(SolidQueue::Job::EnqueueError) { perform_ready_jobs }

    assert SolidQueue::Job.find(job.provider_job_id).failed?
  end

  test "delayed duplicates each get a job with a concurrency discard, and share one with coalesce" do
    5.times { DiscardingDelayedJob.set(wait: 5.minutes).perform_later }
    5.times { CoalescingDelayedJob.set(wait: 5.minutes).perform_later }

    assert_equal 5, SolidQueue::Job.where(class_name: DiscardingDelayedJob.name).count
    assert_equal 1, SolidQueue::Job.where(class_name: CoalescingDelayedJob.name).count
  end

  test "a concurrency discard drops a call during a run" do
    DiscardingDuringRunJob.perform_later("first")

    perform_ready_jobs

    assert_nil CALLS_DURING_RUN.sole.provider_job_id
    assert_equal [ "first" ], JobBuffer.values
    assert_no_unfinished_jobs
  end

  test "a concurrency discard drops calls until duration passes if a semaphore isn't released" do
    held = DiscardingDelayedJob.perform_later
    SolidQueue::ReadyExecution.claim(SolidQueue::Job.find(held.provider_job_id).queue_name, 1, SolidQueue::Process.register(kind: "Worker", pid: 1, name: "dead").id)

    assert_nil DiscardingDelayedJob.perform_later.provider_job_id

    travel_to 6.minutes.from_now do
      SolidQueue::Semaphore.expired.delete_all

      assert DiscardingDelayedJob.perform_later.provider_job_id
    end
  end

  test "a concurrency discard sheds load above a limit of more than one" do
    jobs = 3.times.map { DiscardingAboveLimitJob.perform_later }

    assert_equal [ true, true, false ], jobs.map { |job| job.provider_job_id.present? }
  end

  test "a concurrency discard dedupes across classes that share a group" do
    DiscardingGroupedJob.perform_later

    assert_nil OtherDiscardingGroupedJob.perform_later.provider_job_id
  end

  private
    def calls_at(*seconds)
      seconds.each do |second|
        travel_to(@start + second.seconds) { yield }
      end
    end
end
