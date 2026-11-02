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

  CALLS_DURING_RUN = Concurrent::Array.new

  setup { @start = Time.current.change(usec: 0) }
  teardown { CALLS_DURING_RUN.clear }

  test "every job class in the recipes loads with the options it shows" do
    classes = File.read(File.expand_path("../../../README.md", __dir__))[/^## Recipes$.*?(?=^## )/m].scan(/^class \w+ < ApplicationJob$.*?^end$/m)

    assert_operator classes.size, :>=, 10
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

  private
    def calls_at(*seconds)
      seconds.each do |second|
        travel_to(@start + second.seconds) { yield }
      end
    end
end
