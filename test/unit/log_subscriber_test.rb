# frozen_string_literal: true

require "test_helper"
require "active_support/log_subscriber/test_helper"

class LogSubscriberTest < ActiveSupport::TestCase
  include ActiveSupport::LogSubscriber::TestHelper

  teardown { ActiveSupport::LogSubscriber.log_subscribers.clear }

  # The helper sets a nil logger on teardown, and forked supervisors in later tests need one
  def set_logger(logger)
    @previous_logger ||= SolidQueue.logger
    SolidQueue.logger = logger || @previous_logger
  end

  test "coalesce a call" do
    attach_log_subscriber
    instrument "coalesce.solid_queue", key: "MyJob/1", job_id: 42, active_job_id: "abc", class_name: "MyJob", action: :discarded

    assert_match_logged :debug, "Coalesce call", "key: \"MyJob/1\", job_id: 42, active_job_id: \"abc\", class_name: \"MyJob\", outcome: :discarded"
  end

  test "drop a coalesced call" do
    attach_log_subscriber
    instrument "drop_coalesced_call.solid_queue", call_id: 7, job_id: 42, active_job_id: "abc", exception_class: "ActiveJob::DeserializationError"

    assert_match_logged :warn, "Drop coalesced call for a deleted record", "call_id: 7, job_id: 42, active_job_id: \"abc\", exception_class: \"ActiveJob::DeserializationError\""
  end

  test "neither event logs arguments or payloads, whatever it's given" do
    attach_log_subscriber
    instrument "coalesce.solid_queue", key: "MyJob/1", job_id: 42, active_job_id: "abc", class_name: "MyJob", action: :created, arguments: [ "secret" ], job_data: { "arguments" => [ "secret" ] }
    instrument "drop_coalesced_call.solid_queue", call_id: 7, job_id: 42, active_job_id: "abc", exception_class: "E", arguments: [ "secret" ]

    assert_no_match(/secret/, (@logger.logged(:debug) + @logger.logged(:warn)).join)
  end

  private
    def attach_log_subscriber
      ActiveSupport::LogSubscriber.attach_to :solid_queue, SolidQueue::LogSubscriber.new
    end

    def instrument(...)
      ActiveSupport::Notifications.instrument(...)
      wait
    end

    def assert_match_logged(level, action, attributes)
      assert_equal 1, @logger.logged(level).size
      assert_match /SolidQueue-[\d.]+(\.beta)? #{action} \(\d+\.\d+ms\)  #{Regexp.escape(attributes)}/, @logger.logged(level).last
    end
end
