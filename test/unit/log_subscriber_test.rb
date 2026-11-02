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
    instrument "coalesce.solid_queue", key: "MyJob/1", job_id: 42, active_job_id: "abc", class_name: "MyJob", status: :discarded

    assert_match_logged :debug, "Coalesce call", "job_id: 42, active_job_id: \"abc\", class_name: \"MyJob\", outcome: :discarded"
  end

  test "a coalesce line leaves out the key, which the app defines" do
    attach_log_subscriber
    instrument "coalesce.solid_queue", key: "MyJob/secret", job_id: 42, active_job_id: "abc", class_name: "MyJob", status: :created

    assert_no_match(/secret/, @logger.logged(:debug).join)
  end

  test "a coalesce line isn't built above debug" do
    subscriber = attach_log_subscriber
    @logger.level = Logger::INFO

    assert subscriber.silenced?("coalesce.solid_queue")
    assert_not subscriber.silenced?("drop_coalesced_call.solid_queue")
    assert_not subscriber.silenced?("coalesced_calls_mismatch.solid_queue")
  end

  test "drop a coalesced call" do
    attach_log_subscriber
    instrument "drop_coalesced_call.solid_queue", call_id: 7, job_id: 42, active_job_id: "abc", class_name: "MyJob", exception_class: "ActiveJob::DeserializationError", call_deleted: false

    assert_match_logged :warn, "Drop coalesced call for a deleted record", "call_id: 7, job_id: 42, active_job_id: \"abc\", class_name: \"MyJob\", exception_class: \"ActiveJob::DeserializationError\", call_deleted: false"
  end

  test "calls that don't match the key's count" do
    attach_log_subscriber
    instrument "coalesced_calls_mismatch.solid_queue", job_id: 42, active_job_id: "abc", class_name: "MyJob", expected: 3, actual: 2

    assert_match_logged :warn, "Coalesced calls don't match the key's count", "job_id: 42, active_job_id: \"abc\", class_name: \"MyJob\", expected: 3, actual: 2"
  end

  test "no event logs arguments or payloads, whatever it's given" do
    attach_log_subscriber
    instrument "coalesce.solid_queue", key: "MyJob/1", job_id: 42, active_job_id: "abc", class_name: "MyJob", status: :created, arguments: [ "secret" ], job_data: { "arguments" => [ "secret" ] }
    instrument "drop_coalesced_call.solid_queue", call_id: 7, job_id: 42, active_job_id: "abc", exception_class: "E", arguments: [ "secret" ]
    instrument "coalesced_calls_mismatch.solid_queue", job_id: 42, active_job_id: "abc", expected: 3, actual: 2, arguments: [ "secret" ]

    assert_no_match(/secret/, (@logger.logged(:debug) + @logger.logged(:warn)).join)
  end

  private
    def attach_log_subscriber
      SolidQueue::LogSubscriber.new.tap { |subscriber| SolidQueue::LogSubscriber.attach_to :solid_queue, subscriber }
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
