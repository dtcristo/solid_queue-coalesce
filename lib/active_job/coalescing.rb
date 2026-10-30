# frozen_string_literal: true

module ActiveJob
  module Coalescing
    extend ActiveSupport::Concern

    DEFAULT_COALESCE_LIMIT = 100

    # Sits in front of the job's own #perform, whichever class defines it, so
    # #arguments can keep holding this call's own arguments.
    module CollectedPerform
      def perform(*)
        super(@coalesced_calls)
      end
    end

    included do
      class_attribute :coalesce_key, instance_accessor: false
      class_attribute :coalesce_collect, default: false, instance_accessor: false
      class_attribute :coalesce_limit, instance_accessor: false
      class_attribute :coalesce_flush_when_full, instance_accessor: false

      attr_writer :coalesced
    end

    class_methods do
      def coalesce(key:, collect: false, limit: nil, flush_when_full: nil)
        unless (key.is_a?(String) || key.is_a?(Symbol) || key.is_a?(Proc)) && key.present?
          raise ArgumentError, "`key` must be a non-blank String or Symbol, or a Proc"
        end

        if limit && !collect
          raise ArgumentError, "`limit` needs `collect: true`"
        end

        if !flush_when_full.nil? && !collect
          raise ArgumentError, "`flush_when_full` needs `collect: true`"
        end

        limit ||= DEFAULT_COALESCE_LIMIT if collect

        if limit && !(limit.is_a?(Integer) && limit.positive?)
          raise ArgumentError, "`limit` must be a positive integer"
        end

        flush_when_full = true if collect && flush_when_full.nil?

        unless flush_when_full.nil? || flush_when_full == true || flush_when_full == false
          raise ArgumentError, "`flush_when_full` must be true or false"
        end

        ensure_coalesce_compatible_with_concurrency_controls(collect, concurrency_on_conflict)

        self.coalesce_key = key
        self.coalesce_collect = collect
        self.coalesce_limit = limit
        self.coalesce_flush_when_full = flush_when_full
      end

      def limits_concurrency(**)
        super.tap { ensure_coalesce_compatible_with_concurrency_controls(coalesce_collect, concurrency_on_conflict) }
      end

      private
        # A conflict destroys the job, and with it the calls it has collected
        def ensure_coalesce_compatible_with_concurrency_controls(collect, on_conflict)
          if collect && on_conflict == :discard
            raise ArgumentError, "`coalesce` with `collect: true` can't be combined with `limits_concurrency` with `on_conflict: :discard`"
          end
        end
    end

    # Runs before the arguments are deserialized. Raising here reaches the rescue
    # in ClaimedExecution#execute, which turns it into a failed execution.
    def perform_now
      start_coalesced_run
      super
    end

    # Scoped to the job class name, and built like concurrency_key
    def coalesce_key
      if self.class.coalesce_key
        param = compute_concurrency_parameter(self.class.coalesce_key)

        case param
        when ActiveRecord::Base
          [ self.class.name, param.class.name, param.id ]
        else
          [ self.class.name, param ]
        end.compact.join("/")
      end
    end

    # Whether this call joined a job that was already open
    def coalesced?
      !!@coalesced
    end

    # A retry is the same work continuing, so it can't join or suppress anything
    def coalesced_enqueue?
      self.class.coalesce_key.present? && executions.to_i.zero?
    end

    # The stored key, in case the arguments it was computed from are gone
    def concurrency_key
      @coalesced_arguments_gone ? @coalesced_run["concurrency_key"] : super
    end

    # A retry that doesn't get enqueued has to fail the run: finishing would
    # destroy the job and the calls it owns, with no job left to process them.
    def retry_job(options = {})
      super.tap do |enqueued|
        if @coalesced_run && !enqueued
          raise enqueue_error || SolidQueue::Job::EnqueueError.new("Couldn't enqueue the retry of #{self.class.name} (#{job_id})")
        end
      end
    end

    private
      # Only the run of a job a worker has claimed. perform_now on an instance that
      # went through perform_later also has a provider_job_id.
      def start_coalesced_run
        if self.class.coalesce_key && provider_job_id && self.class.queue_adapter_name == "solid_queue"
          @coalesced_run = SolidQueue::Job.joins(:claimed_execution).find_by(id: provider_job_id)&.start_coalesced_run
        end
      end

      def _perform_job
        return super unless self.class.coalesce_collect

        @coalesced_calls = coalesced_calls

        if @coalesced_calls.any?
          extend CollectedPerform
          super
        end
      end

      # The job's own arguments are a copy of its first call's, which may be for a
      # record that's gone by now. That call's row decides what happens to it.
      def deserialize_arguments_if_needed
        super
      rescue ActiveJob::DeserializationError => error
        raise unless worker_coalesced_rows.any? && error.cause.is_a?(ActiveRecord::RecordNotFound)

        @coalesced_arguments_gone = true
        @arguments = []
      end

      def worker_coalesced_rows
        @coalesced_run&.fetch("calls") || []
      end

      def coalesced_calls
        if worker_coalesced_rows.any?
          calls_from_coalesced_rows
        else
          [ coalesced_call_from(arguments) ]
        end
      end

      # Anything but a deleted record raises on the way, before any row has been
      # dropped, and fails the run with its rows. When every record is gone the rows
      # stay until the job is destroyed: if this run crashes before finishing, the
      # re-run needs them to tell it has nothing to do, because a job without rows
      # runs its own arguments.
      def calls_from_coalesced_rows
        loaded, missing = worker_coalesced_rows.map { |id, serialized| [ id, load_coalesced_row(serialized) ] }.partition(&:last)
        drop_coalesced_rows(missing, delete: loaded.any?)

        loaded.map { |_, call_arguments| coalesced_call_from(call_arguments) }
      end

      def load_coalesced_row(serialized)
        ActiveJob::Arguments.deserialize(serialized)
      rescue ActiveJob::DeserializationError => error
        raise unless error.cause.is_a?(ActiveRecord::RecordNotFound)
      end

      def drop_coalesced_rows(rows, delete:)
        rows.each do |id, _|
          SolidQueue.instrument(:drop_coalesced_argument, argument_id: id, job_id: provider_job_id, active_job_id: job_id) do
            SolidQueue::CoalescedArgument.where(id: id).delete_all if delete
          end
        end
      end

      def coalesced_call_from(call_arguments)
        call_arguments.size == 1 ? call_arguments.first : call_arguments
      end
  end
end
