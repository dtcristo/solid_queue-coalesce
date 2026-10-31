# frozen_string_literal: true

module ActiveJob
  module Coalescing
    extend ActiveSupport::Concern

    COALESCE_ON_CONFLICT_BEHAVIOUR = %i[ discard collect ]
    DEFAULT_COALESCE_BATCH_SIZE = 100

    # A job created as a collecting job can't run the way its class says now, or has
    # lost the calls it collected. Nothing has run, and the job fails with its calls.
    class CollectedJobMismatch < StandardError; end

    # One accepted call in a batch. job_data is the call's Active Job payload as it
    # was at enqueue, frozen at the top level. A call that never went through the
    # queue passes a Proc, so only reading it needs the arguments to be serializable.
    class Call
      attr_reader :id, :arguments

      def initialize(id:, arguments:, job_data:)
        @id = id
        @arguments = arguments
        @job_data = job_data
      end

      def job_data
        @job_data = @job_data.call if @job_data.respond_to?(:call)
        @job_data.freeze
      end
    end

    # Sits in front of the job's own #perform, whichever class defines it, so
    # a collecting job runs #perform_batch instead.
    module CollectedPerform
      def perform(*)
        perform_batch(coalesced_calls)
      end
    end

    included do
      class_attribute :coalesce_key, instance_accessor: false
      class_attribute :coalesce_on_conflict, instance_accessor: false
      class_attribute :coalesce_batch_size, instance_accessor: false

      attr_accessor :coalesce_status
      attr_reader :coalesced_calls
    end

    class_methods do
      def coalesce(key:, on_conflict: :discard, batch_size: nil)
        unless (key.is_a?(String) || key.is_a?(Symbol) || key.is_a?(Proc)) && key.present?
          raise ArgumentError, "`key` must be a non-blank String or Symbol, or a Proc"
        end

        unless on_conflict.in?(COALESCE_ON_CONFLICT_BEHAVIOUR)
          raise ArgumentError, "`on_conflict` must be :discard or :collect"
        end

        if on_conflict == :collect
          batch_size = DEFAULT_COALESCE_BATCH_SIZE if batch_size.nil?

          unless batch_size.is_a?(Integer) && batch_size.positive?
            raise ArgumentError, "`batch_size` must be a positive integer"
          end
        elsif !batch_size.nil?
          raise ArgumentError, "`batch_size` needs `on_conflict: :collect`"
        end

        ensure_coalesce_compatible_with_concurrency_controls(on_conflict, concurrency_on_conflict)

        self.coalesce_key = key
        self.coalesce_on_conflict = on_conflict
        self.coalesce_batch_size = batch_size
      end

      def coalesce_collect?
        coalesce_on_conflict == :collect
      end

      def limits_concurrency(**)
        super.tap { ensure_coalesce_compatible_with_concurrency_controls(coalesce_on_conflict, concurrency_on_conflict) }
      end

      private
        # A conflict destroys the job, and with it the calls it has collected
        def ensure_coalesce_compatible_with_concurrency_controls(coalesce_on_conflict, concurrency_on_conflict)
          if coalesce_on_conflict == :collect && concurrency_on_conflict == :discard
            raise ArgumentError, "`coalesce` with `on_conflict: :collect` can't be combined with `limits_concurrency` with `on_conflict: :discard`"
          end
        end
    end

    # Runs before the arguments are deserialized, and outside the rescue_from of
    # super, so retry_on can't swallow what's raised here. It reaches the rescue in
    # ClaimedExecution#execute, which turns it into a failed execution.
    def perform_now
      start_coalesced_run
      super
    end

    # Whether the job was created collecting is stored with the job, so it survives
    # retries and outlives the class's declaration. Only the job's row starts with
    # this, in create_collecting, so an instance only has it if it was read from one.
    def serialize
      @coalesce_collecting ? super.merge("coalesce_collecting" => true) : super
    end

    def deserialize(job_data)
      super
      @coalesce_collecting = job_data["coalesce_collecting"]
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

    # Whether this call found a job that was already open
    def coalesced?
      coalesce_status == :discarded || coalesce_status == :collected
    end

    def perform_batch(_calls)
      raise NotImplementedError, "#{self.class.name} uses `coalesce` with `on_conflict: :collect`, so it needs a `perform_batch(calls)` method"
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
      ensure_retry_can_take_calls

      super.tap do |enqueued|
        if @coalesced_run && !enqueued
          raise enqueue_error || SolidQueue::Job::EnqueueError.new("Couldn't enqueue the retry of #{self.class.name} (#{job_id})")
        end
      end
    end

    private
      # Only the run of a job a worker has claimed. perform_now on an instance that
      # went through perform_later also has a provider_job_id. A collecting job always
      # looks for its run, because the marker outlives the class's key and adapter.
      def start_coalesced_run
        ensure_still_collecting if @coalesce_collecting

        if @coalesce_collecting || (self.class.coalesce_key && provider_job_id && self.class.queue_adapter_name == "solid_queue")
          @coalesced_run = SolidQueue::Job.joins(:claimed_execution).find_by(id: provider_job_id)&.start_coalesced_run
          ensure_calls_present if @coalesce_collecting
        end
      end

      # Only Solid Queue moves the calls to the retry. On another adapter the retry
      # would succeed, and they'd stay behind on a job that goes on to finish.
      def ensure_retry_can_take_calls
        if worker_coalesced_rows.any? && self.class.queue_adapter_name != "solid_queue"
          raise CollectedJobMismatch, "#{self.class.name} (#{job_id}) owns collected calls, which can't move to a retry on the `#{self.class.queue_adapter_name}` adapter. Its calls are kept: restore the Solid Queue adapter and retry this failed job, or discard the job to delete them."
        end
      end

      def ensure_calls_present
        if worker_coalesced_rows.empty?
          raise CollectedJobMismatch, "#{self.class.name} (#{job_id}) was enqueued with `coalesce on_conflict: :collect`, but it has no calls to run: either something other than Solid Queue deleted them, or this worker no longer has the job's claim. Its own arguments are only a copy of its first call, so it won't run them. If its calls are gone, discard this failed job."
        end
      end

      def ensure_still_collecting
        unless self.class.coalesce_collect?
          raise CollectedJobMismatch, "#{self.class.name} (#{job_id}) was enqueued with `coalesce on_conflict: :collect`, but its class no longer has that. Its calls are kept: restore the declaration and retry this failed job, or discard the job to delete them."
        end
      end

      def _perform_job
        return super unless self.class.coalesce_collect?

        @coalesced_calls = load_coalesced_calls

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
        raise unless worker_coalesced_rows.any? && record_not_found?(error)

        @coalesced_arguments_gone = true
        @arguments = []
      end

      def worker_coalesced_rows
        @coalesced_run&.fetch("calls") || []
      end

      def load_coalesced_calls
        if worker_coalesced_rows.any?
          calls_from_coalesced_rows
        else
          [ Call.new(id: nil, arguments: arguments, job_data: -> { serialize }) ]
        end
      end

      # Anything but a deleted record raises on the way, before any row has been
      # dropped, and fails the run with its rows. When every record is gone the rows
      # stay until the job is destroyed: if this run crashes before finishing, the
      # re-run needs them to tell it has nothing to do, because a collecting job
      # without rows fails with CollectedJobMismatch.
      def calls_from_coalesced_rows
        missing = {}

        calls = worker_coalesced_rows.filter_map do |id, job_data|
          Call.new(id: id, arguments: ActiveJob::Arguments.deserialize(job_data.fetch("arguments")), job_data: job_data)
        rescue ActiveJob::DeserializationError => error
          raise unless record_not_found?(error)

          missing[id] = error
          nil
        end

        drop_coalesced_rows(missing, delete: calls.any?)

        calls
      end

      def drop_coalesced_rows(rows, delete:)
        rows.each do |id, error|
          SolidQueue.instrument(:drop_coalesced_call, call_id: id, job_id: provider_job_id, active_job_id: job_id, exception_class: error.class.name) do
            SolidQueue::CoalescedCall.where(id: id).delete_all if delete
          end
        end
      end

      # Rails main has its own error for this, and its cause isn't Active Record's
      def record_not_found?(error)
        error.cause.is_a?(ActiveRecord::RecordNotFound) ||
          (defined?(ActiveJob::DeserializationError::RecordNotFound) && error.is_a?(ActiveJob::DeserializationError::RecordNotFound))
      end
  end
end
