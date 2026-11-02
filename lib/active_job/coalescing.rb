# frozen_string_literal: true

module ActiveJob
  # Lets many +perform_later+ calls with the same key share one pending job. See #coalesce.
  module Coalescing
    extend ActiveSupport::Concern

    COALESCE_ON_CONFLICT_BEHAVIOUR = %i[ discard collect ]
    COALESCE_ON_FULL_BEHAVIOUR = %i[ wait dispatch ]
    DEFAULT_COALESCE_MAX_CALLS = 100

    # A job created as a collecting job can't run the way its class says now, or has
    # lost the calls it collected. Nothing has run, and the job fails with its calls.
    class CollectedJobMismatch < StandardError; end

    # One accepted call to a collecting job, as #perform_calls receives it.
    #
    # +id+ is the row's ID, or +nil+ for +perform_now+ and other adapters. +arguments+
    # is the call's deserialized positional array.
    #
    # +job_data+ is the call's Active Job payload as it was at enqueue, frozen at the
    # top level. A call that never went through the queue passes a Proc, so only
    # reading it needs the arguments to be serializable.
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
    # a collecting job runs #perform_calls instead.
    module CollectedPerform # :nodoc:
      def perform(*)
        perform_calls(coalesced_calls)
      end
    end

    included do
      class_attribute :coalesce_key, instance_accessor: false
      class_attribute :coalesce_on_conflict, instance_accessor: false
      class_attribute :coalesce_max_calls, instance_accessor: false
      class_attribute :coalesce_on_full, instance_accessor: false
      class_attribute :coalesce_debounce, instance_accessor: false
      class_attribute :coalesce_max_wait, instance_accessor: false
    end

    # What happened to this call: +:created+, +:discarded+, +:collected+, or +nil+ if
    # this instance has no recorded admission. It isn't serialized, so it's +nil+ on
    # a worker's instance.
    #
    #   job = ReindexProductJob.perform_later(product.id)
    #   job.coalesce_status # => :discarded
    attr_reader :coalesce_status

    # The Call objects of this run, oldest first, for perform callbacks to read.
    # It's +nil+ for a job that doesn't collect.
    attr_reader :coalesced_calls

    private attr_writer :coalesce_status

    module ClassMethods
      # Shares one pending job between the +perform_later+ calls with the same key. The
      # key is held until that job starts, so a call during a run opens a new job.
      #
      #   class ReindexProductJob < ApplicationJob
      #     coalesce key: ->(product_id) { product_id }, debounce: 30.seconds, max_wait: 5.minutes
      #
      #     def perform(product_id)
      #       Product.find(product_id).reindex!
      #     end
      #   end
      #
      # ==== Options
      #
      # * +:key+ - Required. A String or Symbol literal, or a Proc that receives the job's
      #   arguments. It's scoped to the job class, and built like the key of +limits_concurrency+.
      # * +:on_conflict+ - +:discard+, the default, suppresses a call that finds a pending
      #   job. +:collect+ stores every call for #perform_calls.
      # * +:max_calls+ - The most calls one run takes with +:collect+. Defaults to 100.
      # * +:on_full+ - With +:collect+, +:wait+, the default, leaves a full job to wait for
      #   its time. +:dispatch+ makes it ready as it fills.
      # * +:debounce+ - A duration. Each call pushes a pending job back to that long from now.
      # * +:max_wait+ - A duration greater than +:debounce+ that caps how far a job can move.
      #
      # An invalid option raises +ArgumentError+. The README has recipes for each of these:
      # https://github.com/dtcristo/solid_queue-coalesce#recipes
      def coalesce(key:, on_conflict: :discard, max_calls: nil, on_full: nil, debounce: nil, max_wait: nil)
        unless (key.is_a?(String) || key.is_a?(Symbol) || key.is_a?(Proc)) && key.present?
          raise ArgumentError, "`key` must be a non-blank String or Symbol, or a Proc"
        end

        unless on_conflict.in?(COALESCE_ON_CONFLICT_BEHAVIOUR)
          raise ArgumentError, "`on_conflict` must be :discard or :collect"
        end

        if on_conflict == :collect
          max_calls = DEFAULT_COALESCE_MAX_CALLS if max_calls.nil?

          unless max_calls.is_a?(Integer) && max_calls.positive?
            raise ArgumentError, "`max_calls` must be a positive integer"
          end

          on_full = :wait if on_full.nil?

          unless on_full.in?(COALESCE_ON_FULL_BEHAVIOUR)
            raise ArgumentError, "`on_full` must be :wait or :dispatch"
          end
        elsif !max_calls.nil?
          raise ArgumentError, "`max_calls` needs `on_conflict: :collect`"
        elsif !on_full.nil?
          raise ArgumentError, "`on_full` needs `on_conflict: :collect`"
        end

        unless debounce.nil? || (debounce.is_a?(ActiveSupport::Duration) && debounce.positive?)
          raise ArgumentError, "`debounce` must be a positive duration"
        end

        unless max_wait.nil?
          raise ArgumentError, "`max_wait` needs `debounce`" if debounce.nil?

          unless max_wait.is_a?(ActiveSupport::Duration) && max_wait > debounce
            raise ArgumentError, "`max_wait` must be a duration greater than `debounce`"
          end
        end

        ensure_coalesce_compatible_with_concurrency_controls(on_conflict, concurrency_on_conflict)

        self.coalesce_key = key
        self.coalesce_on_conflict = on_conflict
        self.coalesce_max_calls = max_calls
        self.coalesce_on_full = on_full
        self.coalesce_debounce = debounce
        self.coalesce_max_wait = max_wait
      end

      def coalesce_collect? # :nodoc:
        coalesce_on_conflict == :collect
      end

      def limits_concurrency(**) # :nodoc:
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
    def perform_now # :nodoc:
      start_coalesced_run
      super
    end

    # How the job was created is stored with the job, so it survives retries and
    # outlives the class's declaration. Only the job's row starts with this, when it
    # gets its key, so an instance only has it if it was read from one.
    def serialize # :nodoc:
      @coalesce_on_conflict ? super.merge("coalesce_on_conflict" => @coalesce_on_conflict) : super
    end

    def deserialize(job_data) # :nodoc:
      super
      @coalesce_on_conflict = job_data["coalesce_on_conflict"]
    end

    # Scoped to the job class name, and built like concurrency_key
    def coalesce_key # :nodoc:
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

    # Runs a collecting job with its Call objects, oldest first, instead of +perform+.
    # Define it on a job with <tt>on_conflict: :collect</tt>. A retry runs all of the
    # calls again, so it has to be idempotent.
    #
    #   def perform_calls(calls)
    #     SearchIndex.bulk_index(Product.where(id: calls.map { |call| call.arguments.last }))
    #   end
    def perform_calls(_calls)
      raise NotImplementedError, "#{self.class.name} uses `coalesce` with `on_conflict: :collect`, so it needs a `perform_calls(calls)` method"
    end

    # A retry is the same work continuing, so it can't join or suppress anything
    def coalesced_enqueue? # :nodoc:
      self.class.coalesce_key.present? && executions.to_i.zero?
    end

    # A call's own time if it has one, and otherwise now plus the window. A longer
    # wait is capped without raising, because a scheduling preference shouldn't fail
    # the caller's request.
    def coalesced_scheduled_at(scheduled_at) # :nodoc:
      scheduled_at ||= self.class.coalesce_debounce&.from_now || Time.current

      self.class.coalesce_max_wait ? [ scheduled_at, self.class.coalesce_max_wait.from_now ].min : scheduled_at
    end

    # The stored key, in case the arguments it was computed from are gone
    def concurrency_key # :nodoc:
      @coalesced_arguments_gone ? @coalesced_run["concurrency_key"] : super
    end

    # A retry that doesn't get enqueued has to fail the run: finishing would
    # destroy the job and the calls it owns, with no job left to process them.
    def retry_job(options = {}) # :nodoc:
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
        ensure_still_collecting if created_collecting?

        if created_collecting? || (self.class.coalesce_key && provider_job_id && self.class.queue_adapter_name == "solid_queue")
          @coalesced_run = SolidQueue::Job.joins(:claimed_execution).find_by(id: provider_job_id)&.start_coalesced_run
          ensure_calls_present if created_collecting?
        end
      end

      def created_collecting?
        @coalesce_on_conflict == "collect"
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

      # Calls whose records are gone are skipped for this attempt. Every row stays
      # until its job is destroyed, and the next attempt deserializes them again.
      # Any other error fails the run before skipped calls have been reported.
      def calls_from_coalesced_rows
        missing = {}

        calls = worker_coalesced_rows.filter_map do |id, job_data|
          Call.new(id: id, arguments: deserialize_call_arguments(job_data), job_data: job_data)
        rescue ActiveJob::DeserializationError => error
          raise unless record_not_found?(error)

          missing[id] = error
          nil
        end

        instrument_skipped_calls(missing)

        calls
      end

      # On an instance of its own, as in a normal run, so the job's overrides apply.
      # That instance has no rows, so its deserialize_arguments_if_needed lets errors out.
      def deserialize_call_arguments(job_data)
        self.class.deserialize(job_data.dup).tap { |call| call.send(:deserialize_arguments_if_needed) }.arguments
      end

      def instrument_skipped_calls(rows)
        rows.each do |id, error|
          SolidQueue.instrument(:drop_coalesced_call, call_id: id, job_id: provider_job_id, active_job_id: job_id, class_name: self.class.name, exception_class: error.class.name)
        end
      end

      # Rails main has its own error for this, and its cause isn't Active Record's
      def record_not_found?(error)
        error.cause.is_a?(ActiveRecord::RecordNotFound) ||
          (defined?(ActiveJob::DeserializationError::RecordNotFound) && error.is_a?(ActiveJob::DeserializationError::RecordNotFound))
      end
  end
end
