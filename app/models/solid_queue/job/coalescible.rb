# frozen_string_literal: true

module SolidQueue
  class Job
    class CoalescedInBatchError < EnqueueError; end

    module Coalescible
      extend ActiveSupport::Concern

      included do
        has_many :coalesced_calls
      end

      class_methods do
        # Every call in active_jobs has the same key. Returns the job if the first
        # call created one, nil if the calls joined the open job.
        def enqueue_coalesced(key, active_jobs)
          # Serialized once and before the job exists, so the first call's payload is
          # like every other call's, and a deadlock retry doesn't take a new snapshot
          payloads = active_jobs.map(&:serialize) if active_jobs.first.class.coalesce_collect?

          enqueued_job, open_job_id = raising_enqueue_errors do
            retrying_deadlocks { transaction { admit_coalesced(key, active_jobs, payloads) } }
          end

          record_coalesced(key, active_jobs, enqueued_job, open_job_id)

          enqueued_job
        end

        def enqueue_all_coalesced(active_jobs)
          active_jobs.group_by(&:coalesce_key).sort_by(&:first).each do |key, calls|
            enqueue_coalesced(key, calls)
          end
        end

        def ensure_coalescible(active_jobs)
          raise CoalesceKey::PendingMigrations unless CoalesceKey.migrated?

          if Batch.current_batch_id || active_jobs.any?(&:batch_id)
            raise CoalescedInBatchError, "#{active_jobs.first.class.name} coalesces its calls, so it can't be enqueued as part of a batch: a call that joins an open job adds no job to the batch"
          end
        end

        MAX_COALESCE_DEADLOCK_RETRIES = 5

        # InnoDB can pick one of these transactions as a deadlock victim. Admissions
        # waiting on a key that doesn't exist yet hold gap locks that block each other,
        # creating a job can do the same on a new semaphore, and a job being destroyed
        # cascades into the key's indexes in a different order. Nothing is lost by
        # running ours again, unless it's part of a bigger transaction. In that case
        # InnoDB has rolled all of it back, and only the caller can run it again.
        def retrying_deadlocks
          nested = connection_pool.with_connection(&:transaction_open?)
          attempts = 0

          begin
            yield
          rescue ActiveRecord::Deadlocked
            raise if nested || (attempts += 1) > MAX_COALESCE_DEADLOCK_RETRIES
            retry
          end
        end

        private
          # A retry of a collect job is a new job with the same active_job_id. The
          # calls it owes move with it, so the old job finishes owning nothing. The
          # block creates the new job, and sets provider_job_id to it.
          def continuing_coalesced_run(active_job)
            previous_job_id = active_job.provider_job_id
            moving_calls = previous_job_id && active_job.class.coalesce_collect? && CoalesceKey.migrated?

            enqueued_job = raising_enqueue_errors do
              transaction do
                yield.tap do |job|
                  CoalescedCall.where(job_id: previous_job_id).update_all(job_id: job.id) if moving_calls && job.persisted?
                end
              end
            end

            # Active Job marks the call as enqueued once we return, so only raising
            # stops on_conflict: :discard from silently dropping a coalesced job's retry
            unless enqueued_job.persisted?
              raise EnqueueError, "The retry of #{active_job.class.name} (#{active_job.job_id}) was discarded by its concurrency limit"
            end

            enqueued_job
          end

          def raising_enqueue_errors
            yield
          rescue ActiveRecord::ActiveRecordError => e
            enqueue_error = EnqueueError.new("#{e.class.name}: #{e.message}").tap do |error|
              error.set_backtrace e.backtrace
            end
            raise enqueue_error
          end

          def admit_coalesced(key, active_jobs, payloads)
            coalesce_key = CoalesceKey.admit(key)
            enqueued_job = create_for_coalesce_key(coalesce_key, active_jobs.first) if coalesce_key.job_id.nil?

            coalesce_key.collect(payloads) if payloads && coalesce_key.job_id

            [ enqueued_job, coalesce_key.job_id ]
          end

          def create_for_coalesce_key(coalesce_key, active_job)
            create!(**attributes_from_active_job(active_job)).tap do |job|
              # limits_concurrency with on_conflict: :discard destroys the job right away
              if job.persisted?
                coalesce_key.update_columns(job_id: job.id)
              else
                coalesce_key.destroy!
              end

              active_job.provider_job_id = job.id if job.persisted?
              active_job.successfully_enqueued = job.persisted?
            end
          end

          # A call has an outcome only if it was admitted: when on_conflict: :discard
          # destroys the job that was just created, nothing holds the key.
          def record_coalesced(key, active_jobs, enqueued_job, open_job_id)
            collect = active_jobs.first.class.coalesce_collect?
            joined_action = collect ? :collected : :discarded

            active_jobs.each_with_index do |active_job, index|
              created = enqueued_job && index.zero?

              unless created
                active_job.provider_job_id = open_job_id
                active_job.successfully_enqueued = collect
              end

              active_job.coalesce_status = open_job_id && (created ? :created : joined_action)

              if open_job_id
                SolidQueue.instrument(:coalesce, action: active_job.coalesce_status, key: key, job_id: open_job_id, active_job_id: active_job.job_id, class_name: active_job.class.name)
              end
            end
          end
      end

      def coalesced?
        job_class.try(:coalesce_key).present?
      end

      def coalesce_batch_size
        job_class.try(:coalesce_batch_size)
      end

      # The start step. Returns what the Active Job needs to know about this run,
      # which stays in memory instead of going into its serialized data.
      def start_coalesced_run
        return unless coalesced? && CoalesceKey.migrated?

        self.class.retrying_deadlocks do
          transaction do
            release_coalesce_key
            { "calls" => coalesced_calls.order(:id).pluck(:id, :arguments), "concurrency_key" => concurrency_key }
          end
        end
      end

      # The next job for the same key, picking up from the rows the job that's
      # starting left waiting, and due when the oldest of them asked to run. Its
      # arguments are the raw ones in that row: deserializing them here would let a
      # deleted record break the handover.
      def enqueue_next_coalesced(oldest)
        due_at = Time.iso8601(oldest.arguments.fetch("scheduled_at"))
        active_job_id = SecureRandom.uuid

        self.class.create!(
          queue_name: queue_name,
          class_name: class_name,
          priority: priority,
          active_job_id: active_job_id,
          scheduled_at: due_at,
          concurrency_key: concurrency_key,
          arguments: next_coalesced_arguments(active_job_id, oldest, due_at)
        )
      end

      private
        def release_coalesce_key
          return unless CoalesceKey.migrated?

          if job_class&.coalesce_key && !job_class.coalesce_collect?
            CoalesceKey.lock_for_release(id)&.destroy!
          elsif job_class.nil? || coalesced?
            CoalesceKey.lock_for_release(id)&.release
          end
        end

        def next_coalesced_arguments(active_job_id, oldest, due_at)
          arguments.except("continuation", "resumptions").merge(
            "job_id" => active_job_id,
            "provider_job_id" => nil,
            "arguments" => oldest.arguments.fetch("arguments"),
            "executions" => 0,
            "exception_executions" => {},
            "enqueued_at" => Time.now.utc.iso8601(9),
            "scheduled_at" => due_at.utc.iso8601(9)
          )
        end
    end
  end
end
