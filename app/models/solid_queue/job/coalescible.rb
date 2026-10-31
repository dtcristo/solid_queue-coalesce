# frozen_string_literal: true

module SolidQueue
  class Job
    class CoalescedInBatchError < EnqueueError; end

    module Coalescible
      extend ActiveSupport::Concern

      included do
        has_many :coalesced_calls

        # Before anything else, so the key is locked before the job, as in admission.
        # Left to the cascade, they'd lock in the opposite order and deadlock.
        before_destroy :release_coalesce_key, prepend: true
      end

      class_methods do
        # Every call in active_jobs has the same key. Returns the job if a call
        # created one, nil if the calls joined the open job.
        def enqueue_coalesced(key, active_jobs, payloads = nil)
          return collect_coalesced(key, active_jobs, payloads || active_jobs.map(&:serialize)) if active_jobs.first.class.coalesce_collect?

          enqueued_job, open_job_id = raising_enqueue_errors do
            retrying_deadlocks { admit_discarding(key, active_jobs.first) }
          end

          record_coalesced(key, active_jobs, enqueued_job&.persisted?, open_job_id)

          enqueued_job
        end

        # So a key or a payload that can't be built raises before anything is written.
        # A repeated :discard call is never stored, so its arguments don't have to
        # serialize, as with perform_later. The first one's only get checked here,
        # because its job is built later, if there's no open job to find.
        def prepare_all_coalesced(active_jobs)
          active_jobs.group_by(&:coalesce_key).sort_by(&:first).map do |key, calls|
            if calls.first.class.coalesce_collect?
              [ key, calls, calls.map(&:serialize) ]
            else
              calls.first.serialize
              [ key, calls ]
            end
          end
        end

        def enqueue_all_coalesced(prepared_calls)
          prepared_calls.each { |key, calls, payloads| enqueue_coalesced(key, calls, payloads) }
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

          # A key with calls belongs to a job from when the class collected. A call
          # can't count on that job, which fails without running it, so it seals the
          # job and tries again. Sealing is outside the transaction that found it: every
          # call waiting on the key holds the same share lock, and upgrading it deadlocks
          # with all of them. Inside the caller's own transaction it can't be outside,
          # so that deadlock can reach the caller, as in retrying_deadlocks.
          def admit_discarding(key, active_job)
            loop do
              admitted = transaction { admit_unless_collecting(key, active_job) }
              return admitted if admitted

              transaction { CoalesceKey.seal_collecting(key) }
            end
          end

          def admit_unless_collecting(key, active_job)
            coalesce_key = CoalesceKey.admit(key)

            unless coalesce_key.collecting?
              enqueued_job = create_for_coalesce_key(coalesce_key, active_job) if coalesce_key.job_id.nil?

              [ enqueued_job, coalesce_key.job_id ]
            end
          end

          def create_for_coalesce_key(coalesce_key, active_job)
            create!(**attributes_from_active_job(active_job)).tap do |job|
              # limits_concurrency with on_conflict: :discard destroys the job right away
              if job.persisted?
                coalesce_key.update_columns(job_id: job.id)
              else
                coalesce_key.destroy!
              end
            end
          end

          # Takes one job's worth of calls at a time, each in its own transaction.
          # A call's outcome is set once its transaction is over, so a deadlock retry
          # can't leave one behind. Serialized once and before any job exists, so the
          # first call's payload is like every other call's, and a retry doesn't take
          # a new snapshot.
          def collect_coalesced(key, active_jobs, payloads)
            batch_size = active_jobs.first.class.coalesce_batch_size
            pending = active_jobs.zip(payloads)
            enqueued_job = nil

            until pending.empty?
              admitted, created_job, job_id = raising_enqueue_errors do
                retrying_deadlocks { transaction { admit_collected(key, pending, batch_size) } }
              end

              record_coalesced(key, pending.shift(admitted).map(&:first), created_job.present?, job_id)
              enqueued_job ||= created_job
            end

            enqueued_job
          end

          # Returns how many of the pending calls went in, and where. It can be none:
          # a smaller batch size can leave the open job over its capacity, and a job
          # from when the class discarded has none, and that only seals it.
          def admit_collected(key, pending, batch_size)
            coalesce_key = CoalesceKey.admit_exclusively(key)
            calls = pending.first([ coalesce_key.capacity(batch_size), 0 ].max)

            if calls.empty?
              coalesce_key.destroy!
              [ 0 ]
            else
              created_job = create_collecting(*calls.first) if coalesce_key.job_id.nil?
              job_id = coalesce_key.job_id || created_job.id

              CoalescedCall.insert_all(calls.map { |_, payload| { job_id: job_id, arguments: payload } })
              coalesce_key.fill(job_id, calls.size, batch_size)

              [ calls.size, created_job, job_id ]
            end
          end

          # Not attributes_from_active_job, which serializes the call again, and that
          # could raise after earlier batches committed. A coalesced call is never in
          # a batch, so it has no batch_id to set.
          def create_collecting(active_job, payload)
            create!(
              queue_name: active_job.queue_name || singleton_class::DEFAULT_QUEUE_NAME,
              active_job_id: active_job.job_id,
              priority: active_job.priority || singleton_class::DEFAULT_PRIORITY,
              scheduled_at: active_job.scheduled_at,
              class_name: active_job.class.name,
              arguments: payload.merge("coalesce_collecting" => true),
              concurrency_key: active_job.concurrency_key
            )
          end

          # A call has an outcome only if it was admitted: when on_conflict: :discard
          # destroys the job that was just created, nothing holds the key.
          def record_coalesced(key, active_jobs, created, job_id)
            active_jobs.each_with_index do |active_job, index|
              collect = active_job.class.coalesce_collect?
              created_by_this_call = created && index.zero?

              active_job.provider_job_id = job_id
              active_job.successfully_enqueued = created_by_this_call || collect
              active_job.coalesce_status = job_id && (created_by_this_call ? :created : (collect ? :collected : :discarded))
            end

            # After every outcome, because a subscriber can raise. These calls are stored,
            # so one without an outcome would look like it needs enqueuing again.
            if job_id
              active_jobs.each do |active_job|
                SolidQueue.instrument(:coalesce, action: active_job.coalesce_status, key: key, job_id: job_id, active_job_id: active_job.job_id, class_name: active_job.class.name)
              end
            end
          end
      end

      def coalesced?
        job_class.try(:coalesce_key).present?
      end

      # The start step. Returns what the Active Job needs to know about this run,
      # which stays in memory instead of going into its serialized data.
      #
      # Closing commits before the calls are read, in a statement of their own. The
      # read that finds the key in lock_for_release sets the snapshot of a MySQL
      # REPEATABLE READ transaction, so a read in the same transaction could miss
      # calls committed while it waited for the lock.
      #
      # Only the first start finds a key, so only it can compare the count. Rows that
      # went missing can't come back, so failing wouldn't help the ones left.
      def start_coalesced_run
        return unless coalesced? && CoalesceKey.migrated?

        released_key = self.class.retrying_deadlocks { transaction { release_coalesce_key } }
        calls = coalesced_calls.order(:id).pluck(:id, :arguments)

        if released_key && released_key.calls_count != calls.size
          SolidQueue.instrument(:coalesced_calls_mismatch, job_id: id, active_job_id: active_job_id, expected: released_key.calls_count, actual: calls.size)
        end

        { "calls" => calls, "concurrency_key" => concurrency_key }
      end

      private
        # A sealed job has no row, and lock_for_release won't touch a newer job's
        def release_coalesce_key
          CoalesceKey.lock_for_release(id)&.destroy! if CoalesceKey.migrated?
        end
    end
  end
end
