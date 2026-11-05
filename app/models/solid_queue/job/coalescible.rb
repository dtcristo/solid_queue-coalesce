# frozen_string_literal: true

module SolidQueue
  class Job
    module Coalescible # :nodoc: all
      extend ActiveSupport::Concern

      included do
        has_many :coalesced_calls

        # Before anything else, so the key is locked before the job, as in admission.
        # Left to the cascade, they'd lock in the opposite order and deadlock.
        before_destroy :release_coalesce_key, prepend: true
      end

      # What a replacement keeps from the job: what identifies it, its place in line,
      # its batch, its marker and the semaphore Solid Queue recorded for it.
      REPLACEMENT_KEPT_KEYS = %w[ job_class job_id queue_name priority enqueued_at scheduled_at batch_id callback_batch_id solid_queue_coalesce_on_conflict ] + [ ConcurrencyControls::CONCURRENCY_LOCK_KEY ]

      class_methods do
        # Every call in active_jobs has the same key. Returns the job if a call
        # created one, nil if the calls joined the open job. A replacing call's
        # payloads are only its last call's.
        def enqueue_coalesced(key, active_jobs, payloads = nil, wrap_errors: false)
          if active_jobs.first.class.coalesce_collect?
            payloads ||= raising_enqueue_errors(wrap_errors) { active_jobs.map(&:serialize) }
            return collect_coalesced(key, active_jobs, payloads, wrap_errors: wrap_errors)
          end

          if active_jobs.first.class.coalesce_replace?
            payloads ||= raising_enqueue_errors(wrap_errors) { [ active_jobs.last.serialize ] }
            return replace_coalesced(key, active_jobs, payloads.last, wrap_errors: wrap_errors)
          end

          enqueued_job, open_job_id = raising_enqueue_errors(wrap_errors) do
            retrying_deadlocks { admit_discarding(key, active_jobs) }
          end

          record_coalesced(key, active_jobs, enqueued_job&.persisted?, open_job_id)

          enqueued_job
        end

        # So a key or a payload that can't be built raises before anything is written.
        # A repeated :discard call is never stored, so its arguments don't have to
        # serialize, as with perform_later. The first one's only get checked here,
        # because its job is built later, if there's no open job to find. Only the
        # last :replace call is stored, so the ones it supersedes don't serialize either.
        def prepare_all_coalesced(active_jobs)
          active_jobs.group_by(&:coalesce_key).sort_by(&:first).map do |key, calls|
            if calls.first.class.coalesce_collect?
              [ key, calls, calls.map(&:serialize) ]
            elsif calls.first.class.coalesce_replace?
              [ key, calls, [ calls.last.serialize ] ]
            else
              calls.first.serialize
              [ key, calls ]
            end
          end
        end

        def enqueue_all_coalesced(prepared_calls)
          prepared_calls.each { |key, calls, payloads| enqueue_coalesced(key, calls, payloads) }
        end

        def ensure_coalescible
          raise CoalesceKey::PendingMigrations unless CoalesceKey.migrated?
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

          def raising_enqueue_errors(wrap_errors = true)
            yield
          rescue ActiveRecord::ActiveRecordError => e
            raise unless wrap_errors

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
          def admit_discarding(key, active_jobs)
            loop do
              admitted = transaction { admit_unless_collecting(key, active_jobs) }
              return admitted if admitted

              transaction { CoalesceKey.seal_collecting(key) }
            end
          end

          def admit_unless_collecting(key, active_jobs)
            coalesce_key = CoalesceKey.admit(key)

            unless coalesce_key.collecting?
              if coalesce_key.job_id.nil?
                attributes = attributes_from_active_job(active_jobs.first)
                enqueued_job = create_for_coalesce_key(coalesce_key, **attributes, arguments: attributes[:arguments].merge("solid_queue_coalesce_on_conflict" => "discard"))
              end

              debounce_coalesced(coalesce_key, coalesce_key.job_id, enqueued_job ? active_jobs.drop(1) : active_jobs)

              [ enqueued_job, coalesce_key.job_id ]
            end
          end

          def create_for_coalesce_key(coalesce_key, **attributes)
            create!(**attributes).tap do |job|
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
          def collect_coalesced(key, active_jobs, payloads, wrap_errors:)
            max_calls = active_jobs.first.class.coalesce_max_calls
            pending = active_jobs.zip(payloads)
            enqueued_job = nil

            until pending.empty?
              admitted, created_job, job_id = raising_enqueue_errors(wrap_errors) do
                retrying_deadlocks { transaction { admit_collected(key, pending, max_calls) } }
              end

              record_coalesced(key, pending.shift(admitted).map(&:first), created_job.present?, job_id)
              enqueued_job ||= created_job
            end

            enqueued_job
          end

          # Returns how many of the pending calls went in, and where. It can be none:
          # a smaller max_calls can leave the open job over its capacity, and a job
          # from when the class discarded has none, and that only seals it.
          def admit_collected(key, pending, max_calls)
            coalesce_key = CoalesceKey.admit_exclusively(key)
            calls = pending.first([ coalesce_key.capacity(max_calls), 0 ].max)

            if calls.empty?
              coalesce_key.destroy!
              [ 0 ]
            else
              created_job = create!(**attributes_from_payload(*calls.first, "collect")) if coalesce_key.job_id.nil?
              job_id = coalesce_key.job_id || created_job.id

              CoalescedCall.insert_all(calls.map { |_, payload| { job_id: job_id, arguments: payload } })
              coalesce_key.fill(job_id, calls.size, max_calls)

              if coalesce_key.destroyed? && calls.first.first.class.coalesce_on_full == :dispatch
                dispatch_full(job_id)
              else
                debounce_coalesced(coalesce_key, job_id, calls.drop(created_job ? 1 : 0).map(&:first))
              end

              [ calls.size, created_job, job_id ]
            end
          end

          # The time moves too, or retrying the job after it fails would wait for the old one.
          def dispatch_full(job_id)
            scheduled = lock_scheduled(ScheduledExecution.where(job_id: job_id))

            if scheduled.any?
              where(id: scheduled).update_all(scheduled_at: Time.current)
              ScheduledExecution.dispatch_jobs(scheduled)
            end
          end

          # The calls that joined the job move it once, to the latest of their times,
          # capped at max_wait from when its key opened. Each open job has its own key,
          # so each has its own clock. A sealed job has no key, so nothing moves it again.
          def debounce_coalesced(coalesce_key, job_id, joined_jobs)
            job_class = joined_jobs.first&.class

            if job_id && job_class&.coalesce_debounce
              scheduled_at = joined_jobs.map(&:scheduled_at).max
              scheduled_at = [ scheduled_at, coalesce_key.created_at + job_class.coalesce_max_wait ].min if job_class.coalesce_max_wait

              move_later(job_id, scheduled_at)
            end
          end

          # Only an execution more than a second before the new time matches. That keeps
          # moves later-only, and saves a hot key from writing on every call. The time
          # moves on the job too, because a failed job's retry goes by the job's. If this
          # skips a row that's held, the job runs early, and the key still covers the call.
          def move_later(job_id, scheduled_at)
            scheduled = lock_scheduled(ScheduledExecution.where(job_id: job_id, scheduled_at: ...(scheduled_at - 1.second)))

            if scheduled.any?
              where(id: scheduled).update_all(scheduled_at: scheduled_at)
              ScheduledExecution.where(job_id: scheduled).update_all(scheduled_at: scheduled_at)
            end
          end

          # A job that's ready, blocked or claimed has no scheduled execution, and has
          # nothing to do. SKIP LOCKED because we hold the key, and a discard holds the
          # execution while it waits for the key. Whoever holds the row is dispatching
          # or discarding the job already. On MySQL, locking a row that isn't there takes
          # a gap lock that blocks other jobs being scheduled, so it checks first.
          def lock_scheduled(executions)
            executions.exists? ? executions.non_blocking_lock.pluck(:job_id) : []
          end

          # Not attributes_from_active_job, which serializes the call again, and that
          # could raise after earlier jobs committed.
          def attributes_from_payload(active_job, payload, on_conflict)
            attributes = {
              queue_name: active_job.queue_name || singleton_class::DEFAULT_QUEUE_NAME,
              active_job_id: active_job.job_id,
              priority: active_job.priority || singleton_class::DEFAULT_PRIORITY,
              scheduled_at: active_job.scheduled_at,
              class_name: active_job.class.name,
              arguments: payload.merge("solid_queue_coalesce_on_conflict" => on_conflict),
              concurrency_key: active_job.concurrency_key
            }
            attributes[:batch_id] = active_job.batch_id if Batch.migrated?
            attributes
          end

          # Like a collecting job, serialized once and before anything is written. The
          # last call is the one admitted: it creates the job, or its payload replaces
          # the job's. The calls before it are superseded, and only move the job later.
          def replace_coalesced(key, active_jobs, payload, wrap_errors:)
            enqueued_job, job_id = raising_enqueue_errors(wrap_errors) do
              retrying_deadlocks { transaction { admit_replacing(key, active_jobs, payload) } }
            end

            record_coalesced(key, active_jobs, enqueued_job.present?, job_id)

            enqueued_job
          end

          # A job from when the class discarded or collected runs the way it was
          # created, and can't take a payload, so the call seals it and tries again.
          # It holds the key exclusively, so it can do that without letting go of it.
          def admit_replacing(key, active_jobs, payload)
            loop do
              coalesce_key = CoalesceKey.admit_exclusively(key)
              job = lock.find_by(id: coalesce_key.job_id) if coalesce_key.discarding?

              if coalesce_key.job_id.nil?
                enqueued_job = create_for_coalesce_key(coalesce_key, **attributes_from_payload(active_jobs.last, payload, "replace"))
                debounce_coalesced(coalesce_key, coalesce_key.job_id, active_jobs[0...-1])

                return [ enqueued_job, coalesce_key.job_id ]
              elsif job&.created_replacing?
                job.replace_arguments(payload)
                debounce_coalesced(coalesce_key, job.id, active_jobs)

                return [ nil, job.id ]
              else
                coalesce_key.destroy!
              end
            end
          end

          # A call has an outcome only if it was admitted: when on_conflict: :discard
          # destroys the job that was just created, nothing holds the key. The first
          # call is the one that creates a job, except with :replace, where it's the last.
          def record_coalesced(key, active_jobs, created, job_id)
            active_jobs.each do |active_job|
              job_class = active_job.class
              admitted = job_class.coalesce_replace? ? active_jobs.last : active_jobs.first

              status = if job_id.nil? then nil
              elsif created && active_job.equal?(admitted) then :created
              elsif job_class.coalesce_collect? then :collected
              elsif active_job.equal?(admitted) && job_class.coalesce_replace? then :replaced
              else :discarded
              end

              active_job.provider_job_id = job_id
              active_job.successfully_enqueued = status.in?(%i[ created replaced collected ])
              active_job.send(:coalesce_status=, status)
            end

            # After every outcome, because a subscriber can raise. These calls are stored,
            # so one without an outcome would look like it needs enqueuing again.
            if job_id
              active_jobs.each do |active_job|
                SolidQueue.instrument(:coalesce, status: active_job.coalesce_status, key: key, job_id: job_id, active_job_id: active_job.job_id, class_name: active_job.class.name)
              end
            end
          end
      end

      def created_replacing?
        arguments.is_a?(Hash) && arguments["solid_queue_coalesce_on_conflict"] == "replace"
      end

      # The caller holds the job's lock, and Solid Queue takes it too before it records
      # the job's semaphore, so neither write can undo the other.
      def replace_arguments(payload)
        update_column :arguments, payload.except(*REPLACEMENT_KEPT_KEYS).merge(arguments.slice(*REPLACEMENT_KEPT_KEYS))
      end

      # A replacing job's start step, before its payload is read. Releasing the key
      # commits on its own, so the payload read after it has every replacement
      # admitted before the release, and a call after it opens a new job.
      def start_replacing_run
        self.class.retrying_deadlocks { transaction { release_coalesce_key } }
        reload
      end

      # The start step. Returns what the Active Job needs to know about this run,
      # which stays in memory instead of going into its serialized data.
      #
      # Closing commits before the calls are read, in a statement of their own. The
      # read that finds the key in lock_for_release sets the snapshot of a MySQL
      # REPEATABLE READ transaction, so a read in the same transaction could miss
      # calls committed while it waited for the lock.
      def start_coalesced_run
        return unless coalesced? && CoalesceKey.migrated?

        self.class.retrying_deadlocks { transaction { release_coalesce_key } }

        { "calls" => coalesced_calls.order(:id).pluck(:id, :arguments), "concurrency_key" => concurrency_key }
      end

      private
        # Whether this job's class coalesces
        def coalesced?
          !!job_class.try(:coalesce_key)
        end

        # Admission marks a job before giving it a key, and the class covers jobs from
        # before the marker. Anything that can't answer gets the read, so that failing
        # or discarding a job still works when its payload or its class can't be loaded.
        def may_hold_coalesce_key?
          !arguments.is_a?(Hash) || arguments.key?("solid_queue_coalesce_on_conflict") || coalesced?
        rescue StandardError, ScriptError
          true
        end

        # A sealed job has no row, and lock_for_release won't touch a newer job's
        def release_coalesce_key
          CoalesceKey.lock_for_release(id)&.destroy! if may_hold_coalesce_key? && CoalesceKey.migrated?
        end
    end
  end
end
