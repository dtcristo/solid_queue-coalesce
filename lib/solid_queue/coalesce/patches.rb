# frozen_string_literal: true

# Every place this gem reaches into Solid Queue. All of it depends on private
# details of the version pinned in the gemspec.
module SolidQueue
  module Coalesce
    module Patches
      # Prepended rather than added to the adapter, because RecurringTask calls
      # Job.enqueue directly.
      module Enqueuing
        def enqueue_all(active_jobs)
          coalesced_jobs, other_jobs = active_jobs.partition(&:coalesced_enqueue?)
          return super if coalesced_jobs.empty?

          coalesced_jobs.each { |active_job| active_job.scheduled_at ||= Time.current }
          ensure_coalescible(coalesced_jobs)

          super(other_jobs) if other_jobs.any?
          enqueue_all_coalesced(coalesced_jobs)

          active_jobs.count(&:successfully_enqueued?)
        end

        def enqueue(active_job, scheduled_at: Time.current)
          if active_job.coalesced_enqueue?
            active_job.scheduled_at = scheduled_at
            ensure_coalescible([ active_job ])
            enqueue_coalesced(active_job.coalesce_key, [ active_job ])
          elsif active_job.class.coalesce_key
            continuing_coalesced_run(active_job) { super }
          else
            super
          end
        end
      end

      module Failing
        def failed_with(exception)
          FailedExecution.transaction(requires_new: true) do
            release_coalesce_key
            super
          end
        end
      end

      # Before super, so the key is locked before the job, as in admission. A key
      # always means a live job: one left on a job that finished without starting,
      # because its class stopped coalescing, would accept calls that never run. A
      # job that isn't preserved gets destroyed, and the destroy callback releases it.
      module Finishing
        def finished!
          release_coalesce_key if SolidQueue.preserve_finished_jobs?
          super
        end
      end

      # A class can start discarding conflicts after it stopped collecting, and a
      # conflict destroys the job. A collecting job blocks instead, whatever its class
      # says now, so it reaches a worker and fails with its calls.
      module ConcurrencyConflicts
        private
          def handle_concurrency_conflict
            arguments["coalesce_collecting"] ? block : super
          end
      end

      # Prepended to each execution class that can be discarded, rather than to their
      # parent, because ReadyExecution signals semaphores in its own discard_jobs.
      # Admission locks the key, then the job and its semaphore, so this has to lock
      # the key before either. Left to the cascade, they'd deadlock. On MySQL they
      # still can, because the locks on the executions also cover the gaps where
      # admission inserts its own, so these get retried like admission.
      module BulkDiscarding
        def discard_all_in_batches(...)
          Job.retrying_deadlocks { super }
        end

        def discard_all_from_jobs(...)
          Job.retrying_deadlocks { super }
        end

        private
          def discard_jobs(job_ids)
            CoalesceKey.release_all(job_ids) if CoalesceKey.migrated?
            super
          end
      end

      module RecurringValidation
        extend ActiveSupport::Concern

        included do
          validate :ensure_job_class_not_collecting
        end

        private
          # A tick that joins an open job would record that job again, and the unique
          # index on recurring_executions.job_id would roll back the call it collected.
          def ensure_job_class_not_collecting
            if class_name.present? && job_class&.try(:coalesce_collect?)
              errors.add :class_name, :collecting, message: "can't be a job that uses `coalesce` with `on_conflict: :collect`"
            end
          end
      end

      def self.apply
        SolidQueue::Job.include SolidQueue::Job::Coalescible
        SolidQueue::Job.singleton_class.prepend Enqueuing
        SolidQueue::Job.prepend Failing
        SolidQueue::Job.prepend Finishing
        SolidQueue::Job.prepend ConcurrencyConflicts
        [ ReadyExecution, ScheduledExecution, BlockedExecution, FailedExecution ].each { |execution| execution.singleton_class.prepend BulkDiscarding }
        SolidQueue::RecurringTask.include RecurringValidation
      end
    end
  end
end

class SolidQueue::LogSubscriber
  def coalesce(event)
    debug formatted_event(event, action: "Coalesce call", **event.payload.slice(:key, :job_id, :active_job_id, :class_name), outcome: event.payload[:action])
  end

  def coalesced_calls_mismatch(event)
    warn formatted_event(event, action: "Coalesced calls don't match the key's count", **event.payload.slice(:job_id, :active_job_id, :expected, :actual))
  end

  def drop_coalesced_call(event)
    warn formatted_event(event, action: "Drop coalesced call for a deleted record", **event.payload.slice(:call_id, :job_id, :active_job_id, :exception_class))
  end
end
