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

      # Maintenance#start runs its steps from an inline block, so this follows
      # the only concurrency step it can reach.
      module Maintaining
        private
          def expire_semaphores
            super
            release_stranded_coalesce_keys
          end

          def release_stranded_coalesce_keys
            wrap_in_app_executor do
              CoalesceKey.release_stranded(batch_size) if CoalesceKey.migrated?
            end
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
            if class_name.present? && job_class&.try(:coalesce_collect)
              errors.add :class_name, :collecting, message: "can't be a job that uses `coalesce` with `collect: true`"
            end
          end
      end

      def self.apply
        SolidQueue::Job.include SolidQueue::Job::Coalescible
        SolidQueue::Job.singleton_class.prepend Enqueuing
        SolidQueue::Job.prepend Failing
        SolidQueue::RecurringTask.include RecurringValidation
        SolidQueue::Dispatcher::Maintenance.prepend Maintaining
      end
    end
  end
end

class SolidQueue::LogSubscriber
  def coalesce(event)
    debug formatted_event(event, action: "Coalesce call", **event.payload.slice(:key, :job_id, :active_job_id), outcome: event.payload[:action])
  end

  def drop_coalesced_argument(event)
    warn formatted_event(event, action: "Drop coalesced call for a deleted record", **event.payload.slice(:argument_id, :job_id, :active_job_id))
  end
end
