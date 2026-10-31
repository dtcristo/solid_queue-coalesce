# frozen_string_literal: true

module SolidQueue
  # A row means "this job is the open job for this key": it exists only while
  # its job exists and hasn't started. job_id is null only between the insert and
  # the end of the admission that inserted it, so it's never visible to others.
  class CoalesceKey < Record
    class PendingMigrations < StandardError
      def initialize(message = "The coalescing schema hasn't been installed yet. Run `bin/rails generate solid_queue:coalesce:install` to copy the migration to your application, and then `bin/rails db:migrate` to run it")
        super
      end
    end

    belongs_to :job, optional: true
    has_many :coalesced_calls

    scope :stranded, -> { left_joins(job: :failed_execution).merge(Job.finished.or(Job.where.not(failed_execution: { id: nil }))) }

    class << self
      # Until the app has run the gem's migration everything else works as before.
      def migrated?
        @migrated ||= table_exists? && CoalescedCall.table_exists?
      end

      # Returns the row for this key, inserted if nobody holds it, and locked so
      # a job start can't claim or delete it until this transaction commits. A row
      # with a null job_id is ours: the caller has to create the job for it.
      def admit(key)
        loop do
          insert_unless_held(key)

          admitted = lock(share_lock).find_by(key: key)
          return admitted if admitted
        end
      end

      # Locks through the key's index, like admission. Going through the job_id
      # index, even by deleting by it, takes InnoDB's locks on this row's indexes in
      # the opposite order to admission, and that deadlocks.
      def lock_for_release(job_id)
        if key = where(job_id: job_id).pick(:key)
          lock.find_by(key: key, job_id: job_id)
        end
      end

      def release_stranded(batch_size)
        stranded.limit(batch_size).pluck(:key).each do |key|
          transaction do
            coalesce_key = lock.find_by(key: key)
            coalesce_key.release(claiming: !coalesce_key.job.finished?) if coalesce_key&.stranded?
          end
        end
      end

      private
        # MariaDB and MySQL before 8 don't have FOR SHARE, and this means the same on MySQL 8
        def share_lock
          connection_pool.with_connection { |connection| connection.adapter_name == "PostgreSQL" } ? "FOR SHARE" : "LOCK IN SHARE MODE"
        end

        # Needs the unique index on key. Skips a conflict instead of raising, because
        # a duplicate would abort an enclosing transaction on PostgreSQL. On MySQL it
        # also avoids a deadlock: a plain INSERT that hits a duplicate takes a shared
        # lock, and two of those on a row being deleted can't both go on to insert.
        def insert_unless_held(key)
          if supports_insert_conflict_target?
            insert({ key: key }, unique_by: :key)
          else
            insert({ key: key })
          end
        end
    end

    def stranded?
      job.finished? || job.failed?
    end

    def collect(payloads)
      CoalescedCall.insert_all(payloads.map { |payload| { coalesce_key_id: id, arguments: payload } })
    end

    # Hands the waiting rows over as the job leaves the queue. Requires this row
    # to be locked: that's what waits for in-flight appends, and it serializes this
    # with other starts. Reads of the rows lock too, because on MySQL a plain read
    # could use a snapshot older than the appends we waited for, and deleting this
    # row with rows we didn't see would cascade to them.
    #
    # A finished job won't run again, so it claims nothing when there's a next job
    # that can process the rows. Without a batch size there can't be one: the class
    # that would process them is gone, or stopped collecting.
    def release(claiming: true)
      batch_size = job.coalesce_batch_size
      claim(claiming || batch_size.nil? ? batch_size : 0)

      if oldest = coalesced_calls.order(:id).lock.first
        next_job = job.enqueue_next_coalesced(oldest)
        update_columns(job_id: next_job.id)
      else
        destroy!
      end
    end

    private
      # A nil batch size means we can't know it, so nothing is left behind
      def claim(batch_size)
        claimed = batch_size ? coalesced_calls.where(id: coalesced_calls.order(:id).limit(batch_size).lock.pluck(:id)) : coalesced_calls
        claimed.update_all(job_id: job_id, coalesce_key_id: nil)
      end
  end
end
