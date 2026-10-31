# frozen_string_literal: true

module SolidQueue
  # The Active Job payload of one collected perform_later call. It waits on a
  # CoalesceKey until a job starts and claims it, and from then on it belongs to
  # that job, across retries, until the job is destroyed.
  class CoalescedCall < Record
    belongs_to :coalesce_key, optional: true
    belongs_to :job, optional: true

    serialize :arguments, coder: JSON

    scope :waiting, -> { where.not(coalesce_key_id: nil) }
    scope :claimed, -> { where.not(job_id: nil) }
  end
end
