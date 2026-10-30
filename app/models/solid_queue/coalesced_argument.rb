# frozen_string_literal: true

module SolidQueue
  # The serialized arguments of one collected perform_later call. It waits on a
  # CoalesceKey until a job starts and claims it, and from then on it belongs to
  # that job, across retries, until the job is destroyed.
  class CoalescedArgument < Record
    belongs_to :coalesce_key, optional: true
    belongs_to :job, optional: true

    serialize :arguments, coder: JSON

    scope :waiting, -> { where.not(coalesce_key_id: nil) }
  end
end
