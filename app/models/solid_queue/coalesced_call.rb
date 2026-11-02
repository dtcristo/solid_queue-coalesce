# frozen_string_literal: true

module SolidQueue
  # The Active Job payload of one collected perform_later call. It belongs to one
  # job from the moment it's admitted, across that job's retries, until the job is
  # destroyed.
  class CoalescedCall < Record
    belongs_to :job

    serialize :arguments, coder: JSON

    ##
    # :singleton-method: waiting
    #
    # Calls whose job hasn't finished, isn't running and hasn't failed, for measuring
    # the backlog.
    #
    #   SolidQueue::CoalescedCall.waiting.count
    #   SolidQueue::CoalescedCall.waiting.minimum(:created_at)
    scope :waiting, -> { where(job_id: Job.where(finished_at: nil).where.missing(:claimed_execution, :failed_execution).select(:id)) }
  end
end
