# frozen_string_literal: true

module SolidQueue
  # The Active Job payload of one collected perform_later call. It's kept from the
  # moment it's admitted until the job that holds it is destroyed, and a retry moves
  # it to the new job.
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
