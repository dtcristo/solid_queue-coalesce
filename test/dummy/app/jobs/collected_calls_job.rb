class CollectedCallsJob < ApplicationJob
  coalesce key: ->(group, *, **) { group }, on_conflict: :collect, batch_size: 3

  def perform_batch(calls)
    JobBuffer.add(calls.map(&:arguments))
  end
end
