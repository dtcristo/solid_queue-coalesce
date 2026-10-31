class CollectedResultsJob < ApplicationJob
  coalesce key: ->(group, *) { group }, on_conflict: :collect, batch_size: 5

  def perform_batch(calls)
    JobResult.create!(status: "batch", value: calls.map { |call| call.arguments.last }.to_json)
  end
end
