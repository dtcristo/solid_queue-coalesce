class CollectedResultsJob < ApplicationJob
  coalesce key: ->(group, *) { group }, collect: true, limit: 5

  def perform(calls)
    JobResult.create!(status: "batch", value: calls.map(&:last).to_json)
  end
end
