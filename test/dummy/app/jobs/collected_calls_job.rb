class CollectedCallsJob < ApplicationJob
  coalesce key: ->(group, *, **) { group }, collect: true, limit: 3

  def perform(calls)
    JobBuffer.add(calls)
  end
end
