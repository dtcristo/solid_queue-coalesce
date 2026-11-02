# frozen_string_literal: true

module SolidQueue
  module Coalesce
    class Engine < ::Rails::Engine # :nodoc:
      initializer "solid_queue.coalesce.active_job", after: "solid_queue.active_job.extensions" do
        ActiveSupport.on_load :active_job do
          include ActiveJob::Coalescing
        end
      end

      # Job and RecurringTask are reloadable, so a reload needs the patches again
      config.to_prepare { Patches.apply }
    end
  end
end
