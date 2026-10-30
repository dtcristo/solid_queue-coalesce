module JobsTestHelper
  private

  # Claims and performs what's ready, the way a worker does, in this thread.
  def perform_ready_jobs
    process = SolidQueue::Process.find_by(name: "test-worker") || SolidQueue::Process.register(kind: "Worker", pid: 42, name: "test-worker")

    SolidQueue::ReadyExecution.claim("*", 100, process.id).each(&:perform)
  end

  # The name is in both places, and a rename in a deploy leaves both unresolvable.
  def rename_job_class(job, name)
    job.update!(class_name: name, arguments: job.arguments.merge("job_class" => name))
  end

  # Calls the block in the middle of each job's run, after it has started.
  def perform_ready_jobs_with_callback(&block)
    results = []
    callback = ->(*) { results << block.call }

    ActiveSupport::Notifications.subscribed(callback, "perform_start.active_job") { perform_ready_jobs }
    results
  end

  def wait_for_jobs_to_finish_for(timeout = 1.second, except: [])
    wait_while_with_timeout(timeout) do
      skip_active_record_query_cache do
        SolidQueue::Job.where.not(active_job_id: Array(except).map(&:job_id)).where(finished_at: nil).any?
      end
    end
  end

  def wait_for_jobs_to_be_released_for(timeout = 1.second)
    wait_while_with_timeout(timeout) do
      skip_active_record_query_cache do
        SolidQueue::ClaimedExecution.count > 0
      end
    end
  end

  def wait_for_batches_to_finish_for(timeout = 1.second)
    wait_while_with_timeout(timeout) do
      skip_active_record_query_cache do
        SolidQueue::Batch.where(finished_at: nil).any?
      end
    end
  end

  def assert_unfinished_jobs(*jobs)
    skip_active_record_query_cache do
      assert_equal jobs.map(&:job_id).sort, SolidQueue::Job.where(finished_at: nil).map(&:active_job_id).sort
    end
  end

  def assert_no_unfinished_jobs
    skip_active_record_query_cache do
      assert SolidQueue::Job.where(finished_at: nil).none?
    end
  end

  def wait_for_jobs_to_be_enqueued(count, timeout: 1.second)
    wait_while_with_timeout(timeout) do
      skip_active_record_query_cache do
        SolidQueue::Job.count < count
      end
    end
  end

  def assert_no_claimed_jobs
    skip_active_record_query_cache do
      assert SolidQueue::ClaimedExecution.none?
    end
  end

  def assert_claimed_jobs(count = 1)
    skip_active_record_query_cache do
      assert_equal count, SolidQueue::ClaimedExecution.count
    end
  end
end
