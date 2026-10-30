# Solid Queue Coalesce

Coalescing for [Solid Queue](https://github.com/rails/solid_queue). Many `perform_later` calls with the same key share one pending job, and that key is held only until the job starts. A call that arrives after that opens a new job, so anything that happens after a run has started is always covered by another run.

It's a separate gem that adds to Solid Queue from the outside, with its own tables. It only works with the Solid Queue adapter, because a foreign key to the job and a shared transaction are what let it work without a time window to configure.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Collecting calls](#collecting-calls)
- [Retries and partial success](#retries-and-partial-success)
- [Discarding and retention](#discarding-and-retention)
- [Rules and limitations](#rules-and-limitations)
- [Monitoring](#monitoring)
- [Testing](#testing)
- [License](#license)

## Installation

Add the gem to your application's Gemfile, alongside Solid Queue:

```ruby
gem "solid_queue-coalesce"
```

Then copy its migration to your queue database's migrations path and run it:

```bash
bundle install
bin/rails generate solid_queue:coalesce:install
bin/rails db:migrate
```

If Solid Queue doesn't use a database called `queue`, pass the right one with `--database`. For example, `bin/rails generate solid_queue:coalesce:install --database primary`.

Until the migration has run, everything else works as before, and enqueuing a job that uses `coalesce` raises `SolidQueue::CoalesceKey::PendingMigrations`.

The gem requires Solid Queue 1.7. It relies on private Solid Queue methods, so its gemspec allows one minor version at a time.

A dispatcher with `concurrency_maintenance` on, which is the default, is what releases a coalesce key left on a job that finished or failed. If you run several dispatchers, at least one needs it.

## Usage

With coalescing, many `perform_later` calls with the same key share one pending job. The key has to be open for this to happen: it's taken when the first call creates a job, and it's free again the moment that job starts. A call that arrives after that opens a new job, so anything that happens after a run has started is always covered by another run.

```ruby
class RefreshAccountJob < ApplicationJob
  coalesce key: ->(account) { account }

  def perform(account)
    account.refresh!
  end
end
```

`key` is the only required parameter, and it works like the one in `limits_concurrency`: a symbol, a string or a proc that receives the job arguments. If the proc returns an Active Record record, the key is built from its class name and `id`. Keys are always scoped to the job class name.

By default a call that finds an open job for its key is dropped. It sets `coalesced?` to `true` on the Active Job instance and `provider_job_id` to the job it found, and emits a `coalesce.solid_queue` event with `key`, `job_id`, `active_job_id` and `action` (`:dropped` or `:collected`). Bear in mind that Active Job sets `successfully_enqueued?` to `true` after `perform_later` regardless, so `coalesced?` is what tells you the call was dropped. `ActiveJob.perform_all_later` counts dropped calls as not enqueued, as it does for `on_conflict: :discard`.

The first call's schedule, queue and priority win. A later call's `wait:` doesn't move the open job.

Coalescing doesn't change `limits_concurrency`, and you can use both in the same job. The concurrency key is the one computed for the call that created the job, and a job that's blocked still holds its coalesce key, since it hasn't started. If `on_conflict: :discard` destroys the job, that frees the key.

### Collecting calls

With `collect: true`, no call is dropped. Each one stores its arguments, and the job receives them all:

```ruby
class ReindexDocumentsJob < ApplicationJob
  coalesce key: ->(document) { document.account_id }, collect: true, limit: 100

  def perform(documents)
    Search.index(documents)
  end
end
```

`perform` receives an array with one element per call, in the order they were stored. For a call with one argument the element is that argument, as above. For a call with several it's an array of them, and keyword arguments are a hash at the end of that array, so `perform_later(document, force: true)` gives `[ document, { force: true } ]`.

`arguments` isn't part of this: it keeps holding the arguments of the call that created the job, which is what callbacks and the `limits_concurrency` key see.

`limit` is 100 by default, and each run gets at most that many calls. If more are waiting when a job starts, it takes the oldest `limit` and a new job is created for the rest, in the same queue and with the same priority and concurrency key. It's due when the oldest call left asked to run, or right away if at least `limit` calls are left, unless you turn off `flush_when_full` (see below).

Reaching `limit` also flushes a job that's scheduled: it's dispatched right away instead of waiting for its time. So you can use `wait:` to collect calls for a while, with `limit` as the point where you stop waiting. Concurrency limits apply to a flushed job as usual, and jobs already ready or blocked aren't affected. A call that arrives after a flush still has its own `wait:` stored, so if it ends up in the next job, that's the time the next job will run. Concurrent calls can leave slightly more than `limit` calls waiting, but that doesn't matter because the limit is enforced when the job starts.

Use `flush_when_full: false` for a job that has to wait for its time, such as one that runs at a fixed time of day. Solid Queue can't work this out on its own, because `wait:` and `wait_until:` both end up as the same `scheduled_at`.

```ruby
class SendDigestJob < ApplicationJob
  coalesce key: ->(entry) { entry.account_id }, collect: true, limit: 100, flush_when_full: false

  def self.perform_tonight(entry)
    set(wait_until: Time.current.tomorrow.change(hour: 2)).perform_later(entry)
  end

  def perform(entries)
    DigestMailer.with(entries: entries).digest.deliver_now
  end
end
```

With this option, reaching `limit` doesn't dispatch the job, and it stays scheduled. When it starts with more than `limit` calls waiting, the new job for the rest is due when the oldest call left asked to run, however many are left. So a backlog that's already due drains in batches of `limit`, one after another, and calls dated in the future wait for their time.

The first call since the previous job started sets the schedule, and later calls' times don't move an open job. A later call that joins a job still waiting to start runs with that job, even if it asked for a later time. A call that arrives after the last batch started opens a new job for its own time. This means any caller that might arrive first needs the right time: a plain `perform_later` would open a job that runs right away. That's why the example wraps `set(wait_until:)` in a class method.

If you alert on the age of the oldest row in `solid_queue_coalesced_arguments`, allow for the wait you expect, because these calls sit there until their time comes.

Each call is deserialized on its own. If a call refers to a record that has been deleted, that call is deleted and a `drop_coalesced_argument.solid_queue` event is emitted, while the others run. If every call refers to a deleted record, the events are emitted, `perform` is skipped and the job finishes normally. Those calls stay until the job is destroyed, so a re-run after a crash skips `perform` too. Any other deserialization error fails the job and keeps all of its calls.

### Retries and partial success

A retry is the same work continuing. It never coalesces: it can't be dropped because another job is open for its key, and later calls can't join it. A job that's retried or resumed keeps the exact calls it started with, including after `retry_on`, `rescue_from` with `retry_job`, and `ActiveJob::Continuable`. If the retry can't be enqueued, the job fails and keeps its calls. Retrying a failed job manually runs the same calls again, and so does a job that gets released after its worker was shut down.

To avoid repeating calls that have already been processed, use `ActiveJob::Continuable` with a cursor. Use a record's `id` as the cursor instead of a position in the array, because calls for deleted records are dropped between runs and positions move:

```ruby
class ReindexDocumentsJob < ApplicationJob
  include ActiveJob::Continuable

  coalesce key: ->(document) { document.account_id }, collect: true

  def perform(documents)
    step :index do |step|
      documents.sort_by(&:id).each do |document|
        next if step.cursor && document.id <= step.cursor

        Search.index(document)
        step.set! document.id
      end
    end
  end
end
```

Coalescing has no per-call acknowledgement. Without a cursor a retry processes every call in the job again.

### Discarding and retention

Calls belong to their job, and are deleted with it. Discarding an open job deletes it, its key and every call waiting on it. If `preserve_finished_jobs` is on, a job's calls stay until the job is cleared, along with the finished job itself.

A job that fails before starting, for example because its worker died or because its class no longer exists, takes care of its calls in the same way as if it had started. If the class is gone, there's no way to know the limit, so the failed job takes all the waiting calls. When you bring the class back and retry the job, it will run them all, even if that's more than `limit`.

### Rules and limitations

- `limit` and `flush_when_full` need `collect: true`, and `collect: true` can't be combined with `limits_concurrency` with `on_conflict: :discard`, because a conflict would destroy a job holding calls. Both raise `ArgumentError` when the class loads. Without `collect`, `on_conflict: :discard` is fine, and the job being discarded frees the key.
- A coalesced job can't be enqueued inside `SolidQueue::Batch.enqueue`, either alone or in bulk. A call that joins an open job adds no job to the batch, which could finish early. Trying raises `SolidQueue::Job::CoalescedInBatchError`.
- A recurring task can't use a job with `collect: true`: its configuration is invalid. Without `collect`, it works. A tick that finds an open job is dropped and isn't recorded.
- Calling `perform_later` inside a transaction on the same database as Solid Queue is safe: a call for an open job doesn't abort the transaction. On MySQL, however, a deadlock rolls back the entire transaction, so we can only retry it automatically outside of one. Inside your own transaction it raises `SolidQueue::Job::EnqueueError`.
- On MySQL, a deadlock while failing a job isn't retried, and it can stop the worker with the error Solid Queue raises for a job it can't finalize.
- Keys longer than 255 characters fail on MySQL. `"a"` and `"a "` count as the same key there, because `utf8mb4_bin` pads spaces.
- Concurrent calls that reach `limit` together can miss the early flush. The next call or the job's schedule covers it.
- Removing `collect: true` from a job, or `coalesce` altogether, isn't safe while calls are waiting for it: they wouldn't be processed. Drain them first: stop enqueuing and wait for `SolidQueue::CoalescedArgument.waiting` to be empty before you deploy the change.

### Monitoring

Queue depth hides the backlog, because any number of calls waiting may be a single job. To see it, look at the calls instead:

```ruby
SolidQueue::CoalescedArgument.waiting.count
SolidQueue::CoalescedArgument.waiting.minimum(:created_at) # nil when nothing is waiting
```

### Testing

Everything above needs the Solid Queue adapter, so other adapters ignore `coalesce`. With the test adapter, the inline adapter or `perform_now`, each call runs on its own, and `perform` receives a batch of one built from that call's arguments. `assert_enqueued_with(args: [ document ])` still works, and the Solid Queue tables aren't touched.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
