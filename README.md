# Solid Queue Coalesce

Coalescing for [Solid Queue](https://github.com/rails/solid_queue). Many `perform_later` calls with the same key share one pending job, and that key is held only until the job starts. A call that arrives after that opens a new job, so anything that happens after a run has started is always covered by another run.

It's a separate gem that adds to Solid Queue from the outside, with its own tables. It only works with the Solid Queue adapter, because a foreign key to the job and a shared transaction are what let it work without a time window to configure.

Coalescing protects pending work. It doesn't stop two runs from overlapping, so use `limits_concurrency` if you need that.

## Table of contents

- [Installation](#installation)
- [Usage](#usage)
- [Keys](#keys)
- [Collecting calls](#collecting-calls)
- [Batches and scheduling](#batches-and-scheduling)
- [Enqueue outcomes](#enqueue-outcomes)
- [Bulk enqueues](#bulk-enqueues)
- [Transactions](#transactions)
- [Retries](#retries)
- [Deleted records](#deleted-records)
- [Recurring jobs and concurrency limits](#recurring-jobs-and-concurrency-limits)
- [Discarding and retention](#discarding-and-retention)
- [Changing a job's configuration](#changing-a-jobs-configuration)
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

## Usage

```ruby
class ReindexProductJob < ApplicationJob
  coalesce key: ->(product_id) { product_id }

  def perform(product_id)
    Product.find(product_id).reindex!
  end
end
```

The default is `on_conflict: :discard`. A call that finds a pending job for its key is suppressed, and the pending job keeps its own schedule, queue, priority and arguments. The key is free again the moment that job starts, so a call that arrives during the run creates the next pending job.

The options are:

```ruby
coalesce key:, on_conflict: :discard, batch_size: nil
```

- `key` is required. See [Keys](#keys).
- `on_conflict` is `:discard` or `:collect`. Anything else raises `ArgumentError`.
- `batch_size` is only allowed with `:collect`. It's a positive integer, 100 by default.

## Keys

`key` works like the one in `limits_concurrency`, with the same derivation:

```ruby
coalesce key: :global                       # Static job-wide key
coalesce key: "global"                      # Same literal component
coalesce key: ->(account_id) { account_id } # Distinct per account
coalesce key: ->(account) { account }       # Active Record class and ID
```

Keys are always scoped to the job class name. `coalesce key: :account_id` doesn't read a parameter called `account_id`. It's a static string, as it is in `limits_concurrency`.

The key column is declared like the one on Solid Queue's semaphores, with the database's default collation and a unique index. That's upstream parity, and it isn't a guarantee of bytewise uniqueness. Under a case- and accent-insensitive collation such as MySQL's `utf8mb4_0900_ai_ci`, `Job/a`, `Job/A` and `Job/á` compare equal and coalesce together. Under a case-sensitive or binary collation they stay distinct.

## Collecting calls

With `on_conflict: :collect`, no call is suppressed. Every accepted call is stored, including the first, and belongs to one job from the moment it's enqueued. That job runs `perform_batch` with its calls:

```ruby
class BulkReindexProductsJob < ApplicationJob
  coalesce key: ->(account_id, *) { account_id },
           on_conflict: :collect,
           batch_size: 100

  def perform_batch(calls)
    account_id = calls.first.arguments.first
    product_ids = calls.map { |call| call.arguments.last }.uniq

    products = Product.where(account_id: account_id, id: product_ids).to_a
    SearchIndex.bulk_index(products)
  end
end
```

```ruby
BulkReindexProductsJob
  .set(wait: 30.seconds)
  .perform_later(product.account_id, product.id)
```

Pass IDs, and do one bounded load and one bulk operation per batch. That's the reason to collect in the first place. `SearchIndex.bulk_index` stands in for your own code.

A collecting job always runs `perform_batch`, and ignores any `perform` it has. If `perform_batch` is missing, the run raises `NotImplementedError` and the job fails with its calls.

`calls` is an array of `ActiveJob::Coalescing::Call`, oldest first, with three readers:

```ruby
call.id        # The row's ID, nil for perform_now and other adapters
call.arguments # The deserialized positional array
call.job_data  # The call's whole serialized Active Job payload
```

`arguments` comes from the same deserialization as a normal run. Each call is rebuilt on its own instance of the job, from its payload, through `deserialize` and `deserialize_arguments`. So GlobalID, custom serializers and your own overrides of those methods all apply. Keyword arguments are a trailing hash with Ruby's keyword marker, so `some_method(*call.arguments)` passes them as keywords. A trailing hash that was passed positionally stays a plain hash, and stays positional.

If a call refers to a deleted record, that call is dropped. Any other error while deserializing, including one from your own override, fails the job with its calls. See [Deleted records](#deleted-records).

`job_data` is the raw snapshot taken when the call was enqueued: `job_id`, `queue_name`, `priority`, `locale`, `timezone`, `enqueued_at`, `scheduled_at`, and whatever your `serialize` adds. It's the payload as stored, so it doesn't go through your `deserialize`, and arguments that `serialize` wrapped are still wrapped. It's frozen at the top level.

Those fields describe the request, not this run. `perform_batch` runs once, in the context of the job that owns the calls, which is its first call's. Nothing restores another call's locale, timezone or custom fields for you, so read them from `job_data` if they matter:

```ruby
def perform_batch(calls)
  calls.group_by { |call| call.job_data["locale"] }.each do |locale, calls_in_locale|
    I18n.with_locale(locale) { notify(calls_in_locale) }
  end
end
```

`coalesced_calls` holds the same array, and perform callbacks can read it. `arguments` on the job itself keeps holding the arguments of the batch's first call, which is what the `limits_concurrency` key sees.

## Batches and scheduling

A key has at most one open job, and any number of sealed ones. A call joins the open job, or opens a new one if there isn't one. The call that brings a job to `batch_size` seals it, and the next call opens a new job. With `batch_size: 100`, 250 calls make 3 jobs: 2 sealed with 100 calls each, and 1 open with 50. An open job also closes when it starts, so a call that arrives during the run opens a new job.

Sealing doesn't dispatch anything. A full job still waits for its time, and it runs like any other job. `batch_size` is the most calls one run takes, not a limit on how many can wait. The one exception comes from lowering it: a job already over the new size gets sealed as it is, and its run takes all its calls.

Each job takes its schedule, queue, priority, concurrency key and own arguments from its first call. Calls that join don't move it, even if they asked for a later time. So any caller that might arrive first needs the right `wait`: a plain `perform_later` opens a job that runs right away.

Batches of the same key are separate jobs, and this has consequences:

- They can start out of order. A later batch with no wait can overtake an earlier delayed one.
- They can overlap. Use `limits_concurrency` if they shouldn't.
- Their concurrency keys, queues and priorities can differ, because each comes from that batch's first call.

Enqueues for the same key take turns, because each one holds an exclusive lock on the key's row until its transaction commits. Inside your own transaction, that lock lasts until your commit, so a key used in a 50ms transaction takes about 20 calls per second, however many connections you add. Keep those transactions short, or enqueue after them.

To raise throughput on one key, use `ActiveJob.perform_all_later`. It stores up to a full batch under one lock. It keeps the order of each key's calls, and uses one transaction per batch. [Bulk enqueues](#bulk-enqueues) covers what happens if it fails partway.

## Enqueue outcomes

`coalesce_status` on the Active Job instance says what happened to the call. It's read-only.

- `:created` means it created a pending job. With `:collect`, that's the first call of each batch.
- `:discarded` means a pending job already held the key, and the call was suppressed.
- `:collected` means it was stored for the pending job.
- `nil` means this instance has no recorded admission.

So a call's data was stored if the status is `:created` or `:collected`, and it wasn't if it's `:discarded`:

```ruby
# on_conflict: :collect
call = BulkReindexProductsJob.perform_later(account_id, product_id)
call.coalesce_status # :created or :collected. Either way, perform_batch gets this call

# on_conflict: :discard, the default
call = ReindexProductJob.perform_later(product_id)
call.coalesce_status # :created, so a job runs with these arguments
                     # :discarded, so the pending job runs with its own, and these are gone
```

`nil` isn't a failure on its own. It's also what you read before an enqueue, on a class that doesn't coalesce, and on a worker's instance, including inside `perform` and `perform_batch` and on retries, because the status isn't serialized. The table has the rest:

| Situation | `coalesce_status` | `provider_job_id` | `successfully_enqueued?` | Call's data stored |
| --- | --- | --- | --- | --- |
| Created a job | `:created` | The new job | `true` | Yes |
| Joined with `:collect` | `:collected` | The open job | `true` | Yes |
| Joined with `:discard`, single | `:discarded` | The open job | `true`, set by Active Job | No |
| Joined with `:discard`, bulk | `:discarded` | The open job | `false` | No |
| Concurrency conflict destroyed the job, single | `nil` | `nil` | `true`, set by Active Job | No |
| Concurrency conflict destroyed the job, bulk | `nil` | `nil` | `false` | No |
| `before_enqueue` abort | `nil` | `nil` | `false` | No |
| Deferred, before the commit | `nil` | `nil` | `true` | Not yet |
| Failed enqueue, fresh instance | `nil` | `nil` | `false` | No |
| Failed enqueue or abort, reused instance | Previous value | Previous value | `false` | No |
| Rollback of your own transaction | Stale | Stale | Unchanged | No |
| Another adapter | `nil` | The adapter's ID, if it sets one | `true` | The adapter has the job |
| `perform_now` on a new instance | `nil` | `nil` | `false` | Not enqueued |
| A worker's instance | `nil` | The current run's job | `false`, and `true` on what `retry_job` returns | Calls move with a retry |

- `false` here includes `nil`, which is what `successfully_enqueued?` returns until an enqueue has been tried.
- After a `before_enqueue` abort, `perform_later` returns `false`, as it does for any job. Pass a block to `perform_later` if you need the instance.
- A failed enqueue raises `SolidQueue::Job::EnqueueError`, so only an instance you built yourself can be read afterwards. Nothing resets a reused instance's status or `provider_job_id`.
- A deferred call that gets rolled back stays as it was before the commit, and is never stored. Rails 7.2 with `load_defaults 7.2` defers by default on Solid Queue, so there a rollback looks like this too, instead of leaving stale values.
- On Rails 7.2 and 8.0, `retry_job` enqueues a copy of the job, so the worker's instance doesn't change at all. Read the result from what `retry_job` returns.

Don't read outcomes from `successfully_enqueued?`. Active Job sets it to `true` after `perform_later` even when the call was discarded. It stays that way on purpose: `false` would make `perform_later` return `false`, and you'd lose the instance you read `coalesce_status` from.

`provider_job_id` is the job that held the key at admission. It's a snapshot. A retry moves collected calls to a new job with a new ID, and their call IDs stay the same, so the ID you read at enqueue can stop being the job that will run the call. A call that joined also has its own `job_id`, which differs from the `active_job_id` of the job that owns it. That job has the `active_job_id` of its first call.

`after_enqueue` can read `coalesce_status` for a single enqueue, including `:collected`. `ActiveJob.perform_all_later` skips enqueue callbacks, as it always does. It sets `coalesce_status` and `provider_job_id` on each job after that call's batch commits, and subscribers to the event below see every outcome.

Three notifications exist:

| Event | When | Payload |
| --- | --- | --- |
| `coalesce.solid_queue` | A call was admitted, including the one that created the job | `action` (`:created`, `:discarded`, `:collected`), `key`, `job_id`, `active_job_id`, `class_name` |
| `drop_coalesced_call.solid_queue` | A collected call was skipped because its record is gone | `call_id`, `job_id`, `active_job_id`, `class_name`, `exception_class`, `call_deleted` |
| `coalesced_calls_mismatch.solid_queue` | A starting job has a different number of calls than its key counted | `job_id`, `active_job_id`, `class_name`, `expected`, `actual` |

`SolidQueue::LogSubscriber` logs the first at debug level and the others at warn level. None logs arguments or payloads, and none logs the key, which your app defines. The key is only in the event's payload.

`coalesce.solid_queue` fires right after the admission of a call's batch. Outside a transaction on the queue database, that's after the commit, and the outcome is final. Inside one, it's before the commit, so the outcome is provisional: a rollback can undo what it reported, and the event stays fired. No event fires for a retry, for a worker starting a job, or for a call that a `limits_concurrency` conflict destroyed.

To count only what committed, register the count on the queue database's transaction. It runs at the outermost commit, straight away if there's no transaction, and never after a rollback, including a rolled-back savepoint. A transaction on your app's own database doesn't affect it, because the queue's write doesn't depend on that one.

A job that [waits for your commit](#transactions) has no admission until then, so a rollback drops the call and no event fires.

```ruby
ActiveSupport::Notifications.subscribe("coalesce.solid_queue") do |*, payload|
  SolidQueue::Record.current_transaction.after_commit do
    Metrics.increment("solid_queue.coalesce", tags: payload.slice(:class_name, :action))
  end
end
```

`current_transaction` needs Rails 7.2. On Rails 7.1 this subscriber raises `NoMethodError`, which fails the enqueue, so count from the event directly there.

The payloads' IDs last for different lengths of time:

- `class_name` and `action` don't change, and have few values. They're the only ones to use as metric labels.
- `active_job_id` is the call's own on `coalesce.solid_queue`, and the owning job's on the other two events. The owning job has the `active_job_id` of its first call, across retries.
- `job_id` is the job at that moment. A retry moves collected calls to a job with a new ID.
- `call_id` exists only once a worker reads the calls, because admission inserts them in bulk. A `coalesce.solid_queue` event can't be matched to a later `call_id`.

## Bulk enqueues

Native Solid Queue stores a whole `ActiveJob.perform_all_later` in one transaction, so it's all-or-nothing. With coalesced jobs in the list it isn't, unless you ask for it. Outside your own transaction, it stores ordinary jobs first, in one native transaction. Coalesced calls follow, sorted by key, with one transaction per discard admission or collecting batch. A key's lock lasts until its transaction commits, and one transaction for the whole call would hold every key's lock until the last batch was in. This way it holds one key's lock for one batch.

Before it writes anything, the call works out the keys of all coalesced calls, the payloads of all `:collect` calls, and the payload of each key's first `:discard` call. If a key Proc raises or an argument can't be serialized, the call raises with nothing written. A repeated `:discard` call is never stored, so an argument on it that can't be serialized stays silently discarded, as with a single `perform_later`.

After that, only a database error or a `coalesce.solid_queue` subscriber that raises can fail the call. There's one exception. A batch's concurrency key comes from its first call, and which call that is depends on the database, so a concurrency key Proc can raise on a later batch's first call. In every case, earlier work stays committed and has its outcomes, and later calls have no outcome.

To recover outside a transaction, enqueue the calls that have no job ID again:

```ruby
ActiveJob.perform_all_later(jobs.reject(&:provider_job_id))
```

This stores each call once. Calls destroyed by a `limits_concurrency` conflict with `on_conflict: :discard` have no ID either, so they get retried, which is harmless.

Trust outcomes only from a call that didn't raise, or for this recovery. A failed enqueue of an instance you've enqueued before can show its old outcome.

A database failure on coalesced calls raises `SolidQueue::Job::EnqueueError`. One on ordinary jobs raises the raw Active Record error, as native bulk does. A single call can raise either.

Only the order of each key's calls is guaranteed. Native bulk creates jobs in input order. Here, ordinary jobs come first, then coalesced calls sorted by key.

Discarded calls count as not enqueued, like the ones a native concurrency conflict discards, so Active Job logs "Failed enqueuing N jobs" for them. `ActiveJob.perform_all_later` returns `nil`. `SolidQueue::Job.enqueue_all` returns the count.

For all-or-nothing, wrap the call in a transaction on the queue database:

```ruby
SolidQueue::Record.transaction do
  ActiveJob.perform_all_later(jobs)
end
```

It has to be `SolidQueue::Record.transaction`, because an app-database transaction doesn't cover a separate queue database. After a rollback, outcomes stay set, as they do in native Solid Queue, and they mean nothing. Run the whole list again. `coalesce.solid_queue` has already fired by then too.

That transaction holds every key's lock and every semaphore's lock until it commits, so other enqueues and workers wait for it. Native bulk has the same problem with ordinary concurrency-limited jobs. It dispatches them one by one inside its one transaction, and each holds a `SELECT ... FOR UPDATE` lock on its semaphore's row until the commit. Enqueue those jobs one at a time or in small slices.

## Transactions

If Solid Queue has its own database, which it recommends, your app's transaction doesn't cover the enqueue. A `perform_later` inside that transaction is committed straight away, whether your transaction commits or not, and coalescing makes this more likely to matter:

1. Your transaction changes a record, and calls `perform_later`.
2. The call finds a pending job for its key, and is discarded.
3. A worker starts that job before your transaction commits, and reads the old record.
4. Your transaction commits, and nothing runs again to pick up the change.

To avoid this, enqueue after your transaction commits:

```ruby
class ReindexProductJob < ApplicationJob
  self.enqueue_after_transaction_commit = true

  coalesce key: ->(product_id) { product_id }

  def perform(product_id)
    Product.find(product_id).reindex!
  end
end
```

The call now waits for your commit, and a rollback drops it. It also holds no lock on the key during your transaction, so it avoids the limit in [Batches and scheduling](#batches-and-scheduling).

- On Rails 7.2, set it to `:always` instead of `true`. Rails 7.1 doesn't have this setting.
- `ActiveJob.perform_all_later` only waits for your commit from Rails 8.1.2. Before that, call it after your transaction.
- `coalesce_status` and `provider_job_id` are `nil` until the commit. From Rails 8.1, `after_enqueue` runs after the commit and can read them. Before that it runs too early, so subscribe to `coalesce.solid_queue` instead.
- There's still a gap between your commit and the enqueue. A process that dies there loses the call. With `:discard`, the next call for that key covers it, if there is one. With `:collect`, that call's data is gone. If it has to survive, store it in your app's database in the same transaction, and process it from there.

If Solid Queue shares your app's database, a `perform_later` inside your transaction commits or rolls back with it, so the job can't start before your commit, and none of this is needed.

## Retries

A retry is the same work continuing, not a new candidate for coalescing. It can't be suppressed by a pending job, and later calls can't join it. A job keeps its calls through `retry_on`, `rescue_from` with `retry_job`, an `ActiveJob::Continuable` resume, a manual retry of a failed job, and a re-run after its worker was released. If a retry can't be enqueued, the job fails and keeps its calls.

Delivery is at-least-once. There's no per-call acknowledgement or per-call continuation, so a retry runs the whole batch again and `perform_batch` has to be idempotent.

## Deleted records

Each call is deserialized on its own. If a call refers to a deleted record, that call is deleted and `drop_coalesced_call.solid_queue` is emitted with `call_deleted: true`, while the others run. If that happens to every call in the batch, `perform_batch` and its callbacks are skipped and the job finishes normally. The job never falls back to its own arguments: those calls stay until the job is destroyed, so a re-run after a crash skips them too. Each of them emits the event with `call_deleted: false`, and again on that re-run, so a count of these events can be higher than the number of calls. Any other deserialization error fails the job and keeps all of its calls.

## Recurring jobs and concurrency limits

A recurring task can use `:discard`. A tick that finds a pending job is suppressed and not recorded in `solid_queue_recurring_executions`. A recurring task can't use `:collect`: its configuration is invalid.

To skip busy ticks, use a concurrency limit on its own. This suits a job that syncs full state, and it doesn't need coalescing:

```ruby
class SyncInventoryJob < ApplicationJob
  limits_concurrency to: 1,
                     key: :global,
                     on_conflict: :discard,
                     duration: 30.minutes

  def perform
    InventorySynchronizer.sync!
  end
end
```

To keep one follow-up, coalesce pending work and block on the limit:

```ruby
class SyncInventoryJob < ApplicationJob
  coalesce key: :global

  limits_concurrency to: 1,
                     key: :global,
                     on_conflict: :block,
                     duration: 30.minutes

  def perform
    InventorySynchronizer.sync!
  end
end
```

Both use the same recurring configuration:

```yaml
production:
  sync_inventory:
    class: SyncInventoryJob
    schedule: every minute
```

These behave differently. With `:block`, one tick during a run creates a blocked job, later ticks are coalesced because that job hasn't started, and it runs when the current one finishes. With concurrency `:discard` alone, busy ticks disappear and nothing catches up afterwards.

`:global` is a literal in both APIs, and coalescing and concurrency keep separate database records. A semaphore's `duration` can expire, so a concurrency limit isn't an absolute guarantee against overlap.

The concurrency key of a coalesced job is the one computed for the call that created it, and a blocked job still holds its coalesce key, since it hasn't started. `:collect` works with `on_conflict: :block`. It can't be combined with `limits_concurrency` with `on_conflict: :discard`, in either order, because a conflict would destroy a job holding calls, so that raises `ArgumentError` when the class loads. `:discard` can, and the destroyed job frees the key.

## Discarding and retention

Calls belong to their job, and are deleted with it. Discarding a pending job deletes it and its calls, and frees the key if the job was open. Other batches for the key aren't affected. If `preserve_finished_jobs` is on, a job's calls stay until the job is cleared, along with the finished job itself.

A job that fails before starting, for example because its worker died or because its class no longer exists, keeps its calls as if it had started. When you bring the class back and retry the job, it runs those same calls.

## Changing a job's configuration

A job that gets a key records how it was created in its own payload, as `"coalesce_on_conflict": "collect"` or `"discard"`, so a worker knows even if the class has changed since. If you remove `:collect` or `coalesce` while calls are waiting, each of those jobs raises `ActiveJob::Coalescing::CollectedJobMismatch` when it runs. Nothing has run at that point, `retry_on` and `discard_on` don't see the error, and the job fails with all of its calls. Restore the declaration and retry the failed job to run them, or discard the job to delete them.

That's a way to recover, not a way to deploy. To remove `:collect` or `coalesce` without failures, stop enqueuing, wait for `SolidQueue::CoalescedCall.waiting` to be empty, and only then deploy the change. Workers on the old and new code can overlap in a rolling deploy, so a job can fail on one of them and need the same recovery.

| Change while calls wait | What happens |
| --- | --- |
| Remove `:collect` or `coalesce` | The job fails with its calls, as above |
| Switch `:collect` to `:discard` | The job fails with its calls, as above. The next call doesn't count on that job, and creates a new one |
| Switch `:discard` to `:collect` | The next call doesn't join the discarding job, and creates a new one. The old job runs a batch of one from its own arguments |
| Change `limits_concurrency` to `on_conflict: :discard` after removing `:collect` | A conflict blocks the job instead of destroying it, so it reaches a worker and fails with its calls |
| Change the queue adapter | Waiting jobs still run their calls. A retry of one raises `CollectedJobMismatch`, and the job fails with its calls |
| Remove `perform_batch` | The run raises `NotImplementedError`, and the job fails with its calls, unless a handler such as `discard_on Exception` takes it |
| Rename or remove the class | The job fails with its calls |
| Change `batch_size` | Waiting calls aren't touched. An open job that's now over the new size gets sealed by the next call |
| Change the key | Waiting jobs still run their calls. New calls open a job under the new key |

Solid Queue only deletes calls along with their job. Deleting call rows yourself isn't supported, and neither is changing `calls_count`. If you do, a collecting job with no calls left raises `CollectedJobMismatch` instead of running its own arguments, which are a copy of its first call. A job that lost only some of its calls runs the rest, and emits `coalesced_calls_mismatch.solid_queue` if it still had its key. A sealed job has no key to compare with, so a partial deletion from one goes unnoticed.

Jobs enqueued by version 0.1 don't have this record. Jobs enqueued by unreleased versions from `main` may have the older `"coalesce_collecting": true` instead, which nothing reads any more. Neither is protected, so drain them before you upgrade. If one of them outlives the upgrade and its class stops coalescing, it keeps its key until it's retried, discarded or cleared, and a call can join it and never run.

## Rules and limitations

- A coalesced job can't be enqueued inside `SolidQueue::Batch.enqueue`, either alone or in bulk. A call that joins a pending job adds no job to the batch, which could finish early. Trying raises `SolidQueue::Job::CoalescedInBatchError`.
- Calling `perform_later` inside a transaction on the same database as Solid Queue doesn't abort the transaction when the call finds a pending job. On MySQL, however, a deadlock rolls back the entire transaction, so we can only retry it automatically outside of one. Inside your own transaction it raises `SolidQueue::Job::EnqueueError`. On any database with row locks, the same can happen inside your own transaction to a call that takes over a key from a collecting job, after its class switched to `:discard`.
- Solid Queue's bulk discards, such as `discard_all_in_batches`, are retried on a deadlock like enqueues are, so the same limit applies inside your own transaction.
- On MySQL, a deadlock while failing a job isn't retried, and it can stop the worker with the error Solid Queue raises for a job it can't finalize.
- Keys longer than 255 characters fail on MySQL.
- A job that isn't coalesced doesn't read the keys table when it finishes, fails or gets destroyed. It only parses its own payload to find that out. A job with the marker, or whose class coalesces, does one indexed read each time, even if it has no key left. Solid Queue's bulk discards do one read per batch, whatever the jobs are.

## Monitoring

Queue depth counts batches, not calls, because a job can hold up to `batch_size` of them. To see the backlog, look at the calls instead:

```ruby
SolidQueue::CoalescedCall.waiting.count
SolidQueue::CoalescedCall.waiting.minimum(:created_at) # nil when nothing is waiting
```

`waiting` is worked out from the job that owns each call: it hasn't finished, isn't running and hasn't failed. So it includes the calls of scheduled and blocked jobs, and a job that's waiting to retry counts again, with all of its calls. A preserved job's calls stop counting once it finishes, though they stay in the table until it's cleared.

If you alert on the age of the oldest call, allow for the wait you expect, because scheduled calls sit there until their time comes. `waiting.minimum` reads a row for every waiting call, so it gets slower as the backlog grows: about 1 second at 1,000,000 waiting calls on PostgreSQL, against about 90 ms for `waiting.count`. See [the benchmarks](test/benchmarks/README.md). Run these from one process, once a minute, not per request or per job.

Failed collected jobs and the calls they hold are separate numbers. A failed job keeps all of its calls, so the second one shows how much work is stuck:

```ruby
SolidQueue::FailedExecution.where(job_id: SolidQueue::CoalescedCall.select(:job_id)).count
SolidQueue::CoalescedCall.where(job_id: SolidQueue::FailedExecution.select(:job_id)).count
```

Batch sizes come from Active Job's own event. `coalesced_calls` is already loaded by then, so reading it adds no query. It's `nil` for a job that doesn't collect, and empty when every record is gone. Each attempt reports its batch, so a retry counts again. A batch whose size reaches `coalesce_batch_size` filled up. No event marks a batch being sealed, and one sealed because a configuration changed looks like any smaller batch.

```ruby
# Metrics stands in for whatever client you use. Keep job IDs and keys out of the
# labels, because there are too many of them, and because your app defines the keys.
ActiveSupport::Notifications.subscribe("coalesce.solid_queue") do |*, payload|
  Metrics.increment("solid_queue.coalesce", tags: payload.slice(:class_name, :action))
end

ActiveSupport::Notifications.subscribe("drop_coalesced_call.solid_queue") do |*, payload|
  Metrics.increment("solid_queue.coalesce.dropped", tags: payload.slice(:class_name, :call_deleted))
end

ActiveSupport::Notifications.subscribe("coalesced_calls_mismatch.solid_queue") do |*, payload|
  Metrics.increment("solid_queue.coalesce.mismatched", tags: payload.slice(:class_name))
end

ActiveSupport::Notifications.subscribe("perform.active_job") do |*, payload|
  if calls = payload[:job].coalesced_calls
    Metrics.histogram("solid_queue.coalesce.batch_size", calls.size, tags: { class_name: payload[:job].class.name })
  end
end

# Once a minute, from one process
waiting = SolidQueue::CoalescedCall.waiting
Metrics.gauge("solid_queue.coalesce.waiting", waiting.count)
Metrics.gauge("solid_queue.coalesce.oldest_age", Time.current - (waiting.minimum(:created_at) || Time.current))
Metrics.gauge("solid_queue.coalesce.failed_jobs", SolidQueue::FailedExecution.where(job_id: SolidQueue::CoalescedCall.select(:job_id)).count)
Metrics.gauge("solid_queue.coalesce.failed_calls", SolidQueue::CoalescedCall.where(job_id: SolidQueue::FailedExecution.select(:job_id)).count)
```

The first subscriber counts provisional outcomes too. Use the one in [Enqueue outcomes](#enqueue-outcomes) if your app enqueues inside queue-database transactions.

## Testing

Everything above needs the Solid Queue adapter, so other adapters ignore `coalesce`. With the test adapter, the inline adapter or `perform_now`, each call runs on its own, and a collecting job's `perform_batch` receives a batch of one whose `id` is `nil`. `assert_enqueued_with(args: [ product_id ])` still works, and the Solid Queue tables aren't touched.

A batch of one builds `job_data` when you read it, so `perform_now` with arguments that can't be serialized only raises if you read it.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
