# Solid Queue Coalesce

Dedupe, debounce and batch [Solid Queue](https://github.com/rails/solid_queue) jobs. Many `perform_later` calls with the same key share one pending job, and that key is held only until the job starts. A call that arrives after that opens a new job, so anything that happens after a run has started is always covered by another run.

Coalescing is what this gem calls merging duplicate calls into one pending job, and deduping, unique pending jobs, debouncing, leading-edge runs and batching are all ways to use it. It has nothing to do with SQL's `COALESCE`.

It isn't strict uniqueness. A key is only held until a job starts, so this doesn't guarantee a job runs once ever, and it doesn't limit how many jobs run per period. It protects pending work and doesn't stop two runs from overlapping, so use `limits_concurrency` if you need that.

It's a separate gem that adds to Solid Queue from the outside, with its own tables. It only works with the Solid Queue adapter, because a foreign key to the job and a shared transaction are what let it work without a time window to configure.

## Table of contents

- [Recipes](#recipes)
  - [Dedupe jobs with one pending job per key](#dedupe-jobs-with-one-pending-job-per-key)
  - [Wait a fixed time for more calls](#wait-a-fixed-time-for-more-calls)
  - [Debounce until activity stops](#debounce-until-activity-stops)
  - [Debounce with a limit](#debounce-with-a-limit)
  - [Run on the leading edge](#run-on-the-leading-edge)
  - [Skip work while a run is busy](#skip-work-while-a-run-is-busy)
  - [Batch every call together](#batch-every-call-together)
  - [Limit how many calls a batch takes](#limit-how-many-calls-a-batch-takes)
  - [Run a batch at N calls or T seconds](#run-a-batch-at-n-calls-or-t-seconds)
  - [Debounce a batch](#debounce-a-batch)
  - [Coalesce a recurring task](#coalesce-a-recurring-task)
  - [Enqueue after your transaction commits](#enqueue-after-your-transaction-commits)
  - [Enqueue many calls at once](#enqueue-many-calls-at-once)
  - [Find out what happened to a call](#find-out-what-happened-to-a-call)
  - [Test a coalesced job with another adapter](#test-a-coalesced-job-with-another-adapter)
- [Before you rely on these](#before-you-rely-on-these)
- [Compatibility](#compatibility)
- [Installation](#installation)
- [Usage](#usage)
- [Keys](#keys)
- [Collecting calls](#collecting-calls)
- [Collecting jobs and scheduling](#collecting-jobs-and-scheduling)
- [Debouncing](#debouncing)
- [Scheduling timelines](#scheduling-timelines)
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

## Recipes

Start from what you want to happen to a burst of calls:

```text
Run once, with the first call's arguments
|-- as soon as a worker is free ....... Dedupe jobs with one pending job per key
|-- after a fixed time ................ Wait a fixed time for more calls
|-- once calls stop ................... Debounce until activity stops
|-- once calls stop, or by a deadline . Debounce with a limit
|-- now, then once more after the run . Run on the leading edge
`-- now, unless a run is busy ......... Skip work while a run is busy

Run once, with every call's arguments
|-- after a fixed time ................ Batch every call together
|-- at most N calls per run ........... Limit how many calls a batch takes
|-- at N calls or T seconds ........... Run a batch at N calls or T seconds
`-- once calls stop, or at N calls .... Debounce a batch
```

`Product`, `SearchIndex` and `InventorySynchronizer` stand in for your own code. [Install the gem](#installation) first.

### Dedupe jobs with one pending job per key

Use this for unique pending jobs, when a duplicate call adds nothing to the work already waiting.

```ruby
class ReindexProductJob < ApplicationJob
  coalesce key: ->(product_id) { product_id }

  def perform(product_id)
    Product.find(product_id).reindex!
  end
end

ReindexProductJob.perform_later(product.id)
```

The first call runs as soon as a worker is free. Later calls get discarded until it starts, so the job only sees the first call's arguments. Pass an ID and read fresh data in `perform`. See [Usage](#usage) and [Keys](#keys).

### Wait a fixed time for more calls

Use this when a short delay lets more calls merge into the same run.

```ruby
ReindexProductJob.set(wait: 30.seconds).perform_later(product.id)
```

The job runs 30 seconds after the call that opened it, and later calls don't move it. A plain `perform_later` that arrives first opens a job that runs right away, so every caller needs the `wait`. See [Collecting jobs and scheduling](#collecting-jobs-and-scheduling) and the [timeline](#scheduling-timelines).

### Debounce until activity stops

Use this when work should wait for a burst to finish, such as reindexing after a run of edits.

```ruby
class ReindexProductJob < ApplicationJob
  coalesce key: ->(product_id) { product_id }, debounce: 30.seconds

  def perform(product_id)
    Product.find(product_id).reindex!
  end
end

ReindexProductJob.perform_later(product.id)                      # Runs 30 seconds after the last call
ReindexProductJob.set(wait: 5.minutes).perform_later(product.id) # This call asks for 5 minutes instead
```

A plain `perform_later` waits for the window here. A key that never goes quiet can delay its job indefinitely, so the next recipe is usually the better choice. See [Debouncing](#debouncing) and the [timeline](#scheduling-timelines).

### Debounce with a limit

Use this when work should wait for a burst to finish, but can't wait forever.

```ruby
class ReindexProductJob < ApplicationJob
  coalesce key: ->(product_id) { product_id }, debounce: 30.seconds, max_wait: 5.minutes

  def perform(product_id)
    Product.find(product_id).reindex!
  end
end

ReindexProductJob.perform_later(product.id)
```

The job comes due 30 seconds after the last call, and no later than 5 minutes after it opened. See [Debouncing](#debouncing) and the [timeline](#scheduling-timelines).

### Run on the leading edge

Use this when the first call should run right away, and calls that arrive during the run should merge into one follow-up.

```ruby
class SyncInventoryJob < ApplicationJob
  coalesce key: :global
  limits_concurrency to: 1, key: :global, on_conflict: :block, duration: 30.minutes

  def perform
    InventorySynchronizer.sync!
  end
end

SyncInventoryJob.perform_later
```

This has no time window. It merges calls while a job is pending, and the next call after the follow-up starts or finishes can run straight away. A cooldown after a run isn't possible, because the key is released when a job starts. See [Recurring jobs and concurrency limits](#recurring-jobs-and-concurrency-limits).

### Skip work while a run is busy

Use this when a call during a run adds nothing, because the next call will catch up. It doesn't need `coalesce`.

```ruby
class SyncInventoryJob < ApplicationJob
  limits_concurrency to: 1, key: :global, on_conflict: :discard, duration: 30.minutes

  def perform
    InventorySynchronizer.sync!
  end
end

SyncInventoryJob.perform_later
```

Calls during a run disappear, and nothing catches up afterwards. See [Recurring jobs and concurrency limits](#recurring-jobs-and-concurrency-limits).

### Batch every call together

Use this when every call carries data you need, and one run should process them all.

```ruby
class BulkReindexProductsJob < ApplicationJob
  coalesce key: ->(account_id, *) { account_id }, on_conflict: :collect

  def perform_calls(calls)
    product_ids = calls.map { |call| call.arguments.last }.uniq
    SearchIndex.bulk_index(Product.where(id: product_ids))
  end
end

BulkReindexProductsJob.set(wait: 30.seconds).perform_later(product.account_id, product.id)
```

A plain `perform_later` that arrives first opens a job that runs right away, and the batch is as small as what got there in time. A retry runs all of the job's calls again, so `perform_calls` has to be idempotent. See [Collecting calls](#collecting-calls) and [Retries](#retries).

### Limit how many calls a batch takes

Use this when one run can only handle so much.

```ruby
class BulkReindexProductsJob < ApplicationJob
  coalesce key: ->(account_id, *) { account_id }, on_conflict: :collect, max_calls: 500

  def perform_calls(calls)
    product_ids = calls.map { |call| call.arguments.last }.uniq
    SearchIndex.bulk_index(Product.where(id: product_ids))
  end
end

BulkReindexProductsJob.set(wait: 30.seconds).perform_later(product.account_id, product.id)
```

The limit is 100 if you don't set it. A full job still waits for its time, and the next call opens a new job. See [Collecting jobs and scheduling](#collecting-jobs-and-scheduling).

### Run a batch at N calls or T seconds

Use this when a batch should run as soon as it's full, and a quiet key still shouldn't wait forever.

```ruby
class BulkReindexProductsJob < ApplicationJob
  coalesce key: ->(account_id, *) { account_id }, on_conflict: :collect, max_calls: 100, on_full: :dispatch

  def perform_calls(calls)
    product_ids = calls.map { |call| call.arguments.last }.uniq
    SearchIndex.bulk_index(Product.where(id: product_ids))
  end
end

BulkReindexProductsJob.set(wait: 30.seconds).perform_later(product.account_id, product.id)
```

The `wait` is the T. A plain `perform_later` that arrives first runs right away, and so doesn't wait for N. See [Collecting jobs and scheduling](#collecting-jobs-and-scheduling) and the [timeline](#scheduling-timelines).

### Debounce a batch

Use this when a batch should run when it's quiet, when it's full or at the cap, whichever comes first.

```ruby
class BulkReindexProductsJob < ApplicationJob
  coalesce key: ->(account_id, *) { account_id },
           on_conflict: :collect,
           max_calls: 100,
           on_full: :dispatch,
           debounce: 30.seconds,
           max_wait: 5.minutes

  def perform_calls(calls)
    product_ids = calls.map { |call| call.arguments.last }.uniq
    SearchIndex.bulk_index(Product.where(id: product_ids))
  end
end

BulkReindexProductsJob.perform_later(product.account_id, product.id)
```

See [Debouncing](#debouncing).

### Coalesce a recurring task

Use this when a tick that finds a job still pending should add nothing.

```ruby
class SyncInventoryJob < ApplicationJob
  coalesce key: :global

  def perform
    InventorySynchronizer.sync!
  end
end
```

```yaml
production:
  sync_inventory:
    class: SyncInventoryJob
    schedule: every minute
```

The key is free once a run starts, so a slow run can overlap the next tick's. A recurring task can't use `:collect` or `debounce:`. See [Recurring jobs and concurrency limits](#recurring-jobs-and-concurrency-limits).

### Enqueue after your transaction commits

Use this when you enqueue inside an app transaction and Solid Queue has its own database. Without it, a pending job can start before your commit, read the old record, and never run again.

```ruby
class ReindexProductJob < ApplicationJob
  self.enqueue_after_transaction_commit = true

  coalesce key: ->(product_id) { product_id }

  def perform(product_id)
    Product.find(product_id).reindex!
  end
end
```

On Rails 7.2, set it to `:always`. Rails 7.1 doesn't have this setting. See [Transactions](#transactions).

### Enqueue many calls at once

Use this to enqueue a list of calls with fewer locks and transactions than one `perform_later` each.

```ruby
jobs = products.map { |product| ReindexProductJob.new(product.id) }

ActiveJob.perform_all_later(jobs)
```

If this raises, some calls can already be admitted. For all-or-nothing, wrap it in a transaction on the queue database:

```ruby
SolidQueue::Record.transaction do
  ActiveJob.perform_all_later(jobs)
end
```

That transaction holds every key's lock until it commits. See [Bulk enqueues](#bulk-enqueues).

### Find out what happened to a call

Use this when your code needs to know whether a call's data will reach a job.

```ruby
call = ReindexProductJob.perform_later(product.id)
call.coalesce_status # :created, :discarded or :collected
```

Don't read this from `successfully_enqueued?`, which is `true` for a discarded call too. See [Enqueue outcomes](#enqueue-outcomes).

### Test a coalesced job with another adapter

Use this to test a job without Solid Queue's tables. Other adapters ignore `coalesce`, so each call runs on its own.

```ruby
class ReindexProductJobTest < ActiveJob::TestCase
  test "enqueues a reindex" do
    assert_enqueued_with(job: ReindexProductJob, args: [ 1 ]) do
      ReindexProductJob.perform_later(1)
    end
  end
end
```

Nothing merges here, so test how calls coalesce against Solid Queue. If your test environment sets `config.active_job.queue_adapter = :solid_queue`, Active Job's helpers use that adapter and this example raises, so set `:test` there. See [Testing](#testing).

## Before you rely on these

- The first call wins. It sets the job's schedule, queue, priority and concurrency key, and later calls don't change them. With `debounce:`, later calls can move the schedule. See [Collecting jobs and scheduling](#collecting-jobs-and-scheduling) and [Debouncing](#debouncing).
- With `:collect`, a full job is sealed but still waits for its time, and the next call opens a new job. See [Collecting jobs and scheduling](#collecting-jobs-and-scheduling).
- Coalesced jobs can overlap, because the key is free as soon as a job starts. Use `limits_concurrency` to prevent that. See [Recurring jobs and concurrency limits](#recurring-jobs-and-concurrency-limits).
- `ActiveJob.perform_all_later` can admit some calls and then raise, unless you wrap it in a queue-database transaction. See [Bulk enqueues](#bulk-enqueues).
- `successfully_enqueued?` doesn't tell you what happened to a call, and `ActiveJob.perform_all_later` skips enqueue callbacks. Read `coalesce_status`. See [Enqueue outcomes](#enqueue-outcomes).
- A transaction on your app's database doesn't cover an enqueue to a separate queue database. See [Transactions](#transactions).

## Compatibility

| Dependency | Supported |
| --- | --- |
| Ruby | 3.2 and newer |
| Rails | 7.1, 7.2, 8.0 and 8.1. CI also runs Rails main, except on Ruby 3.2 |
| Solid Queue | 1.7 |
| Database | MySQL, PostgreSQL and SQLite |

CI runs Ruby 3.2, 3.3, 3.4 and 4.0 against all three databases.

| Feature | Behaviour |
| --- | --- |
| `perform_later` | Coalesces, and stores the call with `:collect` |
| `perform_all_later` | Coalesces in sorted key order, with one transaction per discard admission or collecting job. See [Bulk enqueues](#bulk-enqueues) |
| `perform_now` | Runs the call on its own. `perform_calls` receives one call |
| `SolidQueue::Batch` | Raises `SolidQueue::Job::CoalescedInBatchError` |
| Recurring tasks | `:discard` works. `:collect` and `debounce:` make the task's configuration invalid |
| Retries | Keep the job's calls, and never coalesce. See [Retries](#retries) |
| Other adapters | Ignore `coalesce`, including `debounce:` and `max_wait:`. Each call runs on its own. See [Testing](#testing) |

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

The generator looks up that database in the current environment's `config/database.yml`. If it isn't there, the generator writes to `db/migrate` without warning, and the migration runs against your primary database. A new Rails app only defines `queue` under `production`, so generate the migration for that environment instead, and let `bin/rails db:prepare` run it when you deploy:

```bash
RAILS_ENV=production SECRET_KEY_BASE_DUMMY=1 bin/rails generate solid_queue:coalesce:install
```

The migration should land in `db/queue_migrate`. `db/queue_schema.rb` doesn't pick up the new tables from a production migration. That's fine, because `db:prepare` loads that schema into an empty database and then runs the migration.

Until the migration has run, everything else works as before, and enqueuing a job that uses `coalesce` raises `SolidQueue::CoalesceKey::PendingMigrations`.

The gem requires Solid Queue 1.7. It relies on private Solid Queue methods, so its gemspec allows one minor version at a time.

To upgrade from an earlier version, follow [UPGRADING.md](UPGRADING.md). [CHANGELOG.md](CHANGELOG.md) lists what changed in each version.

## Usage

```ruby
class ReindexProductJob < ApplicationJob
  coalesce key: ->(product_id) { product_id }

  def perform(product_id)
    Product.find(product_id).reindex!
  end
end
```

The default is `on_conflict: :discard`. A call that finds a pending job for its key is suppressed, and the pending job keeps its own queue, priority and arguments. It keeps its schedule too, unless the class uses [`debounce:`](#debouncing). The key is free again the moment that job starts, so a call that arrives during the run creates the next pending job.

The options are:

```ruby
coalesce key:, on_conflict: :discard, max_calls: nil, on_full: nil, debounce: nil, max_wait: nil
```

- `key` is required. See [Keys](#keys).
- `on_conflict` is `:discard` or `:collect`. Anything else raises `ArgumentError`.
- `max_calls` is only allowed with `:collect`. It's a positive integer, 100 by default.
- `on_full` is only allowed with `:collect`. It's `:wait`, the default, or `:dispatch`. See [Collecting jobs and scheduling](#collecting-jobs-and-scheduling).
- `debounce` is a positive duration. Each call pushes a pending job back to that long from now. See [Debouncing](#debouncing).
- `max_wait` is a duration greater than `debounce`, and needs `debounce`. It's optional. See [Debouncing](#debouncing).

An invalid option raises `ArgumentError` when the class loads.

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

With `on_conflict: :collect`, no call is suppressed. Every accepted call is stored, including the first, and belongs to one job from the moment it's enqueued. That job runs `perform_calls` with its calls:

```ruby
class BulkReindexProductsJob < ApplicationJob
  coalesce key: ->(account_id, *) { account_id },
           on_conflict: :collect,
           max_calls: 100

  def perform_calls(calls)
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

Pass IDs, and do one bounded load and one bulk operation per run. That's the reason to collect in the first place. `SearchIndex.bulk_index` stands in for your own code.

A collecting job always runs `perform_calls`, and ignores any `perform` it has. If `perform_calls` is missing, the run raises `NotImplementedError` and the job fails with its calls.

`calls` is an array of `ActiveJob::Coalescing::Call`, oldest first, with three readers:

```ruby
call.id        # The row's ID, nil for perform_now and other adapters
call.arguments # The deserialized positional array
call.job_data  # The call's whole serialized Active Job payload
```

`arguments` comes from the same deserialization as a normal run. Each call is rebuilt on its own instance of the job, from its payload, through `deserialize` and `deserialize_arguments`. So GlobalID, custom serializers and your own overrides of those methods all apply. Keyword arguments are a trailing hash with Ruby's keyword marker, so `some_method(*call.arguments)` passes them as keywords. A trailing hash that was passed positionally stays a plain hash, and stays positional.

If a call refers to a deleted record, that call is skipped for this attempt. Any other error while deserializing, including one from your own override, fails the job with its calls. See [Deleted records](#deleted-records).

`job_data` is the raw snapshot taken when the call was enqueued: `job_id`, `queue_name`, `priority`, `locale`, `timezone`, `enqueued_at`, `scheduled_at`, and whatever your `serialize` adds. It's the payload as stored, so it doesn't go through your `deserialize`, and arguments that `serialize` wrapped are still wrapped. It's frozen at the top level.

Those fields describe the request, not this run. `perform_calls` runs once, in the context of the job that owns the calls, which is its first call's. Nothing restores another call's locale, timezone or custom fields for you, so read them from `job_data` if they matter:

```ruby
def perform_calls(calls)
  calls.group_by { |call| call.job_data["locale"] }.each do |locale, calls_in_locale|
    I18n.with_locale(locale) { notify(calls_in_locale) }
  end
end
```

`coalesced_calls` holds the same array, and perform callbacks can read it. `arguments` on the job itself keeps holding the arguments of its first call, which is what the `limits_concurrency` key sees.

## Collecting jobs and scheduling

A key has at most one open job, and any number of sealed ones. A call joins the open job, or opens a new one if there isn't one. The call that brings a job to `max_calls` seals it, and the next call opens a new job. With `max_calls: 100`, 250 calls make 3 jobs: 2 sealed with 100 calls each, and 1 open with 50. An open job also closes when it starts, so a call that arrives during the run opens a new job.

Here's a key with `on_conflict: :collect` and `max_calls: 2`. A start only releases the key if that job still holds it:

```text
event              key held by   job 1     job 2     job 3
call A             job 1         open
call B, now full   nobody        sealed
call C             job 2         sealed    open
job 1 starts       job 2         running   open
job 2 starts       nobody        running   running
call D             job 3         running   running   open
```

By default, sealing doesn't dispatch anything. A full job still waits for its time, and it runs like any other job. `max_calls` is the most calls one run takes, not a limit on how many can wait. The one exception comes from lowering it: a job already over the new size gets sealed as it is, and its run takes all its calls.

With `on_full: :dispatch`, a job that fills doesn't wait for the rest of its delay:

```ruby
class BulkIndexJob < ApplicationJob
  coalesce key: ->(account_id, *) { account_id },
           on_conflict: :collect,
           max_calls: 100,
           on_full: :dispatch

  def perform_calls(calls)
    # ...
  end
end

BulkIndexJob.set(wait: 30.seconds).perform_later(account_id, document_id)
```

A job that fills in 5 seconds becomes ready as the call that filled it commits, and one that doesn't fill runs at 30 seconds. The job's `scheduled_at` moves to that moment. Workers still have to be free, and if `limits_concurrency` has no room the job blocks until it does. This only matters for a delayed job, because any other job isn't waiting for its time.

Treat it as best effort. A full job keeps its original time if a discard is holding its scheduled execution when it fills, and then rolls back. On MySQL it also keeps it if the call runs in a transaction whose snapshot is older than the job. A job sealed without a new call, after lowering `max_calls`, keeps its time too.

`:dispatch` adds a lock on the scheduled execution to the call that fills a job. With `use_skip_locked` off, that call can deadlock with a discard of the same job. The call retries unless it's inside your own transaction, and a discard raises the error to its caller.

Each job takes its schedule, queue, priority, concurrency key and own arguments from its first call. Calls that join don't move it, even if they asked for a later time. So any caller that might arrive first needs the right `wait`: a plain `perform_later` opens a job that runs right away. [`debounce:`](#debouncing) changes both rules.

A key's jobs are separate, and this has consequences:

- They can start out of order. A later job with no wait can overtake an earlier delayed one.
- They can overlap. Use `limits_concurrency` if they shouldn't.
- Their concurrency keys, queues and priorities can differ, because each comes from that job's first call.

Enqueues for the same key take turns, because each one holds an exclusive lock on the key's row until its transaction commits. Inside your own transaction, that lock lasts until your commit, so a key used in a 50ms transaction takes about 20 calls per second, however many connections you add. Keep those transactions short, or enqueue after them.

To raise throughput on one key, use `ActiveJob.perform_all_later`. It stores up to a full job's calls under one lock. It keeps the order of each key's calls, and uses one transaction per job. [Bulk enqueues](#bulk-enqueues) covers what happens if it fails partway.

## Debouncing

By default a job takes its time from its first call, so during a burst it can run partway through and need another run straight after. With `debounce:`, each call pushes the pending job back, so it runs once calls have stopped:

```ruby
class ReindexProductJob < ApplicationJob
  coalesce key: ->(product_id) { product_id }, debounce: 30.seconds, max_wait: 5.minutes

  def perform(product_id)
    Product.find(product_id).reindex!
  end
end

ReindexProductJob.perform_later(product.id)
```

A job comes due 30 seconds after the last call, and no later than 5 minutes after it opened. It works with `on_conflict: :discard` and `:collect`, and `coalesce_status` reports the same outcomes as before. It adds no table, column, timer or process: the job's `scheduled_at` holds the time, and the dispatcher fires it as usual.

A call's time comes from these rules, in this order:

1. `.set(wait:)` or `.set(wait_until:)` replaces the window for that call.
2. Otherwise, the call's time is now plus `debounce`. On a debounced class, a plain `perform_later` waits for the window instead of running right away.
3. `max_wait` caps both. A call that creates a job can't ask for more than `max_wait` from now, and a call that joins can't move the job past `max_wait` from when it opened. A longer `.set(wait:)` gets capped without raising.

`perform_all_later` follows the same rules. One admission moves a job once, to the latest time among its calls.

A job only moves later. A call with a shorter wait joins without pulling the job forward, so `.set(wait: 0.seconds)` on a job that's already waiting doesn't run it sooner. The same call opening a new job makes it ready straight away, and nothing moves it after that.

Only a scheduled job moves. A job that's ready, blocked or claimed stays as it is, and the call joins as usual. A concurrency-limited job takes its semaphore when it comes due, so a job that's sliding doesn't hold up others.

`max_wait` only sets when a job comes due. It doesn't promise when the job runs: workers have to be free, and `limits_concurrency` can block the job. It's optional, but without it a key that keeps getting calls can delay its job indefinitely.

The clock starts when a job opens, and each open job has its own. A new batch after one fills gets a full `max_wait`, and so does a new job after one starts.

With `:collect`, a job can run when it's quiet, when it's full, or at the cap, whichever comes first:

- With `on_full: :dispatch`, the call that fills a job dispatches it. It doesn't move the job.
- With `on_full: :wait`, the call that fills a job moves it, and sealing it keeps that time. Nothing moves a sealed job.

Moving is best effort. A call skips a job whose scheduled execution is held by the dispatcher or a discard, so that job can run up to a window early. That's safe, because the key is held until the job starts, so the job still covers the call. A move also has to be more than 1 second later than the job's current time. That saves a hot key from writing on every call, and the dispatcher only polls every second by default.

A retry doesn't coalesce, so `debounce` and `max_wait` don't apply to it.

## Scheduling timelines

Three calls with the same key, A, B and C, arrive at 0, 10 and 20 seconds. `run` marks when the job comes due, and workers still have to be free. With `:discard`, one job takes the calls and runs with A's arguments. With `:collect`, its run gets the calls drawn on its line.

A fixed wait, with `.set(wait: 30.seconds)`. The first call sets the time:

```text
seconds   0    10   20   30   40   50
calls     A    B    C
job 1     A----B----C----run
```

A batch at N calls or T seconds, with `max_calls: 2`, `on_full: :dispatch` and `.set(wait: 30.seconds)`. B fills job 1, and C opens job 2 with its own wait:

```text
seconds   0    10   20   30   40   50
calls     A    B    C
job 1     A----run
job 2               C--------------run
```

A debounce, with `debounce: 30.seconds`. Each call pushes the job back:

```text
seconds   0    10   20   30   40   50
calls     A    B    C
job 1     A----B----C--------------run
```

A debounce with a limit, with `debounce: 30.seconds` and `max_wait: 40.seconds`. C asks for 50 seconds, and the cap holds it to 40:

```text
seconds   0    10   20   30   40   50
calls     A    B    C
job 1     A----B----C---------run
```

## Enqueue outcomes

`coalesce_status` on the Active Job instance says what happened to the call. It's read-only.

- `:created` means it created a pending job. With `:collect`, that's the first call of each job.
- `:discarded` means a pending job already held the key, and the call was suppressed.
- `:collected` means it was stored for the pending job.
- `nil` means this instance has no recorded admission.

So a call's data was stored if the status is `:created` or `:collected`, and it wasn't if it's `:discarded`:

```ruby
# on_conflict: :collect
call = BulkReindexProductsJob.perform_later(account_id, product_id)
call.coalesce_status # :created or :collected. Either way, perform_calls gets this call

# on_conflict: :discard, the default
call = ReindexProductJob.perform_later(product_id)
call.coalesce_status # :created, so a job runs with these arguments
                     # :discarded, so the pending job runs with its own, and these are gone
```

`nil` isn't a failure on its own. It's also what you read before an enqueue, on a class that doesn't coalesce, and on a worker's instance, including inside `perform` and `perform_calls` and on retries, because the status isn't serialized. The table has the rest:

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

`after_enqueue` can read `coalesce_status` for a single enqueue, including `:collected`. `ActiveJob.perform_all_later` skips enqueue callbacks, as it always does. It sets `coalesce_status` and `provider_job_id` on each job after that call's admission commits, and subscribers to [`coalesce.solid_queue`](#notifications) see every outcome.

## Bulk enqueues

Native Solid Queue stores a whole `ActiveJob.perform_all_later` in one transaction, so it's all-or-nothing. With coalesced jobs in the list it isn't, unless you ask for it. Outside your own transaction, it stores ordinary jobs first, in one native transaction. Coalesced calls follow, sorted by key, with one transaction per discard admission or collecting job. A key's lock lasts until its transaction commits, and one transaction for the whole call would hold every key's lock until the last job was in. This way it holds one key's lock for one job's calls.

Before it writes anything, the call works out the keys of all coalesced calls, the payloads of all `:collect` calls, and the payload of each key's first `:discard` call. If a key Proc raises or an argument can't be serialized, the call raises with nothing written. A repeated `:discard` call is never stored, so an argument on it that can't be serialized stays silently discarded, as with a single `perform_later`.

After that, only a database error or a `coalesce.solid_queue` subscriber that raises can fail the call. There's one exception. A job's concurrency key comes from its first call, and which call that is depends on the database, so a concurrency key Proc can raise on a later job's first call. In every case, earlier work stays committed and has its outcomes, and later calls have no outcome.

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

The call now waits for your commit, and a rollback drops it. It also holds no lock on the key during your transaction, so it avoids the limit in [Collecting jobs and scheduling](#collecting-jobs-and-scheduling).

- On Rails 7.2, set it to `:always` instead of `true`. Rails 7.1 doesn't have this setting.
- `ActiveJob.perform_all_later` only waits for your commit from Rails 8.1.2. Before that, call it after your transaction.
- `coalesce_status` and `provider_job_id` are `nil` until the commit. From Rails 8.1, `after_enqueue` runs after the commit and can read them. Before that it runs too early, so subscribe to `coalesce.solid_queue` instead.
- There's still a gap between your commit and the enqueue. A process that dies there loses the call. With `:discard`, the next call for that key covers it, if there is one. With `:collect`, that call's data is gone. If it has to survive, store it in your app's database in the same transaction, and process it from there.

If Solid Queue shares your app's database, a `perform_later` inside your transaction commits or rolls back with it, so the job can't start before your commit, and none of this is needed.

## Retries

A retry is the same work continuing, not a new candidate for coalescing. It can't be suppressed by a pending job, and later calls can't join it. A job keeps its calls through `retry_on`, `rescue_from` with `retry_job`, an `ActiveJob::Continuable` resume, a manual retry of a failed job, and a re-run after its worker was released. If a retry can't be enqueued, the job fails and keeps its calls.

Delivery is at-least-once. There's no per-call acknowledgement or per-call continuation, so a retry runs all of the job's calls again and `perform_calls` has to be idempotent.

## Deleted records

Each call is deserialized on its own. If a call refers to a deleted record, that call is skipped for this attempt and `drop_coalesced_call.solid_queue` is emitted, while the others run. If that happens to every call in the job, `perform_calls` and its callbacks are skipped and the job finishes normally. The job never falls back to its own arguments. Any other deserialization error fails the job with all of its calls, before any skipped-call events are emitted.

Every call row stays until its job is destroyed, including calls skipped because their records are gone. Retries and re-runs after a crash deserialize them all again. A record that's still gone is skipped and reported again, so these events count attempts, not unique calls. If the record becomes resolvable again, its call can run on a later attempt. With `preserve_finished_jobs`, skipped payloads stay with the finished job until it's cleared. See [Discarding and retention](#discarding-and-retention).

## Recurring jobs and concurrency limits

A recurring task can use `:discard`. A tick that finds a pending job is suppressed and not recorded in `solid_queue_recurring_executions`. A recurring task can't use `:collect` or `debounce:`: its configuration is invalid. A tick passes no time, so it would get the window, and an interval shorter than the window could stop the job from ever running.

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

Here's how the `:block` version takes calls. `.` is a blocked job and `=` is a running one:

```text
job 1   A ====run====|
job 2        B .. C .|====run====|
job 3                     D .....|====run====|
```

A runs right away. B arrives during that run and opens job 2, which blocks on the limit and holds the key, so C gets discarded into it. Job 2 starts when job 1 finishes, and that frees the key, so D opens job 3.

These behave differently. With `:block`, one tick during a run creates a blocked job, later ticks are coalesced because that job hasn't started, and it runs when the current one finishes. With concurrency `:discard` alone, busy ticks disappear and nothing catches up afterwards.

`:global` is a literal in both APIs, and coalescing and concurrency keep separate database records. A semaphore's `duration` can expire, so a concurrency limit isn't an absolute guarantee against overlap.

The concurrency key of a coalesced job is the one computed for the call that created it, and a blocked job still holds its coalesce key, since it hasn't started. `:collect` works with `on_conflict: :block`. It can't be combined with `limits_concurrency` with `on_conflict: :discard`, in either order, because a conflict would destroy a job holding calls, so that raises `ArgumentError` when the class loads. `:discard` can, and the destroyed job frees the key.

## Discarding and retention

Calls belong to their job, and are deleted with it. Discarding a pending job deletes it and its calls, and frees the key if the job was open. Other jobs for the key aren't affected. If `preserve_finished_jobs` is on, a job's calls stay until the job is cleared, along with the finished job itself.

A job that fails before starting, for example because its worker died or because its class no longer exists, keeps its calls as if it had started. When you bring the class back and retry the job, it runs those same calls.

## Changing a job's configuration

A job that gets a key records how it was created in its own payload, as `"coalesce_on_conflict": "collect"` or `"discard"`, so a worker knows even if the class has changed since. If you remove `:collect` or `coalesce` while calls are waiting, each of those jobs raises `ActiveJob::Coalescing::CollectedJobMismatch` when it runs. Nothing has run at that point, `retry_on` and `discard_on` don't see the error, and the job fails with all of its calls. Restore the declaration and retry the failed job to run them, or discard the job to delete them.

That's a way to recover, not a way to deploy. To remove `:collect` or `coalesce` without failures, stop enqueuing, wait for `SolidQueue::CoalescedCall.waiting` to be empty, and only then deploy the change. Workers on the old and new code can overlap in a rolling deploy, so a job can fail on one of them and need the same recovery.

| Change while calls wait | What happens |
| --- | --- |
| Remove `:collect` or `coalesce` | The job fails with its calls, as above |
| Switch `:collect` to `:discard` | The job fails with its calls, as above. The next call doesn't count on that job, and creates a new one |
| Switch `:discard` to `:collect` | The next call doesn't join the discarding job, and creates a new one. The old job runs one call from its own arguments |
| Change `limits_concurrency` to `on_conflict: :discard` after removing `:collect` | A conflict blocks the job instead of destroying it, so it reaches a worker and fails with its calls |
| Change the queue adapter | Waiting jobs still run their calls. A retry of one raises `CollectedJobMismatch`, and the job fails with its calls |
| Remove `perform_calls` | The run raises `NotImplementedError`, and the job fails with its calls, unless a handler such as `discard_on Exception` takes it |
| Rename or remove the class | The job fails with its calls |
| Change `max_calls` | Waiting calls aren't touched. An open job that's now over the new size gets sealed by the next call |
| Change `on_full` | The class that enqueues the call that fills a job decides, so there's nothing to drain. Jobs already sealed keep their time |
| Change `debounce` or `max_wait` | The class that enqueues each call decides, so there's nothing to drain. A job that has moved keeps its time |
| Change the key | Waiting jobs still run their calls. New calls open a job under the new key |

Solid Queue only deletes calls along with their job. Deleting call rows yourself isn't supported, and neither is changing `calls_count`. If you do, a collecting job with no calls left raises `CollectedJobMismatch` instead of running its own arguments, which are a copy of its first call. A job that lost only some of its calls runs the rest, and emits `coalesced_calls_mismatch.solid_queue` if it still had its key. A sealed job has no key to compare with, so a partial deletion from one goes unnoticed.

Jobs enqueued by version 0.1 don't have this record, so drain them before you upgrade. See [UPGRADING.md](UPGRADING.md).

## Rules and limitations

- A coalesced job can't be enqueued inside `SolidQueue::Batch.enqueue`, either alone or in bulk. A call that joins a pending job adds no job to the batch, which could finish early. Trying raises `SolidQueue::Job::CoalescedInBatchError`.
- Calling `perform_later` inside a transaction on the same database as Solid Queue doesn't abort the transaction when the call finds a pending job. On MySQL, however, a deadlock rolls back the entire transaction, so we can only retry it automatically outside of one. Inside your own transaction it raises `SolidQueue::Job::EnqueueError`. On any database with row locks, the same can happen inside your own transaction to a call that takes over a key from a collecting job, after its class switched to `:discard`.
- Solid Queue's bulk discards, such as `discard_all_in_batches`, are retried on a deadlock like enqueues are, so the same limit applies inside your own transaction.
- On MySQL, a deadlock while failing a job isn't retried, and it can stop the worker with the error Solid Queue raises for a job it can't finalize.
- Keys longer than 255 characters fail on MySQL.

The invariants behind these rules, and the costs they add, are in [the design notes](docs/design.md).

## Monitoring

Queue depth counts jobs, not calls, because a job can hold up to `max_calls` of them. To see the backlog, look at the calls instead:

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

### Notifications

Three notifications exist:

| Event | When | Payload |
| --- | --- | --- |
| `coalesce.solid_queue` | A call was admitted, including the one that created the job | `status` (`:created`, `:discarded`, `:collected`), `key`, `job_id`, `active_job_id`, `class_name` |
| `drop_coalesced_call.solid_queue` | A collected call was skipped on this attempt because its record is gone | `call_id`, `job_id`, `active_job_id`, `class_name`, `exception_class` |
| `coalesced_calls_mismatch.solid_queue` | A starting job has a different number of calls than its key counted | `job_id`, `active_job_id`, `class_name`, `expected`, `actual` |

`SolidQueue::LogSubscriber` logs the first at debug level and the others at warn level. None logs arguments or payloads, and none logs the key, which your app defines. The key is only in the event's payload.

`coalesce.solid_queue` fires right after a call's admission. Outside a transaction on the queue database, that's after the commit, and the outcome is final. Inside one, it's before the commit, so the outcome is provisional: a rollback can undo what it reported, and the event stays fired. No event fires for a retry, for a worker starting a job, or for a call that a `limits_concurrency` conflict destroyed.

To count only what committed, register the count on the queue database's transaction. It runs at the outermost commit, straight away if there's no transaction, and never after a rollback, including a rolled-back savepoint. A transaction on your app's own database doesn't affect it, because the queue's write doesn't depend on that one.

A job that [waits for your commit](#transactions) has no admission until then, so a rollback drops the call and no event fires.

```ruby
ActiveSupport::Notifications.subscribe("coalesce.solid_queue") do |*, payload|
  SolidQueue::Record.current_transaction.after_commit do
    Metrics.increment("solid_queue.coalesce", tags: payload.slice(:class_name, :status))
  end
end
```

`current_transaction` needs Rails 7.2. On Rails 7.1 this subscriber raises `NoMethodError`, which fails the enqueue, so count from the event directly there.

The payloads' IDs last for different lengths of time:

- `class_name` and `status` don't change, and have few values. They're the only ones to use as metric labels.
- `active_job_id` is the call's own on `coalesce.solid_queue`, and the owning job's on the other two events. The owning job has the `active_job_id` of its first call, across retries.
- `job_id` is the job at that moment. A retry moves collected calls to a job with a new ID.
- `call_id` exists only once a worker reads the calls, because admission inserts them in bulk. A `coalesce.solid_queue` event can't be matched to a later `call_id`.

### Metrics

Call counts come from Active Job's own event. `coalesced_calls` is already loaded by then, so reading it adds no query. It's `nil` for a job that doesn't collect, and empty when every record is gone. Each attempt reports its calls, so a retry counts again. A run whose count reaches `coalesce_max_calls` filled up. No event marks a job being sealed, and one sealed because a configuration changed looks like any smaller job.

```ruby
# Metrics stands in for whatever client you use. Keep job IDs and keys out of the
# labels, because there are too many of them, and because your app defines the keys.
ActiveSupport::Notifications.subscribe("coalesce.solid_queue") do |*, payload|
  Metrics.increment("solid_queue.coalesce", tags: payload.slice(:class_name, :status))
end

ActiveSupport::Notifications.subscribe("drop_coalesced_call.solid_queue") do |*, payload|
  Metrics.increment("solid_queue.coalesce.dropped", tags: payload.slice(:class_name))
end

ActiveSupport::Notifications.subscribe("coalesced_calls_mismatch.solid_queue") do |*, payload|
  Metrics.increment("solid_queue.coalesce.mismatched", tags: payload.slice(:class_name))
end

ActiveSupport::Notifications.subscribe("perform.active_job") do |*, payload|
  if calls = payload[:job].coalesced_calls
    Metrics.histogram("solid_queue.coalesce.calls", calls.size, tags: { class_name: payload[:job].class.name })
  end
end

# Once a minute, from one process
waiting = SolidQueue::CoalescedCall.waiting
Metrics.gauge("solid_queue.coalesce.waiting", waiting.count)
Metrics.gauge("solid_queue.coalesce.oldest_age", Time.current - (waiting.minimum(:created_at) || Time.current))
Metrics.gauge("solid_queue.coalesce.failed_jobs", SolidQueue::FailedExecution.where(job_id: SolidQueue::CoalescedCall.select(:job_id)).count)
Metrics.gauge("solid_queue.coalesce.failed_calls", SolidQueue::CoalescedCall.where(job_id: SolidQueue::FailedExecution.select(:job_id)).count)
```

The first subscriber counts provisional outcomes too. Use the one in [Notifications](#notifications) if your app enqueues inside queue-database transactions.

## Testing

Everything above needs the Solid Queue adapter, so other adapters ignore `coalesce`. They ignore `debounce` and `max_wait` too, so a plain call runs right away, and an explicit `.set(wait:)` still applies. With the test adapter, the inline adapter or `perform_now`, each call runs on its own, and a collecting job's `perform_calls` receives one call whose `id` is `nil`. `assert_enqueued_with(args: [ product_id ])` still works, and the Solid Queue tables aren't touched.

That call builds `job_data` when you read it, so `perform_now` with arguments that can't be serialized only raises if you read it.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
