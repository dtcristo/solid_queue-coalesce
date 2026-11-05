# Upgrading from version 0.2.0 to 0.3.0

The serialized job marker is now `solid_queue_coalesce_on_conflict`, and the old `coalesce_on_conflict` name is no longer read. Drain all coalesced work on the old version before deploying. No database migration or payload conversion is provided.

Batched coalescing keys also start with `Batch/<id>/` instead of `SolidQueue::Batch/<id>/`. New calls won't join pending jobs under the old keys and can create separate jobs for the same work, so affected batches must finish too.

1. Pause every producer of coalesced work, including application requests, recurring tasks and batch producers. Keep the old workers and dispatcher running to drain the queue.
2. Wait for all coalesced jobs to finish, including scheduled, ready, blocked, running and retrying jobs. Retry or discard failed jobs on the old version. Check every class that used `coalesce`, including ones whose declarations have since changed, with the query below.
3. Wait for affected batches to finish. To conservatively check all batches, wait until `SolidQueue::Batch.where(finished_at: nil).count` is zero.
4. Stop the old application and Solid Queue processes, deploy the new version to all producers, workers, dispatchers and schedulers together, then resume producers. Don't run both versions against coalesced work or affected batches.

For example, replace these names with all your coalesced job classes and wait until the result is zero. Failed jobs are included, so retry or discard them before continuing:

```ruby
SolidQueue::Job.where(finished_at: nil, class_name: %w[ ReindexDocumentsJob SendDigestJob ]).count
```

Retrying an old failed job after deployment doesn't rewrite its payload and isn't a supported upgrade path. Preserved finished jobs need no drain, but replaying their old payloads after upgrading isn't supported either.

Update code that depends on these errors or key declarations:

- `ActiveJob.perform_all_later` now raises the original Active Record database error for coalesced calls, including `ActiveRecord::ValueTooLong` and `ActiveRecord::Deadlocked`. Update bulk error handlers that rescue `SolidQueue::Job::EnqueueError`. Single enqueues still wrap database errors in `SolidQueue::Job::EnqueueError`, and subscriber errors remain raw.
- `coalesce(key:)` no longer rejects unsupported or blank declarations. Unsupported truthy types, such as `key: 123`, give a class-wide key. Blank strings and empty arrays or hashes still coalesce. Only `nil` and `false` disable coalescing. Check declarations that previously relied on `ArgumentError`; all other option validations remain.

Update notification subscribers, log parsers or other consumers that match the old batch key prefix. `coalesce.solid_queue` now reports keys such as `Batch/7/ReindexProductJob/42`.

# Upgrading from version 0.1.0 to 0.2.0

This version replaces the gem's tables and the options of `coalesce`. There's no in-place migration for waiting calls, so drain them first. The generator skips an app that already has its migration, so this upgrade needs a migration of your own.

Stop enqueuing jobs that use `coalesce`, and on 0.1.0 wait until none of those jobs are left. This query includes scheduled, blocked, running and failed jobs, so retry or discard the failed ones:

```ruby
SolidQueue::Job.where(finished_at: nil, class_name: %w[ ReindexDocumentsJob SendDigestJob ]).count
```

Stop your Solid Queue processes, add this migration to your queue database's migrations path, usually `db/queue_migrate`, and run it. It deletes the rows in both tables, including the calls of any finished jobs you've preserved:

```ruby
class ReplaceSolidQueueCoalesceTables < ActiveRecord::Migration[7.1]
  def up
    drop_table :solid_queue_coalesced_arguments
    drop_table :solid_queue_coalesce_keys

    create_table :solid_queue_coalesce_keys do |t|
      t.string :key, null: false
      t.bigint :job_id
      t.integer :calls_count, null: false, default: 0
      t.datetime :created_at, null: false

      t.index :key, unique: true
      t.index :job_id, unique: true
      t.foreign_key :solid_queue_jobs, column: :job_id, on_delete: :cascade
    end

    create_table :solid_queue_coalesced_calls do |t|
      t.bigint :job_id, null: false
      t.text :arguments, null: false
      t.datetime :created_at, null: false

      t.index :job_id
      t.foreign_key :solid_queue_jobs, column: :job_id, on_delete: :cascade
    end
  end

  def down
    raise ActiveRecord::IrreversibleMigration
  end
end
```

```bash
bin/rails db:migrate:queue
```

If you use a single database, put the migration in `db/migrate` and run `bin/rails db:migrate`.

Deploy these changes together with the migration:

| 0.1.0 | 0.2.0 |
| --- | --- |
| `collect: true` | `on_conflict: :collect` |
| `limit: 100` | `max_calls: 100` |
| `flush_when_full: true`, which was the default | `on_full: :dispatch` |
| `flush_when_full: false` | Remove it. A full job waits for its time by default |
| `perform(array)` on a collecting job | `perform_calls(calls)`, and read `call.arguments` |
| `coalesced?` | `coalesce_status`, which is `:discarded` or `:collected` for a call that joined a job |
| `:dropped` in `coalesce.solid_queue` | `:discarded` |
| `action` in `coalesce.solid_queue` | `status` |
| `drop_coalesced_argument.solid_queue` | `drop_coalesced_call.solid_queue` |
| `argument_id` in that event's payload | `call_id` |
| `SolidQueue::CoalescedArgument` | `SolidQueue::CoalescedCall` |

These behaviours change too:

- Calls that refer to deleted records are skipped for that attempt, and their rows stay until the job is destroyed. Retrying or re-running the job reports each still-missing call again; a restored record's call can run. Allow for these payloads in finished-job retention. To decide which records to process in your own code, enqueue IDs and look them up in `perform_calls` instead of passing records as GlobalIDs.
- `drop_coalesced_call.solid_queue` reports a skip on an attempt, with no deletion outcome. Count these events as skipped attempts, or deduplicate by `call_id` if you need unique calls.
- If you relied on reaching `limit` to dispatch a scheduled job early, jobs now wait for their time unless you set `on_full: :dispatch`.
- A retry runs all of the job's calls again, so make `perform_calls` idempotent. Cursors over `perform`'s array don't carry over.
- `SolidQueue::Job::CoalescedInBatchError` is gone. Remove any `rescue` of it, because Ruby would raise `NameError` when an exception reaches that `rescue`. Coalesced jobs that a `rescue` kept out of a batch now join it.
- The key column uses the database's default collation. If that's case-insensitive, keys that differ only by case now coalesce together. Use a case-sensitive collation in the migration above if they have to stay apart.

If you run pre-release code from `main`, remove `call_deleted` from notification subscribers, log parsers and metric labels. This field was added after 0.1.0 and removed before 0.2.0.
