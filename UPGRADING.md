# Upgrading to version 0.2.x
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
- Remove `call_deleted` from notification subscribers, log parsers and metric labels. `drop_coalesced_call.solid_queue` reports a skip on an attempt, with no deletion outcome. Count these events as skipped attempts, or deduplicate by `call_id` if you need unique calls.
- If you relied on reaching `limit` to dispatch a scheduled job early, jobs now wait for their time unless you set `on_full: :dispatch`.
- A retry runs all of the job's calls again, so make `perform_calls` idempotent. Cursors over `perform`'s array don't carry over.
- The key column uses the database's default collation. If that's case-insensitive, keys that differ only by case now coalesce together. Use a case-sensitive collation in the migration above if they have to stay apart.
