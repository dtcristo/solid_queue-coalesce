# Design notes

These notes are for contributors, and for anyone reviewing the gem against Solid Queue. The [README](../README.md) describes the behaviour applications can rely on. Everything here can change between versions.

## Tables

The gem adds two tables and never alters Solid Queue's.

`solid_queue_coalesce_keys` has one row per open job. A row means that job exists, hasn't started and has room. `job_id` is null only between the insert and the end of the admission that inserted it, so other transactions never see it null. `calls_count` is how many calls the job owns: at least one if it's collecting, and zero if it's discarding. A sealed or started job has no row.

`solid_queue_coalesced_calls` has one row per collected call, holding its Active Job payload. A row points at the job that owns it. Solid Queue only deletes these rows along with their job.

Both tables have a foreign key to `solid_queue_jobs` with `on_delete: :cascade`.

## Admission

`SolidQueue::Job.enqueue` and `enqueue_all` are prepended in `lib/solid_queue/coalesce/patches.rb`. A call coalesces if its class has a key and `executions` is zero, so a retry never does.

A discarding call inserts the key if nobody holds it, skipping a conflict, and locks the row with a share lock. If `job_id` is null, it creates the job and sets `job_id`. Otherwise it takes the job it found. Several discarding calls can hold the share lock together, and a job start waits for them.

A collecting call takes an exclusive lock, because it goes on to change the row. It inserts its calls and either adds to `calls_count` or, once the job is full, deletes the row. Deleting the row seals the job, and the next call opens a new one. Sealing changes nothing about the job itself.

A job that gets a key has a `coalesce_on_conflict` marker in its payload. The marker outlives the class's declaration, so a worker can tell how a job was created after a deploy changes the class.

## Release

The key is released when a job starts, fails, is destroyed, or finishes while `preserve_finished_jobs` is on. A row always means a live job, because a key left on a finished job would accept calls that never run.

`start_coalesced_run` commits the release before it reads the calls, in a statement of its own. The read that finds the key sets the snapshot of a MySQL `REPEATABLE READ` transaction, so a read in the same transaction could miss calls committed while it waited for the lock. Only the first start finds a key, so only that start can compare `calls_count` with the rows it read, and emit `coalesced_calls_mismatch.solid_queue`.

## Lock order

Admission locks the key, then the job and its semaphore. Everything else has to take the key first:

- `before_destroy :release_coalesce_key` is prepended, so it runs before anything else. The foreign key cascade alone would lock in the opposite order.
- `failed_with` and `finished!` release the key before calling `super`.
- Bulk discards call `CoalesceKey.release_all` before `super`, in key order, so that two of them can't deadlock with each other.
- `lock_for_release` finds the row's key and then locks through the key's index. Locking through the `job_id` index takes InnoDB's locks on the row's indexes in the opposite order to admission.
- Bulk enqueues sort their calls by key, and take one transaction per discard admission or collecting batch, so one call holds one key's lock at a time.

## Deadlock retries

`retrying_deadlocks` runs its block again, up to 5 times, on `ActiveRecord::Deadlocked`. It wraps admission, the release in `start_coalesced_run`, and bulk discards. Most of these deadlocks come from InnoDB: admissions waiting on a key that doesn't exist yet hold gap locks that block each other, and creating a job can do the same on a new semaphore.

Inside a bigger transaction it raises straight away, because InnoDB has already rolled all of that transaction back and only the caller can run it again.

Sealing a key that belongs to a collecting job runs after the transaction that found it. Every call waiting on the key holds the same share lock, and upgrading it would deadlock with all of them. Inside the caller's own transaction it can't run after, so that deadlock can reach the caller.

`failed_with` has no retry. A deadlock there can stop the worker, with the error Solid Queue raises for a job it can't finalize.

## Call ownership

A call belongs to one job from admission until that job is destroyed.

A retry of a collecting job creates a new job with the same `active_job_id`, and `continuing_coalesced_run` moves the calls to it in one transaction. If the new job isn't persisted, the retry raises and the old job fails with its calls.

`perform_now` calls `start_coalesced_run` first, outside the `rescue_from` of Active Job's own `perform_now`, so `retry_on` can't swallow what it raises. The error reaches `ClaimedExecution#execute`, which turns it into a failed execution with its calls.

A collecting job never falls back to its own arguments, which are a copy of its first call's. With no calls it raises `CollectedJobMismatch`. A call whose record is gone is deleted if the batch has other calls to run. If every call's record is gone, the rows stay until the job is destroyed, so a re-run after a crash can see that it has nothing to do.

## Cost for other jobs

A job that isn't coalesced doesn't read the keys table when it finishes, fails or gets destroyed. It only parses its own payload to find that out. A job with the marker, or whose class coalesces, does one indexed read each time, even if it has no key left. Solid Queue's bulk discards do one read per batch, whatever the jobs are. [The benchmarks](../test/benchmarks/README.md) have the numbers.
