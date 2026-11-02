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

With `on_full: :dispatch`, the call that seals a job also dispatches it, in the same transaction, so workers can't see the job before its last call. It locks the scheduled execution, moves `scheduled_at` on the job to now, and calls `ScheduledExecution.dispatch_jobs`, the path the dispatcher uses. A job with no scheduled execution is left as it is, because it's ready, blocked or claimed already. Moving `scheduled_at` matters because `FailedExecution#retry` checks `due?`, and would send the job back to its old time. A job sealed without taking a call doesn't dispatch.

With `debounce:`, a call's time is worked out where admission sets `scheduled_at`, for single and bulk enqueues: its own time if the adapter's `enqueue_at` passed one, and otherwise now plus the window, capped at `max_wait` from now. Calls that join then move the job once, to the latest of their times, capped at the key's `created_at` plus `max_wait`. A fill with `on_full: :dispatch` dispatches instead. The move changes `scheduled_at` on the job and on its scheduled execution, for the same reason as a dispatch. The app sets the key's `created_at` on insert, because MySQL's `CURRENT_TIMESTAMP` uses the server's time zone, and that clock has to match the app's `scheduled_at`.

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
- Bulk enqueues sort their calls by key, and take one transaction per discard admission or collecting job, so one call holds one key's lock at a time.

`on_full: :dispatch` and moving a debounced job are the exceptions. They lock a scheduled execution while holding the key, and a discard holds that execution while it waits for the key. So they use `non_blocking_lock`, and skip a row that's held: the holder is either the dispatcher, which is dispatching a job that's due, or a discard. It's why both are best effort. A discard that rolls back leaves the job at its old time. A lost move can only make a job run early, and the job still covers the call, because the key is held until it starts. `:discard` keeps its share lock when it moves a job, because a move doesn't change the key row. Calls sharing that lock skip each other's held row too. A move only matches an execution more than a second before the new time, so it's later-only under the lock, and hot keys skip the write. They check that the row exists first, because on MySQL a locking read of a missing row takes a gap lock, and a stale snapshot can only leave the job at its old time too.

With `use_skip_locked` off, `non_blocking_lock` waits, and a fill or a move and a discard of the same job can deadlock. Admission retries unless it's nested, and a single discard has no retry. That takes a non-default setting and a discard racing a call, so we accept it.

A `Job#discard` that loaded the scheduled execution before the fill dispatched the job raises `ActiveRecord::RecordNotFound` when it locks the row. The same happens when the dispatcher gets there first.

## Deadlock retries

`retrying_deadlocks` runs its block again, up to 5 times, on `ActiveRecord::Deadlocked`. It wraps admission, the release in `start_coalesced_run`, and bulk discards. Most of these deadlocks come from InnoDB: admissions waiting on a key that doesn't exist yet hold gap locks that block each other, and creating a job can do the same on a new semaphore.

Inside a bigger transaction it raises straight away, because InnoDB has already rolled all of that transaction back and only the caller can run it again.

Sealing a key that belongs to a collecting job runs after the transaction that found it. Every call waiting on the key holds the same share lock, and upgrading it would deadlock with all of them. Inside the caller's own transaction it can't run after, so that deadlock can reach the caller.

`failed_with` has no retry. A deadlock there can stop the worker, with the error Solid Queue raises for a job it can't finalize.

## Call ownership

A call belongs to one job from admission until that job is destroyed.

A retry of a collecting job creates a new job with the same `active_job_id`, and `continuing_coalesced_run` moves the calls to it in one transaction. If the new job isn't persisted, the retry raises and the old job fails with its calls.

`perform_now` calls `start_coalesced_run` first, outside the `rescue_from` of Active Job's own `perform_now`, so `retry_on` can't swallow what it raises. The error reaches `ClaimedExecution#execute`, which turns it into a failed execution with its calls.

A collecting job never falls back to its own arguments, which are a copy of its first call's. With no calls it raises `CollectedJobMismatch`. A call whose record is gone is skipped for this attempt, and every row stays until its job is destroyed. Reading the calls never deletes them. Retries and re-runs deserialize all the rows again, so still-missing records are skipped and reported again, and a restored record's call can run. If every record is gone, the job skips `perform_calls` and its callbacks. Skipped-call notifications are emitted only after every call has been deserialized without another error.

## Cost for other jobs

A job that isn't coalesced doesn't read the keys table when it finishes, fails or gets destroyed. It only parses its own payload to find that out. A job with the marker, or whose class coalesces, does one indexed read each time, even if it has no key left. Solid Queue's bulk discards do one read per pass, whatever the jobs are. [The benchmarks](../test/benchmarks/README.md) have the numbers.
