# Changelog

## 0.2.0 (Unreleased)

Upgrading from 0.1.0 needs a drain and a migration. See [UPGRADING.md](UPGRADING.md).

### Breaking changes

- Calls skipped because their records are gone stay until their job is destroyed and are reconsidered on each attempt. `call_deleted` is removed from `drop_coalesced_call.solid_queue`; the event reports a skip on each attempt, not a row deletion.
- `coalesce` takes `on_conflict: :discard` or `:collect`, and `max_calls:`. These replace `collect:`, `limit:` and `flush_when_full:`.
- Collecting jobs run `perform_calls(calls)`, which receives `ActiveJob::Coalescing::Call` objects. It replaces `perform(array)`.
- Each call stores its whole Active Job payload, and belongs to one job from the moment it's enqueued.
- Reaching `max_calls` seals a job, and by default doesn't dispatch it early. A sealed job waits for its time unless `on_full: :dispatch` is set, and each job takes its schedule from its first call.
- `coalesce_status` replaces `coalesced?`. The `:dropped` outcome is now `:discarded`.
- `action` in the `coalesce.solid_queue` payload is now `status`.
- `drop_coalesced_argument.solid_queue` is now `drop_coalesced_call.solid_queue`, and its `argument_id` is now `call_id`.
- `SolidQueue::CoalescedArgument` and its table are now `SolidQueue::CoalescedCall` and `solid_queue_coalesced_calls`.
- The key column uses the database's default collation, like Solid Queue's semaphores. Under a case-insensitive collation, keys that differ only by case now coalesce together.
- A retry runs all of the job's calls again. Calls can no longer be skipped with an `ActiveJob::Continuable` cursor over `perform`'s array.

### Added

- `on_full: :dispatch` for `coalesce`, which dispatches a delayed collecting job as soon as it fills.
- `debounce:` and `max_wait:` for `coalesce`. Each call pushes a pending job back by the window, and `max_wait` caps how far. A recurring task can't use a debounced class.
- The `coalesced_calls_mismatch.solid_queue` notification.
- `class_name` in the `coalesce.solid_queue` and `drop_coalesced_call.solid_queue` payloads.
- `ActiveJob::Coalescing::CollectedJobMismatch`. A collecting job whose configuration changed fails with its calls, and you can recover by restoring the declaration and retrying it.

### Changed

- Keys are released when a job starts, fails or gets destroyed, so a dispatcher no longer has to release stranded keys.
- `ActiveJob.perform_all_later` works out keys and payloads before it writes anything, and stores one transaction per discard admission or collecting job. Wrap it in `SolidQueue::Record.transaction` for all-or-nothing.
- Each collected call is rebuilt through the job's own `deserialize` and `deserialize_arguments`.
- `SolidQueue::CoalescedCall.waiting` is worked out from the job that owns each call: it hasn't finished, isn't running and hasn't failed.
- Jobs that aren't coalesced no longer read the keys table when they finish, fail or get destroyed.
- The README leads with recipes for deduping, debouncing and batching, and has diagrams for choosing a recipe and for timing.

### Fixed

- Solid Queue's bulk discards release keys first and retry deadlocks, so they no longer lock in the opposite order to admission.

## 0.1.0 (2026-10-30)

- First release, with `coalesce key:`, `collect:`, `limit:` and `flush_when_full:`.
- Calls that share a key share one pending job, and the key is free again when that job starts.
- The `solid_queue:coalesce:install` generator.
- The `coalesce.solid_queue` and `drop_coalesced_argument.solid_queue` notifications.
- Requires Solid Queue 1.7.
