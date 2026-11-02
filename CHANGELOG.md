# Changelog

## 0.2.0 (Unreleased)

Adds debouncing with `debounce:` and `max_wait:`, and batches that run as soon as they fill with `on_full: :dispatch`. Collecting jobs now use `perform_calls(calls)`, so upgrading from 0.1.0 needs code changes, a drain and a migration. See [UPGRADING.md](https://github.com/dtcristo/solid_queue-coalesce/blob/main/UPGRADING.md).

### Breaking changes

- Calls skipped because their records are gone stay until their job is destroyed and are reconsidered on each attempt. Skip notifications can repeat on retries, and restored records can run on a later attempt. ([#30](https://github.com/dtcristo/solid_queue-coalesce/pull/30))
- `coalesce` takes `on_conflict: :discard` or `:collect`, and `max_calls:`. These replace `collect:`, `limit:` and `flush_when_full:`. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3), [#27](https://github.com/dtcristo/solid_queue-coalesce/pull/27))
- Collecting jobs run `perform_calls(calls)`, which receives `ActiveJob::Coalescing::Call` objects. It replaces `perform(array)`. ([#27](https://github.com/dtcristo/solid_queue-coalesce/pull/27))
- Each call stores its whole Active Job payload, and belongs to one job from the moment it's enqueued. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3), [#4](https://github.com/dtcristo/solid_queue-coalesce/pull/4))
- Reaching `max_calls` seals a job, and by default doesn't dispatch it early. A sealed job waits for its time unless `on_full: :dispatch` is set, and each job takes its schedule from its first call. ([#27](https://github.com/dtcristo/solid_queue-coalesce/pull/27))
- `coalesce_status` replaces `coalesced?`. The `:dropped` outcome is now `:discarded`. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3), [#17](https://github.com/dtcristo/solid_queue-coalesce/pull/17))
- `action` in the `coalesce.solid_queue` payload is now `status`. ([#27](https://github.com/dtcristo/solid_queue-coalesce/pull/27))
- `drop_coalesced_argument.solid_queue` is now `drop_coalesced_call.solid_queue`, and its `argument_id` is now `call_id`. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3))
- `SolidQueue::CoalescedArgument` and its table are now `SolidQueue::CoalescedCall` and `solid_queue_coalesced_calls`. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3))
- The key column uses the database's default collation, like Solid Queue's semaphores. Under a case-insensitive collation, keys that differ only by case now coalesce together. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3))
- A retry runs all of the job's calls again. Calls can no longer be skipped with an `ActiveJob::Continuable` cursor over `perform`'s array. ([#27](https://github.com/dtcristo/solid_queue-coalesce/pull/27))

### Added

- `on_full: :dispatch` for `coalesce`, which dispatches a delayed collecting job as soon as it fills. ([#21](https://github.com/dtcristo/solid_queue-coalesce/pull/21))
- `debounce:` and `max_wait:` for `coalesce`. Each call pushes a pending job back by the window, and `max_wait` caps how far. A recurring task can't use a debounced class. ([#28](https://github.com/dtcristo/solid_queue-coalesce/pull/28))
- `class_name` in the `coalesce.solid_queue` and `drop_coalesced_call.solid_queue` payloads. ([#19](https://github.com/dtcristo/solid_queue-coalesce/pull/19))
- `ActiveJob::Coalescing::CollectedJobMismatch`. A collecting job whose configuration changed fails with its calls, and you can recover by restoring the declaration and retrying it. ([#15](https://github.com/dtcristo/solid_queue-coalesce/pull/15))

### Changed

- Keys are released when a job starts, fails or gets destroyed, so a dispatcher no longer has to release stranded keys. ([#4](https://github.com/dtcristo/solid_queue-coalesce/pull/4))
- `ActiveJob.perform_all_later` works out keys and payloads before it writes anything, and stores one transaction per discard admission or collecting job. Wrap it in `SolidQueue::Record.transaction` for all-or-nothing. ([#16](https://github.com/dtcristo/solid_queue-coalesce/pull/16))
- Each collected call is rebuilt through the job's own `deserialize` and `deserialize_arguments`. ([#17](https://github.com/dtcristo/solid_queue-coalesce/pull/17))
- `SolidQueue::CoalescedCall.waiting` is worked out from the job that owns each call: it hasn't finished, isn't running and hasn't failed. ([#4](https://github.com/dtcristo/solid_queue-coalesce/pull/4), [#19](https://github.com/dtcristo/solid_queue-coalesce/pull/19))
- Jobs that aren't coalesced no longer read the keys table when they finish, fail or get destroyed. ([#18](https://github.com/dtcristo/solid_queue-coalesce/pull/18))
- The README starts with getting started, then recipes for deduping, debouncing and batching, with diagrams for timing, guidance on `coalesce` and `limits_concurrency`, and links to Solid Queue's docs. ([#29](https://github.com/dtcristo/solid_queue-coalesce/pull/29), [#31](https://github.com/dtcristo/solid_queue-coalesce/pull/31))
- The public API has RDoc comments, and the gem links to its [rubydoc.info](https://www.rubydoc.info/gems/solid_queue-coalesce) documentation. ([#31](https://github.com/dtcristo/solid_queue-coalesce/pull/31))

### Fixed

- Solid Queue's bulk discards release keys first and retry deadlocks, so they no longer lock in the opposite order to admission. ([#5](https://github.com/dtcristo/solid_queue-coalesce/pull/5))

## 0.1.0 (2026-10-30)

The first release lets many `perform_later` calls with the same key share one pending Solid Queue job. Install it with the generator, and there's nothing to upgrade from.

- First release, with `coalesce key:`, `collect:`, `limit:` and `flush_when_full:`. ([#1](https://github.com/dtcristo/solid_queue-coalesce/pull/1))
- Calls that share a key share one pending job, and the key is free again when that job starts. ([#1](https://github.com/dtcristo/solid_queue-coalesce/pull/1))
- The `solid_queue:coalesce:install` generator. ([#1](https://github.com/dtcristo/solid_queue-coalesce/pull/1))
- The `coalesce.solid_queue` and `drop_coalesced_argument.solid_queue` notifications. ([#1](https://github.com/dtcristo/solid_queue-coalesce/pull/1))
- Requires Solid Queue 1.7. ([#1](https://github.com/dtcristo/solid_queue-coalesce/pull/1))
