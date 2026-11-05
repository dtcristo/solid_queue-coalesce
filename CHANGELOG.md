# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Adds `on_conflict: :replace`, which runs a pending job with its latest call's arguments. Aligns key declarations and bulk enqueue errors with upstream Solid Queue, and prefixes the serialized coalescing marker with `solid_queue_`. Batch coalescing keys also use the shorter `Batch/` prefix. Upgrading takes work: drain coalesced jobs and affected batches, retry or discard failed jobs on the old version, and deploy all processes together; see [UPGRADING.md](https://github.com/dtcristo/solid_queue-coalesce/blob/main/UPGRADING.md).

### Added

- `on_conflict: :replace` for `coalesce`. A call writes its arguments over the pending job's, which keeps its ID, queue, priority and place in line, and runs `perform` with the latest ones. It reports `:replaced`, works with `debounce:`, `max_wait:`, blocking concurrency limits and batches, and can't be used by a recurring task. ([#47](https://github.com/dtcristo/solid_queue-coalesce/pull/47))

### Changed

- **Breaking:** Serialized job data uses `solid_queue_coalesce_on_conflict` instead of `coalesce_on_conflict`, with no fallback. Drain all coalesced work, including retrying or discarding failures, before deploying. ([#46](https://github.com/dtcristo/solid_queue-coalesce/pull/46))
- **Breaking:** `coalesce(key:)` accepts declarations without type or blank validation, like `limits_concurrency`. Unsupported truthy types produce a class-wide key, blank declarations coalesce, and `nil` or `false` disables coalescing. Other options retain their validation. ([#46](https://github.com/dtcristo/solid_queue-coalesce/pull/46))
- **Breaking:** Bulk enqueues propagate original Active Record errors for coalesced jobs, like native bulk enqueue. Single enqueues retain `SolidQueue::Job::EnqueueError`; notification subscriber errors remain unwrapped. ([#46](https://github.com/dtcristo/solid_queue-coalesce/pull/46))
- **Breaking:** Batched coalescing keys start with `Batch/<id>/` instead of `SolidQueue::Batch/<id>/`, including keys in `coalesce.solid_queue` notifications. Drain affected batches before upgrading so new calls don't create separate jobs under the new keys. ([#43](https://github.com/dtcristo/solid_queue-coalesce/pull/43))


## [0.2.0] - 2026-11-02

Adds debouncing with `debounce:` and `max_wait:`, batches that run as soon as they fill with `on_full: :dispatch`, and support for [`SolidQueue::Batch`](https://github.com/rails/solid_queue/blob/v1.7.0/README.md#batch-jobs). Collecting jobs now use `perform_calls(calls)`, so upgrading from 0.1.0 needs code changes, a drain and a migration. See [UPGRADING.md](https://github.com/dtcristo/solid_queue-coalesce/blob/main/UPGRADING.md).

### Added

- Coalesced jobs can be part of a `SolidQueue::Batch`. Calls with the same key share one job within a batch, and that job belongs to that batch alone. ([#35](https://github.com/dtcristo/solid_queue-coalesce/pull/35))
- `on_full: :dispatch` for `coalesce`, which dispatches a delayed collecting job as soon as it fills. ([#21](https://github.com/dtcristo/solid_queue-coalesce/pull/21))
- `debounce:` and `max_wait:` for `coalesce`. Each call pushes a pending job back by the window, and `max_wait` caps how far. A recurring task can't use a debounced class. ([#28](https://github.com/dtcristo/solid_queue-coalesce/pull/28))
- `class_name` in the `coalesce.solid_queue` and `drop_coalesced_call.solid_queue` payloads. ([#19](https://github.com/dtcristo/solid_queue-coalesce/pull/19))
- `ActiveJob::Coalescing::CollectedJobMismatch`. A collecting job whose configuration changed fails with its calls, and you can recover by restoring the declaration and retrying it. ([#15](https://github.com/dtcristo/solid_queue-coalesce/pull/15))

### Changed

- **Breaking:** Calls skipped because their records are gone stay until their job is destroyed and are reconsidered on each attempt. Skip notifications can repeat on retries, and restored records can run on a later attempt. ([#30](https://github.com/dtcristo/solid_queue-coalesce/pull/30))
- **Breaking:** `coalesce` takes `on_conflict: :discard` or `:collect`, and `max_calls:`. These replace `collect:`, `limit:` and `flush_when_full:`. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3), [#27](https://github.com/dtcristo/solid_queue-coalesce/pull/27))
- **Breaking:** Collecting jobs run `perform_calls(calls)`, which receives `ActiveJob::Coalescing::Call` objects. It replaces `perform(array)`. ([#27](https://github.com/dtcristo/solid_queue-coalesce/pull/27))
- **Breaking:** Each call stores its whole Active Job payload, and belongs to one job from the moment it's enqueued. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3), [#4](https://github.com/dtcristo/solid_queue-coalesce/pull/4))
- **Breaking:** Reaching `max_calls` seals a job, and by default doesn't dispatch it early. A sealed job waits for its time unless `on_full: :dispatch` is set, and each job takes its schedule from its first call. ([#27](https://github.com/dtcristo/solid_queue-coalesce/pull/27))
- **Breaking:** `coalesce_status` replaces `coalesced?`. The `:dropped` outcome is now `:discarded`. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3), [#17](https://github.com/dtcristo/solid_queue-coalesce/pull/17))
- **Breaking:** `action` in the `coalesce.solid_queue` payload is now `status`. ([#27](https://github.com/dtcristo/solid_queue-coalesce/pull/27))
- **Breaking:** `drop_coalesced_argument.solid_queue` is now `drop_coalesced_call.solid_queue`, and its `argument_id` is now `call_id`. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3))
- **Breaking:** `SolidQueue::CoalescedArgument` and its table are now `SolidQueue::CoalescedCall` and `solid_queue_coalesced_calls`. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3))
- **Breaking:** The key column uses the database's default collation, like Solid Queue's semaphores. Under a case-insensitive collation, keys that differ only by case now coalesce together. ([#3](https://github.com/dtcristo/solid_queue-coalesce/pull/3))
- **Breaking:** A retry runs all of the job's calls again. Calls can no longer be skipped with an `ActiveJob::Continuable` cursor over `perform`'s array. ([#27](https://github.com/dtcristo/solid_queue-coalesce/pull/27))
- Keys are released when a job starts, fails or gets destroyed, so a dispatcher no longer has to release stranded keys. ([#4](https://github.com/dtcristo/solid_queue-coalesce/pull/4))
- `ActiveJob.perform_all_later` works out keys and payloads before it writes anything, and stores one transaction per discard admission or collecting job. Wrap it in `SolidQueue::Record.transaction` for all-or-nothing. ([#16](https://github.com/dtcristo/solid_queue-coalesce/pull/16))
- Each collected call is rebuilt through the job's own `deserialize` and `deserialize_arguments`. ([#17](https://github.com/dtcristo/solid_queue-coalesce/pull/17))
- `SolidQueue::CoalescedCall.waiting` is worked out from the job that owns each call: it hasn't finished, isn't running and hasn't failed. ([#4](https://github.com/dtcristo/solid_queue-coalesce/pull/4), [#19](https://github.com/dtcristo/solid_queue-coalesce/pull/19))
- Jobs that aren't coalesced no longer read the keys table when they finish, fail or get destroyed. ([#18](https://github.com/dtcristo/solid_queue-coalesce/pull/18))
- The README starts with getting started, then recipes for deduping, debouncing and batching, with diagrams for timing, guidance on `coalesce` and `limits_concurrency`, and links to Solid Queue's docs. ([#29](https://github.com/dtcristo/solid_queue-coalesce/pull/29), [#31](https://github.com/dtcristo/solid_queue-coalesce/pull/31))
- The public API has RDoc comments, and the gem links to its [rubydoc.info](https://www.rubydoc.info/gems/solid_queue-coalesce) documentation. ([#31](https://github.com/dtcristo/solid_queue-coalesce/pull/31))

### Removed

- **Breaking:** `SolidQueue::Job::CoalescedInBatchError` is gone, because coalesced jobs can now be part of a `SolidQueue::Batch`. ([#35](https://github.com/dtcristo/solid_queue-coalesce/pull/35))

### Fixed

- Solid Queue's bulk discards release keys first and retry deadlocks, so they no longer lock in the opposite order to admission. ([#5](https://github.com/dtcristo/solid_queue-coalesce/pull/5))

## [0.1.0] - 2026-10-30

The first release lets many `perform_later` calls with the same key share one pending Solid Queue job. Install it with the generator, and there's nothing to upgrade from.

### Added

- First release, with `coalesce key:`, `collect:`, `limit:` and `flush_when_full:`. ([#1](https://github.com/dtcristo/solid_queue-coalesce/pull/1))
- Calls that share a key share one pending job, and the key is free again when that job starts. ([#1](https://github.com/dtcristo/solid_queue-coalesce/pull/1))
- The `solid_queue:coalesce:install` generator. ([#1](https://github.com/dtcristo/solid_queue-coalesce/pull/1))
- The `coalesce.solid_queue` and `drop_coalesced_argument.solid_queue` notifications. ([#1](https://github.com/dtcristo/solid_queue-coalesce/pull/1))
- Requires Solid Queue 1.7. ([#1](https://github.com/dtcristo/solid_queue-coalesce/pull/1))

[Unreleased]: https://github.com/dtcristo/solid_queue-coalesce/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/dtcristo/solid_queue-coalesce/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dtcristo/solid_queue-coalesce/releases/tag/v0.1.0
