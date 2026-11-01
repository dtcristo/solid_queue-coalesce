# Benchmarks

Compares Solid Queue with this gem against unmodified Solid Queue. `bench.rb` isn't part of the gem: the gemspec only ships `app`, `lib`, the license and the README.

## Running

Start the databases as CI does, with more shared memory for PostgreSQL. With the default 64 MB in a container, `VACUUM ANALYZE` in the maintenance mode fails with `PG::DiskFull: could not resize shared memory segment`, because it asks for a 64 MB segment:

```sh
docker run -d --name bench-postgres --shm-size=1g -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1:55432:5432 postgres:15.1 -c max_connections=200
docker run -d --name bench-mysql -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p 127.0.0.1:33066:3306 mysql:8.0.31
```

Then extract the commit to compare with, and run. Every database uses a `bench` database or `tmp/bench.sqlite3`, and drops all of its tables at the start of each run:

```sh
git archive <base-commit> | tar -x -C /tmp/bench-base
export BENCH_BASE_PATH=/tmp/bench-base BENCH_JOBS=5000 BENCH_CALLS=4000

# 5 repetitions of every ordinary and coalesced workload, in a shuffled order
TARGET_DB=postgres bundle exec ruby test/benchmarks/bench.rb suite --reps 5 --out /tmp/bench
bundle exec ruby test/benchmarks/bench.rb summarize /tmp/bench/postgres.jsonl

# One process per measurement
BENCH_VARIANT=head TARGET_DB=postgres bundle exec ruby test/benchmarks/bench.rb statements
BENCH_VARIANT=head TARGET_DB=postgres bundle exec ruby test/benchmarks/bench.rb maintenance retained
BENCH_VARIANT=head TARGET_DB=postgres bundle exec ruby test/benchmarks/bench.rb maintenance backlog
```

`suite --only` takes a comma-separated list of modes or workloads.

## The runs behind the results

`bench.rb` ran from a clean extract of each commit, one database at a time, so they didn't compete for the host. `BENCH_BASE_PATH` pointed at an extract of `a5785d5`.

The main suite, at `cebb294`, with `BENCH_JOBS=5000 BENCH_CALLS=4000`:

```sh
for db in sqlite postgres mysql; do
  for variant in upstream upstream_tables base head; do
    BENCH_VARIANT=$variant TARGET_DB=$db bundle exec ruby test/benchmarks/bench.rb statements >> statements.jsonl
  done
  TARGET_DB=$db bundle exec ruby test/benchmarks/bench.rb suite --reps 5 --out .
done
```

Maintenance, 5 times for each shape, on SQLite at `cebb294`, and on PostgreSQL and MySQL at `de9b99a`:

```sh
BENCH_VARIANT=head TARGET_DB=$db bundle exec ruby test/benchmarks/bench.rb maintenance $shape >> maintenance.jsonl
```

The follow-ups at `de9b99a`, with the same settings. First `discard_hot_bulk`, 20 times for each of `base` and `head`, on MySQL and SQLite:

```sh
BENCH_VARIANT=$variant TARGET_DB=$db bundle exec ruby test/benchmarks/bench.rb coalesced discard_hot_bulk >> hot-bulk.jsonl
```

Then ordinary jobs on MySQL with relaxed durability, which this restores afterwards:

```sh
mysql -h127.0.0.1 -P33066 -uroot -e "SET GLOBAL innodb_flush_log_at_trx_commit=2; SET GLOBAL sync_binlog=0"
TARGET_DB=mysql bundle exec ruby test/benchmarks/bench.rb suite --reps 5 --only ordinary --out relaxed
mysql -h127.0.0.1 -P33066 -uroot -e "SET GLOBAL innodb_flush_log_at_trx_commit=1; SET GLOBAL sync_binlog=1"
```

Two earlier runs were thrown away. The first left code reloading on, so every job checked the watched files for changes, which looked like a 15% to 30% regression. The second failed on PostgreSQL with the default 64 MB `/dev/shm`, before writing any result.

## What's compared

- `upstream` is Solid Queue alone, without the coalescing tables.
- `upstream_tables` is Solid Queue alone, with the coalescing tables and their foreign keys.
- `base` is this gem at `a5785d5`, before the marker and the gate.
- `head` is this gem at the commit being measured.

Neither baseline loads any code from this gem. Every run starts from an empty schema in a new process, and each repetition goes through the variants in a shuffled order, so drift lands on all of them.

## Results

`head` is `cebb294` for the main suite, statements and SQLite's maintenance, and `de9b99a` for the follow-ups and maintenance on PostgreSQL and MySQL. The only differences between those are in `bench.rb`: the drain race, and the MySQL and PostgreSQL maintenance seeding described below.

Environment: Ruby 4.0.7, Rails 8.1.4, Solid Queue 1.7.0, SQLite 3.54.1, PostgreSQL 15.1, MySQL 8.0.31. Ruby ran on an Apple Silicon Mac with 10 cores. PostgreSQL and MySQL ran with default durability settings in Docker, in a 4-core VM with 3.8 GB of memory, through an SSH tunnel. That tunnel and the VM's disk put a floor under each statement, so read the differences, not the absolute rates. SQLite ran on the Mac's own disk. The main suite ran PostgreSQL with the default 64 MB `/dev/shm`, and it logged no shared memory errors. The maintenance rerun used 1 GB.

Ordinary workloads used 5,000 jobs, not the 10,000 first planned, to fit every database in the time available. 4 threads enqueue, and 8 threads claim and run. Every number is a median of 5 repetitions, with the distance from the median to the furthest repetition.

### Statements

The same on all three databases, shown as SQL + transaction statements. `upstream_tables` matches `upstream`. Allocations are SQLite's, and the other databases show the same differences.

| Step | upstream | base | head | head reads of coalescing tables | Allocations, upstream / base / head |
|---|---|---|---|---|---|
| enqueue | 3+4 | 3+4 | 3+4 | 0 | 678 / 691 / 691 |
| claim | 5+2 | 5+2 | 5+2 | 0 | 617 / 617 / 617 |
| finish, preserved | 4+2 | 5+2 | 4+2 | 0 | 722 / 833 / 736 |
| finish, not preserved | 5+2 | 6+2 | 5+2 | 0 | 712 / 813 / 716 |
| fail | 5+4 | 6+6 | 5+4 | 0 | 1187 / 1359 / 1199 |
| fail_all_with, not started | 7+4 | 8+6 | 7+4 | 0 | 1053 / 1215 / 1068 |
| retry | 6+4 | 6+4 | 6+4 | 0 | 1020 / 1032 / 1032 |
| job.discard, ready | 6+2 | 7+2 | 6+2 | 0 | 751 / 852 / 768 |
| job.discard, scheduled | 9+2 | 10+2 | 9+2 | 0 | 1088 / 1188 / 1105 |
| job.discard, failed | 8+2 | 9+2 | 8+2 | 0 | 972 / 1073 / 989 |
| bulk discard, 500 jobs | 5+2 | 6+2 | 6+2 | 1 | 9589 / 10218 / 10217 |
| clear_finished, 500 jobs | 2+0 | 2+0 | 2+0 | 0 | 357 / 357 / 357 |

Ordinary jobs now match Solid Queue on every step except bulk discard, which keeps one read per pass. Those steps allocate 4 to 17 more objects than upstream, where `base` allocated 100 to 172 more.

The instrumentation changes for issue #14 don't touch these paths: ordinary jobs still emit no coalescing event, and on SQLite each step's statements and allocations are the ones above.

### Logging a coalesced call

`coalesce.solid_queue` fires once per call, so `SolidQueue::LogSubscriber` skips it above debug level with `subscribe_log_level`. Measured on SQLite, in one process with the logger at info, alternating between the two, medians of 7 runs. The runs overlap for the bulk enqueue, so the per-event numbers are the ones to rely on.

| | With `subscribe_log_level` | Without |
|---|---|---|
| Time per event | 0.19 µs | 2.23 µs |
| Allocations per event | 3 | 28 |
| 5,000 collected calls in bulk, calls per second | 61,145 (51,761 to 72,326) | 52,306 (43,831 to 60,644) |

### Ordinary jobs, jobs per second from the worker's side

SQLite:

| Workload | upstream | upstream_tables | base | head |
|---|---|---|---|---|
| `finish` | 2705 ±8% | 2839 ±5% | 2443 ±5% | 2733 ±4% |
| `finish_not_preserved` | 2484 ±8% | 2550 ±8% | 2234 ±3% | 2432 ±10% |
| `fail` | 1741 ±10% | 1716 ±8% | 1609 ±4% | 1640 ±9% |
| `discard` | 3055 ±9% | 3053 ±4% | 2828 ±8% | 2955 ±4% |

PostgreSQL:

| Workload | upstream | upstream_tables | base | head |
|---|---|---|---|---|
| `finish` | 927 ±8% | 879 ±5% | 813 ±4% | 920 ±2% |
| `finish_not_preserved` | 840 ±4% | 852 ±6% | 753 ±2% | 838 ±3% |
| `fail` | 652 ±3% | 654 ±7% | 538 ±2% | 661 ±3% |
| `discard` | 864 ±3% | 856 ±5% | 777 ±5% | 862 ±6% |

MySQL, default durability:

| Workload | upstream | upstream_tables | base | head |
|---|---|---|---|---|
| `finish` | 506 ±40% | 556 ±24% | 522 ±25% | 606 ±29% |
| `finish_not_preserved` | 594 ±29% | 647 ±33% | 512 ±20% | 418 ±41% |
| `fail` | 430 ±22% | 448 ±25% | 310 ±31% | 360 ±35% |
| `discard` | 423 ±54% | 463 ±49% | 401 ±47% | 620 ±32% |

MySQL, with `innodb_flush_log_at_trx_commit=2` and `sync_binlog=0`:

| Workload | upstream | upstream_tables | base | head |
|---|---|---|---|---|
| `finish` | 797 ±4% | 804 ±5% | 706 ±5% | 785 ±4% |
| `finish_not_preserved` | 757 ±3% | 753 ±6% | 654 ±3% | 746 ±5% |
| `fail` | 561 ±5% | 559 ±4% | 458 ±3% | 550 ±2% |
| `discard` | 733 ±3% | 744 ±4% | 674 ±2% | 748 ±3% |

`head` is within repetition noise of both baselines on SQLite and PostgreSQL, and in the relaxed MySQL run. It beats `base` by 11% to 23% on PostgreSQL for every workload.

MySQL at default durability is too noisy to show anything small. Its spread is up to ±54%, so `finish_not_preserved` at 418 against 594 can't be told from noise, and neither can `discard` at 620 against 423 in the other direction. That's why it has the relaxed run, which takes disk flushes out of the comparison and brings the spread down to ±6%. The relaxed run doesn't show what a durable MySQL does, only that this change adds no cost of its own.

### Enqueueing ordinary jobs, jobs per second

SQLite:

| Workload | upstream | upstream_tables | base | head |
|---|---|---|---|---|
| `finish` | 2691 ±5% | 2688 ±5% | 2741 ±14% | 2689 ±9% |
| `finish_not_preserved` | 2793 ±8% | 2793 ±6% | 2721 ±6% | 2689 ±11% |
| `fail` | 2817 ±9% | 2922 ±3% | 2696 ±16% | 2692 ±15% |
| `discard` | 2606 ±14% | 2660 ±7% | 2652 ±4% | 2654 ±5% |

PostgreSQL:

| Workload | upstream | upstream_tables | base | head |
|---|---|---|---|---|
| `finish` | 852 ±5% | 786 ±8% | 824 ±5% | 854 ±7% |
| `finish_not_preserved` | 853 ±1% | 838 ±6% | 832 ±8% | 830 ±4% |
| `fail` | 822 ±4% | 800 ±8% | 842 ±2% | 818 ±6% |
| `discard` | 848 ±9% | 844 ±3% | 850 ±2% | 842 ±3% |

MySQL, default durability:

| Workload | upstream | upstream_tables | base | head |
|---|---|---|---|---|
| `finish` | 519 ±28% | 527 ±30% | 518 ±35% | 401 ±25% |
| `finish_not_preserved` | 448 ±23% | 431 ±48% | 523 ±26% | 419 ±25% |
| `fail` | 421 ±28% | 504 ±37% | 410 ±26% | 508 ±26% |
| `discard` | 416 ±44% | 458 ±36% | 472 ±39% | 421 ±58% |

Enqueueing runs no code that changed, and these are within noise.

### Coalesced calls, calls per second

8 threads enqueue 4,000 calls while 2 workers run jobs, and the workers then drain what's left. After every run the script checks that each call has an outcome, every created job ran, every collected call was stored and ran once, and no key, ready or claimed execution is left. All 240 of these runs passed except one, covered below, and none raised an enqueue error or a deadlock.

SQLite:

| Workload | base | head |
|---|---|---|
| `discard_hot_single` | 6668 ±3% | 6493 ±4% |
| `discard_hot_bulk` | 14192 ±19% | 9055 ±61% |
| `discard_distinct_single` | 1130 ±40% | 1414 ±16% |
| `discard_distinct_bulk` | 1298 ±26% | 1207 ±26% |
| `collect_hot_single` | 3307 ±27% | 3896 ±31% |
| `collect_hot_bulk` | 9251 ±1% | 9256 ±23% |
| `collect_distinct_single` | 1039 ±18% | 1286 ±36% |
| `collect_distinct_bulk` | 915 ±60% | 940 ±34% |

PostgreSQL:

| Workload | base | head |
|---|---|---|
| `discard_hot_single` | 1012 ±8% | 1066 ±1% |
| `discard_hot_bulk` | 11403 ±16% | 11625 ±8% |
| `discard_distinct_single` | 554 ±5% | 550 ±8% |
| `discard_distinct_bulk` | 532 ±7% | 546 ±3% |
| `collect_hot_single` | 349 ±3% | 342 ±5% |
| `collect_hot_bulk` | 4390 ±11% | 4313 ±4% |
| `collect_distinct_single` | 461 ±2% | 466 ±3% |
| `collect_distinct_bulk` | 454 ±4% | 460 ±3% |

MySQL:

| Workload | base | head |
|---|---|---|
| `discard_hot_single` | 477 ±29% | 467 ±21% |
| `discard_hot_bulk` | 8671 ±6% | 8852 ±9% |
| `discard_distinct_single` | 424 ±28% | 340 ±30% |
| `discard_distinct_bulk` | 392 ±25% | 431 ±23% |
| `collect_hot_single` | 184 ±42% | 170 ±65% |
| `collect_hot_bulk` | 3607 ±67% | 3883 ±10% |
| `collect_distinct_single` | 302 ±25% | 305 ±22% |
| `collect_distinct_bulk` | 366 ±35% | 364 ±2% |

`head` is within noise of `base` everywhere the spread allows a comparison. Hot-key `perform_all_later` runs finish in under a second, so one slow repetition moves their spread a lot. SQLite's `discard_hot_bulk` looked slower at 9,055 ±61% against 14,192 ±19%, so it got 20 more repetitions of each variant, as did MySQL's. Median and range, in calls per second:

| | base | head |
|---|---|---|
| SQLite | 14351 (7037 to 14694) | 14254 (9520 to 14940) |
| MySQL | 8926 (7316 to 9324) | 8885 (5489 to 9524) |

One MySQL `discard_hot_bulk` run on `head` in the main suite left a ready job holding its key. This was the script's: a worker checked whether enqueuing was over after its last empty claim, so a job committed between the two stayed unclaimed. With the check moved before the claim, these 40 runs had no problems. That's good evidence, not proof, since the failure was 1 in 5 and the sample after the fix is 0 in 20 on `head`.

### Maintenance

Two shapes, seeded with set-based inserts and analyzed before anything is timed. PostgreSQL also gets `VACUUM`, as autovacuum would have done by then. Without it, `waiting.count` on the backlog shape took 1,034 ms instead of 86 ms, because PostgreSQL can't use an index-only scan on rows it hasn't marked visible.

- `retained` has 200,000 finished, 5,000 failed and 1,000 waiting jobs, with 1,045,000 calls, 20,000 of them waiting.
- `backlog` has 5,000 finished, 100,000 failed and 10,000 waiting jobs, with 1,525,000 calls, 1,000,000 of them waiting.

Medians of 5 runs, each the median of 5 timings. Job-owned reads use a different job each time, and run before any write in that process, so inserts can't contaminate them.

| Query, median ms | SQLite retained | SQLite backlog | PostgreSQL retained | PostgreSQL backlog | MySQL retained | MySQL backlog |
|---|---|---|---|---|---|---|
| `waiting.count` | 3.4 | 29.3 | 7.4 | 85.7 | 11.0 | 159.1 |
| `waiting.minimum(:created_at)` | 4.4 | 219.6 | 8.0 | 1014.4 | 15.2 | 762.7 |
| Calls of a waiting job | 0.9 | 0.6 | 1.1 | 2.5 | 1.1 | 3.1 |
| Calls of a finished job | 0.1 | 0.1 | 0.7 | 1.2 | 1.2 | 1.1 |
| Delete 500 finished jobs, cascading | 4.3 | 3.7 | 8.8 | 8.8 | 28.0 | 24.7 |

Job-owned reads and retention depend on the size of the job, not the size of the table. `waiting.count` and `waiting.minimum` scale with the number of waiting calls, because the `waiting` scope finds the live jobs first and then visits each of their calls. `minimum` has to read the table row for each one, unlike `count`, which stays on the `job_id` index. Both read the jobs table first: PostgreSQL scans it in the backlog shape, and uses the `finished_at` index in the retained one. MySQL and SQLite use the `finished_at` index in both.

### Candidate indexes

Measured and not added.

| Backlog shape | SQLite | PostgreSQL | MySQL |
|---|---|---|---|
| `minimum`, no new index | 220 ms | 1014 ms | 763 ms |
| `minimum` with `(created_at)` | 218 ms | 1007 ms | 749 ms |
| `minimum` with `(job_id, created_at)` | 20 ms | 90 ms | 157 ms |
| Insert 100 calls, no new index | 0.5 ms | 2.5 ms | 3.2 ms |
| Insert 100 calls with `(job_id, created_at)` | 0.5 ms | 2.8 ms | 4.1 ms |
| Index bytes on calls, no new index | not measured | 47 MB | 44 MB |
| Index bytes with `(job_id, created_at)` | not measured | 93 MB | 93 MB |

`(created_at)` doesn't help, because the query still starts from the jobs. `(job_id, created_at)` lets `minimum` stay on the index and makes it 5 to 11 times faster at 1,000,000 waiting calls, but it about doubles the index bytes on the calls table, on a query that only monitoring runs. In the retained shape, where most deployments would be, `minimum` already takes 16 ms or less. Neither is worth adding on this evidence.
