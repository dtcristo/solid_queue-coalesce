# frozen_string_literal: true

# Compares Solid Queue with and without this gem. See README.md in this directory.
#
#   TARGET_DB=postgres bundle exec ruby test/benchmarks/bench.rb suite --reps 5 --out tmp/bench
#
# One measurement runs in one process, so that a variant can't load anything of another.
ROOT = File.expand_path("../..", __dir__)
TARGET_DB = ENV.fetch("TARGET_DB", "sqlite")

DATABASE_URLS = {
  "sqlite" => "sqlite3:#{ROOT}/tmp/bench.sqlite3?pool=40&timeout=30000",
  "postgres" => "postgresql://postgres@127.0.0.1:55432/bench?pool=40&gssencmode=disable",
  "mysql" => "mysql2://root@127.0.0.1:33066/bench?pool=40"
}

# Which code runs, and which tables exist. Both baselines load no code from this gem.
VARIANTS = {
  "upstream" => { gem: false, tables: false },
  "upstream_tables" => { gem: false, tables: true },
  "base" => { gem: true, tables: true, path: ENV["BENCH_BASE_PATH"] },
  "head" => { gem: true, tables: true, path: ROOT }
}

ORDINARY_WORKLOADS = %w[ finish finish_not_preserved fail discard ]
COALESCED_WORKLOADS = %w[ discard collect ].product(%w[ hot distinct ], %w[ single bulk ]).map { |parts| parts.join("_") }

def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

def median(values)
  sorted = values.sort
  sorted[sorted.size / 2]
end

module Suite
  module_function

  # Round robin in a shuffled order, so that drift in the database or the host lands
  # on every variant rather than on whichever ran last.
  def run(args)
    options = args.each_slice(2).to_h
    abort "BENCH_BASE_PATH has to point at a checkout of the commit to compare with" unless ENV["BENCH_BASE_PATH"]

    reps = options.fetch("--reps", "5").to_i
    out = options.fetch("--out", "#{ROOT}/tmp/bench")
    only = options["--only"]&.split(",")
    random = Random.new(options.fetch("--seed", "11").to_i)
    FileUtils.mkdir_p(out)

    plan = [
      *ORDINARY_WORKLOADS.map { |workload| [ "ordinary", workload, VARIANTS.keys ] },
      *COALESCED_WORKLOADS.map { |workload| [ "coalesced", workload, %w[ base head ] ] }
    ]
    plan.select! { |mode, workload, _| only.nil? || only.include?(mode) || only.include?(workload) }

    File.open(File.join(out, "#{TARGET_DB}.jsonl"), "a") do |results|
      reps.times do |rep|
        plan.each do |mode, workload, variants|
          variants.shuffle(random: random).each do |variant|
            line = IO.popen({ "BENCH_VARIANT" => variant }, [ RbConfig.ruby, __FILE__, mode, workload ], &:read)
            abort "#{variant} #{workload} failed" unless $?.success?

            results.puts JSON.generate(JSON.parse(line).merge("rep" => rep))
            results.flush
            warn line
          end
        end
      end
    end
  end

  # Spread is the distance from the median to the furthest repetition, so a difference
  # smaller than the spread of both variants can't be told from noise.
  def summarize(args)
    rows = args.flat_map { |path| File.readlines(path).map { |line| JSON.parse(line) } }

    rows.group_by { |row| row.values_at("db", "mode", "workload") }.sort.each do |(db, mode, workload), runs|
      %w[ enqueue_per_second per_second ].each do |metric|
        runs.select { |row| row.key?(metric) }.group_by { |row| row["variant"] }.sort.each do |variant, samples|
          rates = samples.map { |row| row.fetch(metric) }
          middle = median(rates)
          spread = rates.map { |rate| (rate - middle).abs }.max / middle * 100
          checks = samples.flat_map { |row| row["problems"] }.uniq
          errors = samples.sum { |row| row.fetch("errors", 0) }

          puts format("| %s | %s | %s | %s | %d | %.0f | ±%.1f%% | %d | %s |", db, workload, metric, variant, samples.size, middle, spread, errors, checks.empty? ? "ok" : checks.join("; "))
        end
      end
    end
  end
end

mode = ARGV.shift

if mode == "suite" || mode == "summarize"
  require "json"
  require "fileutils"
  require "rbconfig"

  Suite.public_send(mode == "suite" ? :run : :summarize, ARGV)
  exit
end

VARIANT = ENV.fetch("BENCH_VARIANT", "head")
CONFIG = VARIANTS.fetch(VARIANT)

ENV["RAILS_ENV"] = "production"
ENV["DATABASE_URL"] = DATABASE_URLS.fetch(TARGET_DB)
ENV["BUNDLE_GEMFILE"] ||= File.join(ROOT, "Gemfile")

require "bundler/setup"
require "json"
require "logger"
require "rails"
require "active_record/railtie"
require "active_job/railtie"
require "solid_queue"

if CONFIG[:gem]
  $LOAD_PATH.unshift(File.join(CONFIG.fetch(:path), "lib"))
  require "solid_queue/coalesce"
end

class BenchApplication < Rails::Application
  config.root = File.join(ROOT, "tmp")
  config.load_defaults Rails::VERSION::STRING.to_f
  config.eager_load = false
  config.logger = Logger.new(nil)
  config.log_level = :fatal
  config.secret_key_base = "bench"
  config.active_job.queue_adapter = :solid_queue
  config.solid_queue.silence_polling = true
  config.solid_queue.preserve_finished_jobs = true
end

FileUtils.mkdir_p(File.join(ROOT, "tmp"))
BenchApplication.initialize!

ActiveRecord::Base.logger = nil
ActiveJob::Base.logger = Logger.new(nil)
SolidQueue.logger = Logger.new(nil)

# It reports to stdout, which is only for the result, and on MySQL it leaves the
# connection without a database.
def create_database
  $stdout = $stderr
  ActiveRecord::Tasks::DatabaseTasks.create(ActiveRecord::Base.connection_db_config)
  ActiveRecord::Base.establish_connection
ensure
  $stdout = STDOUT
end

def load_schema
  ActiveRecord::Migration.verbose = false
  connection = ActiveRecord::Base.connection

  connection.disable_referential_integrity do
    connection.tables.each { |table| connection.drop_table(table, force: :cascade) }
  end

  load File.join(Gem.loaded_specs.fetch("solid_queue").full_gem_path, "lib/generators/solid_queue/install/templates/db/queue_schema.rb")

  if CONFIG[:tables]
    load File.join(ROOT, "lib/generators/solid_queue/coalesce/install/templates/db/create_solid_queue_coalesce_tables.rb")
    CreateSolidQueueCoalesceTables.migrate(:up)
  end

  ActiveRecord::Base.descendants.each(&:reset_column_information)
end

class OrdinaryJob < ActiveJob::Base
  queue_as :bench

  def perform(*) = nil
end

class FailingJob < ActiveJob::Base
  queue_as :bench

  def perform(*) = raise("boom")
end

if CONFIG[:gem]
  RUNS = Concurrent::AtomicFixnum.new
  COLLECTED = Concurrent::AtomicFixnum.new

  class DiscardingJob < ActiveJob::Base
    queue_as :bench
    coalesce key: ->(key) { key }

    def perform(*) = RUNS.increment
  end

  class CollectingJob < ActiveJob::Base
    queue_as :bench
    coalesce key: ->(key) { key }, on_conflict: :collect, batch_size: 100

    def perform_batch(calls) = COLLECTED.increment(calls.size)
  end
end

def in_threads(count)
  barrier = Concurrent::CyclicBarrier.new(count)

  Array.new(count) do |index|
    Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do
        barrier.wait
        yield index
      end
    end
  end.map(&:value)
end

def worker_process
  @worker_process ||= SolidQueue::Process.register(kind: "Worker", pid: Process.pid, name: "bench-#{Process.pid}")
end

# Claims the way a worker's poll does. A failed job raises what a real worker's pool
# would report, and isn't an error here.
def work_off(threads:, batch: 5, until_done: -> { true })
  in_threads(threads) do
    loop do
      claimed = SolidQueue::ReadyExecution.claim("*", batch, worker_process.id)
      break if claimed.empty? && until_done.call

      claimed.each do |execution|
        execution.perform
      rescue RuntimeError
        nil
      end
    end
  end
end

# Single process, so a job's class is in memory, and the only thing compared is the
# database work and the Ruby around it. Reported separately per phase.
module Ordinary
  module_function

  JOBS = Integer(ENV.fetch("BENCH_JOBS", "10000"))
  ENQUEUERS = 4
  WORKERS = 8

  def run(workload)
    job_class = workload == "fail" ? FailingJob : OrdinaryJob
    SolidQueue.preserve_finished_jobs = workload != "finish_not_preserved"

    started = monotonic
    in_threads(ENQUEUERS) { (JOBS / ENQUEUERS).times { |i| job_class.perform_later("bench", i) } }
    enqueued = monotonic

    if workload == "discard"
      discard
    else
      work_off(threads: WORKERS)
    end
    finished = monotonic

    {
      "per_second" => (JOBS / (finished - enqueued)).round(1),
      "enqueue_per_second" => (JOBS / (enqueued - started)).round(1),
      "problems" => problems(workload)
    }
  end

  def discard
    job_ids = Queue.new
    SolidQueue::Job.pluck(:id).each { |id| job_ids << id }
    job_ids.close

    in_threads(WORKERS) do
      while (id = job_ids.pop)
        SolidQueue::Job.find(id).discard
      end
    end
  end

  def problems(workload)
    expected = {
      "finish" => { "jobs" => JOBS, "finished" => JOBS, "failed" => 0 },
      "finish_not_preserved" => { "jobs" => 0, "finished" => 0, "failed" => 0 },
      "fail" => { "jobs" => JOBS, "finished" => 0, "failed" => JOBS },
      "discard" => { "jobs" => 0, "finished" => 0, "failed" => 0 }
    }.fetch(workload)

    actual = { "jobs" => SolidQueue::Job.count, "finished" => SolidQueue::Job.finished.count, "failed" => SolidQueue::FailedExecution.count }
    list = actual == expected ? [] : [ "expected #{expected}, got #{actual}" ]
    list << "left claimed" if SolidQueue::ClaimedExecution.any?
    list << "left ready" if SolidQueue::ReadyExecution.any?
    list
  end
end

# Every call has to come out as an outcome, an error that says so, or a run.
module Coalesced
  module_function

  CALLS = Integer(ENV.fetch("BENCH_CALLS", "4000"))
  ENQUEUERS = 8
  WORKERS = 2
  BULK_SIZE = 100

  def run(workload)
    mode, keys, shape = workload.split("_")
    job_class = mode == "collect" ? CollectingJob : DiscardingJob
    per_thread = CALLS / ENQUEUERS
    errors = Concurrent::AtomicFixnum.new
    active_jobs = Concurrent::Array.new
    done = Concurrent::AtomicBoolean.new

    workers = Thread.new { work_off(threads: WORKERS, until_done: -> { done.true? }) }

    started = monotonic
    in_threads(ENQUEUERS) do |thread|
      calls = Array.new(per_thread) { |i| job_class.new(keys == "hot" ? "hot" : "key-#{thread}-#{i}") }

      if shape == "bulk"
        calls.each_slice(BULK_SIZE) do |slice|
          ActiveJob.perform_all_later(slice)
        rescue SolidQueue::Job::EnqueueError
          errors.increment(slice.count { |call| call.coalesce_status.nil? })
        ensure
          active_jobs.concat(slice)
        end
      else
        calls.each do |call|
          call.enqueue
        rescue SolidQueue::Job::EnqueueError
          errors.increment
        ensure
          active_jobs << call
        end
      end
    end
    finished = monotonic

    done.make_true
    workers.join

    { "per_second" => (active_jobs.size / (finished - started)).round(1), "errors" => errors.value, "problems" => problems(mode, active_jobs, errors.value) }
  end

  def problems(mode, active_jobs, errors)
    list = []
    outcomes = active_jobs.group_by(&:coalesce_status).transform_values(&:size)
    accounted = outcomes.except(nil).values.sum + errors

    list << "accounted for #{accounted} of #{active_jobs.size} calls: #{outcomes}" unless accounted == active_jobs.size
    list << "left claimed" if SolidQueue::ClaimedExecution.any?
    list << "left ready" if SolidQueue::ReadyExecution.any?
    list << "#{SolidQueue::CoalesceKey.count} keys left after every job started" if SolidQueue::CoalesceKey.any?

    if mode == "collect"
      collected = outcomes.fetch(:created, 0) + outcomes.fetch(:collected, 0)
      list << "ran #{COLLECTED.value} of #{collected} collected calls" unless COLLECTED.value == collected
      list << "stored #{SolidQueue::CoalescedCall.count} of #{collected} calls" unless SolidQueue::CoalescedCall.count == collected
    else
      created = outcomes.fetch(:created, 0)
      list << "ran #{RUNS.value} of #{created} created jobs" unless RUNS.value == created
    end

    list
  end
end

# Statements and a single thread's latency for each step of an ordinary job's life
module Statements
  module_function

  TRANSACTION = /\A(BEGIN|COMMIT|SAVEPOINT|RELEASE|ROLLBACK)/i
  REPEATS = Integer(ENV.fetch("BENCH_REPEATS", "200"))

  STEPS = {
    "enqueue" => [ -> { OrdinaryJob.new("a") }, ->(job) { job.enqueue } ],
    "claim" => [ -> { OrdinaryJob.perform_later("a") }, ->(_) { claim } ],
    "finish, preserved" => [ -> { ready_claimed(OrdinaryJob) }, ->(claimed) { claimed.perform } ],
    "finish, not preserved" => [ -> { SolidQueue.preserve_finished_jobs = false; ready_claimed(OrdinaryJob) }, ->(claimed) { claimed.perform } ],
    "fail" => [ -> { SolidQueue.preserve_finished_jobs = true; ready_claimed(FailingJob) }, ->(claimed) { begin claimed.perform; rescue RuntimeError; end } ],
    "fail_all_with, not started" => [ -> { ready_claimed(OrdinaryJob) }, ->(claimed) { SolidQueue::ClaimedExecution.where(id: claimed.id).fail_all_with(RuntimeError.new("died")) } ],
    "retry" => [ -> { failed_job }, ->(job) { job.retry } ],
    "job.discard, ready" => [ -> { job_for(OrdinaryJob.perform_later("a")) }, ->(job) { job.discard } ],
    "job.discard, scheduled" => [ -> { job_for(OrdinaryJob.set(wait: 1.hour).perform_later("a")) }, ->(job) { job.discard } ],
    "job.discard, failed" => [ -> { failed_job }, ->(job) { job.discard } ],
    "bulk discard, 500 jobs" => [ -> { ActiveJob.perform_all_later(Array.new(500) { OrdinaryJob.new("a") }) }, ->(_) { SolidQueue::ReadyExecution.discard_all_in_batches }, 10 ],
    "clear_finished, 500 jobs" => [ -> { finished_jobs(500) }, ->(_) { SolidQueue::Job.clear_finished_in_batches(batch_size: 500, finished_before: 1.minute.from_now, sleep_between_batches: 0) }, 10 ]
  }

  def run
    statements = []
    subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
      statements << payload[:sql] unless payload[:name] == "SCHEMA"
    end

    STEPS.to_h do |name, (setup, step, repeats)|
      clear
      times = []
      allocations = []

      (repeats || REPEATS).times do
        subject = setup.call
        statements.clear
        before = GC.stat(:total_allocated_objects)
        started = monotonic
        step.call(subject)
        times << (monotonic - started) * 1_000_000
        allocations << GC.stat(:total_allocated_objects) - before
      end

      transaction, sql = statements.partition { |statement| statement.match?(TRANSACTION) }
      [ name, { "sql" => sql.size, "transaction" => transaction.size, "keys" => sql.grep(/solid_queue_coalesce/).size, "p50_us" => median(times).round(1), "allocations" => median(allocations) } ]
    end
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber)
  end

  def clear
    [ SolidQueue::ClaimedExecution, SolidQueue::ReadyExecution, SolidQueue::FailedExecution, SolidQueue::ScheduledExecution, SolidQueue::Job ].each(&:delete_all)
  end

  def claim = SolidQueue::ReadyExecution.claim("*", 1, worker_process.id).first
  def job_for(active_job) = SolidQueue::Job.find(active_job.provider_job_id)

  def ready_claimed(job_class)
    job_class.perform_later("a")
    claim
  end

  def failed_job
    claimed = ready_claimed(FailingJob)
    job_id = claimed.job_id
    begin
      claimed.perform
    rescue RuntimeError
      nil
    end
    SolidQueue::Job.find(job_id)
  end

  def finished_jobs(count)
    ActiveJob.perform_all_later(Array.new(count) { OrdinaryJob.new("a") })
    SolidQueue::ReadyExecution.delete_all
    SolidQueue::Job.update_all(finished_at: 1.day.ago)
  end
end

# Seeded with set-based inserts, because going through admission would take hours and
# nothing here depends on how the rows got there. Only reads get timed against these
# rows, and writes run after every read, so they can't change what the reads see.
module Maintenance
  module_function

  SHAPES = {
    "retained" => { finished: 200_000, failed: 5_000, waiting: 1_000, finished_calls: 5, failed_calls: 5, waiting_calls: 20 },
    "backlog" => { finished: 5_000, failed: 100_000, waiting: 10_000, finished_calls: 5, failed_calls: 5, waiting_calls: 100 }
  }
  TIMINGS = 5
  CANDIDATE_INDEXES = [ %i[ created_at ], %i[ job_id created_at ] ]

  def run(shape_name)
    shape = SHAPES.fetch(shape_name)
    seconds = seed(shape)
    analyze

    result = {
      "seed_seconds" => seconds.round(1),
      "rows" => { "jobs" => SolidQueue::Job.count, "calls" => SolidQueue::CoalescedCall.count, "waiting" => SolidQueue::CoalescedCall.waiting.count, "expected_waiting" => shape[:waiting] * shape[:waiting_calls] },
      "reads" => reads(shape),
      "size" => size
    }

    result["candidate_indexes"] = CANDIDATE_INDEXES.to_h do |columns|
      name = "bench_#{columns.join("_")}"
      built = time_once { connection.add_index :solid_queue_coalesced_calls, columns, name: name }
      analyze
      measured = { "build_seconds" => built.round(1), "size" => size, "reads" => reads(shape).slice("waiting.count", "waiting.minimum(:created_at)"), "insert_100_ms" => inserts(shape) }
      connection.remove_index :solid_queue_coalesced_calls, name: name
      [ name, measured ]
    end

    result["insert_100_ms"] = inserts(shape)
    result["retention"] = retention(shape)
    result
  end

  def connection = SolidQueue::Record.connection
  def time_once = monotonic.then { |started| yield; monotonic - started }
  def time_ms(count = TIMINGS) = median(Array.new(count) { time_once { yield } * 1000 }).round(2)

  def reads(shape)
    first_waiting = shape[:finished] + shape[:failed] + 1
    waiting_jobs = (first_waiting...(first_waiting + shape[:waiting])).to_a.sample(TIMINGS, random: Random.new(1))
    finished_jobs = (1..shape[:finished]).to_a.sample(TIMINGS, random: Random.new(2))
    owned = ->(job_id) { SolidQueue::CoalescedCall.where(job_id: job_id).order(:id) }

    {
      "waiting.count" => timed(-> { SolidQueue::CoalescedCall.waiting.count }, SolidQueue::CoalescedCall.waiting.select("COUNT(*)").to_sql),
      "waiting.minimum(:created_at)" => timed(-> { SolidQueue::CoalescedCall.waiting.minimum(:created_at) }, SolidQueue::CoalescedCall.waiting.select("MIN(created_at)").to_sql),
      "calls of a waiting job" => timed_over(waiting_jobs, owned),
      "calls of a finished job" => timed_over(finished_jobs, owned)
    }
  end

  def timed(query, sql)
    { "median_ms" => time_ms { query.call }, "plan" => explain(sql) }
  end

  # A different job each time, so a repeat can't be served from what the last one cached
  def timed_over(job_ids, relation)
    times = job_ids.map { |job_id| time_once { relation.call(job_id).pluck(:id, :arguments) } * 1000 }
    { "median_ms" => median(times).round(2), "plan" => explain(relation.call(job_ids.first).select(:id, :arguments).to_sql) }
  end

  def explain(sql)
    case TARGET_DB
    when "postgres" then connection.execute("EXPLAIN (ANALYZE, BUFFERS) #{sql}").map { |row| row.values.first }
    when "mysql" then connection.execute("EXPLAIN ANALYZE #{sql}").to_a.flatten.flat_map { |plan| plan.to_s.lines.map(&:rstrip) }
    else connection.execute("EXPLAIN QUERY PLAN #{sql}").map { |row| row["detail"] }
    end
  end

  # Takes its rows back out, so later reads see the same table
  def inserts(shape)
    job_id = shape[:finished] + shape[:failed] + 1
    last_id = SolidQueue::CoalescedCall.maximum(:id)

    time_ms(15) { SolidQueue::CoalescedCall.insert_all(Array.new(100) { { job_id: job_id, arguments: { x: 1 } } }) }.tap do
      SolidQueue::CoalescedCall.where("id > ?", last_id).delete_all
    end
  end

  # How long one batch takes, with cascades into the calls of 500 finished jobs
  def retention(shape)
    batches = Array.new(TIMINGS) do
      ids = SolidQueue::Job.finished.limit(500).pluck(:id)
      time_once { SolidQueue::Job.where(id: ids).delete_all } * 1000
    end

    { "delete_500_finished_jobs_median_ms" => median(batches).round(2), "calls_per_job" => shape[:finished_calls] }
  end

  def size
    case TARGET_DB
    when "postgres"
      connection.select_one("SELECT pg_relation_size('solid_queue_coalesced_calls') AS table_bytes, pg_indexes_size('solid_queue_coalesced_calls') AS index_bytes")
    when "mysql"
      connection.select_one("SELECT data_length AS table_bytes, index_length AS index_bytes FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'solid_queue_coalesced_calls'")
    end
  end

  def analyze
    connection.execute(TARGET_DB == "mysql" ? "ANALYZE TABLE solid_queue_jobs, solid_queue_coalesced_calls, solid_queue_failed_executions, solid_queue_claimed_executions" : "ANALYZE")
  end

  # Oldest first, as the calls of a real table are, which is what an index on
  # created_at would have to walk through to reach the first waiting one.
  def seconds_after(column, timestamp)
    case TARGET_DB
    when "postgres" then "#{timestamp} + #{column} * INTERVAL '1 second'"
    when "mysql" then "#{timestamp} + INTERVAL #{column} SECOND"
    else "datetime(#{timestamp}, '+' || #{column} || ' seconds')"
    end
  end

  def seed(shape)
    finished, failed, waiting = shape.values_at(:finished, :failed, :waiting)
    jobs = finished + failed + waiting
    timestamp = TARGET_DB == "sqlite" ? "'2026-10-01 00:00:00'" : "TIMESTAMP '2026-10-01 00:00:00'"
    payload = connection.quote(CollectingJob.new("a").serialize.merge("coalesce_on_conflict" => "collect").to_json)
    finished_or_failed_calls = finished * shape[:finished_calls] + failed * shape[:failed_calls]
    calls = finished_or_failed_calls + waiting * shape[:waiting_calls]
    call_payload = connection.quote(CollectingJob.new("a").serialize.to_json)

    connection.execute("SET SESSION cte_max_recursion_depth = 100000000") if TARGET_DB == "mysql"

    time_once do
      connection.execute(<<~SQL)
        INSERT INTO solid_queue_jobs (id, queue_name, class_name, arguments, priority, active_job_id, scheduled_at, finished_at, created_at, updated_at)
        WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < #{jobs})
        SELECT i, 'bench', 'CollectingJob', #{payload}, 0, 'bench', #{timestamp}, CASE WHEN i <= #{finished} THEN #{timestamp} ELSE NULL END, #{timestamp}, #{timestamp} FROM n
      SQL
      connection.execute(<<~SQL)
        INSERT INTO solid_queue_failed_executions (job_id, error, created_at)
        WITH RECURSIVE n(i) AS (SELECT #{finished + 1} UNION ALL SELECT i + 1 FROM n WHERE i < #{finished + failed})
        SELECT i, 'bench', #{timestamp} FROM n
      SQL
      connection.execute(<<~SQL)
        INSERT INTO solid_queue_coalesced_calls (job_id, arguments, created_at)
        WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < #{calls})
        SELECT CASE WHEN i <= #{finished_or_failed_calls} THEN ((i - 1) / #{shape[:finished_calls]}) + 1
          ELSE #{finished + failed} + ((i - #{finished_or_failed_calls} - 1) % #{waiting}) + 1 END, #{call_payload}, #{seconds_after("i", timestamp)} FROM n
      SQL
    end
  end
end

create_database
load_schema
output = {
  "db" => TARGET_DB,
  "mode" => mode,
  "variant" => VARIANT,
  "workload" => ARGV.first,
  "versions" => {
    "ruby" => RUBY_VERSION,
    "rails" => Rails.version,
    "solid_queue" => SolidQueue::VERSION,
    "database" => ActiveRecord::Base.connection.select_value(TARGET_DB == "sqlite" ? "SELECT sqlite_version()" : "SELECT version()")
  }
}

result =
  case mode
  when "ordinary" then Ordinary.run(ARGV.fetch(0))
  when "coalesced" then Coalesced.run(ARGV.fetch(0))
  when "statements" then { "steps" => Statements.run }
  when "maintenance" then Maintenance.run(ARGV.fetch(0))
  else abort "Unknown mode #{mode}"
  end

puts JSON.generate(output.merge(result))
