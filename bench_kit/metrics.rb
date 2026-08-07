# frozen_string_literal: true

# Full-picture metrics for pg_pipeline in ONE run. Answers the questions a
# single wall_time RubyProf pass cannot:
#
#   1. allocations / query   (GC.stat total_allocated_objects delta) — the real
#                            per-query churn; the number the guard cache targets.
#   2. GC pressure           (minor/major GC count, heap growth over the run)
#   3. CPU attribution       (RubyProf PROCESS_TIME — where CPU actually goes,
#                            not wall which is dominated by IO wait)
#   4. allocation hot spots  (RubyProf ALLOCATIONS — which methods allocate)
#   5. sampled hot paths     (StackProf :cpu/:wall — no per-call perturbation,
#                            unlike deterministic RubyProf; the trustworthy CPU view)
#   6. wall shape            (RubyProf WALL — kept only as a sanity check)
#
# Correctness assertions run ONCE, OUTSIDE every profiler, so harness bookkeeping
# (Array building, Integer#==) never contaminates the numbers. Warmup runs before
# any measurement, so connection setup (connect_poll) is not in frame.
#
# This is a PROFILE tool, not a benchmark. Throughput / p99 come from bench:ab
# (ideally through latency_proxy). See bench_kit/README.md.
#
#   PG_PIPELINE_URL=... bundle exec rake bench:metrics
#   FIBERS=500 ROUNDS=5 TOP=15 bundle exec rake bench:metrics

require "async"
require "ruby-prof"
require_relative "common"

url      = BenchKit.require_url!
pipeline = Integer(ENV.fetch("PIPELINE_SIZE", "4"))
pinned   = Integer(ENV.fetch("PINNED_SIZE",   "2"))
fibers   = Integer(ENV.fetch("FIBERS",        "500"))
rounds   = Integer(ENV.fetch("ROUNDS",        "5"))
top_n    = Integer(ENV.fetch("TOP",           "12"))
out_dir  = BenchKit.ensure_out_dir!
stamp    = BenchKit.stamp

def with_client(url, pipeline, pinned)
  result = nil
  Sync do |task|
    client = PgPipeline::Client.new(
      url, pipeline_size: pipeline, pinned_size: pinned, health_check: false
    ).start
    begin
      result = yield(client, task)
    ensure
      client.close
    end
  end
  result
end

# Pure gem calls only — no assertions, no comparison-array building.
def workload(db, task, fibers)
  fibers.times.map { |n| task.async { db.query("SELECT $1::int AS n", [n]) } }.each(&:wait)
end

# Richer path coverage (tx, savepoint, error) for the profilers — still no asserts.
def rich_workload(db, task, fibers, pinned)
  workload(db, task, fibers)
  task.async do
    db.query("SELECT 1/0")
  rescue PgPipeline::QueryError
    nil
  end.wait
  db.transaction do |tx|
    tx.exec("CREATE TEMP TABLE IF NOT EXISTS pgp_m(x int)")
    tx.query("INSERT INTO pgp_m(x) VALUES ($1)", [1])
    tx.savepoint { |sp| sp.query("SELECT 1") }
  end
  [pinned * 3, 1].max.times.map { task.async { db.transaction { |tx| tx.query("SELECT 1") } } }.each(&:wait)
end

# One-time correctness verification, OUTSIDE any profiler.
def verify!(url, pipeline, pinned, fibers)
  with_client(url, pipeline, pinned) do |db, task|
    results = fibers.times.map { |n| task.async { db.query("SELECT $1::int AS n", [n]).first["n"].to_i } }.map(&:wait)
    raise "multiplex order/isolation mismatch" unless results == (0...fibers).to_a

    good  = task.async { db.query("SELECT 1 AS ok").first["ok"].to_i }
    bad   = task.async do
      db.query("SELECT 1/0")
      :no_error
    rescue PgPipeline::QueryError
      :query_error
    end
    after = task.async { db.query("SELECT 2 AS ok").first["ok"].to_i }
    raise "error isolation failed" unless good.wait == 1 && bad.wait == :query_error && after.wait == 2

    db.transaction do |tx|
      tx.exec("CREATE TEMP TABLE IF NOT EXISTS pgp_v(x int)")
      tx.query("INSERT INTO pgp_v(x) VALUES ($1)", [1])
      begin
        tx.savepoint do
          tx.query("INSERT INTO pgp_v(x) VALUES ($1)", [2])
          raise "rollback inner"
        end
      rescue RuntimeError
        nil
      end
      raise "savepoint isolation failed" unless tx.query("SELECT count(*)::int AS c FROM pgp_v").first["c"].to_i == 1
    end
  end
  puts "verify: OK (multiplex order, error isolation, savepoint)"
end

def rp_top(measure, top_n, out_dir, stamp, label)
  profile = RubyProf::Profile.new(measure_mode: measure)
  result  = profile.profile { yield }
  agg = Hash.new { |h, k| h[k] = { self: 0.0, calls: 0 } }
  result.threads.each do |t|
    t.methods.each do |m|
      a = agg[m.full_name]
      a[:self]  += m.self_time
      a[:calls] += m.called
    end
  end
  total = agg.values.sum { |a| a[:self] }
  rows  = agg.sort_by { |_, a| -a[:self] }.first(top_n)
  unit  = measure == RubyProf::ALLOCATIONS ? "allocs" : "s"
  path  = File.join(out_dir, "metrics_#{stamp}_#{label}.txt")
  File.open(path, "w") do |f|
    f.puts "# #{label}  total_self=#{format('%.4f', total)} #{unit}"
    f.printf("%8s  %10s  %9s  %s\n", "%self", "self(#{unit})", "calls", "name")
    rows.each do |name, a|
      pct = total.positive? ? a[:self] / total * 100.0 : 0.0
      f.printf("%7.2f%%  %10.4f  %9d  %s\n", pct, a[:self], a[:calls], name)
    end
  end
  puts
  puts "== RubyProf #{label} (top #{top_n} by self) -> #{File.basename(path)}"
  rows.first(top_n).each do |name, a|
    pct = total.positive? ? a[:self] / total * 100.0 : 0.0
    printf("  %6.2f%%  self=%.4f#{unit}  calls=%-7d  %s\n", pct, a[:self], a[:calls], name)
  end
end

puts "metrics: full picture"
puts "url=#{BenchKit.redact_url(url)}"
puts "pipeline_size=#{pipeline} pinned_size=#{pinned} fibers=#{fibers} rounds=#{rounds}"
puts

# ── warmup (setup out of frame) ───────────────────────────────────────────────
with_client(url, pipeline, pinned) { |db, task| workload(db, task, [fibers / 4, 4].max) }

# ── correctness (out of every profiler) ───────────────────────────────────────
verify!(url, pipeline, pinned, fibers)

# ── 1. allocations / query + GC pressure ──────────────────────────────────────
queries = fibers * rounds
GC.start
gc0 = GC.stat
GC.disable
alloc0 = GC.stat(:total_allocated_objects)
with_client(url, pipeline, pinned) { |db, task| rounds.times { workload(db, task, fibers) } }
alloc1 = GC.stat(:total_allocated_objects)
GC.enable
GC.start
gc1 = GC.stat

per_query = (alloc1 - alloc0).to_f / queries
puts
puts "== allocations & GC (#{queries} queries) =="
puts format("  allocations/query   = %.1f objects", per_query)
puts format("  total allocated     = %d objects", alloc1 - alloc0)
puts format("  minor GC (enabled)  = %d", gc1[:minor_gc_count] - gc0[:minor_gc_count])
puts format("  major GC (enabled)  = %d", gc1[:major_gc_count] - gc0[:major_gc_count])
puts format("  heap pages          = %d -> %d", gc0[:heap_allocated_pages], gc1[:heap_allocated_pages])

# ── 2. CPU attribution (process_time) ─────────────────────────────────────────
with_client(url, pipeline, pinned) do |db, task|
  rp_top(RubyProf::PROCESS_TIME, top_n, out_dir, stamp, "process") { rounds.times { rich_workload(db, task, fibers, pinned) } }
end

# ── 3. allocation hot spots ───────────────────────────────────────────────────
with_client(url, pipeline, pinned) do |db, task|
  rp_top(RubyProf::ALLOCATIONS, top_n, out_dir, stamp, "allocations") { rounds.times { rich_workload(db, task, fibers, pinned) } }
end

# ── 4. wall shape (sanity only) ───────────────────────────────────────────────
with_client(url, pipeline, pinned) do |db, task|
  rp_top(RubyProf::WALL_TIME, top_n, out_dir, stamp, "wall") { rounds.times { rich_workload(db, task, fibers, pinned) } }
end

# ── 5. sampling (trustworthy CPU; no per-call perturbation) ───────────────────
begin
  require "stackprof"
  %i[cpu wall].each do |mode|
    path = File.join(out_dir, "stackprof_#{stamp}_#{mode}.dump")
    StackProf.run(mode: mode, out: path, raw: false, interval: 200) do
      with_client(url, pipeline, pinned) { |db, task| rounds.times { rich_workload(db, task, fibers, pinned) } }
    end
    report = StackProf::Report.new(Marshal.load(File.binread(path)))
    puts
    puts "== StackProf #{mode} (sampled, top #{top_n}) -> #{File.basename(path)}"
    report.print_text(false, top_n)
  end
rescue LoadError
  puts
  puts "== StackProf skipped (add `gem \"stackprof\"` for sampled CPU attribution) =="
end

puts
puts "wrote metrics_#{stamp}_{process,allocations,wall}.txt and stackprof_#{stamp}_*.dump to #{out_dir}"
puts "reminder: this is profiling, not a benchmark — throughput/p99 come from bench:ab (+ latency_proxy)."
