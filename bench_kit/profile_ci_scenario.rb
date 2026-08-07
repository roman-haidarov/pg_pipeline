# frozen_string_literal: true

# RubyProf over a CI/integration-shaped live scenario (real call paths, not a
# microbench). Deterministic profiling perturbs Async — prefer this for path
# visibility; use bench:ab + latency_proxy for throughput claims.
#
#   PG_PIPELINE_URL=... bundle exec rake bench:profile
#   MEASURE=process FIBERS=200 ROUNDS=3 bundle exec rake bench:profile
#
# Outputs (in OUT_DIR/):
#   ruby_prof_STAMP.txt          — custom aggregate flat: all methods merged
#                                  across every fiber, sorted by self_time
#   ruby_prof_STAMP_topN.txt     — per-fiber flat for top-N BUSY fibers
#                                  (ranked by fiber's total self_time, not wall)
#   ruby_prof_STAMP.html         — full call graph (all fibers, drill-down)
#
# Why custom printers:
#   ruby_prof 1.7.x FlatPrinter ignores the thread: option and always dumps
#   every fiber on each call. GraphHtmlPrinter does not aggregate across fibers.
#   We write both files directly from RubyProf::MethodInfo to get a single
#   merged view that reflects the true concurrency load.

require "fileutils"
require "async"
require "ruby-prof"
require_relative "common"

url         = BenchKit.require_url!
pipeline    = Integer(ENV.fetch("PIPELINE_SIZE", "2"))
pinned      = Integer(ENV.fetch("PINNED_SIZE",   "2"))
fibers      = Integer(ENV.fetch("FIBERS",        "200"))
rounds      = Integer(ENV.fetch("ROUNDS",        "3"))
out_dir     = BenchKit.ensure_out_dir!
min_percent = Float(ENV.fetch("MIN_PERCENT", "1"))
top_n       = Integer(ENV.fetch("TOP_FIBERS", "8"))

measure = case ENV.fetch("MEASURE", "wall").downcase
          when "wall", "wall_time"               then RubyProf::WALL_TIME
          when "process", "process_time", "cpu"  then RubyProf::PROCESS_TIME
          when "allocations"                     then RubyProf::ALLOCATIONS
          else abort "MEASURE must be wall|process|allocations"
          end

def run_ci_scenario(db, task, fibers, pinned, verify: false)
  runs = fibers.times.map do |n|
    task.async { db.query("SELECT $1::int AS n", [n]).first["n"].to_i }
  end
  if verify
    raise "multiplex mismatch" unless runs.map(&:wait) == (0...fibers).to_a
  else
    runs.each(&:wait)
  end

  good = task.async { db.query("SELECT 1 AS ok").first["ok"].to_i }
  bad = task.async do
    db.query("SELECT 1/0")
    :no_error
  rescue PgPipeline::QueryError
    :query_error
  end
  after = task.async { db.query("SELECT 2 AS ok").first["ok"].to_i }
  if verify
    raise "error isolation failed" unless good.wait == 1 && bad.wait == :query_error && after.wait == 2
  else
    good.wait; bad.wait; after.wait
  end

  sleeper = task.async { db.query("SELECT pg_sleep(0.05), 1 AS n") }
  sleeper.stop
  cancel_ok = db.query("SELECT 3 AS n").first["n"].to_i
  raise "post-cancel query failed" if verify && cancel_ok != 3

  db.transaction do |tx|
    tx.exec("CREATE TEMP TABLE IF NOT EXISTS pgp_prof(x int)")
    tx.query("INSERT INTO pgp_prof(x) VALUES ($1)", [1])
    begin
      tx.savepoint do
        tx.query("INSERT INTO pgp_prof(x) VALUES ($1)", [2])
        raise "rollback inner"
      end
    rescue RuntimeError
      nil
    end
    count = tx.query("SELECT count(*)::int AS c FROM pgp_prof").first["c"].to_i
    raise "savepoint isolation failed" if verify && count != 1
  end

  workers = [pinned * 3, 1].max.times.map do
    task.async do
      db.transaction { |tx| tx.query("SELECT $1::int AS n", [1]) }
    end
  end
  workers.each(&:wait)

  50.times.map do |i|
    task.async { db.query("SELECT $1::int AS i, now() AS t", [i]) }
  end.each(&:wait)

  db.stats
end

puts "profile: CI-shaped scenario"
puts "url=#{BenchKit.redact_url(url)}"
puts "pipeline_size=#{pipeline} pinned_size=#{pinned} fibers=#{fibers} rounds=#{rounds}"
puts "measure=#{ENV.fetch('MEASURE', 'wall')} min_percent=#{min_percent} out=#{out_dir}"
puts

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

with_client(url, pipeline, pinned) do |client, task|
  5.times { |i| client.query("SELECT $1::int AS n", [i]) }
  client.transaction { |tx| tx.query("SELECT 1") } if pinned.positive?
  run_ci_scenario(client, task, [fibers, 32].min, pinned, verify: true)
  puts "verify: OK (asserts run outside the profiler)"
  $stdout.flush
end

GC.start
profile = RubyProf::Profile.new(measure_mode: measure)
result  = profile.profile do
  with_client(url, pipeline, pinned) do |client, task|
    rounds.times do |round|
      stats = run_ci_scenario(client, task, fibers, pinned, verify: false)
      puts "round=#{round + 1} live=#{stats.dig(:pipeline, :live)} reconnects=#{stats[:reconnects]}"
    end
    $stdout.flush
  end
end

# ─────────────────────────────────────────────────────────────────────────────
# Helper: unique fiber label.
# ruby_prof 1.7.x: thread#id returns OS thread id (same for all Async fibers).
# thread#fiber_id returns the unique Ruby fiber id.
def fiber_label(t)
  fid = t.respond_to?(:fiber_id) ? t.fiber_id : t.id
  "fiber=#{fid}"
end

# ─────────────────────────────────────────────────────────────────────────────
stamp      = BenchKit.stamp
html_path  = File.join(out_dir, "ruby_prof_#{stamp}.html")
flat_path  = File.join(out_dir, "ruby_prof_#{stamp}.txt")
top_path   = File.join(out_dir, "ruby_prof_#{stamp}_top#{top_n}.txt")

all_threads = result.threads

# Profile wall-clock span = longest fiber's total_time.
profile_wall = all_threads.map(&:total_time).max

# Rank fibers by their total self_time across all profiled methods.
# total_time in wall mode includes sleep/wait; self_time = actual Ruby execution.
# This separates the BUSY fibers (driver loops, heavy request fibers) from the
# purely waiting ones (short-lived request fibers that spend 99% in IO wait).
fiber_self   = all_threads.map { |t| [t, t.methods.sum(&:self_time)] }
top_busy     = fiber_self.sort_by { |_, s| -s }.first(top_n).map(&:first)

puts
puts "profile  result_fibers=#{all_threads.size}  wall=#{format('%.4f', profile_wall)}s"
puts "  top #{top_n} fibers by self_time (actual Ruby execution):"
top_busy.each_with_index do |t, i|
  st = t.methods.sum(&:self_time)
  puts format("    #%-2d  %-12s  self=%.4fs  total=%.4fs",
              i + 1, fiber_label(t), st, t.total_time)
end
puts

# ── 1. Custom aggregate flat ──────────────────────────────────────────────────
# Merge every method across every fiber: sum self/wait/calls, keep max total.
# %self = method_aggregate_self / profile_wall  (natural normalization for
# wall-time mode: "what fraction of the wall-clock did this code execute?").
agg = Hash.new { |h, k| h[k] = { self: 0.0, total: 0.0, wait: 0.0, calls: 0 } }
all_threads.each do |t|
  t.methods.each do |m|
    s = agg[m.full_name]
    s[:self]  += m.self_time
    s[:total] += m.total_time   # summed across fibers — shows cumulative load
    s[:wait]  += m.wait_time
    s[:calls] += m.called
  end
end

min_self = profile_wall * min_percent / 100.0
rows = agg.sort_by  { |_, s| -s[:self] }
          .select   { |_, s| s[:self] >= min_self }

puts "=== Aggregate flat  (#{all_threads.size} fibers merged, min_percent=#{min_percent}) ==="

row_fmt  = " %6.2f  %9.4f  %9.4f  %9.4f  %9.4f  %7d  %s\n"
head_fmt = " %-6s  %-9s  %-9s  %-9s  %-9s  %-7s  %s\n"
head_args = ["%self", "self", "total", "wait", "child", "calls", "name"]

File.open(flat_path, "w") do |f|
  f.puts "# pg_pipeline — aggregated flat profile"
  f.puts "# stamp=#{stamp}  measure=#{ENV.fetch('MEASURE', 'wall')}"
  f.puts "# pipeline_size=#{pipeline}  pinned_size=#{pinned}"
  f.puts "# fibers=#{fibers}  rounds=#{rounds}  result_fibers=#{all_threads.size}"
  f.puts "# min_percent=#{min_percent}  profile_wall=#{format('%.4f', profile_wall)}s"
  f.puts "#"
  f.puts "# Columns: self/total/wait are summed across all fibers for that method."
  f.puts "# child = total - self - wait (can differ from per-fiber view due to summing)."
  f.puts "# %self = method_self / profile_wall — fraction of wall-clock spent executing."
  f.puts "#"
  f.puts "# top #{top_n} busy fibers (see #{File.basename(top_path)} for detail):"
  top_busy.each_with_index do |t, i|
    st = t.methods.sum(&:self_time)
    f.puts format("#   #%-2d  %-12s  self=%.4fs  total=%.4fs", i + 1, fiber_label(t), st, t.total_time)
  end
  f.puts
  f.printf(head_fmt, *head_args)
  rows.each do |name, s|
    pct   = profile_wall.positive? ? s[:self] / profile_wall * 100.0 : 0.0
    child = [s[:total] - s[:self] - s[:wait], 0.0].max
    f.printf(row_fmt, pct, s[:self], s[:total], s[:wait], child, s[:calls], name)
  end
  f.puts
  f.puts "# #{rows.size} methods shown (>= #{min_percent}% of wall time)"
  f.puts "# #{agg.size} unique methods profiled across #{all_threads.size} fibers"
end

# Echo to STDOUT (same table, no file overhead)
printf(head_fmt, *head_args)
rows.each do |name, s|
  pct   = profile_wall.positive? ? s[:self] / profile_wall * 100.0 : 0.0
  child = [s[:total] - s[:self] - s[:wait], 0.0].max
  printf(row_fmt, pct, s[:self], s[:total], s[:wait], child, s[:calls], name)
end

# ── 2. Per-fiber flat for top-N busy fibers ───────────────────────────────────
File.open(top_path, "w") do |f|
  f.puts "# pg_pipeline — per-fiber flat, top #{top_n} fibers by self_time"
  f.puts "# stamp=#{stamp}  measure=#{ENV.fetch('MEASURE', 'wall')}  min_percent=#{min_percent}"
  f.puts "# Ranked by fiber self_time (actual Ruby execution, not wall duration)."
  f.puts "# Expected: ConnectionDriver owner_loop, reader_watcher, writer_watcher,"
  f.puts "# and the request fibers with the most pending work."

  top_busy.each_with_index do |thread, idx|
    total           = thread.total_time
    fiber_self_sum  = thread.methods.sum(&:self_time)
    threshold       = total.positive? ? total * min_percent / 100.0 : 0.0
    methods         = thread.methods
                            .sort_by { |m| -m.self_time }
                            .select  { |m| m.self_time >= threshold }

    f.puts
    f.puts "--- ##{idx + 1}  #{fiber_label(thread)}" \
           "  self=#{format('%.4f', fiber_self_sum)}s" \
           "  total=#{format('%.4f', total)}s" \
           "  methods_shown=#{methods.size} ---"
    f.puts "Measure Mode: #{ENV.fetch('MEASURE', 'wall')}_time"
    f.puts

    if methods.empty?
      f.puts "  (no methods above #{min_percent}% of this fiber's total_time)"
      next
    end

    f.printf(head_fmt, *head_args)
    methods.each do |m|
      pct   = total.positive? ? m.self_time / total * 100.0 : 0.0
      child = [m.total_time - m.self_time - m.wait_time, 0.0].max
      f.printf(row_fmt, pct, m.self_time, m.total_time, m.wait_time, child,
               m.called, m.full_name)
    end
  end
end

# ── 3. Full call graph HTML (all fibers, for drill-down) ──────────────────────
File.open(html_path, "w") do |f|
  RubyProf::GraphHtmlPrinter.new(result).print(f, min_percent: min_percent)
end

puts
puts "wrote #{flat_path}"
puts "wrote #{top_path}"
puts "wrote #{html_path}"
puts
puts "  flat  — #{rows.size} methods merged across #{all_threads.size} fibers, sorted by self_time"
puts "  top   — per-fiber detail for top #{top_n} busy fibers (self_time ranking)"
puts "  html  — full call graph, all #{all_threads.size} fibers (open in browser)"
