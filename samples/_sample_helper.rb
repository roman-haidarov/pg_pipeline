# frozen_string_literal: true

$stdout.sync = true
$stderr.sync = true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

begin
  require "async"
rescue LoadError
  warn "async is required for client-facing samples (Fiber::Scheduler host)"
  raise
end

begin
  require "pg_pipeline"
rescue LoadError => e
  warn "failed to require pg_pipeline: #{e.message}"
  warn "run from project root after compiling the extension:"
  warn "  bundle install && bundle exec rake compile"
  warn "  bundle exec ruby samples/<sample>.rb"
  raise
end

module PgPipelineSample
  module_function

  DEFAULT_NATIVE_GREP =
    "PgPipeline|pg_pipeline|Native|native_|" \
    "pp_|PQsend|PQpipeline|PQflush|PQconsume|PQisBusy|PQgetResult|" \
    "seal|dispatch|drain|SessionGuard|BoundedQueue|" \
    "owner_loop|pump_requests|process_event|select_driver|" \
    "block|unblock|io_wait|fiber"

  CANDIDATE_DATABASE_URLS = [
    "postgres://postgres:postgres@127.0.0.1:5417/postgres",
    "postgres://postgres:postgres@127.0.0.1:5418/postgres",
    "postgres://postgres:postgres@127.0.0.1:5416/postgres",
    "postgres://postgres:postgres@127.0.0.1:5414/postgres",
    "postgres://postgres:postgres@127.0.0.1:5432/postgres",
    "postgres://postgres@127.0.0.1:5432/postgres"
  ].freeze

  def monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def profile?
    ENV.fetch("PROFILE", "0") == "1"
  end

  def reps
    env_int("REPS", profile? ? 1 : 7)
  end

  def window
    if profile?
      Float(ENV.fetch("DURATION", ENV.fetch("WINDOW", "25.0")))
    else
      Float(ENV.fetch("WINDOW", "3.0"))
    end
  end

  def duration
    window
  end

  def sleep_before_hot_loop
    Float(ENV.fetch("SLEEP_BEFORE_HOT_LOOP", profile? ? "2.0" : "0.5"))
  end

  def deadline_batch
    env_int("BATCH", 64)
  end

  def preheat_iterations(default = 20)
    Integer(ENV.fetch("PREHEAT_ITERATIONS", default.to_s))
  end

  def disable_gc?
    ENV.fetch("DISABLE_GC", "1") != "0"
  end

  def env_int(name, default)
    Integer(ENV.fetch(name, default.to_s))
  end

  def env_string(name, default)
    ENV.fetch(name, default)
  end

  def database_url!
    @database_url ||= begin
      explicit = ENV["PG_PIPELINE_URL"]
      if explicit && !explicit.strip.empty?
        explicit
      else
        discover_database_url! || abort(<<~MSG)
          no PostgreSQL found for samples

          tried:
            #{CANDIDATE_DATABASE_URLS.join("\n  ")}

          start one from the repo:
            docker compose up -d pg17

          or point at any server (optional override):
            PG_PIPELINE_URL=postgres://user:pass@host:5432/db \\
              bundle exec ruby samples/client_query_hot_path.rb
        MSG
      end
    end
  end

  def discover_database_url!
    require "socket"

    CANDIDATE_DATABASE_URLS.each do |url|
      return url if postgres_accepting?(url)
    end
    nil
  end

  def postgres_accepting?(url)
    host, port = host_port_from_url(url)
    return false unless host && port

    Socket.tcp(host, port, connect_timeout: 0.25) { true }
  rescue StandardError
    false
  end

  def host_port_from_url(url)
    uri = url.sub(/\Apostgres(ql)?:\/\//, "")
    hostport = uri.split("/", 2).first.to_s
    hostport = hostport.split("@", 2).last
    host, port = hostport.split(":", 2)
    [host, Integer(port || 5432)]
  rescue ArgumentError
    [nil, nil]
  end

  def redact_url(url)
    url.to_s.sub(%r{://([^:/@]+):([^@/]+)@}, '://\1:***@')
  end

  def gc_snapshot
    stat = GC.stat
    {
      total_allocated_objects: stat.fetch(:total_allocated_objects),
      minor_gc_count: stat.fetch(:minor_gc_count),
      major_gc_count: stat.fetch(:major_gc_count)
    }
  end

  def gc_delta(before, after)
    before.each_with_object({}) do |(key, value), out|
      out[key] = after.fetch(key) - value
    end
  end

  def anchor_ns
    s = "pg_pipeline anchor"
    n = 2_000_000
    t0 = monotonic
    i = 0
    while i < n
      s.bytesize
      i += 1
    end
    (monotonic - t0) * 1e9 / n
  end

  def print_banner(sample_name:, call:, expected:, native_grep: DEFAULT_NATIVE_GREP, extra: {})
    sleep_s = sleep_before_hot_loop
    window_s = window
    sample_seconds = (window_s + 2).ceil
    sample_file = "/tmp/#{sample_name}.sample"
    txt_file = File.expand_path("results/#{sample_name}.txt", __dir__)
    txt_dir = File.dirname(txt_file)

    puts "pid=#{Process.pid}"
    puts "ruby=#{RUBY_DESCRIPTION}"
    puts "platform=#{RUBY_PLATFORM}"
    puts "mode=#{sample_name}"
    puts "call=#{call}"
    puts "pg_pipeline=#{PgPipeline::VERSION}"
    if defined?(PgPipeline::Native)
      versions = PgPipeline::Native.libpq_versions
      puts "libpq_native=#{versions[:native]} libpq_ruby_pg=#{versions[:ruby_pg]}"
    end
    extra.each { |key, value| puts "#{key}=#{value}" }
    puts "profile=#{profile?}"
    puts "reps=#{reps}"
    puts "window=#{window_s}"
    puts "batch=#{deadline_batch}"
    puts "sleep_before_hot_loop=#{sleep_s}"
    puts "disable_gc=#{disable_gc?}"
    puts "sample_seconds=#{sample_seconds}"
    puts "sample_file=#{sample_file}"
    puts "txt_file=#{txt_file}"
    puts
    if profile?
      puts "Copy this one-line macOS capture command in another console:"
      puts %(mkdir -p "#{txt_dir}"; OUT="#{txt_file}"; SAMPLE="#{sample_file}"; { sample #{Process.pid} #{sample_seconds} -f "$SAMPLE"; echo; echo "===== focused pg_pipeline/native symbols ====="; filtercalltree "$SAMPLE" | grep -E "#{native_grep}" | head -320; echo; echo "===== filtercalltree head -320 ====="; filtercalltree "$SAMPLE" | head -320; } 2>&1 | tee "$OUT")
      puts
      puts "Optional Linux perf command:"
      puts %(perf record -F 997 -g -p #{Process.pid} -- sleep #{sample_seconds}; perf report --stdio | head -240)
      puts
    end
    puts "Expected hot symbols (fact path, not a guarantee):"
    expected.each { |line| puts "  #{line}" }
    puts
    puts "sleep=#{sleep_s} seconds before hot loop"
    puts
  end

  def params_ring(width, size: 256)
    raise ArgumentError, "width must be >= 0" if width.negative?

    return nil if width.zero?

    Array.new(size) do |i|
      Array.new(width) do |j|
        (j.zero? ? "parameter-value-#{i}-#{"x" * 24}" : "parameter-value-#{j}-#{"x" * 24}").freeze
      end.freeze
    end.freeze
  end

  def warm_stable_pct
    Float(ENV.fetch("WARM_STABLE_PCT", "2.0"))
  end

  def max_warmup_reps
    env_int("MAX_WARMUP_REPS", 8)
  end

  def alloc_budget
    env_int("ALLOC_BUDGET", 20_000_000)
  end

  def run_hot_loop
    run_timed_reps { |i| yield(i) }
  end

  def run_hot_loop_in_scheduler
    run_timed_reps { |i| yield(i) }
  end

  def timed_window_ops
    batch = deadline_batch
    before_gc = gc_snapshot
    count = 0
    last = nil
    started = monotonic
    deadline = started + window

    while monotonic < deadline
      i = 0
      while i < batch
        last = yield(count)
        count += 1
        i += 1
      end
    end

    elapsed = monotonic - started
    delta = gc_delta(before_gc, gc_snapshot)
    ops = count / [elapsed, 1e-9].max
    alloc = delta[:total_allocated_objects].to_f / [count, 1].max
    [{ops: ops, alloc: alloc, count: count, elapsed: elapsed, gc: delta}, last]
  end

  # Single implementation of "discard windows until two consecutive ones agree
  # within warm_stable_pct, then keep `reps` measurements". Shared, because
  # native_dispatch times a batch rather than a per-iteration block and needs the
  # same warm-up rule -- duplicating it there is how the two drift apart.
  #
  # The block returns whatever the caller wants collected; `rate` extracts the
  # comparable Float from it (identity by default).
  # Returns [values, warmup_discarded].
  def collect_stable_series(rate: nil)
    return [[yield], 0] if profile?

    to_rate = rate || ->(value) { value }
    values = []
    discarded = 0
    prev = nil
    stable = false

    max_warmup_reps.times do
      value = yield
      current = to_rate.call(value).to_f

      if prev && prev.positive? && ((current - prev).abs / prev) * 100.0 <= warm_stable_pct
        values << value
        stable = true
        break
      end

      prev = current
      discarded += 1
    end

    values << yield unless stable
    (reps - values.length).times { values << yield }
    [values, discarded]
  end

  def run_timed_reps(&block)
    GC.start
    GC.disable if disable_gc?
    sleep sleep_before_hot_loop

    puts "anchor_ns_before=#{format('%.3f', anchor_ns)}"
    puts "HOT_LOOP_START"

    last = nil
    windows = 0
    gc_auto_enabled = false
    gc_projected = nil

    results, warmup_discarded = collect_stable_series(rate: ->(row) { row[:ops] }) do
      row, this_last = timed_window_ops(&block)
      last = this_last

      # The first window doubles as an allocation probe: it is always discarded
      # (nothing to compare it against yet), so it is the cheapest place to learn
      # what this sample costs in objects. With GC disabled a heavy sample
      # retains everything it allocates and slides into memory pressure, which
      # shows up as decaying ops/s, a warm-up loop that never converges, and an
      # anchor that drifts -- the allocator, not the gem.
      if windows.zero? && disable_gc? && !gc_auto_enabled
        projected = row[:alloc] * row[:ops] * window * (max_warmup_reps + reps)
        if projected > alloc_budget
          GC.enable
          GC.start
          gc_auto_enabled = true
          gc_projected = projected
        end
      end
      windows += 1

      row
    end

    puts "anchor_ns_after=#{format('%.3f', anchor_ns)}"
    unless profile?
      puts "warmup_reps_discarded=#{warmup_discarded}"
      puts "warmup_converged=#{warmup_discarded < max_warmup_reps}"
      if gc_auto_enabled
        puts "gc_auto_enabled=true"
        puts "gc_projected_objects=#{gc_projected.round}"
      end
    end
    report_reps(results, last)
    last
  ensure
    GC.enable
  end

  def report_rate_series(raw, prefix:)
    sorted = raw.sort
    med = sorted[sorted.length / 2]
    mad = raw.map { |o| (o - med).abs }.sort[raw.length / 2]
    mad_pct = med.positive? ? (100.0 * mad / med) : 0.0
    spread = med.positive? ? ((sorted.last - sorted.first) / med) : 0.0
    drift = med.positive? ? ((raw.last - raw.first) / med) : 0.0

    puts "#{prefix}_runs=#{raw.map { |o| o.round }.join(',')}"
    puts "#{prefix}_min=#{sorted.first.round}"
    puts "#{prefix}_max=#{sorted.last.round}"
    puts "#{prefix}_spread_pct=#{format('%.1f', 100.0 * spread)}"
    puts "#{prefix}_drift_pct=#{format('%.1f', 100.0 * drift)}"
    puts "#{prefix}_mad_pct=#{format('%.1f', mad_pct)}"

    if profile?
      puts "#{prefix}_unreliable=#{format('%.6f', med)}"
    else
      puts "#{prefix}_median=#{format('%.6f', med)}"
    end

    med
  end

  def report_reps(results, last)
    raw = results.map { |r| r[:ops] }
    allocs = results.map { |r| r[:alloc] }
    total_count = results.sum { |r| r[:count] }
    total_elapsed = results.sum { |r| r[:elapsed] }

    puts "count=#{total_count}"
    puts "elapsed=#{format('%.6f', total_elapsed)}"
    puts "reps_kept=#{results.length}"
    med = report_rate_series(raw, prefix: "ops_per_sec")
    puts "alloc_per_op=#{format('%.3f', allocs.min)}"
    puts "alloc_per_op_runs=#{allocs.map { |a| format('%.3f', a) }.join(',')}"
    puts "sec_per_op=#{format('%.9f', 1.0 / [med, 1e-9].max)}"

    puts "last_result_class=#{last.class if last}"
    if last.respond_to?(:bytesize)
      puts "last_result_bytes=#{last.bytesize}"
    elsif last.respond_to?(:ntuples) && !(last.respond_to?(:cleared?) && last.cleared?)
      puts "last_result_ntuples=#{last.ntuples}"
    end
    puts "gc_delta=#{results.last[:gc]}" if results.any?
    puts "disable_gc=#{disable_gc?}"
    puts "profile=#{profile?}"
  end

  def with_client(**opts)
    url = database_url!
    Sync do
      client = PgPipeline::Client.new(
        url,
        pipeline_size: env_int("PIPELINE_SIZE", 1),
        pinned_size: env_int("PINNED_SIZE", 0),
        health_check: false,
        reconnect: false,
        **opts
      ).start
      begin
        yield client
      ensure
        client.close
      end
    end
  end
end
