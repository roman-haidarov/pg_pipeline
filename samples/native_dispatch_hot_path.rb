# frozen_string_literal: true

require_relative "_sample_helper"

sample_name = "pg_pipeline_native_dispatch_hot_path"
url = PgPipelineSample.database_url!
width = Integer(ENV.fetch("PARAMS", "2"))
batch = Integer(ENV.fetch("BATCH", "32"))
raise "BATCH must be >= 1" if batch < 1
raise "PARAMS must be >= 0" if width.negative?

sql =
  if width.zero?
    "SELECT 1".freeze
  else
    placeholders = Array.new(width) { |i| "$#{i + 1}::text" }.join(", ")
    "SELECT #{placeholders}".freeze
  end

params =
  if width.zero?
    nil
  else
    Array.new(width) { |i| "parameter-value-#{i}-#{"x" * 24}" }.map(&:freeze)
  end

def connect!(url, capacity)
  core = PgPipeline::Native::Driver.new(url, capacity)
  socket = nil
  loop do
    socket ||= begin
      IO.for_fd(core.socket, autoclose: false)
    rescue PgPipeline::ConnectionLostError
      nil
    end
    case core.connect_poll
    when :ok then break
    when :reading then socket&.wait_readable(5) || abort("connect read timeout")
    when :writing then socket&.wait_writable(5) || abort("connect write timeout")
    when :failed then abort("connect failed: #{core.error_message}")
    when :active then Fiber.scheduler&.yield || sleep(0.001)
    end
  end
  core.enter_pipeline_mode
  [core, socket || IO.for_fd(core.socket, autoclose: false)]
end

def seal_batch(sql, params, size)
  Array.new(size) do
    request = PgPipeline::Request.build(sql, params)
    request.queued!
    request
  end
end

def settle!(core, socket, requests)
  socket.wait_writable(5) until core.flush
  until requests.all?(&:settled?)
    socket.wait_readable(5) || abort("drain read timeout")
    core.consume_and_drain
  end
  requests.each do |request|
    request.result&.clear
  end
end

Sync do
  core, socket = connect!(url, batch)
  preheat = PgPipelineSample.preheat_iterations(5)

  preheat.times do
    batch_requests = seal_batch(sql, params, batch)
    batch_requests.each { |request| core.dispatch(request) }
    settle!(core, socket, batch_requests)
  end

  PgPipelineSample.print_banner(
    sample_name: sample_name,
    call: "Native::Driver#dispatch(request)  # batch=#{batch}, settle outside timer",
    native_grep: "dispatch|PQsend|PQpipeline|PQflush|pp_send|pp_pipeline|pp_driver_dispatch|seal|consume|PQgetResult",
    extra: {
      url: PgPipelineSample.redact_url(url),
      params: width,
      batch: batch,
      preheat_iterations: preheat
    },
    expected: [
      "pp_driver_dispatch → pp_send_query (PQsendQueryParams|Prepare|QueryPrepared)",
      "pp_pipeline_sync → PQsendPipelineSync (libpq>=17) or PQpipelineSync",
      "no rb_funcall into Request SQL/params (sealed arena only)",
      "flush/consume_and_drain appear between batches, outside the timed dispatch window"
    ]
  )

  GC.start
  GC.disable if PgPipelineSample.disable_gc?
  sleep PgPipelineSample.sleep_before_hot_loop
  puts "anchor_ns_before=#{format('%.3f', PgPipelineSample.anchor_ns)}"
  puts "HOT_LOOP_START"

  last_gc = nil
  total_dispatches = 0
  measure = lambda do
    before_gc = PgPipelineSample.gc_snapshot
    dispatches = 0
    dispatch_elapsed = 0.0
    started = PgPipelineSample.monotonic
    deadline = started + PgPipelineSample.window

    while PgPipelineSample.monotonic < deadline
      batch_requests = seal_batch(sql, params, batch)
      t0 = PgPipelineSample.monotonic
      batch_requests.each { |request| core.dispatch(request) }
      dispatch_elapsed += PgPipelineSample.monotonic - t0
      dispatches += batch_requests.size
      settle!(core, socket, batch_requests)
    end

    total_dispatches += dispatches
    last_gc = PgPipelineSample.gc_delta(before_gc, PgPipelineSample.gc_snapshot)
    dispatches / [dispatch_elapsed, 1e-9].max
  end

  rates, warmup_discarded = PgPipelineSample.collect_stable_series { measure.call }

  puts "anchor_ns_after=#{format('%.3f', PgPipelineSample.anchor_ns)}"
  puts "warmup_reps_discarded=#{warmup_discarded}" unless PgPipelineSample.profile?
  puts "count=#{total_dispatches}"
  med = PgPipelineSample.report_rate_series(rates, prefix: "dispatch_per_sec")
  puts "us_per_dispatch=#{format('%.3f', 1_000_000.0 / [med, 1e-9].max)}"
  puts "gc_delta=#{last_gc.inspect}"
  puts "driver_stats=#{core.stats.inspect}"
  puts "profile=#{PgPipelineSample.profile?}"
  GC.enable

  core.exit_pipeline_mode
  core.close
end
