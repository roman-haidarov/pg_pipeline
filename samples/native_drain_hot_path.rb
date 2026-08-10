# frozen_string_literal: true

require_relative "_sample_helper"

sample_name = "pg_pipeline_native_drain_hot_path"
url = PgPipelineSample.database_url!
batch = Integer(ENV.fetch("BATCH", "64"))
sql = ENV.fetch("SQL", "SELECT 1").freeze
raise "BATCH must be >= 1" if batch < 1

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

def seal_batch(sql, size)
  Array.new(size) do
    request = PgPipeline::Request.build(sql, nil)
    request.queued!
    request
  end
end

Sync do
  core, socket = connect!(url, batch)
  preheat = PgPipelineSample.preheat_iterations(5)

  fill = lambda do
    requests = seal_batch(sql, batch)
    requests.each { |request| core.dispatch(request) }
    socket.wait_writable(5) until core.flush
    requests
  end

  preheat.times do
    requests = fill.call
    until requests.all?(&:settled?)
      socket.wait_readable(5)
      core.consume_and_drain
    end
    requests.each { |request| request.result&.clear }
  end

  PgPipelineSample.print_banner(
    sample_name: sample_name,
    call: "Native::Driver#consume_and_drain after dispatch+flush of batch=#{batch}",
    native_grep: "consume_and_drain|PQconsume|PQisBusy|PQgetResult|pp_driver_drain|pp_request_finish|unblock|readable",
    extra: {
      url: PgPipelineSample.redact_url(url),
      batch: batch,
      preheat_iterations: preheat
    },
    expected: [
      "PQconsumeInput on each readable wake",
      "loop PQisBusy / PQgetResult until busy or FIFO empty",
      "PGRES_TUPLES_OK / COMMAND_OK → Result wrap; PGRES_PIPELINE_SYNC → finish+unblock",
      "units_completed / results_read counters in C"
    ]
  )

  GC.start
  GC.disable if PgPipelineSample.disable_gc?
  sleep PgPipelineSample.sleep_before_hot_loop
  puts "anchor_ns_before=#{format('%.3f', PgPipelineSample.anchor_ns)}"
  puts "HOT_LOOP_START"

  last_meta = nil
  drain_reps = PgPipelineSample.profile? ? PgPipelineSample.reps : Integer(ENV.fetch("REPS", "15"))
  measure = lambda do
    before_gc = PgPipelineSample.gc_snapshot
    batches = 0
    units = 0
    drain_calls = 0
    drain_elapsed = 0.0
    started = PgPipelineSample.monotonic
    deadline = started + PgPipelineSample.window

    while PgPipelineSample.monotonic < deadline
      requests = fill.call
      t0 = PgPipelineSample.monotonic
      until requests.all?(&:settled?)
        socket.wait_readable(5) || abort("drain read timeout")
        core.consume_and_drain
        drain_calls += 1
      end
      drain_elapsed += PgPipelineSample.monotonic - t0
      units += requests.size
      batches += 1
      requests.each { |request| request.result&.clear }
    end

    last_meta = {
      batches: batches,
      units: units,
      drain_calls: drain_calls,
      drain_calls_per_batch: drain_calls.to_f / [batches, 1].max,
      gc: PgPipelineSample.gc_delta(before_gc, PgPipelineSample.gc_snapshot)
    }
    units / [drain_elapsed, 1e-9].max
  end

  rates = []
  warmup_discarded = 0
  if PgPipelineSample.profile?
    rates << measure.call
  else
    prev = nil
    stable = false
    PgPipelineSample.max_warmup_reps.times do
      rate = measure.call
      if prev && prev.positive?
        rel = ((rate - prev).abs / prev) * 100.0
        if rel <= PgPipelineSample.warm_stable_pct
          rates << rate
          stable = true
          break
        end
      end
      prev = rate
      warmup_discarded += 1
    end
    rates << measure.call unless stable
    (drain_reps - rates.length).times { rates << measure.call }
  end

  puts "anchor_ns_after=#{format('%.3f', PgPipelineSample.anchor_ns)}"
  puts "warmup_reps_discarded=#{warmup_discarded}" unless PgPipelineSample.profile?
  puts "batches=#{last_meta[:batches]}"
  puts "units=#{last_meta[:units]}"
  puts "drain_calls=#{last_meta[:drain_calls]}"
  PgPipelineSample.report_rate_series(rates, prefix: "units_per_sec")
  puts "drain_calls_per_batch=#{format('%.3f', last_meta[:drain_calls_per_batch])}"
  puts "gc_delta=#{last_meta[:gc].inspect}"
  puts "driver_stats=#{core.stats.inspect}"
  puts "profile=#{PgPipelineSample.profile?}"
  GC.enable

  core.exit_pipeline_mode
  core.close
end
