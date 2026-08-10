# frozen_string_literal: true

require_relative "_sample_helper"

sample_name = "pg_pipeline_client_query_multiplex_hot_path"
sql = ENV.fetch("SQL", "SELECT 1").freeze
fibers = Integer(ENV.fetch("FIBERS", "64"))
pipeline_size = Integer(ENV.fetch("PIPELINE_SIZE", "2"))
preheat = PgPipelineSample.preheat_iterations(20)
raise "FIBERS must be >= 1" if fibers < 1

url = PgPipelineSample.database_url!

Sync do |task|
  client = PgPipeline::Client.new(
    url,
    pipeline_size: pipeline_size,
    pinned_size: 0,
    health_check: false,
    reconnect: false
  ).start

  begin
    preheat.times { client.query(sql).clear }

    PgPipelineSample.print_banner(
      sample_name: sample_name,
      call: "#{fibers} fibers × client.query(#{sql.inspect}) on pipeline_size=#{pipeline_size}",
      extra: {
        url: PgPipelineSample.redact_url(url),
        fibers: fibers,
        pipeline_size: pipeline_size,
        preheat_iterations: preheat
      },
      expected: [
        "PoolOps.select_driver_into / load-based RR",
        "in_flight peak toward max_in_flight",
        "PQflush coalescing under concurrent submit",
        "consume_and_drain multi-unit wakeups"
      ]
    )

    GC.start
    GC.disable if PgPipelineSample.disable_gc?
    sleep PgPipelineSample.sleep_before_hot_loop
    puts "anchor_ns_before=#{format('%.3f', PgPipelineSample.anchor_ns)}"
    puts "HOT_LOOP_START"

    measure = lambda do
      before_gc = PgPipelineSample.gc_snapshot
      started = PgPipelineSample.monotonic
      deadline = started + PgPipelineSample.window

      workers = fibers.times.map do
        task.async do
          local = 0
          while PgPipelineSample.monotonic < deadline
            result = client.query(sql)
            result.clear
            local += 1
          end
          local
        end
      end
      counts = workers.map(&:wait)
      elapsed = PgPipelineSample.monotonic - started
      count = counts.sum
      delta = PgPipelineSample.gc_delta(before_gc, PgPipelineSample.gc_snapshot)
      ops = count / [elapsed, 1e-9].max
      alloc = delta[:total_allocated_objects].to_f / [count, 1].max
      {ops: ops, alloc: alloc, count: count, elapsed: elapsed, gc: delta}
    end

    results = []
    warmup_discarded = 0
    if PgPipelineSample.profile?
      results << measure.call
    else
      prev_ops = nil
      stable = false
      PgPipelineSample.max_warmup_reps.times do
        row = measure.call
        if prev_ops && prev_ops.positive?
          rel = ((row[:ops] - prev_ops).abs / prev_ops) * 100.0
          if rel <= PgPipelineSample.warm_stable_pct
            results << row
            stable = true
            break
          end
        end
        prev_ops = row[:ops]
        warmup_discarded += 1
      end
      results << measure.call unless stable
      (PgPipelineSample.reps - results.length).times { results << measure.call }
    end

    puts "anchor_ns_after=#{format('%.3f', PgPipelineSample.anchor_ns)}"
    puts "warmup_reps_discarded=#{warmup_discarded}" unless PgPipelineSample.profile?
    PgPipelineSample.report_reps(results, nil)
    puts "fibers=#{fibers}"
    puts "stats=#{client.stats.inspect}"
  ensure
    GC.enable
    client.close
  end
end
