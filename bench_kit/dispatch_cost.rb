# frozen_string_literal: true

# Isolated send-path cost: request encoding + PQsend* + Sync, and nothing else.
#
# An earlier version of this bench sized the in-flight FIFO at COUNT + 16 and
# never flushed or drained, so 50,000 units accumulated in libpq's output
# buffer. That measured memcpy into a buffer growing into the tens of megabytes,
# with its reallocations folded into the per-dispatch number, at a FIFO depth no
# real driver ever reaches. The run below keeps the FIFO at a production-shaped
# depth, flushes each batch to the socket and drains it before the next one, and
# accumulates only the time spent inside #dispatch -- flush and drain are
# excluded from the reported cost but are actually performed, so the output
# buffer stays bounded.

require_relative "common"

URL = ENV.fetch("PG_PIPELINE_URL") { abort "set PG_PIPELINE_URL" }
COUNT = Integer(ENV.fetch("COUNT", "50000"))
BATCH = Integer(ENV.fetch("BATCH", "64"))
WIDTHS = ENV.fetch("PARAMS", "0,1,2,8").split(",").map { |value| Integer(value) }

def connect(capacity)
  core = PgPipeline::Native::Driver.new(URL, capacity)
  socket = nil

  loop do
    socket ||= begin
      IO.for_fd(core.socket, autoclose: false)
    rescue PgPipeline::ConnectionLostError
      nil
    end

    case core.connect_poll
    when :ok then break
    when :reading then socket&.wait_readable(5)
    when :writing then socket&.wait_writable(5)
    when :failed then abort "connect failed: #{core.error_message}"
    end
  end

  core.enter_pipeline_mode
  [core, IO.for_fd(core.socket, autoclose: false)]
end

def build_batch(sql, params, size)
  Array.new(size) do
    request = PgPipeline::Request.build(sql, params)
    request.queued!
    request
  end
end

def sql_for(width)
  return "SELECT 1" if width.zero?

  "SELECT #{Array.new(width) { |i| "$#{i + 1}::text" }.join(", ")}"
end

# Push the batch all the way out and consume its results, so the next batch
# starts from an empty output buffer and an empty FIFO.
def settle(core, socket, requests)
  socket.wait_writable(5) until core.flush
  until requests.all?(&:settled?)
    socket.wait_readable(5)
    core.consume_and_drain
  end
  requests.each { |request| request.result&.clear }
end

def measure(width)
  core, socket = connect(BATCH)
  sql = sql_for(width)
  params = Array.new(width) { |i| "parameter-value-#{i}-#{"x" * 24}" }

  batches = Array.new((COUNT + BATCH - 1) / BATCH) { build_batch(sql, params, BATCH) }
  dispatched = batches.sum(&:size)

  GC.start
  GC.disable
  before = GC.stat(:total_allocated_objects)
  elapsed = 0.0

  batches.each do |batch|
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    batch.each { |request| core.dispatch(request) }
    elapsed += Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    settle(core, socket, batch)
  end

  objects = GC.stat(:total_allocated_objects) - before
  GC.enable

  stats = core.stats
  core.close

  {
    params: width,
    per_second: (dispatched / elapsed).round(0),
    microseconds: (elapsed / dispatched * 1_000_000).round(3),
    objects_per_unit: (objects.to_f / dispatched).round(2),
    bytes_dispatched: (stats.fetch(:bytes_dispatched).to_f / dispatched).round(1)
  }
end

# The zero-allocation claim is about #dispatch specifically, so measure it where
# nothing else runs: one bounded batch, no flush, no drain.
def dispatch_only_objects(width)
  core, = connect(BATCH)
  requests = build_batch(sql_for(width),
                         Array.new(width) { |i| "parameter-value-#{i}-#{"x" * 24}" },
                         BATCH)

  GC.start
  GC.disable
  before = GC.stat(:total_allocated_objects)
  requests.each { |request| core.dispatch(request) }
  objects = GC.stat(:total_allocated_objects) - before
  GC.enable
  core.close
  (objects.to_f / BATCH).round(2)
end

puts "dispatch cost, #{COUNT} units per row, flushed and drained every #{BATCH}"
puts format("%-8s %12s %14s %18s %18s %22s", "params", "dispatch/s", "us/dispatch",
            "objects/dispatch", "objects/unit e2e", "sealed bytes/dispatch")
WIDTHS.each do |width|
  isolated = dispatch_only_objects(width)
  row = measure(width)
  puts format("%-8d %12d %14.3f %18.2f %18.2f %22.1f", row[:params], row[:per_second],
              row[:microseconds], isolated, row[:objects_per_unit], row[:bytes_dispatched])
end

puts
puts "objects/dispatch is #dispatch in isolation. objects/unit e2e additionally"
puts "covers the Request, its sealed arena, the drained Result and the flush/drain"
puts "loop, so it is the number to compare against an end-to-end query."
