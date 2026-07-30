# frozen_string_literal: true

# A/B: pg_pipeline (few connections + pipeline) vs naive connection-pool baseline
# under the same concurrency. Point at a latency_proxy endpoint to see RTT wins.
#
#   # terminal 1
#   bundle exec rake bench:proxy RTT_MS=10 UPSTREAM=127.0.0.1:5417
#   # terminal 2
#   PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:6432/postgres \
#     CONCURRENCY=500 PIPELINE_SIZE=4 BASELINE_POOL=32 \
#     bundle exec rake bench:ab

require "async"
require "async/queue"
require "pg"
require_relative "common"

url = BenchKit.require_url!
concurrency = Integer(ENV.fetch("CONCURRENCY", ENV.fetch("FIBERS", "500")))
queries = Integer(ENV.fetch("QUERIES", "5"))
pipeline = Integer(ENV.fetch("PIPELINE_SIZE", "4"))
baseline_pool = Integer(ENV.fetch("BASELINE_POOL", "32"))
sql = ENV.fetch("SQL", "SELECT 1")

def bench_pipeline(task, url, concurrency, queries, pipeline, sql)
  app = "pgp_bench_pipeline"
  client_url = BenchKit.url_with_app(url, app)
  client = PgPipeline::Client.new(
    client_url,
    pipeline_size: pipeline,
    pinned_size: 1,
    health_check: false
  ).start(parent: task)

  total = concurrency * queries
  lat = Array.new(total)
  peak = 0
  idx = 0
  mutex = Mutex.new

  probe = task.async do
    loop do
      peak = [peak, BenchKit.app_conn_count(url, app)].max
      task.sleep(0.05)
    end
  end

  t0 = BenchKit.now
  concurrency.times.map do
    task.async do
      queries.times do
        i = mutex.synchronize { idx += 1; idx - 1 }
        s = BenchKit.now
        client.query(sql)
        lat[i] = (BenchKit.now - s) * 1000.0
      end
    end
  end.each(&:wait)
  wall = BenchKit.now - t0
  probe.stop
  client.close
  BenchKit.report_latencies("pipeline", lat, wall, "server_conns" => peak, "pool" => pipeline)
end

def bench_baseline(task, url, concurrency, queries, baseline_pool, sql)
  app = "pgp_bench_baseline"
  client_url = BenchKit.url_with_app(url, app)
  pool = baseline_pool.times.map { PG::Connection.new(client_url) }
  free = Async::Queue.new
  pool.each { |c| free.enqueue(c) }

  total = concurrency * queries
  lat = Array.new(total)
  peak = 0
  idx = 0
  mutex = Mutex.new

  probe = task.async do
    loop do
      peak = [peak, BenchKit.app_conn_count(url, app)].max
      task.sleep(0.05)
    end
  end

  t0 = BenchKit.now
  concurrency.times.map do
    task.async do
      queries.times do
        i = mutex.synchronize { idx += 1; idx - 1 }
        s = BenchKit.now
        conn = free.dequeue
        begin
          conn.exec_params(sql, [])
        ensure
          free.enqueue(conn)
        end
        lat[i] = (BenchKit.now - s) * 1000.0
      end
    end
  end.each(&:wait)
  wall = BenchKit.now - t0
  probe.stop
  pool.each(&:close)
  BenchKit.report_latencies("baseline", lat, wall, "server_conns" => peak, "pool" => baseline_pool)
end

puts "A/B: pipeline vs baseline"
puts "url=#{BenchKit.redact_url(url)}"
puts "concurrency=#{concurrency} queries/fiber=#{queries} pipeline_size=#{pipeline} baseline_pool=#{baseline_pool}"
puts "(use rake bench:proxy + proxy URL to see RTT amortization)"
puts

Sync do |task|
  # small warmup
  warm = PgPipeline::Client.new(url, pipeline_size: 1, pinned_size: 0, health_check: false).start(parent: task)
  3.times { warm.query(sql) }
  warm.close

  bench_pipeline(task, url, concurrency, queries, pipeline, sql)
  bench_baseline(task, url, concurrency, queries, baseline_pool, sql)
end
