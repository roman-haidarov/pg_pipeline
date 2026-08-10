# frozen_string_literal: true

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
prepared = ENV["PREPARED"] == "1"

def bench_pipeline(task, url, concurrency, queries, pipeline, sql, prepared)
  app = "pgp_bench_pipeline"
  client_url = BenchKit.url_with_app(url, app)
  client = PgPipeline::Client.new(
    client_url,
    pipeline_size: pipeline,
    pinned_size: 1,
    health_check: false
  ).start
  statement = client.prepare("bench_ab", sql) if prepared

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
        result = prepared ? statement.query : client.query(sql)
        result.clear
        lat[i] = (BenchKit.now - s) * 1000.0
      end
    end
  end.each(&:wait)
  wall = BenchKit.now - t0
  probe.stop
  BenchKit.report_latencies("pipeline", lat, wall, "server_conns" => peak, "pool" => pipeline)
  client.close
end

def bench_baseline(task, url, concurrency, queries, baseline_pool, sql, prepared)
  app = "pgp_bench_baseline"
  client_url = BenchKit.url_with_app(url, app)
  pool = baseline_pool.times.map { PG::Connection.new(client_url) }
  pool.each { |conn| conn.prepare("bench_ab", sql).clear } if prepared
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
          result = prepared ? conn.exec_prepared("bench_ab", []) : conn.exec_params(sql, [])
          result.clear
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
puts "concurrency=#{concurrency} queries/fiber=#{queries} pipeline_size=#{pipeline} " \
     "baseline_pool=#{baseline_pool} prepared=#{prepared}"
puts "(use rake bench:proxy + proxy URL to see RTT amortization)"
puts

Sync do |task|
  warm = PgPipeline::Client.new(url, pipeline_size: 1, pinned_size: 0, health_check: false).start
  3.times { warm.query(sql).clear }
  warm.close

  bench_pipeline(task, url, concurrency, queries, pipeline, sql, prepared)
  bench_baseline(task, url, concurrency, queries, baseline_pool, sql, prepared)
end
