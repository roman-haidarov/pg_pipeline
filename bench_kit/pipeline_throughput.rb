# frozen_string_literal: true

require "async"
require_relative "common"

url = BenchKit.require_url!
fibers = Integer(ENV.fetch("FIBERS", "2000"))
pipeline = Integer(ENV.fetch("PIPELINE_SIZE", "4"))
queries_per_fiber = Integer(ENV.fetch("QUERIES_PER_FIBER", "32"))
sql = ENV.fetch("SQL", "SELECT 1")
prepared = ENV["PREPARED"] == "1"

puts "throughput bench"
puts "url=#{BenchKit.redact_url(url)} fibers=#{fibers} pipeline_size=#{pipeline} " \
     "queries_per_fiber=#{queries_per_fiber} prepared=#{prepared}"
puts

Sync do |task|
  client = PgPipeline::Client.new(url, pipeline_size: pipeline, pinned_size: 1, health_check: false).start
  statement = client.prepare("bench_throughput", sql) if prepared
  total = fibers * queries_per_fiber
  latencies = Array.new(total)
  t0 = BenchKit.now

  fibers.times.map do |f|
    task.async do
      base = f * queries_per_fiber
      queries_per_fiber.times do |q|
        s = BenchKit.now
        result = prepared ? statement.query : client.query(sql)
        result.clear
        latencies[base + q] = (BenchKit.now - s) * 1000.0
      end
    end
  end.each(&:wait)

  wall = BenchKit.now - t0
  BenchKit.report_latencies("pipeline", latencies, wall, "pipeline_size" => pipeline,
                                                        "queries_per_fiber" => queries_per_fiber)
  puts "stats=#{client.stats}"
  client.close
end
