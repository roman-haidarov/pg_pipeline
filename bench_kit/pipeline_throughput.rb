# frozen_string_literal: true

# Throughput / latency under N concurrent fibers on pipeline_size connections.
#
#   PG_PIPELINE_URL=... FIBERS=2000 PIPELINE_SIZE=4 bundle exec rake bench:throughput

require "async"
require_relative "common"

url = BenchKit.require_url!
fibers = Integer(ENV.fetch("FIBERS", "2000"))
pipeline = Integer(ENV.fetch("PIPELINE_SIZE", "4"))
sql = ENV.fetch("SQL", "SELECT 1")

puts "throughput bench"
puts "url=#{BenchKit.redact_url(url)} fibers=#{fibers} pipeline_size=#{pipeline}"
puts

Sync do |task|
  client = PgPipeline::Client.new(url, pipeline_size: pipeline, pinned_size: 1, health_check: false)
    .start(parent: task)
  latencies = Array.new(fibers)
  t0 = BenchKit.now

  fibers.times.map do |i|
    task.async do
      s = BenchKit.now
      client.query(sql)
      latencies[i] = (BenchKit.now - s) * 1000.0
    end
  end.each(&:wait)

  wall = BenchKit.now - t0
  BenchKit.report_latencies("pipeline", latencies, wall, "pipeline_size" => pipeline)
  puts "stats=#{client.stats}"
  client.close
end
