#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "open3"
require "rbconfig"
require "socket"
require "time"

ROOT = File.expand_path("..", __dir__)
BENCH_DIR = File.join(ROOT, "bench_kit")
RESULTS_DIR = File.join(__dir__, "results")
OUT_PATH = File.join(RESULTS_DIR, "all_benchmarks.txt")

CANDIDATE_DATABASE_URLS = [
  "postgres://postgres:postgres@127.0.0.1:5417/postgres",
  "postgres://postgres:postgres@127.0.0.1:5418/postgres",
  "postgres://postgres:postgres@127.0.0.1:5416/postgres",
  "postgres://postgres:postgres@127.0.0.1:5414/postgres",
  "postgres://postgres:postgres@127.0.0.1:5432/postgres",
  "postgres://postgres@127.0.0.1:5432/postgres"
].freeze

BENCHMARKS = [
  {
    id: "rtt_demo",
    title: "RTT amortization (no PostgreSQL)",
    script: "rtt_amortization_demo.rb",
    needs_pg: false
  },
  {
    id: "throughput",
    title: "Pipeline throughput (ad-hoc SQL)",
    script: "pipeline_throughput.rb",
    needs_pg: true,
    env: {"PREPARED" => "0"}
  },
  {
    id: "throughput_prepared",
    title: "Pipeline throughput (prepared)",
    script: "pipeline_throughput.rb",
    needs_pg: true,
    env: {"PREPARED" => "1"}
  },
  {
    id: "ab",
    title: "Pipeline vs baseline A/B",
    script: "pipeline_vs_baseline.rb",
    needs_pg: true
  },
  {
    id: "smoke",
    title: "Multi-worker connection smoke",
    script: "multiworker_smoke.rb",
    needs_pg: true
  },
  {
    id: "dispatch",
    title: "Isolated dispatch cost",
    script: "dispatch_cost.rb",
    needs_pg: true
  },
  {
    id: "metrics",
    title: "Full metrics (RubyProf + StackProf + alloc)",
    script: "metrics.rb",
    needs_pg: true
  },
  {
    id: "profile",
    title: "RubyProf CI-shaped scenario",
    script: "profile_ci_scenario.rb",
    needs_pg: true,
    skip_if: -> { ENV["SKIP_PROFILE"] == "1" }
  }
].freeze

def host_port_from_url(url)
  uri = url.sub(/\Apostgres(ql)?:\/\//, "")
  hostport = uri.split("/", 2).first.to_s.split("@", 2).last
  host, port = hostport.split(":", 2)
  [host, Integer(port || 5432)]
rescue ArgumentError
  [nil, nil]
end

def postgres_accepting?(url)
  host, port = host_port_from_url(url)
  return false unless host && port

  Socket.tcp(host, port, connect_timeout: 0.25) { true }
rescue StandardError
  false
end

def discover_database_url
  explicit = ENV["PG_PIPELINE_URL"]
  return explicit if explicit && !explicit.strip.empty?

  CANDIDATE_DATABASE_URLS.find { |url| postgres_accepting?(url) }
end

def selected_benchmarks
  list = BENCHMARKS.reject { |b| b[:skip_if]&.call }
  if ENV["ONLY"] && !ENV["ONLY"].strip.empty?
    want = ENV["ONLY"].split(",").map(&:strip)
    list = list.select { |b| want.include?(b[:id]) }
  end
  list
end

def run_bench(entry, env)
  script = File.join(BENCH_DIR, entry[:script])
  unless File.file?(script)
    return {id: entry[:id], ok: false, error: "missing #{script}", output: "", seconds: 0.0}
  end

  child_env = env.merge(entry[:env] || {})
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  output, status = Open3.capture2e(child_env, RbConfig.ruby, script, chdir: ROOT)
  seconds = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

  {
    id: entry[:id],
    title: entry[:title],
    script: entry[:script],
    ok: status.success?,
    exitstatus: status.exitstatus,
    error: status.success? ? nil : "exit #{status.exitstatus}",
    output: output,
    seconds: seconds
  }
end

def write_report(runs, path, meta)
  FileUtils.mkdir_p(File.dirname(path))
  File.open(path, "w") do |io|
    io.puts "# pg_pipeline bench_kit suite"
    io.puts "generated_at=#{Time.now.utc.iso8601}"
    io.puts "ruby=#{RUBY_DESCRIPTION}"
    io.puts "host=#{RbConfig::CONFIG["host"]}"
    io.puts "pwd=#{ROOT}"
    io.puts "pg_pipeline_url=#{meta[:url] || "(none — rtt_demo only)"}"
    io.puts "skip_profile=#{ENV["SKIP_PROFILE"] == "1"}"
    io.puts
    io.puts "## summary"
    io.puts
    io.puts format("%-22s  %-6s  %8s  %s", "bench", "status", "wall_s", "title")
    io.puts "-" * 72
    runs.each do |run|
      io.puts format(
        "%-22s  %-6s  %8.2f  %s",
        run[:id],
        run[:ok] ? "ok" : "FAIL",
        run[:seconds],
        run[:title]
      )
    end
    io.puts
    io.puts "passed=#{runs.count { |r| r[:ok] }}/#{runs.size}"
    io.puts

    runs.each do |run|
      io.puts "=" * 72
      io.puts "bench=#{run[:id]}"
      io.puts "title=#{run[:title]}"
      io.puts "script=bench_kit/#{run[:script]}"
      io.puts "status=#{run[:ok] ? "ok" : "FAIL"}"
      io.puts "wall_seconds=#{format("%.3f", run[:seconds])}"
      io.puts "error=#{run[:error]}" if run[:error]
      io.puts
      io.puts "### stdout+stderr"
      io.puts run[:output].to_s.rstrip
      io.puts
    end
  end
end

benchmarks = selected_benchmarks
url = discover_database_url
needs_pg = benchmarks.any? { |b| b[:needs_pg] }

if needs_pg && url.nil?
  abort <<~MSG
    no PostgreSQL found for bench_kit

    start:
      docker compose up -d pg17

    or:
      PG_PIPELINE_URL=postgres://user:pass@host:5432/db \\
        bundle exec ruby samples/run_all_benchmark_task.rb
  MSG
end

env = ENV.to_h
env["PG_PIPELINE_URL"] = url if url

puts "pg_pipeline bench_kit suite"
puts "benches=#{benchmarks.map { |b| b[:id] }.join(",")}"
puts "pg_pipeline_url=#{url || "(not needed)"}"
puts "out=#{OUT_PATH}"
puts

runs = benchmarks.map do |entry|
  print "→ #{entry[:id]} ... "
  $stdout.flush
  run = run_bench(entry, env)
  if run[:ok]
    puts "ok  wall=#{format("%.1f", run[:seconds])}s"
  else
    puts "FAIL  #{run[:error]}"
  end
  run
end

write_report(runs, OUT_PATH, url: url)

puts
puts format("%-22s  %-6s  %8s", "bench", "status", "wall_s")
puts "-" * 42
runs.each do |run|
  puts format("%-22s  %-6s  %8.2f", run[:id], run[:ok] ? "ok" : "FAIL", run[:seconds])
end
puts
puts "wrote #{OUT_PATH}"

exit(runs.all? { |r| r[:ok] } ? 0 : 1)
