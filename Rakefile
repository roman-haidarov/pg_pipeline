# frozen_string_literal: true

begin
  require "rspec/core/rake_task"
  RSpec::Core::RakeTask.new(:spec) do |t|
    t.exclude_pattern = "spec/integration/**/*_spec.rb"
  end
  RSpec::Core::RakeTask.new(:integration) do |t|
    t.pattern = "spec/integration/**/*_spec.rb"
  end
rescue LoadError
  nil
end

namespace :bench do
  def bench_ruby(script)
    path = File.expand_path("bench_kit/#{script}", __dir__)
    abort "missing #{path}" unless File.file?(path)

    sh "bundle", "exec", "ruby", path
  end

  desc "List available bench_kit tasks"
  task :list do
    puts <<~LIST
      bench:list         this help
      bench:rtt_demo     RTT amortization (no PostgreSQL)
      bench:proxy        latency proxy (long-running; RTT_MS UPSTREAM LISTEN)
      bench:throughput   N-fiber thrpt/p99 on pg_pipeline
      bench:ab           pipeline vs naive pool A/B (+ server_conns)
      bench:smoke        multi-worker connection occupancy
      bench:profile      RubyProf CI-shaped scenario → tmp/bench_kit/
      bench:metrics      full picture: cpu+alloc+sampling+GC → tmp/bench_kit/
      bench:all          rtt_demo + throughput + smoke (needs PG_PIPELINE_URL for last two)

      Docs: bench_kit/README.md
    LIST
  end

  desc "RTT amortization demo (no PostgreSQL)"
  task :rtt_demo do
    bench_ruby "rtt_amortization_demo.rb"
  end

  desc "Latency proxy (long-running). Env: RTT_MS UPSTREAM LISTEN"
  task :proxy do
    ENV["UPSTREAM"] ||= "127.0.0.1:5417"
    ENV["LISTEN"] ||= "127.0.0.1:6432"
    ENV["RTT_MS"] ||= "10"
    bench_ruby "latency_proxy.rb"
  end

  desc "Throughput / latency (needs PG_PIPELINE_URL)"
  task :throughput do
    bench_ruby "pipeline_throughput.rb"
  end

  desc "A/B pipeline vs baseline (needs PG_PIPELINE_URL; prefer via proxy)"
  task :ab do
    bench_ruby "pipeline_vs_baseline.rb"
  end

  desc "Multi-worker connection smoke (needs PG_PIPELINE_URL)"
  task :smoke do
    bench_ruby "multiworker_smoke.rb"
  end

  desc "RubyProf CI-shaped scenario (needs PG_PIPELINE_URL) → tmp/bench_kit/"
  task :profile do
    bench_ruby "profile_ci_scenario.rb"
  end

  desc "Full metrics: process+allocations+wall+sampling+GC (needs PG_PIPELINE_URL)"
  task :metrics do
    bench_ruby "metrics.rb"
  end

  desc "Run rtt_demo + throughput + smoke"
  task :all do
    Rake::Task["bench:rtt_demo"].invoke
    puts
    Rake::Task["bench:throughput"].invoke
    puts
    Rake::Task["bench:smoke"].invoke
  end
end

# Back-compat aliases
desc "Alias for bench:profile"
task profile: "bench:profile"

task default: :spec
