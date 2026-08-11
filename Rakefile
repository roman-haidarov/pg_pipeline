# frozen_string_literal: true

require "rbconfig"
require "fileutils"

include FileUtils

EXT_DIR = File.expand_path("ext/pg_pipeline_native", __dir__)

# Object files and shared objects are build products and are not tracked. If a
# checkout still carries them from an older revision, `rake clean compile` will
# clear them out -- but they must never come back into git, or a clone on a
# different platform loads a foreign binary.
BUILD_PRODUCTS = "*.{o,so,bundle,dylib}"

desc "Compile the native libpq extension"
task :compile do
  Dir.chdir(EXT_DIR) do
    # Never link leftover objects: a git checkout or shared worktree may carry
    # macOS .o/.bundle into a Linux CI job ("file format not recognized").
    # Always drop products before make so sources recompile for this platform.
    rm_f Dir[BUILD_PRODUCTS]
    rm_rf Dir["*.dSYM"]

    stale_makefile = File.file?("Makefile") &&
                     !File.read("Makefile").include?(RbConfig::CONFIG.fetch("arch"))
    if stale_makefile
      rm_f "Makefile"
      rm_f "mkmf.log"
    end

    sh RbConfig.ruby, "extconf.rb" unless File.file?("Makefile")
    sh ENV.fetch("MAKE", "make")
  end
end

desc "Remove native build products and force extconf to run again"
task :clean do
  Dir.chdir(EXT_DIR) do
    sh ENV.fetch("MAKE", "make"), "clean" if File.file?("Makefile")
    rm_f "Makefile"
    rm_f "mkmf.log"
    rm_f Dir[BUILD_PRODUCTS]
    rm_rf Dir["*.dSYM"]
  end
end

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

# Both suites need the extension. Only :spec used to depend on :compile, so
# `rake integration` could silently run against a stale build -- exactly the
# suite where that matters most.
Rake::Task[:spec].enhance([:compile]) if Rake::Task.task_defined?(:spec)
Rake::Task[:integration].enhance([:compile]) if Rake::Task.task_defined?(:integration)
task test: :spec

desc "Fail if a build product was ever committed"
task :verify_no_build_products do
  tracked = `git ls-files ext`.split("\n").grep(/\.(o|so|bundle|dylib)\z/)
  next if tracked.empty?

  abort <<~MSG
    These build products are tracked in git and must not be:
      #{tracked.join("\n  ")}
    Remove them with: git rm --cached #{tracked.join(" ")}
  MSG
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
      bench:dispatch     isolated send-path cost by parameter width
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

  desc "Isolated send-path cost per parameter width (needs PG_PIPELINE_URL)"
  task :dispatch do
    bench_ruby "dispatch_cost.rb"
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
