# frozen_string_literal: true

# Multi-process connection occupancy smoke.
# Expects workers × pipeline .. workers × (pipeline + pinned) backends.
#
# Uses Process.spawn (not fork) so macOS does not crash after ObjC/async load:
#   objc: initialize may have been in progress when fork() was called
#
#   PG_PIPELINE_URL=... WORKERS=4 PIPELINE_SIZE=4 PINNED_SIZE=2 \
#     bundle exec rake bench:smoke

require "pg"
require_relative "common"

url = BenchKit.require_url!
workers = Integer(ENV.fetch("WORKERS", "4"))
pipeline = Integer(ENV.fetch("PIPELINE_SIZE", "4"))
pinned = Integer(ENV.fetch("PINNED_SIZE", "2"))
appname = ENV.fetch("APPNAME", "pg_pipeline_smoke")
hold_s = Float(ENV.fetch("HOLD_S", "2"))

puts "multiworker smoke workers=#{workers} pipeline=#{pipeline} pinned=#{pinned}"
puts "url=#{BenchKit.redact_url(url)}"

# Child script: fresh process, no fork-after-threads.
worker_script = <<~'RUBY'
  require "bundler/setup"
  require "async"
  require "pg_pipeline"

  url = ENV.fetch("PG_PIPELINE_URL")
  app = ENV.fetch("APPNAME")
  pipeline = Integer(ENV.fetch("PIPELINE_SIZE"))
  pinned = Integer(ENV.fetch("PINNED_SIZE"))
  hold_s = Float(ENV.fetch("HOLD_S", "2"))
  sep = url.include?("?") ? "&" : "?"
  conn_args = "#{url}#{sep}application_name=#{app}"

  Sync do |task|
    client = PgPipeline::Client.new(
      conn_args,
      pipeline_size: pipeline,
      pinned_size: pinned,
      health_check: false
    ).start(parent: task)
    begin
      8.times.map { task.async { client.query("SELECT pg_sleep(0.05)") } }.each(&:wait)
      client.transaction { |tx| tx.query("SELECT 1") } if pinned.positive?
      sleep hold_s
    ensure
      client.close
    end
  end
RUBY

env = ENV.to_h.merge(
  "PG_PIPELINE_URL" => url,
  "APPNAME" => appname,
  "PIPELINE_SIZE" => pipeline.to_s,
  "PINNED_SIZE" => pinned.to_s,
  "HOLD_S" => hold_s.to_s
)

pids = workers.times.map do
  Process.spawn(env, "bundle", "exec", "ruby", "-e", worker_script, chdir: BenchKit::ROOT)
end

# Wait until workers hold connections (or timeout).
deadline = BenchKit.now + 8.0
count = 0
min_expected = workers * pipeline
max_expected = workers * (pipeline + pinned)

loop do
  count = BenchKit.app_conn_count(url, appname)
  break if count >= min_expected || BenchKit.now >= deadline

  sleep 0.1
end

puts "expected=#{min_expected}..#{max_expected} observed=#{count}"
ok = count >= min_expected && count <= max_expected
puts(ok ? "SMOKE OK" : "SMOKE MISMATCH")

statuses = pids.map { |pid| Process.wait2(pid) }
failed_children = statuses.count { |(_pid, st)| !st.success? }
if failed_children.positive?
  puts "SMOKE MISMATCH (#{failed_children}/#{workers} workers exited non-zero)"
  ok = false
end

exit(ok ? 0 : 1)
