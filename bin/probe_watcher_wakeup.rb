#!/usr/bin/env ruby
# frozen_string_literal: true

require "pg"

scheduler_name = ENV.fetch("SCHEDULER", "Async::Scheduler")
grace = Float(ENV.fetch("GRACE", "1.5"))
url = ENV.fetch("DATABASE_URL") { abort("set DATABASE_URL") }

case scheduler_name
when /Async/ then require "async"
when /Itsi/  then require "itsi/scheduler"
end

scheduler_class = Object.const_get(scheduler_name)

Results = Struct.new(:name, :woke, :elapsed, :outcome)

def park(conn, grace)
  socket = conn.socket_io
  state = {woke: false, outcome: nil, started: Process.clock_gettime(Process::CLOCK_MONOTONIC)}

  fiber = Fiber.schedule do
    socket.wait_readable
    state[:woke] = true
    state[:outcome] = "wait_readable returned"
  rescue Exception => e # rubocop:disable Lint/RescueException
    state[:woke] = true
    state[:outcome] = "#{e.class}: #{e.message}"
  ensure
    state[:elapsed] = Process.clock_gettime(Process::CLOCK_MONOTONIC) - state[:started]
  end

  [fiber, state, socket]
end

def settle(grace)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + grace
  sleep(0.02) while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
end

def case_run(label, grace, url)
  conn = PG.connect(url)
  conn.setnonblocking(true)
  fiber, state, socket = park(conn, grace)
  sleep(0.05) # let the fiber reach wait_readable

  begin
    yield(conn, socket, fiber)
  rescue StandardError => e
    state[:outcome] ||= "teardown raised #{e.class}"
  end

  settle(grace)
  Results.new(label, state[:woke], state[:elapsed], state[:outcome] || "still parked")
ensure
  begin
    conn.close unless conn.nil? || conn.finished?
  rescue StandardError
    nil
  end
end

rows = []

Thread.new do
  Fiber.set_scheduler(scheduler_class.new)

  Fiber.schedule do
    rows << case_run("conn.close (PQfinish)", grace, url) { |conn, _s, _f| conn.close }
    rows << case_run("socket_io.close", grace, url) { |_c, socket, _f| socket.close }
    rows << case_run("fiber.raise", grace, url) { |_c, _s, fiber| fiber.raise(RuntimeError.new("stop")) }
    rows << case_run("nothing (control)", grace, url) { |_c, _s, _f| nil }
  end
end.join

puts
puts "scheduler: #{scheduler_name}"
puts "ruby:      #{RUBY_VERSION}   pg gem: #{PG::VERSION}   libpq: #{PG.library_version}"
puts "fiber_interrupt supported: #{scheduler_class.instance_methods.include?(:fiber_interrupt)}"
puts
printf("%-26s %-6s %-9s %s\n", "teardown action", "woke", "elapsed", "outcome")
puts "-" * 78
rows.each do |r|
  printf("%-26s %-6s %-9s %s\n", r.name, r.woke ? "yes" : "NO", r.elapsed ? format("%.3fs", r.elapsed) : "-", r.outcome)
end
puts
puts "Read: any row with woke=yes is a usable teardown mechanism for this"
puts "combination. If ONLY 'fiber.raise' wakes, then stop_watchers depends on"
puts "Fiber::Scheduler#fiber_interrupt and the join timeout is load-bearing."
