#!/usr/bin/env ruby
# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "pg_pipeline/errors"
require "pg_pipeline/runtime"

class StubScheduler
  attr_reader :hook_calls

  def initialize
    @ready = []
    @hook_calls = []
  end

  def fiber(&block)
    f = Fiber.new(blocking: false, &block)
    f.resume
    f
  end

  def block(_blocker, _timeout) = Fiber.yield
  def unblock(_blocker, fiber) = (@ready << fiber)
  def kernel_sleep(_duration = nil) = nil
  def io_wait(_io, _events, _timeout) = nil
  def close = drain

  def drain
    until @ready.empty?
      fiber = @ready.shift
      fiber.resume if fiber&.alive?
    end
  end

  def timeout_after(duration, exception, message, &block)
    @hook_calls << [duration, exception, message]
    yield(duration)
  end
end

class NoHookScheduler < StubScheduler
  undef_method :timeout_after
end

FAILURES = []

def check(label)
  ok = yield
  puts format("  %-62s %s", label, ok ? "ok" : "FAIL")
  FAILURES << label unless ok
rescue Exception => e # rubocop:disable Lint/RescueException
  puts format("  %-62s FAIL (%s: %s)", label, e.class, e.message)
  FAILURES << label
end

def with_scheduler(scheduler)
  Thread.new do
    Fiber.set_scheduler(scheduler)
    yield
  end.join
end

[StubScheduler, NoHookScheduler].each do |klass|
  puts "\n#{klass} (timeout_after hook: #{klass.instance_methods.include?(:timeout_after)})"

  with_scheduler(klass.new) do
    check "with_timeout does not call the timeout_after hook directly" do
      PgPipeline::Runtime.with_timeout(0.5) { :ok } == :ok
    end

    check "with_timeout passes nil through untouched" do
      PgPipeline::Runtime.with_timeout(nil) { :ok } == :ok
    end

    check "Notification#wait blocks until THIS waiter is signalled" do
      n = PgPipeline::Runtime::Notification.new
      order = []
      PgPipeline::Runtime::Task.spawn { n.wait; order << :woke }
      order << :before
      n.signal
      Fiber.scheduler.drain
      order == %i[before woke]
    end

    check "Queue#enqueue on a closed queue is a no-op, not an exception" do
      q = PgPipeline::Runtime::Queue.new
      q.close
      q.enqueue(:x).nil? && q.empty?
    end

    check "Queue#dequeue returns nil once closed" do
      q = PgPipeline::Runtime::Queue.new
      result = :unset
      PgPipeline::Runtime::Task.spawn { result = q.dequeue }
      q.close
      Fiber.scheduler.drain
      result.nil?
    end

    check "Queue drains buffered items before reporting closed" do
      q = PgPipeline::Runtime::Queue.new
      q.enqueue(:a)
      q.close
      q.dequeue == :a
    end

    check "Semaphore bounds concurrency" do
      sem = PgPipeline::Runtime::Semaphore.new(2)
      peak = 0
      live = 0
      4.times do
        PgPipeline::Runtime::Task.spawn do
          sem.acquire do
            live += 1
            peak = live if live > peak
            live -= 1
          end
        end
      end
      Fiber.scheduler.drain
      peak <= 2
    end

    check "Task#fiber is set before the body's first slice" do
      seen = nil
      t = PgPipeline::Runtime::Task.spawn { seen = Fiber.current }
      seen.equal?(t.fiber)
    end

    check "Task#wait surfaces a real error" do
      t = PgPipeline::Runtime::Task.spawn { raise "boom" }
      begin
        t.wait
        false
      rescue RuntimeError => e
        e.message == "boom"
      end
    end

    check "cooperative stop unwinds a parked task without Fiber#raise" do
      gate = PgPipeline::Runtime::Notification.new
      t = PgPipeline::Runtime::Task.spawn(name: :parked) { gate.wait }
      woken = t.stop
      Fiber.scheduler.drain
      woken && t.finished? && t.error.nil?
    end

    check "stop returns false when the task cannot be reached" do
      finished = PgPipeline::Runtime::Task.spawn { :ok }
      Fiber.scheduler.drain
      finished.stop == false
    end

    check "closing a queue unwinds its consumer with no cancellation at all" do
      q = PgPipeline::Runtime::Queue.new
      got = :unset
      PgPipeline::Runtime::Task.spawn { got = q.dequeue }
      q.close
      Fiber.scheduler.drain
      got.nil?
    end

    check "Runtime.park is the only place that touches scheduler.block" do
      source = File.read(File.expand_path("../lib/pg_pipeline/runtime.rb", __dir__))
      body = source.scan(/^\s*(?:scheduler|waiter\[:scheduler\])\.block\(/)
      body.length == 1
    end

    check "a signalled waiter leaves the list O(1), without a rescan" do
      n = PgPipeline::Runtime::Notification.new
      parked = []
      8.times { |i| parked << PgPipeline::Runtime::Task.spawn { n.wait; i } }
      8.times { n.signal }
      Fiber.scheduler.drain
      w = Thread.current[PgPipeline::Runtime::WAITER_KEY]
      parked.all?(&:finished?) && (w.nil? || w[:queued] == false)
    end

    check "a timed-out waiter is still removed from the list" do
      n = PgPipeline::Runtime::Notification.new
      t = PgPipeline::Runtime::Task.spawn { n.wait }
      t.stop
      Fiber.scheduler.drain
      t.finished? && n.signal == false
    end

    check "the gem only uses Fiber::Scheduler hooks block/unblock/fiber_interrupt/yield" do
      allowed = %w[block unblock fiber_interrupt yield equal? nil? respond_to?]
      root = File.expand_path("../lib", __dir__)
      offenders = Dir[File.join(root, "**/*.rb")].flat_map do |path|
        File.readlines(path).each_with_index.filter_map do |line, i|
          next if line =~ /^\s*#/
          name = line[/(?:Fiber\.scheduler|@?scheduler)\.(\w+\??)/, 1]
          next if name.nil? || allowed.include?(name)
          "#{path.sub(root + "/", "")}:#{i + 1} -> #{name}"
        end
      end
      offenders.each { |o| puts "        #{o}" }
      offenders.empty?
    end

    check "Task#stop uses fiber_interrupt, not Fiber#raise, to interrupt work" do
      source = File.read(File.expand_path("../lib/pg_pipeline/runtime/task.rb", __dir__))
      body = source[/def interrupt_fiber.*?\n      end/m].to_s
      body.include?("fiber_interrupt") && !body.include?("fiber.raise")
    end

    check "bounded waits never invoke the timeout_after hook with bad arity" do
      scheduler = Fiber.scheduler
      scheduler.hook_calls.clear if scheduler.respond_to?(:hook_calls)

      done = PgPipeline::Runtime::Task.spawn { :ok }
      Fiber.scheduler.drain
      done.wait(1.0)

      n = PgPipeline::Runtime::Notification.new
      PgPipeline::Runtime::Task.spawn { n.signal }
      Fiber.scheduler.drain

      calls = scheduler.respond_to?(:hook_calls) ? scheduler.hook_calls : []
      calls.all? { |args| args.length >= 2 }
    end
  end
end

puts
if FAILURES.empty?
  puts "all checks passed"
  exit 0
else
  puts "#{FAILURES.size} failed:"
  FAILURES.each { |f| puts "  - #{f}" }
  exit 1
end
