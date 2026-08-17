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
    @timeouts = []
    @blocked = []
  end

  def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  def fiber(&block)
    fiber = Fiber.new(blocking: false, &block)
    fiber.resume
    fiber
  end

  def block(_blocker, timeout = nil)
    @blocked << [monotonic + timeout, Fiber.current] if timeout
    Fiber.yield
  end

  def unblock(_blocker, fiber)
    @blocked.reject! { |(_, blocked)| blocked == fiber }
    @ready << fiber
  end

  def kernel_sleep(_duration = nil) = nil
  def io_wait(_io, _events, _timeout) = nil
  def close = drain

  def drain
    loop do
      until @ready.empty?
        fiber = @ready.shift
        next if fiber.nil? || fiber.equal?(Fiber.current)

        fiber.resume if fiber.alive?
      end

      next_block = @blocked.min_by(&:first)
      next_timeout = @timeouts.min_by(&:first)
      break if next_block.nil? && next_timeout.nil?

      if next_timeout && (next_block.nil? || next_timeout.first <= next_block.first)
        expire_timeout(next_timeout)
      else
        resume_blocked(next_block)
      end
    end
  end

  def resume_blocked(entry)
    @blocked.delete(entry)
    fiber = entry[1]
    @ready << fiber if fiber.alive?
  end

  def expire_timeout(entry)
    @timeouts.delete(entry)
    _, fiber, klass, message = entry
    return unless fiber.alive?

    @ready.delete(fiber)
    fiber.raise(klass, message)
  end

  def timeout_after(duration, exception, message, &block)
    @hook_calls << [duration, exception, message]
    entry = [monotonic + duration, Fiber.current, exception, message]
    @timeouts << entry
    begin
      yield(duration)
    ensure
      @timeouts.delete(entry)
    end
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
    Fiber.schedule { yield }
    scheduler.drain
  end.join
end

[StubScheduler, NoHookScheduler].each do |klass|
  puts "\n#{klass} (timeout_after hook: #{klass.instance_methods.include?(:timeout_after)})"

  with_scheduler(klass.new) do
    check "with_timeout uses the scheduler hook when the scheduler has one" do
      scheduler = Fiber.scheduler
      if scheduler.respond_to?(:timeout_after)
        before = scheduler.hook_calls.size
        PgPipeline::Runtime.with_timeout(5) { :done }
        last = scheduler.hook_calls.last
        scheduler.hook_calls.size > before && last.length >= 3 && last[1] == PgPipeline::Runtime::Deadline
      else
        PgPipeline::Runtime.with_timeout(5) { :done } == :done
      end
    end

    check "with_timeout passes nil through untouched" do
      PgPipeline::Runtime.with_timeout(nil) { :ok } == :ok
    end

    check "Deadline is not a StandardError" do
      PgPipeline::Runtime::Deadline < Exception && !(PgPipeline::Runtime::Deadline < StandardError)
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

    check "Notification#wait(timeout) returns false without the timeout_after hook" do
      PgPipeline::Runtime::Notification.new.wait(0.01) == false
    end

    check "Notification#wait(0) polls instead of parking" do
      PgPipeline::Runtime::Notification.new.wait(0) == false
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

    check "Task#wait(timeout) raises TimeoutError" do
      parked = PgPipeline::Runtime::Notification.new
      task = PgPipeline::Runtime::Task.spawn { parked.wait }
      begin
        task.wait(0.01)
        false
      rescue PgPipeline::Runtime::TimeoutError
        parked.signal
        true
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

    check "signal skips a waiter already released by Task#stop" do
      notification = PgPipeline::Runtime::Notification.new
      woken = []
      stopped = PgPipeline::Runtime::Task.spawn { notification.wait; woken << :stopped }
      PgPipeline::Runtime::Task.spawn { notification.wait; woken << :live }
      stopped.stop
      notification.signal
      Fiber.scheduler.drain
      woken == [:live]
    end

    check "the gem only uses Fiber::Scheduler hooks block/unblock/fiber_interrupt/timeout_after" do
      allowed = %w[block unblock fiber_interrupt timeout_after respond_to? equal? nil?]
      root = File.expand_path("../lib", __dir__)
      offenders = Dir[File.join(root, "**/*.rb")].flat_map do |path|
        File.readlines(path).each_with_index.filter_map do |line, i|
          next if line =~ /^\s*#/
          name = line[/(?:Fiber\.scheduler|@?scheduler)\.(\w+\??)/, 1]
          next if name.nil? || allowed.include?(name)
          "#{path.sub("#{root}/", "")}:#{i + 1} -> #{name}"
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

    check "bounded waits never invoke the timeout_after hook" do
      scheduler = Fiber.scheduler
      scheduler.hook_calls.clear if scheduler.respond_to?(:hook_calls)

      done = PgPipeline::Runtime::Task.spawn { :ok }
      Fiber.scheduler.drain
      done.wait(1.0)

      n = PgPipeline::Runtime::Notification.new
      n.wait(0)

      calls = scheduler.respond_to?(:hook_calls) ? scheduler.hook_calls : []
      calls.empty?
    end

    if Fiber.scheduler.respond_to?(:timeout_after)
      check "a bounded inner wait does not swallow an enclosing deadline" do
        begin
          PgPipeline::Runtime.with_timeout(0.05) { PgPipeline::Runtime::Notification.new.wait(10) }
          false
        rescue PgPipeline::Runtime::TimeoutError
          true
        end
      end

      check "an enclosing deadline does not leak out of a bounded inner wait" do
        PgPipeline::Runtime.with_timeout(5) { PgPipeline::Runtime::Notification.new.wait(0.01) } == false
      end
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
