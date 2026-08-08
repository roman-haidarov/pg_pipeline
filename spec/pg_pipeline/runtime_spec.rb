# frozen_string_literal: true

require "spec_helper"


class InterruptibleWaitScheduler
  attr_reader :interrupts, :io_waiters, :io_wait_timeouts, :sleeps

  def initialize
    @interrupts = 0
    @io_waiters = {}
    @io_wait_timeouts = []
    @sleeps = []
  end

  def fiber(&block)
    Fiber.new(blocking: false, &block).tap(&:resume)
  end

  def block(_blocker, _timeout = nil)
    Fiber.yield
  end

  def unblock(_blocker, fiber)
    fiber.resume if fiber.alive?
  end

  def io_wait(_io, _events, timeout = nil)
    fiber = Fiber.current
    @io_wait_timeouts << timeout
    @io_waiters[fiber] = true
    Fiber.yield
  ensure
    @io_waiters.delete(fiber) if fiber
  end

  def fiber_interrupt(fiber, exception)
    @interrupts += 1
    fiber.raise(exception)
    true
  rescue FiberError
    false
  end

  def kernel_sleep(duration = nil)
    @sleeps << duration
    nil
  end

  def close = nil
end

class DeferredUnblockScheduler
  attr_accessor :on_unblock

  def initialize
    @pending = []
    @on_unblock = nil
  end

  def fiber(&block)
    Fiber.new(blocking: false, &block).tap(&:resume)
  end

  def block(_blocker, _timeout = nil)
    Fiber.yield
  end

  def unblock(_blocker, fiber)
    @on_unblock&.call(fiber)
    @pending << fiber unless @pending.include?(fiber)
  end

  def flush
    batch, @pending = @pending, []
    batch.each { |fiber| fiber.resume if fiber.alive? }
  end

  def kernel_sleep(_duration = nil) = nil
  def io_wait(*) = true
  def close = flush
end

RSpec.describe PgPipeline::Runtime do
  describe ".with_timeout" do
    it "uses Timeout.timeout rather than calling scheduler.timeout_after directly" do
      Async do
        scheduler = Fiber.scheduler
        calls = []
        if scheduler.respond_to?(:timeout_after)
          original = scheduler.method(:timeout_after)
          allow(scheduler).to receive(:timeout_after) do |*args, &block|
            calls << args
            original.call(*args, &block)
          end
        end

        expect(described_class.with_timeout(1.0) { :ok }).to eq(:ok)

        # Timeout.timeout may invoke the hook with the full arity; we must never
        # call it ourselves with a single duration argument.
        calls.each do |args|
          expect(args.length).to be >= 2
        end
      end.wait
    end

    it "raises Runtime::TimeoutError when the block exceeds the limit" do
      Async do
        expect {
          described_class.with_timeout(0.01) { sleep 1 }
        }.to raise_error(PgPipeline::Runtime::TimeoutError)
      end.wait
    end

    it "works when the scheduler has no timeout_after hook" do
      fake = Object.new
      def fake.block(_blocker, _timeout = nil) = Fiber.yield
      def fake.unblock(_blocker, fiber) = fiber.resume
      def fake.fiber(&block)
        fiber = Fiber.new(blocking: false, &block)
        fiber.resume
        fiber
      end
      def fake.kernel_sleep(duration)
        # no-op immediate wake for tests that only exercise Timeout path
        duration
      end
      def fake.io_wait(*) = true

      previous = Fiber.scheduler
      Fiber.set_scheduler(fake)
      begin
        # Fiber.schedule needs scheduler#fiber on some Rubies; use Timeout path only.
        expect(described_class.with_timeout(0.05) { :ok }).to eq(:ok)
      ensure
        Fiber.set_scheduler(previous)
      end
    end
  end

  describe PgPipeline::Runtime::Notification do
    it "does not return from wait until this waiter is signaled" do
      Async do |task|
        notification = described_class.new
        order = []

        waiter = task.async do
          notification.wait
          order << :woke
          :done
        end

        task.yield
        expect(order).to be_empty
        notification.signal
        expect(waiter.wait).to eq(:done)
        expect(order).to eq([:woke])
      end.wait
    end

    it "returns false from timed wait without raising when the timeout elapses" do
      Async do
        notification = described_class.new
        expect(notification.wait(0.01)).to be(false)
      end.wait
    end
  end

  describe PgPipeline::Runtime::Queue do
    it "uses Ruby core Queue as the scheduler-aware wait primitive" do
      queue = described_class.new
      expect(queue.instance_variable_get(:@queue)).to be_a(Thread::Queue)
    end

    it "treats enqueue on a closed queue as a no-op" do
      Async do
        queue = described_class.new
        queue.close
        expect(queue.enqueue(:x)).to be_nil
        expect(queue.dequeue).to be_nil
      end.wait
    end

    it "unblocks dequeue when closed" do
      Async do |task|
        queue = described_class.new
        consumer = task.async { queue.dequeue }
        task.yield
        queue.close
        expect(consumer.wait).to be_nil
      end.wait
    end
  end

  describe PgPipeline::Runtime::Semaphore do
    it "bounds concurrency under Async" do
      Async do |task|
        sem = described_class.new(2)
        peak = 0
        live = 0
        mutex = Mutex.new

        workers = 8.times.map do
          task.async do
            sem.acquire do
              mutex.synchronize do
                live += 1
                peak = live if live > peak
              end
              sleep 0.01
              mutex.synchronize { live -= 1 }
            end
          end
        end

        workers.each(&:wait)
        expect(peak).to be <= 2
      end.wait
    end

    it "is FIFO when permits are contended under Async" do
      Async do |task|
        sem = described_class.new(1)
        order = []

        holder = task.async do
          sem.acquire do
            order << :holder
            task.yield
          end
        end

        task.yield until order.include?(:holder)

        first = task.async do
          sem.acquire { order << :first }
        end
        second = task.async do
          sem.acquire { order << :second }
        end

        task.yield
        holder.wait
        first.wait
        second.wait

        expect(order).to eq([:holder, :first, :second])
      end.wait
    end

    it "does not allow barging or double-unblock under deferred unblock" do
      scheduler = DeferredUnblockScheduler.new
      previous = Fiber.scheduler
      Fiber.set_scheduler(scheduler)

      begin
        sem = described_class.new(1)
        log = []
        unblocks = Hash.new(0)
        scheduler.on_unblock = ->(fiber) { unblocks[fiber] += 1 }

        hold_blocker = Object.new
        release_holder = false
        waiter_entered = false
        barger_entered = false

        holder = Fiber.schedule do
          sem.acquire do
            log << :holder_in
            Fiber.scheduler.block(hold_blocker, nil) until release_holder
            log << :holder_out
          end
        end

        waiter = Fiber.schedule do
          log << :waiter_attempt
          sem.acquire do
            log << :waiter_in
            waiter_entered = true
          end
          log << :waiter_out
        end

        expect(log).to include(:holder_in, :waiter_attempt)
        expect(waiter_entered).to be(false)
        expect(unblocks[waiter]).to eq(0)

        release_holder = true
        scheduler.unblock(hold_blocker, holder)
        scheduler.flush

        expect(log).to include(:holder_out)
        expect(unblocks[waiter]).to eq(1)
        expect(waiter_entered).to be(false)

        barger = Fiber.schedule do
          log << :barger_attempt
          sem.acquire do
            log << :barger_in
            barger_entered = true
          end
          log << :barger_out
        end

        expect(barger_entered).to be(false)
        expect(unblocks[waiter]).to eq(1)
        expect(unblocks[barger]).to eq(0)

        scheduler.flush
        expect(waiter_entered).to be(true)
        expect(barger_entered).to be(false)
        expect(unblocks[barger]).to eq(1)

        scheduler.flush
        expect(barger_entered).to be(true)

        expect(log).to eq([
          :holder_in, :waiter_attempt, :holder_out,
          :barger_attempt, :waiter_in, :waiter_out,
          :barger_in, :barger_out
        ])
        expect(unblocks[waiter]).to eq(1)
        expect(unblocks[barger]).to eq(1)
      ensure
        Fiber.set_scheduler(previous)
      end
    end
  end

  describe PgPipeline::Runtime::Task do
    it "joins a completed task" do
      Async do
        task = described_class.spawn { :ok }
        expect(task.wait).to eq(:ok)
        expect(task.finished?).to be(true)
      end.wait
    end

    it "times out wait instead of hanging forever when the fiber never exits" do
      Async do
        gate = PgPipeline::Runtime::Notification.new
        task = described_class.spawn { gate.wait }

        expect {
          task.wait(0.05)
        }.to raise_error(PgPipeline::Runtime::TimeoutError)

        gate.signal
        task.wait
      end.wait
    end

    it "surfaces the child error from wait" do
      Async do
        task = described_class.spawn { raise RuntimeError, "boom" }
        expect { task.wait }.to raise_error(RuntimeError, "boom")
      end.wait
    end

    it "unwinds a parked task through the host scheduler cancellation capability" do
      Async do
        gate = PgPipeline::Runtime::Notification.new
        task = described_class.spawn(name: :parked) { gate.wait }

        expect(task.stop).to be(true)
        expect { task.wait(1.0) }.not_to raise_error
        expect(task.error).to be_nil
        expect(task.finished?).to be(true)
      end.wait
    end

    it "interrupts a task parked in scheduler IO without timeout polling" do
      scheduler = InterruptibleWaitScheduler.new
      previous = Fiber.scheduler
      Fiber.set_scheduler(scheduler)

      begin
        token = Object.new
        cancelled = false

        task = described_class.spawn(name: :io_wait) do
          Fiber.scheduler.io_wait(token, IO::READABLE, nil)
        rescue PgPipeline::Runtime::Cancel
          cancelled = true
          raise
        end

        expect(scheduler.io_waiters).to include(task.fiber)
        expect(scheduler.io_wait_timeouts).to eq([nil])
        expect(scheduler.sleeps).to be_empty
        expect(task.stop).to be(true)
        expect(cancelled).to be(true)
        expect(scheduler.interrupts).to eq(1)
        expect(scheduler.io_waiters).to be_empty
        expect(task.finished?).to be(true)
        expect(task.error).to be_nil
        expect(scheduler.sleeps).to be_empty
        expect(scheduler.io_wait_timeouts).to eq([nil])
      ensure
        Fiber.set_scheduler(previous)
      end
    end

    it "reports false from stop when the task cannot be reached" do
      Async do
        finished = described_class.spawn { :ok }
        finished.wait
        expect(finished.stop).to be(false)
      end.wait
    end

    it "unwinds a queue consumer by closing the queue, with no cancellation" do
      Async do
        queue = PgPipeline::Runtime::Queue.new
        task = described_class.spawn { queue.dequeue }
        queue.close
        expect(task.wait(1.0)).to be_nil
      end.wait
    end

    it "assigns #fiber before the body gets its first slice" do
      Async do
        seen = nil
        task = described_class.spawn { seen = Fiber.current }
        task.wait
        expect(seen).to equal(task.fiber)
      end.wait
    end
  end
end
