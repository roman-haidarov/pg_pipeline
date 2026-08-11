# frozen_string_literal: true

require "fiber"

module PgPipeline
  module Test
    class ReferenceScheduler
      class Timeout < StandardError; end

      IO_READABLE = IO::READABLE
      IO_WRITABLE = IO::WRITABLE

      def initialize
        @readable = {}
        @writable = {}
        @waiting = {}
        @sleepers = {}
        @blocked = {}
        @ready = []
        @closed = false
      end

      def fiber(&block)
        fiber = Fiber.new(blocking: false) do
          block.call
        ensure
          @waiting.delete(Fiber.current)
        end
        fiber.tap { |f| resume(f) }
      end

      def io_wait(io, events, timeout)
        fiber = Fiber.current
        @readable[io] = fiber if (events & IO_READABLE).positive?
        @writable[io] = fiber if (events & IO_WRITABLE).positive?
        @sleepers[fiber] = monotonic + timeout if timeout

        result = Fiber.yield

        result || 0
      ensure
        @readable.delete(io) if @readable[io] == fiber
        @writable.delete(io) if @writable[io] == fiber
        @sleepers.delete(fiber)
      end

      def kernel_sleep(duration = nil)
        block(:sleep, duration)
        true
      end

      def block(_blocker, timeout = nil)
        fiber = Fiber.current
        @blocked[fiber] = true
        @sleepers[fiber] = monotonic + timeout if timeout
        Fiber.yield
        true
      ensure
        @blocked.delete(fiber)
        @sleepers.delete(fiber)
      end

      def unblock(_blocker, fiber)
        @ready << fiber unless @ready.include?(fiber)
        nil
      end

      def fiber_interrupt(fiber, exception)
        return false unless fiber.alive?

        @readable.delete_if { |_io, waiter| waiter == fiber }
        @writable.delete_if { |_io, waiter| waiter == fiber }
        @blocked.delete(fiber)
        @sleepers.delete(fiber)
        @ready.delete(fiber)
        safe_raise(fiber, exception)
        true
      end

      def close
        pump until idle?
      ensure
        @closed = true
      end

      def closed? = @closed

      def pump
        run_once
      end

      def idle?
        @ready.empty? && @readable.empty? && @writable.empty? && @sleepers.empty? && @blocked.empty?
      end

      private

      def run_once
        drain_ready
        expire_sleepers
        if @readable.empty? && @writable.empty?
          if (timeout = select_timeout)
            @spins = 0
            sleep(timeout) if timeout.positive?
          elsif !@blocked.empty? || !@ready.empty?
            idle_backoff
          end
        else
          @spins = 0
          select_io
        end
      end

      def drain_ready
        until @ready.empty?
          fiber = @ready.shift
          resume(fiber)
        end
      end

      def expire_sleepers
        now = monotonic
        due = @sleepers.select { |_fiber, deadline| deadline <= now }.keys
        due.each do |fiber|
          @sleepers.delete(fiber)
          @readable.delete_if { |_io, waiter| waiter == fiber }
          @writable.delete_if { |_io, waiter| waiter == fiber }
          resume(fiber)
        end
      end

      def select_io
        return if @readable.empty? && @writable.empty?

        readable, writable = IO.select(@readable.keys, @writable.keys, nil, select_timeout)

        Array(readable).each do |io|
          fiber = @readable.delete(io)
          resume(fiber, IO_READABLE)
        end

        Array(writable).each do |io|
          fiber = @writable.delete(io)
          resume(fiber, IO_WRITABLE)
        end
      end

      # Only another thread can unblock us here, so yield first and then back off
      # rather than spinning a core for the lifetime of a stuck example.
      def idle_backoff
        @spins += 1
        if @spins < 100
          Thread.pass
        else
          sleep([0.0001 * (@spins - 99), 0.01].min)
        end
      end

      def select_timeout
        return 0 unless @ready.empty?
        return nil if @sleepers.empty?

        [@sleepers.values.min - monotonic, 0].max
      end

      def resume(fiber, value = nil)
        return unless fiber&.alive?

        fiber.resume(value)
      rescue FiberError
        nil
      end

      def safe_raise(fiber, exception)
        fiber.raise(exception)
      rescue FiberError
        nil
      end

      def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      class << self
        def run(&block)
          value = error = nil
          finished = false
          thread = Thread.new do
            scheduler = new
            Fiber.set_scheduler(scheduler)
            Fiber.schedule do
              begin
                value = block.call
              rescue Exception => e
                error = e
              ensure
                finished = true
              end
            end
            scheduler.pump until finished
          ensure
            Fiber.set_scheduler(nil)
          end
          thread.join

          raise error if error

          value
        end
      end
    end
  end
end
