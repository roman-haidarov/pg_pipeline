# frozen_string_literal: true

module PgPipeline
  module Runtime
    class Task
      attr_reader :fiber, :name, :error

      def self.spawn(name: nil, &block)
        raise ArgumentError, "block required" unless block

        task = new(name: name)
        task.__send__(:start, &block)
        task
      end

      def initialize(name: nil)
        @fiber, @scheduler, @blocker, @result, @error = nil, nil, nil, nil, nil
        @done, @cancelled = false, false

        @name = name
        @waiters = []
      end

      def wait(timeout = nil)
        unless @done
          deadline = Runtime.deadline_for(timeout)
          raise TimeoutError, "task did not finish in time" unless join(deadline)
        end
        raise @error if @error

        @result
      end

      def stop
        return false if @done

        @cancelled = true

        return true if interrupt_fiber
        return true if release_blocker

        false
      end

      def cancelled? = @cancelled

      def raise_if_cancelled!
        raise Cancel, "task stopped" if @cancelled
      end

      def enter_block(waiter)
        @blocker = waiter
      end

      def exit_block
        @blocker = nil
      end

      def finished? = @done

      private

      def start(&block)
        @scheduler = Runtime.scheduler!
        this = self

        scheduled = Fiber.schedule do
          this.__send__(:adopt_fiber, Fiber.current)
          Thread.current[Runtime::CURRENT_TASK_KEY] = this
          begin
            this.__send__(:complete, block.call, nil)
          rescue Cancel
            this.__send__(:complete, nil, nil)
          rescue Exception => e
            this.__send__(:complete, nil, e)
          end
        end

        @fiber ||= scheduled
      end

      def adopt_fiber(fiber)
        @fiber ||= fiber
      end

      def interrupt_fiber
        fiber = @fiber
        scheduler = @scheduler
        return false unless fiber&.alive?
        return false unless scheduler&.respond_to?(:fiber_interrupt)

        result = scheduler.fiber_interrupt(fiber, Cancel.new("task stopped"))
        result != false
      rescue FiberError
        false
      end

      def release_blocker
        waiter = @blocker or return false

        Runtime.wake(waiter, waiter[:blocker]) && waiter[:fiber].alive?
      end

      def complete(result, error)
        return if @done

        @result = result
        @error = error
        @done = true
        wake_waiters
      end

      def join(deadline)
        return true if @done

        Runtime.with_waiter(self, @waiters) do |waiter|
          Runtime.park(self, waiter, deadline) { @done }
        end
      end

      def wake_waiters
        pending, @waiters = @waiters, []
        pending.each { |waiter| Runtime.wake_dequeued(waiter, self) }
      end
    end
  end
end
