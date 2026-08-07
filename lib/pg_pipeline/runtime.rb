# frozen_string_literal: true

require "timeout"

require_relative "errors"

module PgPipeline
  module Runtime
    class Cancel < Exception; end
    class TimeoutError < Error; end

    module_function

    CURRENT_TASK_KEY = :pg_pipeline_current_task
    WAITER_KEY = :pg_pipeline_waiter

    def spawn(name: nil, &block)
      Task.spawn(name: name, &block)
    end

    def scheduler!
      Fiber.scheduler or raise Error, "operation requires an active Fiber scheduler"
    end

    def current_task
      Thread.current[CURRENT_TASK_KEY]
    end

    def build_waiter(blocker)
      waiter = Thread.current[WAITER_KEY] ||= {
        fiber: nil, scheduler: nil, ready: false, blocker: nil
      }

      if waiter[:blocker]
        raise Error, "waiter already parked on #{waiter[:blocker].class}; " \
                     "a fiber may only park in one place at a time"
      end

      waiter[:fiber] = Fiber.current
      waiter[:scheduler] = scheduler!
      waiter[:ready] = false
      waiter[:queued] = false
      waiter[:blocker] = blocker
      waiter
    end

    def park(blocker, waiter)
      scheduler = waiter[:scheduler]
      task = current_task
      task&.enter_block(waiter)

      until waiter[:ready] || yield
        scheduler.block(blocker, nil)
        task&.raise_if_cancelled!
      end
      nil
    ensure
      waiter[:blocker] = nil
      task&.exit_block
    end

    def with_waiter(blocker, waiters)
      waiter = build_waiter(blocker)
      waiter[:queued] = true
      waiters << waiter
      yield waiter
    ensure
      waiters.delete(waiter) if waiter[:queued]
    end

    def wake_dequeued(waiter, blocker)
      waiter[:queued] = false
      wake(waiter, blocker)
    end

    def with_timeout(duration)
      return yield if duration.nil?

      timeout = Float(duration)
      raise ArgumentError, "timeout must be non-negative and finite" unless timeout.finite? && timeout >= 0

      ::Timeout.timeout(timeout, TimeoutError) { yield }
    end

    def wake(waiter, blocker)
      waiter[:ready] = true
      fiber = waiter[:fiber]
      return unless fiber.alive?

      scheduler = waiter[:scheduler]
      if waiter[:prefer_resume] && scheduler.respond_to?(:resume)
        scheduler.resume(fiber)
      else
        scheduler.unblock(blocker, fiber)
      end
    end
  end
end

require_relative "runtime/notification"
require_relative "runtime/queue"
require_relative "runtime/semaphore"
require_relative "runtime/task"
