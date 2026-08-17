# frozen_string_literal: true

require "timeout"

require_relative "errors"

module PgPipeline
  module Runtime
    class Cancel < Exception; end
    class Deadline < Exception; end
    class TimeoutError < Error; end

    module_function

    CURRENT_TASK_KEY = :pg_pipeline_current_task
    WAITER_KEY = :pg_pipeline_waiter
    DEADLINE_MESSAGE = "execution expired"

    MISSING_TIMEOUT_HOOK_WARNING = <<~MESSAGE
      pg_pipeline: %s does not implement #timeout_after.

      Falling back to stdlib Timeout, which uses Thread#raise and can deliver
      the timeout to an unrelated fiber. Use a scheduler that implements
      #timeout_after (async, itsi-scheduler) or avoid Runtime.with_timeout
      on this host.
    MESSAGE

    @warned_schedulers = {}

    def spawn(name: nil, &block)
      Task.spawn(name: name, &block)
    end

    def scheduler!
      Fiber.scheduler or raise Error, "operation requires an active Fiber scheduler"
    end

    def native_timeouts?(target = Fiber.scheduler)
      !target.nil? && target.respond_to?(:timeout_after)
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

    def park(blocker, waiter, deadline = nil)
      scheduler = waiter[:scheduler]
      task = current_task
      task&.enter_block(waiter)

      until waiter[:ready] || yield
        remaining = deadline ? (deadline - monotonic_now) : nil
        return false if deadline && remaining <= 0

        scheduler.block(blocker, remaining)
        task&.raise_if_cancelled!
      end
      task&.raise_if_cancelled!
      true
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
      if waiter[:queued]
        waiters.delete(waiter)
        waiter[:queued] = false
      end
    end

    def wake_dequeued(waiter, blocker)
      waiter[:queued] = false
      wake(waiter, blocker)
    end

    def wake(waiter, blocker)
      return false if waiter[:ready]

      waiter[:ready] = true
      fiber = waiter[:fiber]
      return false unless fiber.alive?

      waiter[:scheduler].unblock(blocker, fiber)
      true
    end

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def deadline_for(timeout)
      return nil if timeout.nil?

      seconds = Float(timeout)
      raise ArgumentError, "timeout must be non-negative and finite" unless seconds.finite? && seconds >= 0

      monotonic_now + seconds
    end

    def with_timeout(duration)
      return yield if duration.nil?

      seconds = Float(duration)
      raise ArgumentError, "timeout must be non-negative and finite" unless seconds.finite? && seconds >= 0

      begin
        arm_deadline(seconds) { yield }
      rescue Deadline => e
        raise TimeoutError, e.message
      end
    end

    def arm_deadline(seconds, &block)
      target = Fiber.scheduler
      if native_timeouts?(target)
        return target.timeout_after(seconds, Deadline, DEADLINE_MESSAGE, &block)
      end

      warn_missing_timeout_hook(target) if target
      ::Timeout.timeout(seconds, Deadline, DEADLINE_MESSAGE, &block)
    end

    def warn_missing_timeout_hook(target)
      key = target.class
      return if @warned_schedulers[key]

      @warned_schedulers[key] = true
      warn(format(MISSING_TIMEOUT_HOOK_WARNING, key))
    end
  end
end

require_relative "runtime/notification"
require_relative "runtime/queue"
require_relative "runtime/semaphore"
require_relative "runtime/task"
