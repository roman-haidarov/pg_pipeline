# frozen_string_literal: true

module PgPipeline
  module Runtime
    class Notification
      def initialize
        @waiters = []
      end

      def wait(timeout = nil)
        Runtime.with_waiter(self, @waiters) do |waiter|
          Runtime.with_timeout(timeout) { Runtime.park(self, waiter) { false } }
          true
        rescue TimeoutError
          false
        end
      end

      def signal
        waiter = @waiters.shift or return false
        Runtime.wake_dequeued(waiter, self)
        true
      end

      def signal_all
        pending, @waiters = @waiters, []
        pending.each { |waiter| Runtime.wake_dequeued(waiter, self) }
        nil
      end
    end
  end
end
