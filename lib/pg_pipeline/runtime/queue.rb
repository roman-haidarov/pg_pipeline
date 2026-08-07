# frozen_string_literal: true

module PgPipeline
  module Runtime
    class Queue
      def initialize
        @queue = ::Thread::Queue.new
      end

      def enqueue(item)
        @queue.push(item)
        item
      rescue ClosedQueueError
        nil
      end

      def dequeue
        @queue.pop
      end

      def close
        @queue.close
        nil
      end

      def closed? = @queue.closed?
      def empty? = @queue.empty?
      def size = @queue.size
    end
  end
end
