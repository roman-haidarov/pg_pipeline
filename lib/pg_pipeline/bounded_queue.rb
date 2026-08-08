# frozen_string_literal: true

require_relative "errors"
require_relative "runtime"

module PgPipeline
  class BoundedQueue
    def initialize(limit)
      @limit = Integer(limit)
      raise ArgumentError, "limit must be >= 1" if @limit < 1
    rescue ArgumentError, TypeError
      raise ArgumentError, "limit must be an integer >= 1"
    else
      @items, @consumers, @producers = [], [], []

      @closed = false
      @close_error = nil
    end

    def enqueue(item)
      while true
        raise_close_error if @closed

        if @items.size < @limit
          @items << item
          wake_one(@consumers)
          return item
        end

        wait_on(@producers)
      end
    end

    def dequeue
      while true
        unless @items.empty?
          item = @items.shift
          wake_one(@producers)
          return item
        end

        return nil if @closed

        wait_on(@consumers)
      end
    end

    def close(error)
      return if @closed

      @closed = true
      @close_error = error
      wake_all(@consumers)
      wake_all(@producers)
      nil
    end

    def empty? = @items.empty?
    def size = @items.size
    def waiting_producers = @producers.size
    def waiting_consumers = @consumers.size

    def drain
      items = @items
      @items = []
      wake_all(@producers)
      items
    end

    private

    def wait_on(list)
      notification = Runtime::Notification.new
      list << notification
      completed = false
      notification.wait
      completed = true
    ensure
      if notification
        still_queued = list.delete(notification)
        wake_one(list) if !completed && still_queued.nil? && !@closed
      end
    end

    def wake_one(list)
      list.shift&.signal
    end

    def wake_all(list)
      pending = list.dup
      list.clear
      pending.each(&:signal)
    end

    def raise_close_error
      error = @close_error || ShutdownError.new("queue closed")
      raise error.class, error.message
    end
  end
end
