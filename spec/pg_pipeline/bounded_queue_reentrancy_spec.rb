# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::BoundedQueue do
  describe "#close" do
    it "re-raises the exact exception object it was closed with" do
      error = PgPipeline::NotDispatchedError.new("driver went away")
      queue = described_class.new(1)
      queue.close(error)

      expect { queue.enqueue(:x) }.to raise_error { |raised| expect(raised).to equal(error) }
    end

    it "preserves the backtrace of the closing error" do
      error = begin
        raise PgPipeline::ShutdownError, "pool stopped"
      rescue PgPipeline::ShutdownError => e
        e
      end
      queue = described_class.new(1)
      queue.close(error)

      expect { queue.enqueue(:x) }.to raise_error(PgPipeline::ShutdownError) { |raised|
        expect(raised.backtrace).to eq(error.backtrace)
      }
    end

    # Rebuilding the error from (class, message) required every close error to
    # have a one-String constructor. Anything else turned a shutdown into an
    # unrelated ArgumentError at the call site.
    it "works for an error class whose #initialize is not (String)" do
      klass = Class.new(PgPipeline::ShutdownError) do
        def initialize(code, message)
          @code = code
          super("#{message} (#{code})")
        end
      end
      error = klass.new(42, "boom")
      queue = described_class.new(1)
      queue.close(error)

      expect { queue.enqueue(:x) }.to raise_error(klass, "boom (42)")
    end

    it "falls back to ShutdownError when closed with a non-exception" do
      queue = described_class.new(1)
      queue.close(:not_an_exception)

      expect { queue.enqueue(:x) }.to raise_error(PgPipeline::ShutdownError, /queue closed/)
    end

    it "only honours the first close" do
      first = PgPipeline::ShutdownError.new("first")
      queue = described_class.new(1)
      queue.close(first)
      queue.close(PgPipeline::ShutdownError.new("second"))

      expect { queue.enqueue(:x) }.to raise_error(PgPipeline::ShutdownError, "first")
    end
  end

  describe "waking every blocked waiter" do
    # Fiber::Scheduler#unblock is allowed to resume the woken fiber immediately.
    # bq_wake_all used to capture the waiter count, run Ruby for each entry, and
    # only then zero the length -- so anything that queued during the walk was
    # dropped, and the walk itself indexed a list that could have been realloc'd
    # underneath it.
    def wait_until(limit = 2000)
      limit.times do
        return true if yield

        sleep(0)
      end
      false
    end

    it "does not drop waiters while close is waking others" do
      results = []

      PgPipeline::Test::ReferenceScheduler.run do
        queue = described_class.new(1)
        queue.enqueue(:occupied)

        3.times do |i|
          Fiber.schedule do
            queue.enqueue(:"blocked#{i}")
            results << :"enqueued#{i}"
          rescue PgPipeline::ShutdownError
            results << :"shutdown#{i}"
          end
        end

        expect(wait_until { queue.waiting_producers == 3 }).to be(true)
        queue.close(PgPipeline::ShutdownError.new("stopping"))
        expect(wait_until { results.size == 3 }).to be(true)
      end

      expect(results.sort).to eq(%i[shutdown0 shutdown1 shutdown2])
    end

    it "hands the slot to exactly one waiting producer per dequeue" do
      accepted = []

      PgPipeline::Test::ReferenceScheduler.run do
        queue = described_class.new(1)
        queue.enqueue(0)

        5.times do |i|
          Fiber.schedule do
            queue.enqueue(i + 1)
            accepted << (i + 1)
          end
        end

        expect(wait_until { queue.waiting_producers == 5 }).to be(true)
        5.times do
          queue.dequeue
          sleep(0)
        end
        expect(wait_until { accepted.size == 5 }).to be(true)
      end

      expect(accepted.sort).to eq([1, 2, 3, 4, 5])
    end
  end

  describe "#drain" do
    it "empties the queue and releases blocked producers" do
      queue = described_class.new(2)
      queue.enqueue(:a)
      queue.enqueue(:b)

      expect(queue.drain).to eq(%i[a b])
      expect(queue).to be_empty
      expect(queue.waiting_producers).to eq(0)
    end
  end
end
