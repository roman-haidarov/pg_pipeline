# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::BoundedQueue do
  it "applies producer backpressure and resumes when capacity is released" do
    Async do |task|
      queue = described_class.new(1)
      queue.enqueue(:first)

      producer = task.async do
        queue.enqueue(:second)
        :done
      end

      task.yield
      expect(producer.finished?).to be(false)
      expect(queue.dequeue).to eq(:first)
      expect(producer.wait).to eq(:done)
      expect(queue.dequeue).to eq(:second)
    end.wait
  end

  it "wakes blocked producers with the close error" do
    Async do |task|
      queue = described_class.new(1)
      queue.enqueue(:first)

      producer = task.async do
        queue.enqueue(:second)
      end

      task.yield
      queue.close(PgPipeline::ShutdownError.new("closing"))

      expect { producer.wait }.to raise_error(PgPipeline::ShutdownError, "closing")
    end.wait
  end

  it "wakes only one producer per free slot (no thundering herd)" do
    Async do |task|
      queue = described_class.new(1)
      producers = []
      begin
        queue.enqueue(:held)

        producers = 5.times.map do |i|
          task.async do
            queue.enqueue(i)
            i
          rescue PgPipeline::ShutdownError
            nil
          end
        end

        task.yield
        expect(queue.waiting_producers).to eq(5)
        expect(queue.dequeue).to eq(:held)

        10.times do
          break if producers.any?(&:finished?)

          task.yield
        end

        finished = producers.select(&:finished?)
        expect(finished.size).to eq(1)
        expect(queue.waiting_producers).to eq(4)
        expect(queue.size).to eq(1)
      ensure
        queue.close(PgPipeline::ShutdownError.new("test cleanup"))
        producers.each do |producer|
          producer.wait
        rescue StandardError
          nil
        end
      end
    end.wait
  end

  it "does not strand capacity when a blocked producer is cancelled" do
    Async do |task|
      queue = described_class.new(1)
      queue.enqueue(:held)

      first = task.async { queue.enqueue(:first) }
      second = task.async do
        queue.enqueue(:second)
        :ok
      end

      task.yield
      expect(queue.waiting_producers).to eq(2)

      first.stop
      task.yield

      expect(queue.dequeue).to eq(:held)
      expect(second.wait).to eq(:ok)
      expect(queue.dequeue).to eq(:second)
      expect(queue.waiting_producers).to eq(0)
    end.wait
  end
end
