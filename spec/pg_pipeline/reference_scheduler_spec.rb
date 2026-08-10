# frozen_string_literal: true

require "spec_helper"

# The harness gets its own coverage so that a broken scheduler cannot show up as
# a pg_pipeline bug in the native specs that depend on it.
RSpec.describe PgPipeline::Test::ReferenceScheduler do
  it "runs a block and returns its value" do
    expect(described_class.run { 1 + 1 }).to eq(2)
  end

  it "propagates exceptions out of the scheduled fiber" do
    expect { described_class.run { raise ArgumentError, "boom" } }
      .to raise_error(ArgumentError, "boom")
  end

  it "interleaves fibers parked on Runtime primitives" do
    order = described_class.run do
      log = []
      queue = PgPipeline::Runtime::Queue.new

      consumer = PgPipeline::Runtime.spawn do
        3.times { log << queue.dequeue }
      end

      producer = PgPipeline::Runtime.spawn do
        3.times { |i| queue.enqueue(i) }
      end

      producer.wait
      consumer.wait
      log
    end

    expect(order).to eq([0, 1, 2])
  end

  it "wakes a fiber blocked on IO readability" do
    result = described_class.run do
      reader, writer = IO.pipe
      waiter = PgPipeline::Runtime.spawn do
        reader.wait_readable(5)
        reader.read_nonblock(4)
      end
      PgPipeline::Runtime.spawn { writer.write("ping") }.wait
      value = waiter.wait
      [reader, writer].each(&:close)
      value
    end

    expect(result).to eq("ping")
  end

  it "supports kernel_sleep" do
    elapsed = described_class.run do
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      sleep 0.02
      Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    end

    expect(elapsed).to be >= 0.02
  end

  it "interrupts a fiber parked in io_wait so watcher shutdown cannot hang" do
    stopped = described_class.run do
      reader, writer = IO.pipe
      task = PgPipeline::Runtime.spawn { reader.wait_readable(30) }
      PgPipeline::Runtime.spawn { nil }.wait
      result = task.stop
      task.wait(1)
      [reader, writer].each(&:close)
      result
    end

    expect(stopped).to be(true)
  end
end
