# frozen_string_literal: true

require "spec_helper"
require "async"

RSpec.describe "watcher wait timeout selection" do
  after { Fiber.set_scheduler(nil) if Fiber.respond_to?(:set_scheduler) }

  describe PgPipeline::NativeDriverOps do
    it "uses the zero-overhead io_wait fast path (no timeout) when the scheduler " \
       "implements #fiber_interrupt, e.g. Async and Itsi" do
      scheduler = Struct.new(:x) { def fiber_interrupt(*); end }.new(nil)
      allow(Fiber).to receive(:scheduler).and_return(scheduler)

      expect(described_class.watcher_wait_timeout).to be_nil
    end

    it "falls back to a bounded poll when the scheduler has no #fiber_interrupt" do
      scheduler = Struct.new(:x).new(nil)
      allow(Fiber).to receive(:scheduler).and_return(scheduler)

      expect(described_class.watcher_wait_timeout).to eq(described_class::WATCHER_POLL_INTERVAL)
    end

    it "falls back to a bounded poll when no scheduler is installed at all" do
      allow(Fiber).to receive(:scheduler).and_return(nil)

      expect(described_class.watcher_wait_timeout).to eq(described_class::WATCHER_POLL_INTERVAL)
    end

    it "wait_socket_readable stops as soon as d.running flips false, even mid-poll, " \
       "and never spends longer than one extra poll interval past shutdown" do
      driver = Struct.new(:running, :socket).new(true, double("socket"))
      allow(driver.socket).to receive(:wait_readable).with(0.01).and_return(nil)

      task = Thread.new do
        sleep 0.03
        driver.running = false
      end

      result = described_class.wait_socket_readable(driver, 0.01)
      task.join

      expect(result).to be(false)
      expect(driver.socket).to have_received(:wait_readable).at_least(:twice)
    end

    it "wait_socket_readable returns true immediately once the socket is actually readable" do
      driver = Struct.new(:running, :socket).new(true, double("socket"))
      allow(driver.socket).to receive(:wait_readable).with(nil).and_return(true)

      expect(described_class.wait_socket_readable(driver, nil)).to be(true)
      expect(driver.socket).to have_received(:wait_readable).once
    end

    it "wait_socket_writable follows the same contract as wait_socket_readable" do
      driver = Struct.new(:running, :socket).new(true, double("socket"))
      allow(driver.socket).to receive(:wait_writable).with(nil).and_return(true)

      expect(described_class.wait_socket_writable(driver, nil)).to be(true)
    end
  end

  it "releases a reader watcher through plain Task#stop on Async, with no self-pipe " \
     "and no IO.select involved -- Async's own #fiber_interrupt does the work" do
    watched_reader, watched_writer = IO.pipe
    driver = Struct.new(:running, :socket, :events, :reader_rearm).new(
      true,
      watched_reader,
      PgPipeline::Runtime::Queue.new,
      PgPipeline::Runtime::Queue.new
    )

    Sync do
      task = PgPipeline::Runtime.spawn(name: :reader_regression) do
        PgPipeline::NativeDriverOps.reader_watcher(driver)
      end

      sleep 0.01
      driver.running = false
      task.stop

      expect { task.wait(0.5) }.not_to raise_error
    end

    expect(watched_reader.closed?).to be(false)
  ensure
    watched_reader&.close unless watched_reader&.closed?
    watched_writer&.close unless watched_writer&.closed?
  end
end
