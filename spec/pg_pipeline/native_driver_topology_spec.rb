# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::NativeDriverOps do
  FakeDriver = Struct.new(
    :running, :draining, :reader_draining, :submitting, :dispatching,
    :needs_flush, :flush_pending, :flush_event_pending, :max_in_flight,
    :core, :requests, :events,
    keyword_init: true
  )

  def driver(attrs = {})
    core = attrs.delete(:core) || instance_double(
      PgPipeline::Native::Driver,
      inflight_count: attrs.fetch(:inflight_count, 0)
    )
    requests = attrs.delete(:requests) || instance_double(
      PgPipeline::BoundedQueue,
      empty?: attrs.fetch(:requests_empty, true),
      size: attrs.fetch(:requests_size, 0)
    )

    FakeDriver.new(
      running: true,
      draining: false,
      reader_draining: false,
      submitting: 0,
      dispatching: nil,
      needs_flush: false,
      flush_pending: false,
      flush_event_pending: false,
      max_in_flight: 64,
      core: core,
      requests: requests,
      events: nil,
      **attrs.except(:inflight_count, :requests_empty, :requests_size)
    )
  end

  describe ".inline_dispatchable?" do
    it "is true when the queue is empty and there is free in-flight capacity" do
      expect(described_class.inline_dispatchable?(driver)).to be(true)
      expect(described_class.inline_dispatchable?(driver(inflight_count: 1))).to be(true)
    end

    it "is false when the in-flight queue is full" do
      expect(described_class.inline_dispatchable?(driver(inflight_count: 64, max_in_flight: 64))).to be(false)
    end

    it "is false while the reader is mid-drain (blocks re-entrant PQsend)" do
      expect(described_class.inline_dispatchable?(driver(reader_draining: true))).to be(false)
    end

    it "is false when the request queue is non-empty (FIFO)" do
      expect(described_class.inline_dispatchable?(driver(requests_empty: false))).to be(false)
    end

    it "is false when another fiber is already dispatching or submitting" do
      expect(described_class.inline_dispatchable?(driver(dispatching: :busy))).to be(false)
      expect(described_class.inline_dispatchable?(driver(submitting: 1))).to be(false)
    end

    it "is false when the driver is not running or is draining for close" do
      expect(described_class.inline_dispatchable?(driver(running: false))).to be(false)
      expect(described_class.inline_dispatchable?(driver(draining: true))).to be(false)
    end
  end

  describe ".notify_flush" do
    it "enqueues a single coalesced :flush event" do
      events = []
      d = driver
      d.events = Object.new
      d.events.define_singleton_method(:enqueue) { |item| events << item; item }
      d.flush_event_pending = false

      described_class.notify_flush(d)
      described_class.notify_flush(d)

      expect(events).to eq([:flush])
      expect(d.flush_event_pending).to be(true)
    end

    it "clears flush_event_pending when the event queue is already closed" do
      d = driver
      d.events = Object.new
      d.events.define_singleton_method(:enqueue) { |_item| nil }
      d.flush_event_pending = false

      described_class.notify_flush(d)

      expect(d.flush_event_pending).to be(false)
    end
  end

  describe ".drained?" do
    it "requires flush_pending to be clear before graceful close can finish" do
      d = driver(flush_pending: true)
      allow(d.core).to receive(:inflight_count).and_return(0)
      allow(d.requests).to receive(:empty?).and_return(true)

      expect(described_class.drained?(d)).to be(false)

      d.flush_pending = false
      expect(described_class.drained?(d)).to be(true)
    end
  end

  describe ".raise_inline_submit_error!" do
    it "re-raises a NotDispatchedError already recorded on the request" do
      request = PgPipeline::Request.build("SELECT 1", nil)
      err = PgPipeline::NotDispatchedError.new("libpq rejected")
      request.reject!(err)

      expect {
        described_class.raise_inline_submit_error!(request, PgPipeline::ConnectionLostError.new("lost"))
      }.to raise_error(PgPipeline::NotDispatchedError, /libpq rejected/)
    end

    it "settles an unset request as IndeterminateResultError on post-wire loss" do
      request = PgPipeline::Request.build("SELECT 1", nil)
      lost = PgPipeline::ConnectionLostError.new("sync failed")

      expect {
        described_class.raise_inline_submit_error!(request, lost)
      }.to raise_error(PgPipeline::IndeterminateResultError)

      expect(request).to be_settled
      expect(request.error).to be_a(PgPipeline::IndeterminateResultError)
    end
  end
end
