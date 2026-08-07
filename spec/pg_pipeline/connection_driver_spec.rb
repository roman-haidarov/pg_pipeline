# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::ConnectionDriver do
  let(:driver_result) do
    Struct.new(:result_status, :error_message, :cleared) do
      def clear
        self.cleared = true
      end
    end
  end

  def request
    PgPipeline::Request.new(sql: "SELECT 1").tap do |value|
      value.queued!
      value.dispatched!
    end
  end

  def driver_for(connection, inflight)
    driver = described_class.allocate
    driver.instance_variable_set(:@conn, connection)
    driver.instance_variable_set(:@inflight, inflight)
    driver
  end

  def drain(driver)
    PgPipeline::DriverOps.drain_results(driver)
  end

  it "does not pop the FIFO on PGRES_FATAL_ERROR before query nil + Sync" do
    Async do
      req = request
      error_result = driver_result.new(PG::PGRES_FATAL_ERROR, "bad sql", false)
      sync_result = driver_result.new(PG::PGRES_PIPELINE_SYNC, nil, false)
      connection = instance_double("PG::Connection")

      allow(connection).to receive(:is_busy).and_return(false, true, false, false)
      allow(connection).to receive(:sync_get_result).and_return(error_result, nil, sync_result)

      driver = driver_for(connection, [req])
      drain(driver)

      expect(driver.instance_variable_get(:@inflight)).to eq([req])
      expect(req.settled?).to be(false)

      drain(driver)

      expect(driver.instance_variable_get(:@inflight)).to be_empty
      expect(req.settled?).to be(true)
      expect { req.wait }.to raise_error(PgPipeline::QueryError, "bad sql")
    end.wait
  end

  it "keeps later requests aligned after a failed request" do
    Async do
      first = request
      second = request
      error_result = driver_result.new(PG::PGRES_FATAL_ERROR, "bad sql", false)
      first_sync = driver_result.new(PG::PGRES_PIPELINE_SYNC, nil, false)
      second_result = driver_result.new(PG::PGRES_TUPLES_OK, nil, false)
      second_sync = driver_result.new(PG::PGRES_PIPELINE_SYNC, nil, false)
      connection = instance_double("PG::Connection")

      allow(connection).to receive(:is_busy).and_return(false)
      allow(connection).to receive(:sync_get_result)
        .and_return(error_result, nil, first_sync, second_result, nil, second_sync)

      driver = driver_for(connection, [first, second])
      drain(driver)

      expect(driver.instance_variable_get(:@inflight)).to be_empty
      expect { first.wait }.to raise_error(PgPipeline::QueryError, "bad sql")
      expect(second.wait).to equal(second_result)
    end.wait
  end

  it "pops a successful request only on PGRES_PIPELINE_SYNC" do
    Async do
      req = request
      query_result = driver_result.new(PG::PGRES_TUPLES_OK, nil, false)
      sync_result = driver_result.new(PG::PGRES_PIPELINE_SYNC, nil, false)
      connection = instance_double("PG::Connection")

      allow(connection).to receive(:is_busy).and_return(false, false, true, false)
      allow(connection).to receive(:sync_get_result).and_return(query_result, nil, sync_result)

      driver = driver_for(connection, [req])
      drain(driver)

      expect(driver.instance_variable_get(:@inflight)).to eq([req])
      expect(req.settled?).to be(false)

      drain(driver)

      expect(req.wait).to equal(query_result)
      expect(driver.instance_variable_get(:@inflight)).to be_empty
    end.wait
  end

  it "distinguishes undispatched requests from indeterminate in-flight requests on connection loss" do
    Async do
      inflight = request
      queued = PgPipeline::Request.new(sql: "SELECT 2").tap(&:queued!)
      queue = instance_double(PgPipeline::BoundedQueue)
      allow(queue).to receive(:drain).and_return([queued])

      driver = described_class.allocate
      driver.instance_variable_set(:@dispatching, nil)
      driver.instance_variable_set(:@inflight, [inflight])
      driver.instance_variable_set(:@requests, queue)

      PgPipeline::DriverOps.fail_all(driver, PgPipeline::ConnectionLostError.new("lost"))

      expect { inflight.wait }.to raise_error(PgPipeline::IndeterminateResultError, /outcome is indeterminate/)
      expect { queued.wait }.to raise_error(PgPipeline::NotDispatchedError, /was not dispatched/)
    end.wait
  end

  it "fails closed on result modes the public API did not enable" do
    Async do
      req = request
      single_row = driver_result.new(PG::PGRES_SINGLE_TUPLE, nil, false)
      connection = instance_double("PG::Connection", is_busy: false, sync_get_result: single_row)
      driver = driver_for(connection, [req])

      expect { drain(driver) }
        .to raise_error(PgPipeline::ProtocolError, /unexpected pipeline result status/)
      expect(single_row.cleared).to be(true)
    end.wait
  end
  it "keeps client-side parameter errors request-local" do
    Async do
      req = PgPipeline::Request.new(sql: "SELECT $1", params: [Object.new]).tap(&:queued!)
      connection = instance_double("PG::Connection")
      caps = instance_double(PgPipeline::ServerCaps)

      allow(connection).to receive(:send_query_params).and_raise(TypeError, "bad bind")
      expect(caps).not_to receive(:place_sync)

      driver = described_class.allocate
      driver.instance_variable_set(:@conn, connection)
      driver.instance_variable_set(:@caps, caps)
      driver.instance_variable_set(:@dispatching, req)

      expect(PgPipeline::DriverOps.send_unit(driver, req)).to be(false)
      expect(req.settled?).to be(true)
      expect { req.wait }.to raise_error(TypeError, "bad bind")
    end.wait
  end

  it "keeps arbitrary Ruby-side parameter encoder errors request-local" do
    Async do
      req = PgPipeline::Request.new(sql: "SELECT $1", params: [Object.new]).tap(&:queued!)
      connection = instance_double("PG::Connection")
      caps = instance_double(PgPipeline::ServerCaps)

      allow(connection).to receive(:send_query_params).and_raise(RuntimeError, "custom encoder exploded")
      expect(caps).not_to receive(:place_sync)

      driver = described_class.allocate
      driver.instance_variable_set(:@conn, connection)
      driver.instance_variable_set(:@caps, caps)
      driver.instance_variable_set(:@dispatching, req)

      expect(PgPipeline::DriverOps.send_unit(driver, req)).to be(false)
      expect(req.settled?).to be(true)
      expect { req.wait }.to raise_error(RuntimeError, "custom encoder exploded")
    end.wait
  end

  it "keeps a non-fatal PG::UnableToSend request-local and definitely not dispatched" do
    Async do
      req = PgPipeline::Request.new(sql: "SELECT 1").tap(&:queued!)
      connection = instance_double(
        "PG::Connection",
        finished?: false,
        status: PG::CONNECTION_OK,
        pipeline_status: PG::PQ_PIPELINE_ON
      )
      caps = instance_double(PgPipeline::ServerCaps)

      allow(connection).to receive(:send_query_params).and_raise(PG::UnableToSend, "send rejected")
      expect(caps).not_to receive(:place_sync)

      driver = described_class.allocate
      driver.instance_variable_set(:@conn, connection)
      driver.instance_variable_set(:@caps, caps)
      driver.instance_variable_set(:@dispatching, req)

      expect(PgPipeline::DriverOps.send_unit(driver, req)).to be(false)

      expect(driver.instance_variable_get(:@dispatching)).to be_nil
      expect { req.wait }.to raise_error(PgPipeline::NotDispatchedError, /before libpq accepted/)
    end.wait
  end

  it "tears down the driver after PG::UnableToSend when the connection is no longer reusable" do
    Async do
      req = PgPipeline::Request.new(sql: "SELECT 1").tap(&:queued!)
      connection = instance_double(
        "PG::Connection",
        finished?: false,
        status: PG::CONNECTION_BAD,
        pipeline_status: PG::PQ_PIPELINE_ON
      )
      caps = instance_double(PgPipeline::ServerCaps)

      allow(connection).to receive(:send_query_params).and_raise(PG::UnableToSend, "send rejected")
      expect(caps).not_to receive(:place_sync)

      driver = described_class.allocate
      driver.instance_variable_set(:@conn, connection)
      driver.instance_variable_set(:@caps, caps)
      driver.instance_variable_set(:@dispatching, req)

      expect {
        PgPipeline::DriverOps.send_unit(driver, req)
      }.to raise_error(PgPipeline::ConnectionLostError, /dispatch failed/)

      expect { req.wait }.to raise_error(PgPipeline::NotDispatchedError, /before libpq accepted/)
    end.wait
  end

  it "treats other PG send errors as driver-fatal even though the current unit was not dispatched" do
    Async do
      req = PgPipeline::Request.new(sql: "SELECT 1").tap(&:queued!)
      connection = instance_double("PG::Connection")
      caps = instance_double(PgPipeline::ServerCaps)

      allow(connection).to receive(:send_query_params).and_raise(PG::ConnectionBad, "connection closed")
      expect(caps).not_to receive(:place_sync)

      driver = described_class.allocate
      driver.instance_variable_set(:@conn, connection)
      driver.instance_variable_set(:@caps, caps)
      driver.instance_variable_set(:@dispatching, req)

      expect {
        PgPipeline::DriverOps.send_unit(driver, req)
      }.to raise_error(PgPipeline::ConnectionLostError, /dispatch failed/)

      expect { req.wait }.to raise_error(PgPipeline::NotDispatchedError, /before libpq accepted/)
    end.wait
  end

  it "aborts a timed-out health probe only while it is the driver's sole work" do
    Async do
      probe = request
      queue = instance_double(PgPipeline::BoundedQueue, empty?: true)
      driver = described_class.allocate
      driver.instance_variable_set(:@accepting, true)
      driver.instance_variable_set(:@running, true)
      driver.instance_variable_set(:@dispatching, nil)
      driver.instance_variable_set(:@submitting, 0)
      driver.instance_variable_set(:@requests, queue)
      driver.instance_variable_set(:@inflight, [probe])

      expect(PgPipeline::DriverOps).to receive(:abort!)
        .with(driver, an_instance_of(PgPipeline::ConnectionLostError))

      expect(PgPipeline::DriverOps.abort_timed_out_health_probe(driver, probe)).to be(false)
    end.wait
  end

  it "does not let a timed-out health probe abort concurrent user work" do
    Async do
      probe = request
      user = request
      queue = instance_double(PgPipeline::BoundedQueue, empty?: true)
      driver = described_class.allocate
      driver.instance_variable_set(:@accepting, true)
      driver.instance_variable_set(:@running, true)
      driver.instance_variable_set(:@dispatching, nil)
      driver.instance_variable_set(:@submitting, 0)
      driver.instance_variable_set(:@requests, queue)
      driver.instance_variable_set(:@inflight, [probe, user])

      expect(PgPipeline::DriverOps).not_to receive(:abort!)
      expect(PgPipeline::DriverOps.abort_timed_out_health_probe(driver, probe)).to be(true)
    end.wait
  end

  it "keeps a request new when the pending queue closes before accepting it" do
    Async do
      req = PgPipeline::Request.new(sql: "SELECT 1")
      queue = instance_double(PgPipeline::BoundedQueue)
      allow(queue).to receive(:enqueue)
        .and_raise(PgPipeline::NotDispatchedError, "queue closed; request was not dispatched")

      driver = described_class.allocate
      driver.instance_variable_set(:@accepting, true)
      driver.instance_variable_set(:@requests, queue)
      driver.instance_variable_set(:@submitting, 0)
      driver.instance_variable_set(:@draining, false)
      driver.instance_variable_set(:@running, true)

      expect {
        PgPipeline::DriverOps.submit(driver, req)
      }.to raise_error(PgPipeline::NotDispatchedError)

      expect(req.state).to eq(:new)
      expect(req.settled?).to be(false)
    end.wait
  end

  describe "#load" do
    it "counts the dispatching slot so a driver is not transiently reported idle" do
      driver = described_class.allocate
      driver.instance_variable_set(:@requests, instance_double(PgPipeline::BoundedQueue, size: 0))
      driver.instance_variable_set(:@inflight, [])
      driver.instance_variable_set(:@submitting, 0)
      driver.instance_variable_set(:@dispatching, Object.new)

      expect(driver.load).to eq(1)
    end
  end

  it "closes wait points and socket_io before stopping watchers so IO waits unblock" do
    driver = described_class.allocate
    reader = instance_double(PgPipeline::Runtime::Task, stop: true, wait: nil)
    writer = instance_double(PgPipeline::Runtime::Task, stop: true, wait: nil)
    socket = instance_double(IO)
    rearm = instance_double(PgPipeline::Runtime::Queue, close: nil)
    commands = instance_double(PgPipeline::Runtime::Queue, close: nil)
    events = instance_double(PgPipeline::Runtime::Queue, close: nil)
    conn = instance_double("PG::Connection", finished?: true)
    driver.instance_variable_set(:@reader_task, reader)
    driver.instance_variable_set(:@writer_task, writer)
    driver.instance_variable_set(:@reader_rearm, rearm)
    driver.instance_variable_set(:@writer_commands, commands)
    driver.instance_variable_set(:@events, events)
    driver.instance_variable_set(:@conn, conn)
    driver.instance_variable_set(:@socket, socket)
    driver.instance_variable_set(:@leaked_watchers, 0)

    expect(rearm).to receive(:close).ordered
    expect(commands).to receive(:close).ordered
    expect(events).to receive(:close).ordered
    expect(socket).to receive(:close).ordered
    expect(reader).to receive(:stop).ordered
    expect(writer).to receive(:stop).ordered
    expect(conn).not_to receive(:close) # finished? true → safe_close_conn no-ops

    PgPipeline::DriverOps.teardown_watchers(driver)
    expect(driver.socket).to be_nil
  end

  it "joins both watcher tasks even when the first wait raises" do
    driver = described_class.allocate
    reader = instance_double(PgPipeline::Runtime::Task, stop: true)
    writer = instance_double(PgPipeline::Runtime::Task, stop: true)
    rearm = instance_double(PgPipeline::Runtime::Queue, close: nil)
    commands = instance_double(PgPipeline::Runtime::Queue, close: nil)
    events = instance_double(PgPipeline::Runtime::Queue, close: nil)
    conn = instance_double("PG::Connection", finished?: true)
    driver.instance_variable_set(:@reader_task, reader)
    driver.instance_variable_set(:@writer_task, writer)
    driver.instance_variable_set(:@reader_rearm, rearm)
    driver.instance_variable_set(:@writer_commands, commands)
    driver.instance_variable_set(:@events, events)
    driver.instance_variable_set(:@conn, conn)
    driver.instance_variable_set(:@leaked_watchers, 0)

    allow(reader).to receive(:wait).and_raise(RuntimeError, "reader failed")
    expect(writer).to receive(:wait).with(PgPipeline::DriverOps::WATCHER_JOIN_TIMEOUT)

    expect { PgPipeline::DriverOps.teardown_watchers(driver) }.not_to raise_error
    expect(driver.reader_task).to be_nil
    expect(driver.writer_task).to be_nil
  end

  it "still joins the second watcher when the first wait raises Runtime::Cancel" do
    driver = described_class.allocate
    reader = instance_double(PgPipeline::Runtime::Task, stop: true)
    writer = instance_double(PgPipeline::Runtime::Task, stop: true)
    rearm = instance_double(PgPipeline::Runtime::Queue, close: nil)
    commands = instance_double(PgPipeline::Runtime::Queue, close: nil)
    events = instance_double(PgPipeline::Runtime::Queue, close: nil)
    conn = instance_double("PG::Connection", finished?: true)
    driver.instance_variable_set(:@reader_task, reader)
    driver.instance_variable_set(:@writer_task, writer)
    driver.instance_variable_set(:@reader_rearm, rearm)
    driver.instance_variable_set(:@writer_commands, commands)
    driver.instance_variable_set(:@events, events)
    driver.instance_variable_set(:@conn, conn)
    driver.instance_variable_set(:@leaked_watchers, 0)

    allow(reader).to receive(:wait).and_raise(PgPipeline::Runtime::Cancel.new("cancelled"))
    expect(writer).to receive(:wait).with(PgPipeline::DriverOps::WATCHER_JOIN_TIMEOUT)

    expect { PgPipeline::DriverOps.teardown_watchers(driver) }.not_to raise_error
  end

  it "joins the owner after a failed start tears down watchers" do
    driver = described_class.allocate
    owner = instance_double(PgPipeline::Runtime::Task, fiber: Object.new, name: :owner)
    driver.instance_variable_set(:@running, false)
    driver.instance_variable_set(:@accepting, false)
    driver.instance_variable_set(:@owner_task, owner)
    driver.instance_variable_set(:@reader_task, nil)
    driver.instance_variable_set(:@writer_task, nil)
    driver.instance_variable_set(:@reader_rearm, instance_double(PgPipeline::Runtime::Queue, close: nil))
    driver.instance_variable_set(:@writer_commands, instance_double(PgPipeline::Runtime::Queue, close: nil))
    driver.instance_variable_set(:@events, instance_double(PgPipeline::Runtime::Queue, close: nil))
    driver.instance_variable_set(:@conn, instance_double("PG::Connection", finished?: true))
    driver.instance_variable_set(:@leaked_watchers, 0)

    expect(owner).to receive(:wait).with(PgPipeline::DriverOps::OWNER_JOIN_TIMEOUT)

    # Simulate the start rescue path: watchers already torn down, owner still set.
    PgPipeline::DriverOps.teardown_watchers(driver)
    PgPipeline::DriverOps.join_owner(driver)
  end

  it "reports the owner join timeout in leak warnings" do
    driver = described_class.allocate
    driver.instance_variable_set(:@leaked_watchers, 1)
    task = instance_double(PgPipeline::Runtime::Task, name: :owner)

    expect {
      PgPipeline::DriverOps.warn_leaked_task(driver, task, PgPipeline::DriverOps::OWNER_JOIN_TIMEOUT)
    }.to output(/task :owner.*#{PgPipeline::DriverOps::OWNER_JOIN_TIMEOUT}s/).to_stderr
  end

  it "does not call wait on a nil owner_task during abort!" do
    driver = described_class.allocate
    driver.instance_variable_set(:@running, true)
    driver.instance_variable_set(:@accepting, true)
    driver.instance_variable_set(:@owner_task, nil)
    driver.instance_variable_set(
      :@requests,
      instance_double(PgPipeline::BoundedQueue, close: nil)
    )
    driver.instance_variable_set(
      :@events,
      instance_double(PgPipeline::Runtime::Queue, enqueue: nil)
    )

    expect { PgPipeline::DriverOps.abort!(driver, PgPipeline::ConnectionLostError.new("gone")) }
      .not_to raise_error
  end

  describe ".process_event" do
    def event_driver(connection, inflight:)
      driver = described_class.allocate
      driver.instance_variable_set(:@conn, connection)
      driver.instance_variable_set(:@inflight, inflight)
      driver.instance_variable_set(:@max_in_flight, inflight.length)
      driver.instance_variable_set(:@requests, instance_double(PgPipeline::BoundedQueue, empty?: true))
      driver.instance_variable_set(:@reader_rearm, instance_double(PgPipeline::Runtime::Queue, enqueue: nil))
      driver.instance_variable_set(:@writer_commands, instance_double(PgPipeline::Runtime::Queue, enqueue: nil))
      driver.instance_variable_set(:@running, true)
      driver.instance_variable_set(:@draining, false)
      driver.instance_variable_set(:@needs_flush, false)
      driver.instance_variable_set(:@writer_armed, false)
      driver.instance_variable_set(:@request_event_pending, true)
      driver.instance_variable_set(:@submitting, 0)
      driver.instance_variable_set(:@dispatching, nil)
      driver
    end

    it "does not poll PQisBusy for a request-queue event" do
      req = request
      connection = instance_double("PG::Connection")
      expect(connection).not_to receive(:is_busy)
      driver = event_driver(connection, inflight: [req])

      PgPipeline::DriverOps.process_event(driver, :requests)

      expect(driver.request_event_pending).to be(false)
    end

    it "consumes input and drains exactly once for a readable event" do
      req = request
      connection = instance_double("PG::Connection")
      expect(connection).to receive(:consume_input).once
      expect(connection).to receive(:is_busy).once.and_return(true)
      driver = event_driver(connection, inflight: [req])

      PgPipeline::DriverOps.process_event(driver, :readable)
    end

    it "flushes a writable event without polling for results" do
      req = request
      connection = instance_double("PG::Connection")
      expect(connection).to receive(:sync_flush).once.and_return(true)
      expect(connection).not_to receive(:is_busy)
      driver = event_driver(connection, inflight: [req])
      driver.writer_armed = true

      PgPipeline::DriverOps.process_event(driver, :writable)

      expect(driver.writer_armed).to be(false)
    end

    it "does not retry a blocked flush on an unrelated request event" do
      req = request
      connection = instance_double("PG::Connection")
      expect(connection).not_to receive(:sync_flush)
      driver = event_driver(connection, inflight: [req])
      driver.needs_flush = true
      driver.writer_armed = true

      PgPipeline::DriverOps.process_event(driver, :requests)

      expect(driver.needs_flush).to be(true)
      expect(driver.writer_armed).to be(true)
    end
  end

  describe ".send_command" do
    let(:statement) do
      PgPipeline::PreparedStatement.new(
        client: Object.new,
        name: "by_id",
        physical_name: "pgp_1",
        sql: "SELECT $1::int",
        param_types: [23]
      )
    end

    it "uses send_prepare for prepare units" do
      connection = instance_double("PG::Connection")
      request = PgPipeline::Request.prepare(statement)
      expect(connection).to receive(:send_prepare).with("pgp_1", "SELECT $1::int", [23])

      PgPipeline::DriverOps.send_command(connection, request)
    end

    it "uses send_query_prepared for prepared executions" do
      connection = instance_double("PG::Connection")
      request = PgPipeline::Request.prepared_query(statement, params: [7])
      expect(connection).to receive(:send_query_prepared).with("pgp_1", [7])

      PgPipeline::DriverOps.send_command(connection, request)
    end
  end
end
