# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::Request do
  let(:request_result) do
    Struct.new(:cleared) do
      def clear
        self.cleared = true
      end
    end
  end

  def dispatched_request
    described_class.new(sql: "SELECT 1").tap do |request|
      request.queued!
      request.dispatched!
    end
  end

  def async_example(&block)
    Async(&block).wait
  end

  it "snapshots query parameters before asynchronous dispatch" do
    async_example do
      string = +"one"
      value = +"two"
      params = [string, {value: value, type: 0}]
      request = described_class.new(sql: "SELECT $1, $2", params: params)

      string.replace("changed")
      value.replace("changed")
      params << "three"

      expect(request.params).to eq(["one", {value: "two", type: 0}])
      expect(request.params).to be_frozen
      expect(request.params[0]).to be_frozen
      expect(request.params[1]).to be_frozen
      expect(request.params[1][:value]).to be_frozen
    end
  end

  it "does not settle a successful result until the Sync boundary" do
    async_example do
      request = dispatched_request
      result = request_result.new(false)

      request.accept_result(result)
      request.query_boundary!

      expect(request.settled?).to be(false)
      request.finish!
      expect(request.wait).to equal(result)
    end
  end

  it "records a query error but keeps the FIFO slot open until Sync" do
    async_example do
      request = dispatched_request
      error = PgPipeline::QueryError.new("boom")

      request.record_error!(error)
      request.query_boundary!

      expect(request.settled?).to be(false)
      request.finish!
      expect { request.wait }.to raise_error(PgPipeline::QueryError, "boom")
    end
  end

  it "discards a buffered success if the connection fails before Sync" do
    async_example do
      request = dispatched_request
      result = request_result.new(false)

      request.accept_result(result)
      request.reject!(PgPipeline::ConnectionLostError.new("lost"))

      expect(result.cleared).to be(true)
      expect { request.wait }.to raise_error(PgPipeline::ConnectionLostError, "lost")
    end
  end

  it "clears a received result when the waiter is cancelled" do
    async_example do
      request = dispatched_request
      result = request_result.new(false)

      request.accept_result(result)
      request.cancel!

      expect(result.cleared).to be(true)
      expect(request.cancelled?).to be(true)
    end
  end

  it "parks one fiber and wakes it when the request settles" do
    async_example do |task|
      request = dispatched_request
      result = request_result.new(false)
      waiter = task.async { request.wait }

      task.yield

      request.accept_result(result)
      request.query_boundary!
      request.finish!

      expect(waiter.wait).to equal(result)
      expect(request.waiter).to be_nil
      expect(request.waiter_scheduler).to be_nil
    end
  end

  it "rejects a second concurrent waiter instead of losing the first one" do
    async_example do |task|
      request = dispatched_request
      first_waiter = task.async { request.wait }

      task.yield

      expect { request.wait }
        .to raise_error(PgPipeline::ProtocolError, "request already has a waiter")

      request.reject!(PgPipeline::ShutdownError.new("closing"))
      expect { first_waiter.wait }.to raise_error(PgPipeline::ShutdownError, "closing")
    end
  end

  it "clears an interrupted waiter before a late completion" do
    async_example do |task|
      request = dispatched_request
      waiter = task.async do |child|
        child.with_timeout(0.01) { request.wait }
      end

      expect { waiter.wait }.to raise_error(Async::TimeoutError)
      expect(request.waiter).to be_nil
      expect(request.waiter_scheduler).to be_nil

      request.reject!(PgPipeline::ConnectionLostError.new("late failure"))
      expect { request.wait }.to raise_error(PgPipeline::ConnectionLostError, "late failure")
    end
  end

  it "requires a scheduler only while an unsettled request must block" do
    request = dispatched_request

    expect { request.wait }
      .to raise_error(PgPipeline::Error, "request wait requires an active Fiber scheduler")

    request.reject!(PgPipeline::ShutdownError.new("closed"))
    expect { request.wait }.to raise_error(PgPipeline::ShutdownError, "closed")
  end

  it "reuses an already-frozen SQL string without duplicating it" do
    sql = "SELECT 1".freeze
    request = described_class.new(sql: sql)

    expect(request.sql).to equal(sql)
    expect(request.instance_variables).not_to include(:@statement_name, :@param_types, :@operation)
  end

  it "builds prepared query requests without copying the statement SQL" do
    statement = instance_double(
      PgPipeline::PreparedStatement,
      physical_name: "pgp_1".freeze,
      sql: "SELECT $1::int".freeze
    )

    request = described_class.prepared_query(statement, params: [1])

    expect(request.operation).to eq(:prepared_query)
    expect(request.statement_name).to eq("pgp_1")
    expect(request.sql).to equal(statement.sql)
    expect(request.params).to eq([1])
  end

  it "builds prepare requests with a stable parameter type snapshot" do
    param_types = [23, nil]
    statement = instance_double(
      PgPipeline::PreparedStatement,
      physical_name: "pgp_2".freeze,
      sql: "SELECT $1::int, $2".freeze,
      param_types: param_types.freeze
    )

    request = described_class.prepare(statement)

    expect(request.operation).to eq(:prepare)
    expect(request.param_types).to eq([23, nil])
    expect(request.param_types).to be_frozen
  end
end
