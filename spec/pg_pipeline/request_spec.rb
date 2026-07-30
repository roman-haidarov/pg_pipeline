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
end
