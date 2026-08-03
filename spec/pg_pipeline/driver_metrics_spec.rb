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
    PgPipeline::Request.build("SELECT 1", nil).tap do |value|
      value.queued!
      value.dispatched!
    end
  end

  it "reads counters as zero on a driver that was never initialised" do
    driver = described_class.allocate

    expect(driver.readable_events).to eq(0)
    expect(driver.results_read).to eq(0)
    expect(driver.units_completed).to eq(0)
    expect(driver.flush_calls).to eq(0)
    expect(driver.flush_incomplete).to eq(0)
    expect(driver.dispatches).to eq(0)
  end

  it "counts results and completed units while draining" do
    Async do
      req = request
      tuples = driver_result.new(PG::PGRES_TUPLES_OK, nil, false)
      sync = driver_result.new(PG::PGRES_PIPELINE_SYNC, nil, false)
      connection = instance_double("PG::Connection")

      allow(connection).to receive(:is_busy).and_return(false, false, false, true)
      allow(connection).to receive(:sync_get_result).and_return(tuples, nil, sync)

      driver = described_class.allocate
      driver.instance_variable_set(:@conn, connection)
      driver.instance_variable_set(:@inflight, [req])

      PgPipeline::DriverOps.drain_results(driver)

      expect(req.settled?).to be(true)
      expect(driver.results_read).to eq(3)
      expect(driver.units_completed).to eq(1)
    end.wait
  end

  it "writes the accumulated result counter even when the drain raises" do
    Async do
      req = request
      connection = instance_double("PG::Connection")

      allow(connection).to receive(:is_busy).and_return(false, false)
      allow(connection).to receive(:sync_get_result).and_return(
        driver_result.new(PG::PGRES_TUPLES_OK, nil, false),
        driver_result.new(PG::PGRES_BAD_RESPONSE, nil, false)
      )

      driver = described_class.allocate
      driver.instance_variable_set(:@conn, connection)
      driver.instance_variable_set(:@inflight, [req])

      expect { PgPipeline::DriverOps.drain_results(driver) }
        .to raise_error(PgPipeline::ProtocolError)
      expect(driver.results_read).to eq(2)
    end.wait
  end

  describe PgPipeline::DriverOps do
    it "reports a zero ratio instead of dividing by zero" do
      expect(described_class.ratio(5, 0)).to eq(0.0)
      expect(described_class.ratio(12, 4)).to eq(3.0)
      expect(described_class.ratio(1, 3)).to eq(0.333)
    end
  end
end
