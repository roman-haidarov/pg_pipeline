# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::ServerCaps do
  def caps(libpq:, protocol: 3, pipeline_api: true, fast: false, raw_sync: false)
    described_class.new(
      libpq_version: libpq,
      protocol_version: protocol,
      pipeline_api: pipeline_api,
      fast_sync_api: fast,
      raw_pipeline_sync_api: raw_sync
    )
  end

  it "rejects libpq below 14 even when the server protocol is v3" do
    value = caps(libpq: 130_000)

    expect(value.supported?).to be(false)
    expect { value.assert_supported! }
      .to raise_error(PgPipeline::UnsupportedServerError, /libpq >= 14/)
  end

  it "accepts libpq 14 with protocol v3" do
    value = caps(libpq: 140_000)

    expect(value.supported?).to be(true)
    expect(value.fast_sync?).to be(false)
  end

  it "rejects a non-v3 server protocol independently of server version" do
    value = caps(libpq: 170_000, protocol: 2, fast: true)

    expect(value.supported?).to be(false)
  end

  it "rejects a ruby-pg build without pipeline bindings" do
    value = caps(libpq: 170_000, pipeline_api: false, fast: true)

    expect(value.supported?).to be(false)
  end

  it "enables the flush-decoupled sync path only when both libpq and ruby-pg expose it" do
    expect(caps(libpq: 170_000, fast: true).fast_sync?).to be(true)
    expect(caps(libpq: 170_000, fast: false).fast_sync?).to be(false)
    expect(caps(libpq: 160_000, fast: true).fast_sync?).to be(false)
  end

  describe "#place_sync" do
    it "uses send_pipeline_sync on the libpq 17+ fast path" do
      connection = instance_double("PG::Connection")
      value = caps(libpq: 170_000, fast: true, raw_sync: true)

      expect(connection).to receive(:send_pipeline_sync)
      value.place_sync(connection)
    end

    it "uses raw sync_pipeline_sync when ruby-pg exposes it" do
      connection = instance_double("PG::Connection")
      value = caps(libpq: 160_000, raw_sync: true)

      expect(connection).to receive(:sync_pipeline_sync)
      value.place_sync(connection)
    end

    it "falls back to pipeline_sync for older ruby-pg 1.x releases" do
      connection = instance_double("PG::Connection")
      value = caps(libpq: 140_000)

      expect(connection).to receive(:pipeline_sync)
      value.place_sync(connection)
    end
  end
end
