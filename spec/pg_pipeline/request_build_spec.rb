# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::Request do
  describe ".build" do
    it "produces the same state as the keyword constructor" do
      built = described_class.build("SELECT $1", [1])
      kwargs = described_class.new(sql: "SELECT $1", params: [1])

      expect(built.sql).to eq(kwargs.sql)
      expect(built.params).to eq(kwargs.params)
      expect(built.state).to eq(:new)
      expect(built.settled?).to be(false)
      expect(built.cancelled?).to be(false)
      expect(built.query_boundary_seen?).to be(false)
      expect(built.result).to be_nil
      expect(built.error).to be_nil
    end

    it "assigns ivars in the same order so both constructors share one shape" do
      expect(described_class.build("SELECT 1", nil).instance_variables)
        .to eq(described_class.new(sql: "SELECT 1").instance_variables)
    end

    it "freezes SQL that the caller did not snapshot" do
      request = described_class.build(+"SELECT 1", nil)

      expect(request.sql).to be_frozen
      expect(request.sql).to eq("SELECT 1")
    end

    it "reuses an already frozen SQL string without copying it" do
      sql = "SELECT 1".freeze

      expect(described_class.build(sql, nil).sql).to equal(sql)
    end

    it "stringifies non-String SQL the same way as the keyword constructor" do
      expect(described_class.build(nil, nil).sql).to eq("")
      expect(described_class.build(1, nil).sql).to eq("1")
      expect(described_class.build(:sym, nil).sql).to eq("sym")
      expect(described_class.build(nil, nil).sql).to be_frozen
    end

    it "shares one frozen empty array for missing and empty params" do
      expect(described_class.build("SELECT 1", nil).params)
        .to equal(PgPipeline::RequestOps::EMPTY_PARAMS)
      expect(described_class.build("SELECT 1", []).params)
        .to equal(PgPipeline::RequestOps::EMPTY_PARAMS)
      expect(PgPipeline::RequestOps::EMPTY_PARAMS).to be_frozen
    end
  end

  describe "params snapshotting" do
    it "reuses a frozen array of immutable values without copying" do
      params = [1, nil, true, :sym, 2.5, "frozen".freeze].freeze

      expect(described_class.build("SELECT 1", params).params).to equal(params)
    end

    it "copies when the array is mutable" do
      params = [1, 2]
      snapshot = described_class.build("SELECT 1", params).params

      expect(snapshot).to eq([1, 2])
      expect(snapshot).not_to equal(params)
      expect(snapshot).to be_frozen
    end

    it "copies when a frozen array holds a mutable string" do
      params = [+"mutable"].freeze
      snapshot = described_class.build("SELECT 1", params).params

      expect(snapshot).not_to equal(params)
      expect(snapshot.first).to be_frozen
      expect(snapshot.first).to eq("mutable")
    end

    it "copies when a frozen array holds a non-immediate value" do
      params = [{ a: +"x" }].freeze
      snapshot = described_class.build("SELECT 1", params).params

      expect(snapshot).not_to equal(params)
      expect(snapshot.first[:a]).to be_frozen
    end

    it "still rejects params that are not an array" do
      expect { described_class.build("SELECT 1", "nope") }
        .to raise_error(ArgumentError, "params must be an Array")
    end
  end

  describe PgPipeline::PreparedQueryRequest do
    let(:statement) do
      Struct.new(:sql, :physical_name, :param_types).new("SELECT $1", "pgp_1", nil)
    end

    it "builds the same state as the keyword constructor" do
      built = described_class.build(statement, [1])
      kwargs = described_class.new(statement, params: [1])

      expect(built.sql).to eq(kwargs.sql)
      expect(built.params).to eq(kwargs.params)
      expect(built.statement_name).to eq(kwargs.statement_name)
      expect(built.operation).to eq(:prepared_query)
      expect(built.state).to eq(:new)
      expect(built.instance_variables).to eq(kwargs.instance_variables)
    end

    it "is what Request.prepared_query returns" do
      request = PgPipeline::Request.prepared_query(statement, params: [1])

      expect(request).to be_a(described_class)
      expect(request.statement_name).to eq("pgp_1")
      expect(request.sql).to equal(statement.sql)
    end
  end
end
