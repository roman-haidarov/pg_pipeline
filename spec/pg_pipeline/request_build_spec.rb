# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::Request do
  describe ".build" do
    it "produces the same state as the keyword constructor" do
      built = described_class.build("SELECT $1", [1])
      kwargs = described_class.new(sql: "SELECT $1", params: [1])

      expect(built.sql).to eq(kwargs.sql)
      expect(built.payload_digest).to eq(kwargs.payload_digest)
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

    it "treats missing and empty params alike" do
      expect(described_class.build("SELECT 1", nil).payload_digest[:count]).to eq(0)
      expect(described_class.build("SELECT 1", []).payload_digest[:count]).to eq(0)
    end
  end

  # Sealing copies parameter bytes into the request's libpq arena at build time,
  # so the Ruby side neither dups, freezes nor retains what the caller passed.
  describe "parameter capture" do
    it "renders every supported value into the sealed payload" do
      params = [1, nil, true, :sym, 2.5, "frozen".freeze]

      expect(described_class.build("SELECT 1", params).payload_digest[:values])
        .to eq(["1", nil, "true", "sym", "2.5", "frozen"])
    end

    it "is unaffected by later mutation of the caller's array or strings" do
      string = +"mutable"
      params = [string]
      request = described_class.build("SELECT 1", params)

      string.replace("changed")
      params << "extra"

      expect(request.payload_digest[:values]).to eq(["mutable"])
      expect(request.payload_digest[:count]).to eq(1)
    end

    it "reads value, format and type out of a Hash parameter" do
      digest = described_class.build("SELECT 1", [{value: "x", format: 1, type: 25}]).payload_digest

      expect(digest[:values]).to eq(["x"])
      expect(digest[:formats]).to eq([1])
      expect(digest[:types]).to eq([25])
    end

    it "does not retain the caller's parameters on the Ruby object" do
      request = described_class.build("SELECT 1", [+"value"])

      expect(request.instance_variables).not_to include(:@params)
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
      expect(built.payload_digest).to eq(kwargs.payload_digest)
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
