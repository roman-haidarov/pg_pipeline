# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::PreparedStatement do
  it "is immutable and delegates query execution to its owning client" do
    client = Object.new
    statement = described_class.new(
      client: client,
      name: :user_by_id,
      physical_name: "pgp_1",
      sql: "SELECT * FROM users WHERE id = $1",
      param_types: [23]
    )

    expect(statement).to be_frozen
    expect(statement.name).to eq("user_by_id")
    expect(statement.sql).to be_frozen
    expect(statement.param_types).to eq([23])
    expect(statement.typed?).to be(false)

    expect(PgPipeline::ClientOps).to receive(:query_prepared)
      .with(client, statement, [7])
      .and_return(:result)

    expect(statement.query([7])).to eq(:result)
  end

  it "records typed: true on the frozen handle" do
    statement = described_class.new(
      client: Object.new,
      name: "typed_n",
      physical_name: "pgp_2",
      sql: "SELECT $1::int",
      typed: true
    )

    expect(statement).to be_frozen
    expect(statement.typed?).to be(true)
    expect(statement.inspect).to include("typed")
  end

  it "rejects empty names and invalid parameter OIDs" do
    expect {
      described_class.new(client: Object.new, name: "", physical_name: "pgp_1", sql: "SELECT 1")
    }.to raise_error(ArgumentError, /must not be empty/)

    expect {
      described_class.new(
        client: Object.new,
        name: "x",
        physical_name: "pgp_1",
        sql: "SELECT $1",
        param_types: [Object.new]
      )
    }.to raise_error(ArgumentError, /integer OIDs/)
  end
end
