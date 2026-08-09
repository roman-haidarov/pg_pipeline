# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::SessionGuard, "PostgreSQL dollar-quote tags" do
  it "masks tags containing non-ASCII identifier bytes" do
    sql = "SELECT $тег$set_config('search_path', 'unsafe', false)$тег$"

    expect(described_class::NEEDS_MASK.match?(sql)).to be(true)
    expect(described_class.unsafe_reason(sql)).to be_nil
  end

  it "keeps bind parameters on the no-mask fast path" do
    sql = "SELECT $1::int, $2::text"

    expect(described_class::NEEDS_MASK.match?(sql)).to be(false)
    expect(described_class.unsafe_reason(sql)).to be_nil
  end

  it "still rejects forbidden code after a non-ASCII dollar quote" do
    sql = "SELECT $тег$safe text$тег$, set_config('search_path', 'unsafe', false)"

    expect(described_class.unsafe_reason(sql)).to eq("set_config")
  end
end
