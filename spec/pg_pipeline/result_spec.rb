# frozen_string_literal: true

require "spec_helper"

# 0.4 stopped returning PG::Result from the multiplexed path. The ruby-pg
# spellings below are defined once on the shared base class, in terms of the
# primitives each subclass implements, so the native and pinned paths cannot
# answer them differently -- which is the failure mode that would make this
# compatibility layer worse than not having one.
RSpec.describe PgPipeline::Result do
  let(:result_class) do
    Class.new(described_class) do
      def initialize(rows)
        @rows = rows
      end

      def ntuples = @rows.size
      def nfields = @rows.empty? ? 0 : @rows.first.size
      def tuple_values(index) = @rows.fetch(index)
    end
  end

  let(:result) { result_class.new([%w[1 alice], %w[2 bob], %w[3 carol]]) }

  it "answers to ruby-pg's num_tuples and num_fields" do
    expect(result.num_tuples).to eq(3)
    expect(result.num_fields).to eq(2)
  end

  describe "#each_row" do
    it "yields each row as an array of values" do
      expect { |block| result.each_row(&block) }
        .to yield_successive_args(%w[1 alice], %w[2 bob], %w[3 carol])
    end

    it "returns self so it can be chained like ruby-pg's" do
      expect(result.each_row { |_row| nil }).to equal(result)
    end

    it "returns an Enumerator without a block" do
      expect(result.each_row).to be_a(Enumerator)
      expect(result.each_row.to_a).to eq([%w[1 alice], %w[2 bob], %w[3 carol]])
    end

    it "yields nothing for an empty result" do
      expect { |block| result_class.new([]).each_row(&block) }.not_to yield_control
    end
  end

  describe PgPipeline::RubyResult do
    # The pinned path wraps a real PG::Result, and must expose the same surface
    # as the native one.
    let(:raw) do
      instance_double("PG::Result", ntuples: 2, nfields: 1,
                                    tuple_values: nil)
    end

    it "shares the base-class spellings" do
      allow(raw).to receive(:tuple_values).with(0).and_return(["a"])
      allow(raw).to receive(:tuple_values).with(1).and_return(["b"])

      wrapped = described_class.new(raw)
      expect(wrapped.num_tuples).to eq(2)
      expect(wrapped.num_fields).to eq(1)
      expect(wrapped.each_row.to_a).to eq([["a"], ["b"]])
    end

    # Only the native result owns off-heap bytes the GC has to be told about;
    # the accessor exists on both so callers need not branch on the class.
    it "reports no externally held bytes" do
      expect(described_class.new(raw).external_bytes).to eq(0)
    end

    it "raises rather than returning stale rows after clear" do
      allow(raw).to receive(:clear)
      wrapped = described_class.new(raw)
      wrapped.clear

      expect(wrapped).to be_cleared
      expect { wrapped.ntuples }.to raise_error(PgPipeline::ProtocolError, /cleared/)
    end
  end

  describe ".wrap" do
    it "passes a PgPipeline::Result through untouched" do
      already = result_class.new([])
      expect(described_class.wrap(already)).to equal(already)
    end
  end
end
