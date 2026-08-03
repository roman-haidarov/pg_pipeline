# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::SessionGuard do
  let(:unmasked_sql) do
    [
      "SELECT 1",
      "SELECT $1::int AS n",
      "SELECT id, name FROM users WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 50",
      "INSERT INTO t (a, b) VALUES ($1, $2)",
      "UPDATE t SET a = $1 WHERE id = $2",
      "WITH c AS (SELECT 1) SELECT * FROM c",
      "VALUES (1), (2)",
      "SELECT\n  a,\n  b\nFROM t"
    ]
  end

  let(:masked_sql) do
    [
      "SELECT 'x'",
      'SELECT "col" FROM t',
      "SELECT 1 -- comment",
      "SELECT /* c */ 1",
      "SELECT $tag$ body $tag$"
    ]
  end

  describe "masking fast path" do
    it "is an identity transform when nothing needs masking" do
      unmasked_sql.each do |sql|
        expect(described_class.code_only(sql)).to eq(sql.b)
        expect(described_class::NEEDS_MASK.match?(sql)).to be(false)
      end
    end

    it "still detects SQL that requires masking" do
      masked_sql.each do |sql|
        expect(described_class::NEEDS_MASK.match?(sql)).to be(true)
      end
    end

    it "keeps masking semantics for literals, comments and dollar quotes" do
      expect(described_class.unsafe_reason("SELECT 'a; b'")).to be_nil
      expect(described_class.unsafe_reason("SELECT 1 -- ;\n")).to be_nil
      expect(described_class.unsafe_reason("SELECT $$; SELECT 2$$")).to be_nil
      expect(described_class.unsafe_reason("SELECT * FROM t WHERE name = 'set_config('")).to be_nil
    end
  end

  describe "pattern prefilter" do
    it "does not let a masked comment hide a forbidden call" do
      expect(described_class.unsafe_reason("SELECT nextval/* c */('s')", mode: :strict))
        .to eq("strict:nextval")
      expect(described_class.unsafe_reason("SELECT currval\n('s')")).to eq("currval")
    end

    it "still rejects the into-anchored patterns that contain no parenthesis" do
      expect(described_class.unsafe_reason("SELECT * INTO TEMP foo FROM t")).to eq("select-into-temp")
      expect(described_class.unsafe_reason("SELECT * INTO pg_temp.x FROM t")).to eq("select-into-pg-temp")
    end

    it "accepts safe SQL that contains neither anchor" do
      expect(described_class.unsafe_reason("SELECT a, b FROM t WHERE id = $1")).to be_nil
    end
  end

  describe "statement separator detection" do
    it "allows a single trailing semicolon and rejects anything after it" do
      expect(described_class.unsafe_reason("SELECT 1;")).to be_nil
      expect(described_class.unsafe_reason("SELECT 1;   ")).to be_nil
      expect(described_class.unsafe_reason("SELECT 1;\n\t ")).to be_nil
      expect(described_class.unsafe_reason("SELECT 1; SELECT 2")).to eq("multiple-statements")
      expect(described_class.unsafe_reason("SELECT 1 ; SELECT 2;")).to eq("multiple-statements")
    end
  end

  describe "cache eviction" do
    around do |example|
      previous = described_class.instance_variable_get(:@guard_cache)
      described_class.instance_variable_set(:@guard_cache, nil)
      example.run
    ensure
      described_class.instance_variable_set(:@guard_cache, previous)
    end

    it "evicts one entry at a time instead of clearing the whole cache" do
      limit = described_class::GUARD_CACHE_LIMIT
      cache = described_class.guard_cache.fetch(:default)

      limit.times { |i| described_class.unsafe_reason("SELECT #{i} FROM t") }
      expect(cache.size).to eq(limit)

      newest = "SELECT #{limit} FROM t"
      described_class.unsafe_reason(newest)

      expect(cache.size).to eq(limit)
      expect(cache).to have_key(newest)
      expect(cache).not_to have_key("SELECT 0 FROM t")
      expect(cache).to have_key("SELECT #{limit - 1} FROM t")
    end

    it "caches a safe verdict without returning the sentinel" do
      expect(described_class.unsafe_reason("SELECT 1")).to be_nil
      expect(described_class.unsafe_reason("SELECT 1")).to be_nil
    end
  end
end
