# frozen_string_literal: true

require "spec_helper"

# A delimited identifier is code, not data. Masking "..." like a string literal
# meant SELECT "set_config"('a','b',false) -- a perfectly ordinary call -- was
# classified session-neutral and allowed onto a multiplexed connection.
RSpec.describe PgPipeline::SessionGuard do
  describe "forbidden calls written as delimited identifiers" do
    {
      %q{SELECT "set_config"('a','b',false)} => "set_config",
      %q{SELECT pg_catalog."set_config"('a','b',false)} => "set_config",
      %q{SELECT U&"set_config"('a','b',false)} => "set_config",
      %q{SELECT "currval"('users_id_seq')} => "currval",
      %q{SELECT "lastval"()} => "lastval",
      %q{SELECT "setseed"(0.5)} => "setseed",
      %q{SELECT "pg_advisory_lock"(1)} => "session-advisory-lock",
      %q{SELECT "pg_advisory_unlock_all"()} => "session-advisory-unlock",
      %q{SELECT pg_catalog . "pg_advisory_lock" (1)} => "session-advisory-lock"
    }.each do |sql, reason|
      it "classifies #{sql.inspect} as #{reason}" do
        expect(described_class.unsafe_reason(sql)).to eq(reason)
      end
    end

    it "rejects them through assert_multiplexable! too" do
      expect { described_class.assert_multiplexable!(%q{SELECT "set_config"('a','b',false)}) }
        .to raise_error(PgPipeline::UnsafeMultiplexError, /set_config/)
    end

    it "still applies in strict mode" do
      expect(described_class.unsafe_reason(%q{SELECT "nextval"('s')}, mode: :strict))
        .to eq("strict:nextval")
    end
  end

  describe "a delimited identifier cannot inject syntax into the masked code" do
    {
      %q{SELECT "a; b" FROM t} => nil,          # no statement separator leaks
      %q{SELECT "weird(col" FROM t} => nil,     # no fake call site leaks
      %q{SELECT "col" FROM t} => nil,
      %q{SELECT "into temp" FROM t} => nil,     # the words are inside the quotes
      %q{SELECT * FROM "my table"} => nil,
      %q{INSERT INTO t ("order", "group") VALUES ($1, $2)} => nil
    }.each do |sql, reason|
      it "classifies #{sql.inspect} as #{reason.inspect}" do
        expect(described_class.unsafe_reason(sql)).to eq(reason)
      end
    end
  end

  describe "string literals stay opaque" do
    {
      %q{SELECT 'set_config('} => nil,
      %q{SELECT 'a; SET ROLE x'} => nil,
      %q{SELECT $$ set_config( $$} => nil,
      %q{SELECT E'\\'; SET ROLE x'} => nil,
      %q{SELECT * FROM t WHERE name = 'pg_advisory_lock(1)'} => nil
    }.each do |sql, reason|
      it "classifies #{sql.inspect} as #{reason.inspect}" do
        expect(described_class.unsafe_reason(sql)).to eq(reason)
      end
    end
  end

  describe "reason tags" do
    it "does not emit strict: tags that default mode already rejects" do
      # These four used to exist in the C source but were unreachable: default
      # mode returns first, so the strict variants could never be produced.
      %w[set_config setseed pg_advisory_lock pg_advisory_unlock_all].each do |call|
        reason = described_class.unsafe_reason("SELECT #{call}(1)", mode: :strict)
        expect(reason).not_to start_with("strict:")
      end
    end

    it "emits exactly the strict tags default mode does not cover" do
      expect(described_class.unsafe_reason("SELECT nextval('s')", mode: :strict))
        .to eq("strict:nextval")
      expect(described_class.unsafe_reason("SELECT setval('s', 1)", mode: :strict))
        .to eq("strict:setval")
      expect(described_class.unsafe_reason("SELECT pg_export_snapshot()", mode: :strict))
        .to eq("strict:pg_export_snapshot")
    end
  end

  describe "verdict cache" do
    it "rotates generations instead of evicting one key per miss" do
      described_class.clear_cache!
      expect(described_class.cache_size).to eq(0)

      3000.times { |i| described_class.unsafe_reason("SELECT #{i} FROM t") }

      # Young generation was retired to old and a fresh one started; nothing
      # scanned an Array of every key to do it.
      expect(described_class.cache_size).to be <= 2 * described_class::GUARD_CACHE_LIMIT
      expect(described_class.unsafe_reason("SELECT 2999 FROM t")).to be_nil
    end

    it "returns a stable verdict across a rotation" do
      described_class.clear_cache!
      sql = "SELECT set_config('a','b',false)"

      expect(described_class.unsafe_reason(sql)).to eq("set_config")
      3000.times { |i| described_class.unsafe_reason("SELECT #{i}") }
      expect(described_class.unsafe_reason(sql)).to eq("set_config")
    end
  end

  describe "the public C entry points" do
    it "exposes the mode-normalizing wrappers that were compiled but unreachable" do
      expect(described_class.unsafe_reason_c("SELECT 1", :default)).to be_nil
      expect(described_class.assert_multiplexable_c!("SELECT 1")).to be(true)
      expect { described_class.assert_multiplexable_c!("BEGIN") }
        .to raise_error(PgPipeline::UnsafeMultiplexError)
    end
  end
end
