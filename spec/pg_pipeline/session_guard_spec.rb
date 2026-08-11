# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::SessionGuard do
  describe ".unsafe_reason" do
    {
      "SELECT * FROM users WHERE id = $1" => nil,
      "INSERT INTO events(name) VALUES ($1)" => nil,
      "UPDATE accounts SET balance = balance + $1 WHERE id = $2" => nil,
      "DELETE FROM sessions WHERE expires_at < now()" => nil,
      "VALUES (1), (2)" => nil,
      "WITH x AS (SELECT 1) SELECT * FROM x" => nil,
      "SELECT '; SET ROLE x'" => nil,
      "SELECT $$; SET ROLE x$$" => nil,
      "SELECT 1 /* ; SET ROLE x */" => nil,
      "/* outer /* nested ; */ comment */ SELECT 1" => nil,
      "SELECT 1;" => nil,
      "BEGIN" => "leading:begin",
      "COMMIT" => "leading:commit",
      "SET search_path = foo" => "leading:set",
      "RESET ALL" => "leading:reset",
      "PREPARE p AS SELECT 1" => "leading:prepare",
      "LISTEN chan" => "leading:listen",
      "DISCARD ALL" => "leading:discard",
      "CREATE TEMP TABLE t(x int)" => "leading:create",
      "SELECT set_config('search_path', 'foo', false)" => "set_config",
      "SELECT setseed(0.5)" => "setseed",
      "SELECT currval('users_id_seq')" => "currval",
      "SELECT lastval()" => "lastval",
      "SELECT pg_advisory_lock(1)" => "session-advisory-lock",
      "SELECT pg_advisory_unlock_all()" => "session-advisory-unlock",
      "SELECT * INTO TEMP TABLE t FROM users" => "select-into-temp",
      "SELECT * INTO pg_temp.t FROM users" => "select-into-pg-temp",
      "SELECT * INTO TABLE pg_temp_3.t FROM users" => "select-into-pg-temp",
      "SELECT 1; SELECT 2" => "multiple-statements",
      "SELECT 1;  " => nil,
      "SELECT 1;\t\n" => nil,
      "" => "empty"
    }.each do |sql, expected|
      it "classifies #{sql.inspect}" do
        expect(described_class.unsafe_reason(sql)).to eq(expected)
      end
    end
  end

  it "raises with the pinned-session guidance for rejected SQL" do
    expect { described_class.assert_multiplexable!("SET timezone = 'UTC'") }
      .to raise_error(PgPipeline::UnsafeMultiplexError, /Client#session/)
  end

  describe "optimized classification path" do
    it "treats parameterized SQL without quotes or comments as session-neutral" do
      sql = "SELECT id, name FROM users WHERE tenant_id = $1 AND status = $2 ORDER BY created_at DESC LIMIT 50"

      expect(described_class.unsafe_reason(sql)).to be_nil
      expect(described_class.needs_mask?(sql)).to be(false)
    end

    it "still masks quoted and commented hazards before classification" do
      expect(described_class.unsafe_reason("SELECT 1 -- ;\n")).to be_nil
      expect(described_class.unsafe_reason("SELECT $$; SELECT 2$$")).to be_nil
      expect(described_class.unsafe_reason("SELECT 1 /* ; */")).to be_nil
    end

    it "caches safe results without allocating a new reason string each hit" do
      sql = "SELECT $1::int AS n"

      first = described_class.unsafe_reason_normalized(sql, mode: :default)
      second = described_class.unsafe_reason_normalized(sql, mode: :default)

      expect(first).to be_nil
      expect(second).to be_nil
    end
  end

  describe ".normalize_mode!" do
    it "accepts the documented guard modes" do
      expect(described_class.normalize_mode!(:default)).to eq(:default)
      expect(described_class.normalize_mode!("strict")).to eq(:strict)
    end

    it "rejects unknown guard modes instead of silently weakening the guard" do
      expect { described_class.normalize_mode!(:strcit) }
        .to raise_error(ArgumentError, /guard must be one of/)
    end
  end

end
