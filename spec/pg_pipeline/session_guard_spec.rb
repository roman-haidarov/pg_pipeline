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
