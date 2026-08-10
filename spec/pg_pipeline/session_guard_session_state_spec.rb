# frozen_string_literal: true

require "spec_helper"

# Both families below leave state on the backend session that outlives the unit
# that created it, which is exactly what a multiplexed connection cannot have:
# the next unit on that connection belongs to an unrelated fiber.
RSpec.describe PgPipeline::SessionGuard do
  describe "dblink" do
    # A named dblink connection is stored against the session, so one fiber's
    # dblink_connect is visible to every other fiber sharing the connection.
    {
      %q{SELECT dblink_connect('remote', 'dbname=other')} => "dblink-session",
      %q{SELECT dblink_connect_u('remote', 'dbname=other')} => "dblink-session",
      %q{SELECT dblink_disconnect('remote')} => "dblink-session",
      %q{SELECT public."dblink_connect"('remote', 'dbname=other')} => "dblink-session"
    }.each do |sql, reason|
      it "refuses #{sql.inspect}" do
        expect(described_class.unsafe_reason(sql)).to eq(reason)
      end
    end

    # dblink itself is only unsafe where it opens or closes a named connection;
    # the single-shot form carries its conninfo per call.
    it "allows a one-shot dblink query" do
      expect(described_class.unsafe_reason(%q{SELECT * FROM dblink('dbname=other', 'SELECT 1') AS t(x int)}))
        .to be_nil
    end
  end

  describe "large objects" do
    # Descriptors are session- and transaction-scoped: one opened by a
    # multiplexed unit is closed by the implicit commit at that unit's Sync, so
    # the handle a caller gets back is already dead. lo_import/lo_export
    # additionally read and write the server's filesystem.
    %w[
      lo_open lo_close lo_creat lo_create lo_import lo_export lo_unlink
      lo_read lo_write lo_lseek lo_lseek64 lo_tell lo_tell64
      lo_truncate lo_truncate64 loread lowrite
    ].each do |function|
      it "refuses #{function}" do
        expect(described_class.unsafe_reason("SELECT #{function}(1)")).to eq("large-object")
      end
    end

    # lo_get/lo_put are ordinary single-statement accessors with no descriptor
    # to leak, so they stay multiplexable.
    it "allows lo_get and lo_put" do
      expect(described_class.unsafe_reason("SELECT lo_get(1234)")).to be_nil
      expect(described_class.unsafe_reason("SELECT lo_put(1234, 0, $1)")).to be_nil
    end
  end

  describe "advisory locks" do
    # Transaction-scoped advisory locks release at the unit's Sync, which is a
    # boundary the multiplexed path already guarantees, so they are allowed.
    it "allows the transaction-scoped family" do
      expect(described_class.unsafe_reason("SELECT pg_advisory_xact_lock(1)")).to be_nil
      expect(described_class.unsafe_reason("SELECT pg_try_advisory_xact_lock(1)")).to be_nil
    end

    it "still refuses the session-scoped family" do
      expect(described_class.unsafe_reason("SELECT pg_advisory_lock(1)"))
        .to eq("session-advisory-lock")
    end
  end

  # Named here so the limit is a decision on record rather than a gap someone
  # rediscovers in production. See the README section on the guard.
  describe "what the scanner structurally cannot see" do
    it "allows a call whose session side effects are inside the function body" do
      expect(described_class.unsafe_reason("SELECT my_report(1)")).to be_nil
    end

    it "allows an INSERT that advances a sequence through a column default" do
      expect(described_class.unsafe_reason("INSERT INTO t(name) VALUES ($1)")).to be_nil
    end

    # The blast radius of the above is bounded by currval/lastval being refused,
    # so no fiber can read a sequence value another fiber set.
    it "refuses reading the sequence state back" do
      expect(described_class.unsafe_reason("SELECT currval('t_id_seq')")).to eq("currval")
      expect(described_class.unsafe_reason("SELECT lastval()")).to eq("lastval")
    end
  end
end
