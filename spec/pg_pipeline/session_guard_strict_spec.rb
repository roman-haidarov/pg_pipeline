# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::SessionGuard do
  describe "strict mode" do
    it "rejects sequence calls that default mode allows" do
      expect(described_class.unsafe_reason("SELECT nextval('s')")).to be_nil
      expect(described_class.unsafe_reason("SELECT nextval('s')", mode: :strict)).to eq("strict:nextval")
      expect(described_class.unsafe_reason("SELECT setval('s', 1)", mode: :strict)).to eq("strict:setval")
      expect(described_class.unsafe_reason("SELECT pg_export_snapshot()", mode: :strict))
        .to eq("strict:pg_export_snapshot")
    end

    it "still rejects default-mode hazards (reason is non-strict tag)" do
      expect(described_class.unsafe_reason("SELECT pg_advisory_lock(1)")).to eq("session-advisory-lock")
      expect(described_class.unsafe_reason("SELECT pg_advisory_lock(1)", mode: :strict))
        .to eq("session-advisory-lock")
      expect(described_class.unsafe_reason("SELECT set_config('x','y',true)")).to eq("set_config")
      expect(described_class.unsafe_reason("SELECT set_config('x','y',true)", mode: :strict))
        .to eq("set_config")
    end

    it "does not false-positive on safe functions" do
      expect(described_class.unsafe_reason("SELECT pg_typeof(id) FROM t", mode: :strict)).to be_nil
      expect(described_class.unsafe_reason("SELECT setup(x) FROM t", mode: :strict)).to be_nil
      expect(described_class.unsafe_reason("SELECT pg_advisory_xact_lock(1)", mode: :strict)).to be_nil
      expect(described_class.unsafe_reason("SELECT set_masklen(inet '10/8', 16)", mode: :strict)).to be_nil
    end

    it "still allows plain session-neutral SQL" do
      expect(described_class.unsafe_reason("SELECT 1", mode: :strict)).to be_nil
      expect(described_class.assert_multiplexable!("SELECT * FROM t WHERE id = $1", mode: :strict)).to be(true)
    end

    it "raises for a strict violation" do
      expect { described_class.assert_multiplexable!("SELECT nextval('s')", mode: :strict) }
        .to raise_error(PgPipeline::UnsafeMultiplexError, /strict:nextval/)
    end
  end
end
