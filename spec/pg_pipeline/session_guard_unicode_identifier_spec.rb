# frozen_string_literal: true

require "spec_helper"

# `U&"..."` is a delimited identifier whose body may carry Unicode escapes, so
# U&"pg_advisory_\006Cock"(1) calls pg_advisory_lock. The escaped spelling is
# invisible to a scanner working on masked SQL: masking hides the body, and not
# masking it would let the escape sequence reach the forbidden-call scan as
# literal text that matches nothing. Resolving the escapes would mean
# reimplementing them here, including the UESCAPE clause's configurable escape
# character, so the guard refuses what it cannot read instead.
RSpec.describe PgPipeline::SessionGuard do
  describe "Unicode-escaped delimited identifiers" do
    {
      %q{SELECT U&"pg_advisory_\006Cock"(1)} => "unicode-escaped-identifier",
      %q{SELECT U&"set_\0063onfig"('a','b',false)} => "unicode-escaped-identifier",
      %q{SELECT u&"curr\0076al"('s')} => "unicode-escaped-identifier",
      %q{SELECT U&"col\0075mn" FROM t} => "unicode-escaped-identifier"
    }.each do |sql, reason|
      it "refuses #{sql.inspect}" do
        expect(described_class.unsafe_reason(sql)).to eq(reason)
      end
    end

    # A body that is one run of identifier bytes has nothing to resolve, so it
    # is still scanned as code and the ordinary verdicts still apply.
    it "still scans an unescaped U& identifier as code" do
      expect(described_class.unsafe_reason(%q{SELECT U&"pg_advisory_lock"(1)}))
        .to eq("session-advisory-lock")
    end

    it "allows an unescaped U& identifier that names nothing forbidden" do
      expect(described_class.unsafe_reason(%q{SELECT U&"plain_column" FROM t})).to be_nil
    end

    # `U&` only has this meaning directly before the quote and at a word
    # boundary; `foo&"bar"` is not a Unicode literal.
    it "does not treat a quoted identifier after an identifier byte as U&" do
      expect(described_class.unsafe_reason(%q{SELECT a&"b c" FROM t})).to be_nil
    end
  end

  describe "UESCAPE" do
    # PostgreSQL allows the escape character to be an identifier byte such as
    # `_`, which puts an escape sequence inside a body this scanner would
    # otherwise accept as a plain identifier run.
    it "refuses a UESCAPE clause attached to a U& literal" do
      expect(described_class.unsafe_reason(%q{SELECT U&"pg_advisory_l_006Fck" UESCAPE '_'}))
        .to eq("uescape")
    end

    it "refuses it regardless of case" do
      expect(described_class.unsafe_reason(%q{SELECT U&"abc" uescape '!'})).to eq("uescape")
    end

    # Without a U& literal there is nothing for UESCAPE to modify, so an
    # ordinary column that happens to be named `uescape` must still pass.
    it "allows a column named uescape" do
      expect(described_class.unsafe_reason("SELECT uescape FROM t")).to be_nil
      expect(described_class.unsafe_reason("SELECT * FROM t WHERE uescape = $1")).to be_nil
    end

    it "allows the word inside a literal or a comment" do
      expect(described_class.unsafe_reason("SELECT * FROM t WHERE note = 'uescape'")).to be_nil
      expect(described_class.unsafe_reason("SELECT 1 /* uescape */")).to be_nil
    end
  end

  it "raises through assert_multiplexable! as well" do
    expect { described_class.assert_multiplexable!(%q{SELECT U&"pg_advisory_\006Cock"(1)}) }
      .to raise_error(PgPipeline::UnsafeMultiplexError, /unicode-escaped-identifier/)
  end
end
