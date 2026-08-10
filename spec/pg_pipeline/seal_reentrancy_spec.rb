# frozen_string_literal: true

require "spec_helper"

# Sealing converts every parameter with #to_s (and, for the Hash form, reads a
# Hash, which can run #hash/#eql?). All of that is caller-supplied Ruby and can
# reach back into the array being sealed. Before 0.4.0 the walk used a length
# captured up front, so a #to_s that shrank the array read past its end -- a
# segfault, not an exception -- and a #to_s that replaced elements had its
# replacements shipped to PostgreSQL.
RSpec.describe PgPipeline::Native::RequestState, :native_only do
  def seal(params)
    described_class.new.tap { |s| s.seal!(:query, "SELECT 1", params, nil, nil) }
  end

  describe "#seal! against a hostile #to_s" do
    it "does not read past the end when the array shrinks mid-conversion" do
      params = []
      hostile = Object.new
      hostile.define_singleton_method(:to_s) do
        params.clear
        GC.start
        "converted"
      end
      params << hostile
      40.times { params << ("A" * 64) }

      digest = seal(params).payload_digest

      expect(digest[:count]).to eq(41)
      expect(digest[:values].first).to eq("converted")
      expect(digest[:values].last).to eq("A" * 64)
    end

    it "ships the parameters as they were at call time, not as rewritten" do
      params = ["first", "second"]
      hostile = Object.new
      hostile.define_singleton_method(:to_s) do
        params[1] = "hijacked"
        params[2] = "hijacked-too"
        "converted"
      end
      params.unshift(hostile)

      expect(seal(params).payload_digest[:values]).to eq(%w[converted first second])
    end

    it "is unaffected by mutation of a String the caller still holds" do
      mutable = +"original"
      state = seal([mutable])
      mutable << "-mutated"

      expect(state.payload_digest[:values]).to eq(["original"])
    end

    it "survives a #to_s that grows the array" do
      params = []
      hostile = Object.new
      hostile.define_singleton_method(:to_s) do
        10.times { params << "appended" }
        "converted"
      end
      params << hostile
      params << "tail"

      digest = seal(params).payload_digest

      expect(digest[:count]).to eq(2)
      expect(digest[:values]).to eq(%w[converted tail])
    end
  end

  describe "#seal! validation" do
    it "rejects an embedded NUL in a text parameter" do
      expect { seal(["a\0b"]) }.to raise_error(ArgumentError, /NUL/)
    end

    it "copies binary parameters byte for byte" do
      body = "\xFF\x00\xFE".b
      digest = seal([{value: body, format: 1}]).payload_digest

      expect(digest[:values]).to eq([body])
      expect(digest[:formats]).to eq([1])
    end

    it "leaves an all-ASCII payload insensitive to the connection encoding" do
      expect(seal(["plain"]).payload_digest[:encoding_sensitive]).to be(false)
    end

    it "marks a payload carrying non-ASCII text as encoding sensitive" do
      expect(seal(["héllo"]).payload_digest[:encoding_sensitive]).to be(true)
    end

    it "does not mark binary parameters as encoding sensitive" do
      digest = seal([{value: "\xC3\x28".b, format: 1}]).payload_digest

      expect(digest[:encoding_sensitive]).to be(false)
    end
  end

  describe "#adopt_payload!" do
    it "reproduces the source payload exactly" do
      origin = described_class.new
      origin.seal!(:query, "SELECT $1::text, $2::bytea",
                   ["héllo", {value: "\xFF\x00\xFE".b, format: 1}], nil, nil)

      copy = described_class.new
      copy.adopt_payload!(origin)

      expect(copy.payload_digest).to eq(origin.payload_digest)
    end

    it "leaves the copy independent of the source" do
      origin = described_class.new
      origin.seal!(:query, "SELECT $1", ["value"], nil, nil)
      copy = described_class.new.tap { |c| c.adopt_payload!(origin) }

      origin = nil # rubocop:disable Lint/UselessAssignment
      GC.start

      expect(copy.payload_digest[:values]).to eq(["value"])
    end

    it "refuses to adopt over an already sealed payload" do
      origin = described_class.new
      origin.seal!(:query, "SELECT 1", [], nil, nil)
      target = described_class.new
      target.seal!(:query, "SELECT 2", [], nil, nil)

      expect { target.adopt_payload!(origin) }
        .to raise_error(PgPipeline::ProtocolError, /already sealed/)
    end
  end
end
