# frozen_string_literal: true

require "spec_helper"

RSpec.describe "sealed request payloads" do
  describe PgPipeline::Request do
    it "seals a plain query at build time" do
      request = described_class.build("SELECT $1::int", [42])

      expect(request).to be_sealed
      expect(request.payload_digest).to include(
        operation: :query,
        sql: "SELECT $1::int",
        statement_name: nil,
        count: 1,
        values: ["42"],
        formats: [0],
        types: [0]
      )
    end

    it "seals requests built through the keyword constructor" do
      expect(described_class.new(sql: "SELECT 1")).to be_sealed
    end

    it "carries binary parameters through untouched" do
      body = "\x00\x01\xFF".b
      request = described_class.build("SELECT $1::bytea", [{value: body, format: 1, type: 17}])

      digest = request.payload_digest
      expect(digest[:values]).to eq([body])
      expect(digest[:formats]).to eq([1])
      expect(digest[:types]).to eq([17])
    end

    it "represents SQL NULL as a null pointer rather than an empty string" do
      request = described_class.build("SELECT $1::text", [nil])

      expect(request.payload_digest[:values]).to eq([nil])
    end

    it "copies parameter bodies instead of borrowing the caller's string" do
      value = +"mutable"
      request = described_class.build("SELECT $1::text", [value])
      value << "-changed-after-build"

      expect(request.payload_digest[:values]).to eq(["mutable"])
    end

    it "rejects an embedded NUL in text parameters at build time" do
      expect { described_class.build("SELECT $1::text", ["a\0b"]) }
        .to raise_error(ArgumentError, /embedded NUL/)
    end

    it "rejects an embedded NUL in SQL at build time" do
      expect { described_class.build("SELECT 1\0", []) }
        .to raise_error(ArgumentError, /embedded NUL/)
    end

    it "refuses to seal twice" do
      request = described_class.build("SELECT 1", [])

      expect { request.__send__(:native_seal!, :query, "SELECT 1", [], nil, nil) }
        .to raise_error(PgPipeline::ProtocolError, /already sealed/)
    end
  end

  describe PgPipeline::PrepareRequest do
    let(:statement) do
      PgPipeline::PreparedStatement.new(client: nil, name: "by_id", physical_name: "pgp_by_id",
                                        sql: "SELECT $1::int", param_types: [23])
    end

    it "seals the statement name and declared OIDs" do
      digest = described_class.new(statement).payload_digest

      expect(digest[:operation]).to eq(:prepare)
      expect(digest[:statement_name]).to eq(statement.physical_name)
      expect(digest[:types]).to eq([23])
      expect(digest[:values]).to be_empty
    end
  end

  describe PgPipeline::PreparedQueryRequest do
    let(:statement) do
      PgPipeline::PreparedStatement.new(client: nil, name: "by_id", physical_name: "pgp_by_id",
                                        sql: "SELECT $1::int", param_types: [23])
    end

    it "seals the statement name and bound values" do
      digest = described_class.build(statement, [7]).payload_digest

      expect(digest[:operation]).to eq(:prepared_query)
      expect(digest[:statement_name]).to eq(statement.physical_name)
      expect(digest[:values]).to eq(["7"])
    end

    it "does not copy statement SQL into the prepared execute arena" do
      short = PgPipeline::PreparedStatement.new(
        client: nil, name: "short", physical_name: "pgp_same",
        sql: "SELECT $1::int", param_types: [23]
      )
      long = PgPipeline::PreparedStatement.new(
        client: nil, name: "long", physical_name: "pgp_same",
        sql: "SELECT $1::int /* #{"x" * 4096} */", param_types: [23]
      )

      short_digest = described_class.build(short, [{value: 7, type: 23}]).payload_digest
      long_digest = described_class.build(long, [{value: 7, type: 23}]).payload_digest

      expect(long_digest[:sql]).to eq(long.sql)
      expect(long_digest[:types]).to eq([23])
      expect(long_digest[:bytes]).to eq(short_digest[:bytes])
    end
  end

  describe "text encoding" do
    it "exports text parameters to the sealing encoding, as ruby-pg does" do
      request = PgPipeline::Request.build("SELECT $1::text", ["café".encode("ISO-8859-1")])

      expect(request.payload_digest[:values].first.bytes).to eq("café".bytes)
    end

    it "exports non-ASCII SQL to the sealing encoding" do
      request = PgPipeline::Request.build("SELECT 'привет'".encode("Windows-1251"), [])

      expect(request.payload_digest[:sql].bytes).to eq("SELECT 'привет'".bytes)
    end

    it "leaves binary parameters byte-for-byte alone" do
      body = "\xEF\xF0\xE8".b
      request = PgPipeline::Request.build("SELECT $1::bytea", [{value: body, format: 1}])

      expect(request.payload_digest[:values].first.bytes).to eq(body.bytes)
    end

    it "does not copy ASCII-only strings through a conversion" do
      expect(PgPipeline::Request.build("SELECT $1::text", ["plain"]).payload_digest[:values])
        .to eq(["plain"])
    end

    it "defaults to UTF-8 before any connection has negotiated one" do
      expect(PgPipeline::Native.seal_encoding).to be_a(Encoding)
    end
  end

  describe "#respawn" do
    it "produces an independent request with an identical payload" do
      origin = PgPipeline::Request.build("SELECT $1::text", ["value"])
      origin.queued!

      copy = origin.respawn

      expect(copy).not_to be(origin)
      expect(copy.state).to eq(:new)
      expect(copy).to be_sealed
      expect(copy.payload_digest).to eq(origin.payload_digest)
      expect(copy.sql).to eq(origin.sql)
    end

    it "keeps the same class for prepared queries" do
      statement = PgPipeline::PreparedStatement.new(client: nil, name: "s", physical_name: "pgp_s",
                                                   sql: "SELECT $1::int", param_types: nil)
      origin = PgPipeline::Request.prepared_query(statement, params: [1])

      copy = origin.respawn

      expect(copy).to be_a(PgPipeline::PreparedQueryRequest)
      expect(copy.statement_name).to eq(origin.statement_name)
      expect(copy.payload_digest).to eq(origin.payload_digest)
    end

    it "keeps the object shape of a freshly built request" do
      plain = PgPipeline::Request.build("SELECT 1", [])

      expect(plain.respawn.instance_variables).to eq(plain.instance_variables)
    end

    it "keeps the object shape of a freshly built prepared query" do
      statement = PgPipeline::PreparedStatement.new(client: nil, name: "s", physical_name: "pgp_s",
                                                    sql: "SELECT $1::int", param_types: nil)
      origin = PgPipeline::Request.prepared_query(statement, params: [1])

      expect(origin.respawn.instance_variables).to eq(origin.instance_variables)
    end

    it "keeps the object shape of a freshly built prepare request" do
      statement = PgPipeline::PreparedStatement.new(client: nil, name: "s", physical_name: "pgp_s",
                                                    sql: "SELECT $1::int", param_types: [23])
      origin = PgPipeline::Request.prepare(statement)

      expect(origin.respawn.instance_variables).to eq(origin.instance_variables)
    end

    it "settles independently of the request it was copied from" do
      origin = PgPipeline::Request.build("SELECT 1", [])
      copy = origin.respawn

      copy.reject!(PgPipeline::ShutdownError.new("gone"))

      expect(copy).to be_settled
      expect(origin).not_to be_settled
    end

    it "refuses to adopt a payload twice" do
      origin = PgPipeline::Request.build("SELECT 1", [])
      copy = origin.respawn

      expect { copy.__send__(:native_adopt_payload!, origin) }
        .to raise_error(PgPipeline::ProtocolError, /already sealed/)
    end
  end
end
