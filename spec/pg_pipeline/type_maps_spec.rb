# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::TypeMaps do
  def statement(name: "pgp_1", typed: true)
    instance_double(PgPipeline::PreparedStatement, physical_name: name, typed?: typed)
  end

  def request(name: "pgp_1", type_map: nil)
    req = PgPipeline::PreparedQueryRequest.allocate
    req.instance_variable_set(:@statement_name, name)
    req.type_map = type_map
    req
  end

  def tuples_result(nfields: 1)
    instance_double("PG::Result", result_status: PG::PGRES_TUPLES_OK, nfields: nfields).tap do |result|
      allow(result).to receive(:type_map=)
    end
  end

  it "does not mark an untyped statement" do
    maps = described_class.new
    maps.register(statement(typed: false))

    expect(maps.typed?("pgp_1")).to be_falsey
    expect(maps.cached("pgp_1")).to be_nil
  end

  it "binds PENDING until the first TUPLES_OK builds a column map" do
    maps = described_class.new
    maps.register(statement)
    req = request
    maps.bind(req, statement)

    expect(req.type_map).to equal(described_class::PENDING)
  end

  it "reuses a cached map on later binds without building again" do
    maps = described_class.new
    maps.register(statement)
    cached = Object.new
    maps.instance_variable_get(:@cache)["pgp_1"] = cached

    req = request
    maps.bind(req, statement)
    expect(req.type_map).to equal(cached)
  end

  it "looks the cache up by physical name without allocating a composite key" do
    maps = described_class.new
    name = "pgp_1"
    maps.instance_variable_get(:@cache)[name] = :map

    expect(maps.cached(name)).to eq(:map)

    GC.start
    GC.disable
    n = 20_000
    before = GC.stat(:total_allocated_objects)
    n.times { maps.cached(name) }
    delta = GC.stat(:total_allocated_objects) - before
    GC.enable

    expect(delta.fdiv(n)).to be < 0.01
  end

  it "applies a resolved map and skips work when type_map is nil" do
    maps = described_class.new
    result = tuples_result
    resolved = Object.new

    maps.apply!(request(type_map: nil), result, Object.new)
    expect(result).not_to have_received(:type_map=)

    maps.apply!(request(type_map: resolved), result, Object.new)
    expect(result).to have_received(:type_map=).with(resolved)
  end

  it "does not swallow a failed column-map build" do
    maps = described_class.new
    maps.register(statement)
    req = request(type_map: described_class::PENDING)
    result = tuples_result
    maps.instance_variable_set(:@bundle, :failed)

    expect { maps.apply!(req, result, nil) }
      .to raise_error(PgPipeline::Error, /unavailable/)
  end

  it "does not query the catalog from apply!" do
    maps = described_class.new
    maps.register(statement)
    maps.instance_variable_set(:@bundle, :ready)
    maps.instance_variable_set(:@text_map_for_results, instance_double("PG::BasicTypeMapForResults"))
    allow(maps.instance_variable_get(:@text_map_for_results))
      .to receive(:build_column_map).and_return(:built)

    expect(maps).not_to receive(:ensure_bundle!)
    maps.apply!(request(type_map: described_class::PENDING), tuples_result, Object.new)
  end

  it "unregisters cache and typed flag together" do
    maps = described_class.new
    maps.register(statement)
    maps.instance_variable_get(:@cache)["pgp_1"] = :map
    maps.unregister("pgp_1")

    expect(maps.typed?("pgp_1")).to be_falsey
    expect(maps.cached("pgp_1")).to be_nil
  end
end
