# frozen_string_literal: true

require "spec_helper"

RSpec.describe "native backend (live)", :integration, :native_only do
  before(:all) do
    skip "set PG_PIPELINE_URL to run live integration specs" unless ENV["PG_PIPELINE_URL"]
  end

  def with_client(**opts, &block)
    PgPipeline::Test::ReferenceScheduler.run do
      client = PgPipeline::Client.new(
        SpecSupport.database_url,
        pipeline_size: 2, pinned_size: 0,
        health_check: false, reconnect: false, **opts
      )
      client.start
      begin
        block.call(client)
      ensure
        client.close
      end
    end
  end

  it "returns rows for a bound query" do
    rows = with_client { |client| client.query("SELECT $1::int + $2::int AS sum", [40, 2]).to_a }

    expect(rows).to eq([{"sum" => "42"}])
  end

  it "round-trips non-ASCII text in the connection encoding" do
    rows = with_client { |client| client.query("SELECT $1::text AS t", ["привет мир"]).to_a }

    expect(rows.first["t"]).to eq("привет мир")
    expect(rows.first["t"].encoding).to eq(Encoding::UTF_8)
  end

  it "round-trips a parameter that is not already in the connection encoding" do
    rows = with_client do |client|
      client.query("SELECT $1::text AS t", ["café".encode("ISO-8859-1")]).to_a
    end

    expect(rows.first["t"]).to eq("café")
  end

  it "round-trips binary parameters" do
    rows = with_client do |client|
      client.query("SELECT length($1::bytea) AS len",
                   [{value: "\x00\x01\x02".b, format: 1, type: 17}]).to_a
    end

    expect(rows).to eq([{"len" => "3"}])
  end

  it "keeps a server-side error request-local" do
    outcome = with_client do |client|
      failed = begin
        client.query("SELECT 1 / 0")
        :no_error
      rescue PgPipeline::QueryError
        :query_error
      end

      [failed, client.query("SELECT 'alive' AS state").to_a]
    end

    expect(outcome).to eq([:query_error, [{"state" => "alive"}]])
  end

  it "multiplexes many fibers onto a small number of connections" do
    answers = with_client(max_in_flight: 64, max_pending: 512) do |client|
      200.times.map do |index|
        PgPipeline::Runtime.spawn do
          client.query("SELECT $1::int AS n", [index]).to_a.first["n"].to_i
        end
      end.map(&:wait)
    end

    expect(answers).to eq((0...200).to_a)
  end

  it "dispatches without reading any Ruby accessor on the request" do
    rows = with_client do |client|
      request = PgPipeline::Request.build("SELECT $1::text AS t", ["sealed"])
      %i[sql params operation statement_name param_types].each do |accessor|
        request.define_singleton_method(accessor) do
          raise "dispatch read ##{accessor} from Ruby"
        end
      end

      driver = client.__send__(:pool).__send__(:pipeline_driver)
      driver.submit(request)
      request.wait.to_a
    end

    expect(rows).to eq([{"t" => "sealed"}])
  end

  it "amortises many completed units per socket wakeup" do
    stats = with_client(max_in_flight: 64, max_pending: 512) do |client|
      100.times.map { PgPipeline::Runtime.spawn { client.query("SELECT 1").clear } }.each(&:wait)
      client.stats
    end

    drivers = stats[:pipeline][:drivers]
    completed = drivers.sum { |driver| driver[:units_completed] }
    readable = drivers.sum { |driver| driver[:readable_events] }

    expect(completed).to be >= 100
    expect(readable).to be_positive
    expect(completed.to_f / readable).to be > 1.0
  end

  it "coalesces flushes under concurrent multiplex load" do
    stats = with_client(pipeline_size: 1, max_in_flight: 64, max_pending: 512) do |client|
      64.times.map do
        PgPipeline::Runtime.spawn do
          30.times { client.query("SELECT 1").clear }
        end
      end.each(&:wait)
      client.stats
    end

    driver = stats[:pipeline][:drivers].first
    units = driver[:units_completed].to_f
    flushes = driver[:flush_calls].to_f
    expect(units).to be >= 64 * 30
    expect(flushes / units).to be < 0.20
    expect(driver[:in_flight_peak]).to be > 1
  end

  it "returns each concurrent request its own bound parameter" do
    answers = with_client(pipeline_size: 1, max_in_flight: 4, max_pending: 256) do |client|
      80.times.map do |index|
        PgPipeline::Runtime.spawn do
          client.query("SELECT $1::int AS n", [index]).to_a.first["n"].to_i
        end
      end.map(&:wait)
    end

    expect(answers).to eq((0...80).to_a)
  end

  it "graceful_close drains in-flight SELECT 1 work without hanging" do
    outcome = with_client(pipeline_size: 1, max_in_flight: 16, max_pending: 64) do |client|
      4.times { client.query("SELECT 1").clear }

      tasks = 40.times.map do |index|
        PgPipeline::Runtime.spawn do
          client.query("SELECT $1::int AS n", [index]).to_a.first["n"].to_i
        end
      end

      client.close

      tasks.map do |task|
        task.wait
      rescue PgPipeline::ShutdownError, PgPipeline::NotDispatchedError,
             PgPipeline::IndeterminateResultError, PgPipeline::ConnectionLostError
        :rejected
      end
    end

    finished = outcome.count { |o| o.is_a?(Integer) }
    rejected = outcome.count { |o| o == :rejected }
    expect(finished + rejected).to eq(40)
    expect(finished).to be_positive
  end

  it "reports hot-path counters straight from the C driver" do
    driver_stats = with_client do |client|
      client.query("SELECT 1").clear
      client.stats[:pipeline][:drivers].find { |driver| driver[:dispatches].positive? }
    end

    expect(driver_stats[:dispatches]).to be_positive
    expect(driver_stats[:units_completed]).to be_positive
    expect(driver_stats[:results_read]).to be >= driver_stats[:units_completed]
    expect(driver_stats[:bytes_dispatched]).to be_positive
    expect(driver_stats[:in_flight_peak]).to be >= 1
  end

  it "runs prepared statements over the multiplexed path" do
    rows = with_client do |client|
      statement = client.prepare("sum_two", "SELECT $1::int + $2::int AS sum", [23, 23])
      statement.query([20, 22]).to_a
    end

    expect(rows).to eq([{"sum" => "42"}])
  end
  # A PGresult lives entirely outside the Ruby heap and libpq exposes no size
  # accessor, so without an explicit estimate the collector sees a ~40 byte
  # object and has no reason to run while the rows behind it accumulate.
  it "tells the GC how much memory a result is holding" do
    small, large = with_client do |client|
      one = client.query("SELECT 1 AS x")
      many = client.query("SELECT repeat('x', 400) AS blob FROM generate_series(1, 2000)")
      begin
        [one.external_bytes, many.external_bytes]
      ensure
        one.clear
        many.clear
      end
    end

    expect(small).to be_positive
    # 2000 rows of 400 bytes cannot plausibly be accounted as less than the
    # payload itself; the estimate samples rows, so this is a floor, not an
    # equality.
    expect(large).to be > 800_000
    expect(large).to be > small * 100
  end

  it "gives the accounted bytes back on clear" do
    external, after = with_client do |client|
      result = client.query("SELECT repeat('y', 200) AS blob FROM generate_series(1, 500)")
      before = result.external_bytes
      result.clear
      [before, result.external_bytes]
    end

    expect(external).to be_positive
    expect(after).to eq(0)
  end

  # The multiplexed path no longer returns a PG::Result, so the ruby-pg
  # spellings callers already have must work on the native one too.
  it "exposes the ruby-pg row and metadata spellings" do
    rows, tuples, fields = with_client do |client|
      result = client.query("SELECT i, i * 2 AS doubled FROM generate_series(1, 3) AS i")
      begin
        [result.each_row.to_a, result.num_tuples, result.num_fields]
      ensure
        result.clear
      end
    end

    expect(rows).to eq([%w[1 2], %w[2 4], %w[3 6]])
    expect(tuples).to eq(3)
    expect(fields).to eq(2)
  end

  it "reports the connection encoding alongside the process-wide seal encoding" do
    stats = with_client { |client| client.stats }

    expect(stats[:pipeline][:seal_encoding]).to eq(Encoding::UTF_8.name)
    expect(stats[:pipeline][:drivers].map { |driver| driver[:encoding] })
      .to all(eq(Encoding::UTF_8.name))
  end
end
