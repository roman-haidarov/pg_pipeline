# frozen_string_literal: true

require "async"
require "pg_pipeline"

RSpec.describe "pg_pipeline live", :integration do
  before(:all) do
    @url = ENV["PG_PIPELINE_URL"]
    skip "set PG_PIPELINE_URL to run live integration specs" unless @url
  end

  def with_client(**opts, &block)
    Sync do |task|
      client = PgPipeline::Client.new(@url, **opts).start
      begin
        block.call(client, task)
      ensure
        client.close
      end
    end
  end

  it "multiplexes N fibers over K connections and preserves per-request results" do
    with_client(pipeline_size: 2, pinned_size: 1) do |db, task|
      results = (1..200).map do |n|
        task.async { db.query("SELECT $1::int AS n", [n]).first["n"].to_i }
      end.map(&:wait)
      expect(results).to eq((1..200).to_a)
    end
  end

  it "isolates a mid-load SQL error to its own unit" do
    with_client(pipeline_size: 1, pinned_size: 1) do |db, task|
      good = task.async { db.query("SELECT 1 AS ok").first["ok"].to_i }
      bad = task.async do
        begin
          db.query("SELECT 1/0")
          :no_error
        rescue PgPipeline::QueryError
          :query_error
        end
      end
      after = task.async { db.query("SELECT 2 AS ok").first["ok"].to_i }
      expect(good.wait).to eq(1)
      expect(bad.wait).to eq(:query_error)
      expect(after.wait).to eq(2)
    end
  end


  it "keeps a client-side bind error local to its request" do
    with_client(pipeline_size: 1, pinned_size: 1) do |db, task|
      bad = task.async do
        begin
          db.query("SELECT $1", [{value: "1", format: Object.new}])
          :no_error
        rescue TypeError, ArgumentError
          :bind_error
        end
      end
      good = task.async { db.query("SELECT 9 AS ok").first["ok"].to_i }

      expect(bad.wait).to eq(:bind_error)
      expect(good.wait).to eq(9)
      expect(db.stats[:pipeline][:live]).to eq(1)
    end
  end

  it "still drains a cancelled waiter's Sync" do
    with_client(pipeline_size: 1, pinned_size: 1) do |db, task|
      t = task.async { db.query("SELECT pg_sleep(0.2), 1 AS n") }
      t.stop
      value = db.query("SELECT 3 AS n").first["n"].to_i
      expect(value).to eq(3)
    end
  end

  it "runs an explicit transaction with a savepoint on a pinned connection" do
    with_client(pipeline_size: 1, pinned_size: 1) do |db, _task|
      db.transaction do |tx|
        tx.exec("CREATE TEMP TABLE t(x int)")
        tx.query("INSERT INTO t(x) VALUES ($1)", [1])
        begin
          tx.savepoint do |_sp|
            tx.query("INSERT INTO t(x) VALUES ($1)", [2])
            raise "rollback inner"
          end
        rescue RuntimeError
          nil
        end
        rows = tx.query("SELECT count(*)::int AS c FROM t").first["c"].to_i
        expect(rows).to eq(1)
      end
    end
  end

  it "exposes stats" do
    with_client(pipeline_size: 2, pinned_size: 1) do |db, _task|
      stats = db.stats
      expect(stats[:pipeline][:size]).to eq(2)
      expect(stats[:pipeline][:live]).to be <= 2
      expect(stats[:pipeline][:drivers].size).to eq(2)
      expect(stats).to include(:reconnects, :health_failures, :pinned, :closing)
      expect(stats[:pinned]).to include(:size, :active, :free, :in_use)
    end
  end

  it "tears down pipeline watchers without leaks after close" do
    Sync do |task|
      client = PgPipeline::Client.new(@url, pipeline_size: 2, pinned_size: 0).start
      client.query("SELECT 1")
      client.close

      pool = client.__send__(:pool)
      drivers = pool.instance_variable_get(:@drivers)
      leaked = drivers.sum { |d| d.leaked_watchers }
      expect(leaked).to eq(0), "expected no leaked watchers after close, got #{leaked}"
    end
  end

  it "wakes producers blocked on max_pending when the driver closes" do
    with_client(pipeline_size: 1, pinned_size: 0, max_pending: 1, max_in_flight: 1) do |db, task|
      slow = task.async do
        db.query("SELECT pg_sleep(2)")
      rescue PgPipeline::Error
        nil
      end
      task.sleep(0.05)

      filler = task.async do
        db.query("SELECT 1")
      rescue PgPipeline::Error
        nil
      end
      task.sleep(0.05)

      blocked = task.async do
        begin
          db.query("SELECT 2")
          :ok
        rescue PgPipeline::ShutdownError, PgPipeline::NotDispatchedError, PgPipeline::ConnectionLostError
          :woken
        end
      end

      task.sleep(0.05)
      expect(blocked.finished?).to be(false)

      db.abort!
      expect(blocked.wait).to eq(:woken)
      slow.wait
      filler.wait
    end
  end

  it "limits concurrent pinned transactions to pinned_size" do
    with_client(pipeline_size: 1, pinned_size: 1) do |db, task|
      entered = 0
      max_entered = 0
      lock = Mutex.new

      workers = 4.times.map do
        task.async do
          db.transaction do |_tx|
            lock.synchronize do
              entered += 1
              max_entered = entered if entered > max_entered
            end
            task.sleep(0.05)
            lock.synchronize { entered -= 1 }
          end
        end
      end

      workers.each(&:wait)
      expect(max_entered).to eq(1)
    end
  end

  it "reports IndeterminateResultError for in-flight work when a driver is aborted" do
    with_client(pipeline_size: 1, pinned_size: 0, reconnect: false) do |db, task|
      driver = db.__send__(:pool).instance_variable_get(:@drivers).first
      result = nil
      error = nil

      waiter = task.async do
        begin
          result = db.query("SELECT pg_sleep(1), 1 AS n")
        rescue => e
          error = e
        end
      end

      task.sleep(0.05)
      driver.abort!(PgPipeline::ConnectionLostError.new("simulated drop"))
      waiter.wait

      expect(error).to be_a(PgPipeline::IndeterminateResultError)
      expect(result).to be_nil
    end
  end

  it "replaces a dead pipeline driver under reconnect: true" do
    with_client(pipeline_size: 1, pinned_size: 0, reconnect: true, reconnect_interval: 0.05) do |db, task|
      driver = db.__send__(:pool).instance_variable_get(:@drivers).first
      driver.abort!(PgPipeline::ConnectionLostError.new("simulated drop"))

      deadline = Time.now + 5
      until db.stats[:reconnects].positive? || Time.now > deadline
        task.sleep(0.05)
      end

      expect(db.stats[:reconnects]).to be >= 1
      expect(db.stats[:pipeline][:live]).to eq(1)
      expect(db.query("SELECT 7 AS n").first["n"].to_i).to eq(7)
    end
  end

  it "executes an explicit prepared statement across all pipeline drivers" do
    with_client(pipeline_size: 2, pinned_size: 0) do |db, task|
      statement = db.prepare("typed_value", "SELECT $1::int AS n", [23])

      values = (1..100).map do |n|
        task.async do
          result = statement.query([n])
          begin
            result.first["n"].to_i
          ensure
            result.clear
          end
        end
      end.map(&:wait)

      expect(values).to eq((1..100).to_a)
      expect(db.stats[:prepared_statements]).to eq(1)
    end
  end

  it "re-prepares registered statements on replacement drivers" do
    with_client(pipeline_size: 1, pinned_size: 0, reconnect: true, reconnect_interval: 0.05) do |db, task|
      statement = db.prepare("after_reconnect", "SELECT $1::int AS n", [23])
      first = statement.query([7])
      begin
        expect(first.first["n"].to_i).to eq(7)
      ensure
        first.clear
      end

      driver = db.__send__(:pool).instance_variable_get(:@drivers).first
      driver.abort!(PgPipeline::ConnectionLostError.new("simulated drop"))

      deadline = Time.now + 5
      until db.stats[:reconnects].positive? || Time.now > deadline
        task.sleep(0.05)
      end

      result = statement.query([9])
      begin
        expect(result.first["n"].to_i).to eq(9)
      ensure
        result.clear
      end
    end
  end

  it "decodes prepared results as Ruby types when typed: true" do
    with_client(pipeline_size: 2, pinned_size: 0) do |db, task|
      statement = db.prepare(
        "typed_int",
        "SELECT $1::int AS n, true AS b, TIMESTAMPTZ '2020-01-02 03:04:05+00' AS ts",
        typed: true
      )
      plain = db.prepare("plain_int", "SELECT $1::int AS n")

      typed_values = (1..50).map do |n|
        task.async do
          result = statement.query([n])
          begin
            row = result.first
            [row["n"], row["b"], row["ts"]]
          ensure
            result.clear
          end
        end
      end.map(&:wait)

      expect(typed_values.map { |n, _, _| n }).to eq((1..50).to_a)
      typed_values.each do |n, bool, ts|
        expect(n).to be_a(Integer)
        expect(bool).to equal(true)
        expect(ts).to be_a(Time)
      end

      plain_result = plain.query([7])
      begin
        expect(plain_result.first["n"]).to eq("7")
      ensure
        plain_result.clear
      end
    end
  end

  it "surfaces a type-map failure as a request-local QueryError" do
    with_client(pipeline_size: 1, pinned_size: 0) do |db, _task|
      statement = db.prepare("typed_boom", "SELECT $1::int AS n", typed: true)
      maps = db.__send__(:pool).instance_variable_get(:@type_maps)
      maps.instance_variable_set(:@bundle, :failed)
      maps.instance_variable_set(:@text_map_for_results, nil)

      expect { statement.query([1]) }
        .to raise_error(PgPipeline::QueryError, /unavailable|typed result mapping/)
    end
  end

  it "rejects non-session-neutral SQL on the multiplexed path" do
    with_client(pipeline_size: 1, pinned_size: 1) do |db, _task|
      expect { db.query("SET application_name = 'x'") }
        .to raise_error(PgPipeline::UnsafeMultiplexError)
    end
  end
end

RSpec.describe "pg_pipeline reliability", :integration do
  before(:all) do
    @url = ENV["PG_PIPELINE_URL"]
    skip "set PG_PIPELINE_URL to run live integration specs" unless @url
  end

  it "reconnects a pipeline driver after its backend is terminated" do
    app = "pgp_reconnect_#{Process.pid}"
    url = @url.include?("?") ? "#{@url}&application_name=#{app}" : "#{@url}?application_name=#{app}"

    Sync do |task|
      client = PgPipeline::Client.new(
        url, pipeline_size: 1, pinned_size: 1, reconnect: true, reconnect_interval: 0.2
      ).start

      begin
        expect(client.query("SELECT 1 AS n").first["n"].to_i).to eq(1)

        control = PG::Connection.new(@url)
        control.exec_params(
          "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " \
          "WHERE application_name = $1 AND pid <> pg_backend_pid()",
          [app]
        )
        control.close

        recovered = false
        20.times do
          client.query("SELECT 2 AS n")
          recovered = true
          break
        rescue PgPipeline::Error
          task.sleep(0.2)
        end

        expect(recovered).to be(true)
        expect(client.stats[:reconnects]).to be >= 1
      ensure
        client.close
      end
    end
  end

  it "cancels in-flight pinned work on abort!" do
    Sync do |task|
      client = PgPipeline::Client.new(@url, pipeline_size: 1, pinned_size: 1).start
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      worker = task.async do
        client.transaction { |tx| tx.query("SELECT pg_sleep(5)") }
      rescue PgPipeline::Error, PG::Error
        :cancelled
      end

      task.sleep(0.3)
      client.abort!
      worker.wait

      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      expect(elapsed).to be < 3.0
    end
  end
end
