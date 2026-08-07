# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgPipeline::Pool do
  describe "pinned connection recycling" do
    it "reconnects PostgreSQL 9.3 sessions because DISCARD ALL cannot clear sequence state" do
      pool = described_class.allocate
      pool.instance_variable_set(:@connection_args, :args)
      old_conn = instance_double("PG::Connection", server_version: 90_300, finished?: false)
      replacement = instance_double("PG::Connection")

      expect(PgPipeline::PoolOps).to receive(:safe_close).with(old_conn)
      expect(PgPipeline::PoolOps).to receive(:new_connection).with(:args).and_return(replacement)

      expect(pool.send(:recycle_pinned_connection, old_conn)).to equal(replacement)
    end

    it "sanitizes and reuses PostgreSQL 9.4+ sessions" do
      pool = described_class.allocate
      conn = instance_double(
        "PG::Connection",
        server_version: 90_400,
        finished?: false,
        status: PG::CONNECTION_OK,
        transaction_status: PG::PQTRANS_IDLE
      )

      expect(conn).to receive(:exec).with("DISCARD ALL")

      expect(pool.send(:recycle_pinned_connection, conn)).to equal(conn)
    end
  end

  describe "pipeline driver reconnect" do
    def bare_pool
      pool = described_class.allocate
      pool.instance_variable_set(:@connection_args, nil)
      pool.instance_variable_set(:@pipeline_size, 1)
      pool.instance_variable_set(:@pinned_size, 0)
      pool.instance_variable_set(:@max_pending, 8)
      pool.instance_variable_set(:@max_in_flight, 8)
      pool.instance_variable_set(:@reconnect, true)
      pool.instance_variable_set(:@reconnect_interval, 0.01)
      pool.instance_variable_set(:@reconnect_backoff_max, 1.0)
      pool.instance_variable_set(:@health_check, false)
      pool.instance_variable_set(:@health_interval, 10.0)
      pool.instance_variable_set(:@health_timeout, 5.0)
      pool.instance_variable_set(:@health_failures, 0)
      pool.instance_variable_set(:@supervisor_error, nil)
      pool.instance_variable_set(:@cancel_pinned_on_abort, true)
      pool.instance_variable_set(:@rr, 0)
      pool.instance_variable_set(:@reconnects, 0)
      pool.instance_variable_set(:@supervisor, nil)
      pool.instance_variable_set(:@supervisor_wake, PgPipeline::Runtime::Notification.new)
      pool.instance_variable_set(:@pinned_free, [])
      pool.instance_variable_set(:@pinned_in_use, {})
      pool.instance_variable_set(:@pinned_gate, nil)
      pool.instance_variable_set(:@pinned_error, nil)
      pool.instance_variable_set(:@pinned_active, 0)
      pool.instance_variable_set(:@pinned_owners, Hash.new(0))
      pool.instance_variable_set(:@pinned_idle, PgPipeline::Runtime::Notification.new)
      pool.instance_variable_set(:@started, true)
      pool.instance_variable_set(:@closing, false)
      pool.instance_variable_set(:@closed, false)
      pool.instance_variable_set(:@driver_backoff, [0.0])
      pool.instance_variable_set(:@driver_attempts, [0])
      pool.instance_variable_set(:@driver_last_health, [0.0])
      pool
    end

    def fake_driver(available:, dead:, load: 0, health: true)
      instance_double(
        PgPipeline::ConnectionDriver,
        available?: available,
        dead?: dead,
        load: load,
        stats: {available: available, load: load, pending: 0, in_flight: 0, submitting: 0, needs_flush: false},
        health_check: health
      ).tap do |driver|
        allow(driver).to receive(:abort!)
        allow(driver).to receive(:health_check).with(anything).and_return(health)
      end
    end

    it "replaces a dead pipeline driver and increments reconnects" do
      Sync do
        pool = bare_pool
        dead = fake_driver(available: false, dead: true)
        live = fake_driver(available: true, dead: false)
        pool.instance_variable_set(:@drivers, [dead])

        expect(pool).to receive(:start_pipeline_driver).and_return(live)

        pool.send(:reap_and_replace)

        expect(pool.instance_variable_get(:@drivers)).to eq([live])
        expect(pool.reconnects).to eq(1)
        expect(pool.stats[:pipeline][:live]).to eq(1)
        expect(pool.stats[:reconnects]).to eq(1)
      end
    end

    it "applies exponential backoff when replacement fails" do
      Sync do
        pool = bare_pool
        dead = fake_driver(available: false, dead: true)
        pool.instance_variable_set(:@drivers, [dead])
        pool.instance_variable_set(:@reconnect_interval, 0.5)

        expect(pool).to receive(:start_pipeline_driver).and_raise(RuntimeError, "pg down")

        before = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        pool.send(:reap_and_replace)

        expect(pool.reconnects).to eq(0)
        expect(pool.instance_variable_get(:@driver_attempts)).to eq([1])
        backoff_until = pool.instance_variable_get(:@driver_backoff).first
        expect(backoff_until).to be >= before + 0.5

        expect(pool).not_to receive(:start_pipeline_driver)
        pool.send(:reap_and_replace)
        expect(pool.instance_variable_get(:@driver_attempts)).to eq([1])
      end
    end

    it "skips drivers that are still draining (not dead yet)" do
      Sync do
        pool = bare_pool
        draining = fake_driver(available: false, dead: false)
        pool.instance_variable_set(:@drivers, [draining])

        expect(pool).not_to receive(:start_pipeline_driver)
        pool.send(:reap_and_replace)
        expect(pool.reconnects).to eq(0)
      end
    end

    it "reports no live drivers via select when all are dead" do
      dead = fake_driver(available: false, dead: true)
      driver, rr = PgPipeline::PoolOps.select_driver([dead], 0)
      expect(driver).to be_nil
      expect(rr).to eq(0)
    end

    it "does not reconnect dead drivers when reconnect is false but health checks are enabled" do
      Sync do
        pool = bare_pool
        pool.instance_variable_set(:@reconnect, false)
        pool.instance_variable_set(:@health_check, true)
        pool.instance_variable_set(:@closing, false)
        wake = pool.instance_variable_get(:@supervisor_wake)

        expect(pool).not_to receive(:reap_and_replace)
        expect(pool).to receive(:health_probe).once
        allow(wake).to receive(:wait) do
          pool.instance_variable_set(:@closing, true)
        end

        pool.send(:supervise)
      end
    end

    it "records supervisor errors and keeps looping" do
      Sync do
        pool = bare_pool
        pool.instance_variable_set(:@drivers, [])
        pool.instance_variable_set(:@reconnect, true)
        pool.instance_variable_set(:@health_check, false)
        pool.instance_variable_set(:@closing, false)
        calls = 0

        wake = pool.instance_variable_get(:@supervisor_wake)
        allow(pool).to receive(:reap_and_replace) do
          calls += 1
          raise "boom" if calls == 1
        end
        allow(wake).to receive(:wait) do
          pool.instance_variable_set(:@closing, true) if calls >= 2
        end

        pool.send(:supervise)

        expect(calls).to be >= 2
        expect(pool.stats[:supervisor_error]).to be_nil
      end
    end

    it "records errors from the supervisor sleep path and keeps looping" do
      Sync do
        pool = bare_pool
        pool.instance_variable_set(:@drivers, [])
        pool.instance_variable_set(:@reconnect, false)
        pool.instance_variable_set(:@health_check, false)
        pool.instance_variable_set(:@closing, false)
        wake = pool.instance_variable_get(:@supervisor_wake)
        waits = 0

        allow(wake).to receive(:wait) do
          waits += 1
          raise ArgumentError, "wrong number of arguments" if waits == 1

          pool.instance_variable_set(:@closing, true)
        end

        pool.send(:supervise)

        expect(waits).to be >= 2
        expect(pool.stats[:supervisor_error]).to be_nil
      end
    end

    it "reports supervisor_alive from stats" do
      Sync do
        pool = bare_pool
        pool.instance_variable_set(:@drivers, [])
        task = instance_double(PgPipeline::Runtime::Task, finished?: false)
        pool.instance_variable_set(:@supervisor, task)
        expect(pool.stats[:supervisor_alive]).to be(true)

        allow(task).to receive(:finished?).and_return(true)
        expect(pool.stats[:supervisor_alive]).to be(false)

        pool.instance_variable_set(:@supervisor, nil)
        expect(pool.stats[:supervisor_alive]).to be(false)
      end
    end

    it "health_probe aborts an idle driver that fails its probe" do
      Sync do
        pool = bare_pool
        pool.instance_variable_set(:@health_check, true)
        pool.instance_variable_set(:@health_interval, 0.0)
        pool.instance_variable_set(:@health_timeout, 0.1)
        unhealthy = fake_driver(available: true, dead: false, load: 0, health: false)
        pool.instance_variable_set(:@drivers, [unhealthy])
        pool.instance_variable_set(:@driver_last_health, [0.0])

        expect(unhealthy).to receive(:abort!)
        pool.send(:health_probe)
        expect(pool.stats[:health_failures]).to eq(1)
      end
    end

    it "health_probe skips busy drivers" do
      Sync do
        pool = bare_pool
        pool.instance_variable_set(:@health_check, true)
        pool.instance_variable_set(:@health_interval, 0.0)
        busy = fake_driver(available: true, dead: false, load: 3, health: false)
        pool.instance_variable_set(:@drivers, [busy])
        pool.instance_variable_set(:@driver_last_health, [0.0])

        expect(busy).not_to receive(:health_check)
        expect(busy).not_to receive(:abort!)
        pool.send(:health_probe)
        expect(pool.stats[:health_failures]).to eq(0)
      end
    end

    it "abort! cancels checked-out pinned connections" do
      Sync do
        pool = bare_pool
        pool.instance_variable_set(:@started, true)
        pool.instance_variable_set(:@closing, false)
        pool.instance_variable_set(:@drivers, [])
        cancelled = false
        conn = Object.new
        conn.define_singleton_method(:cancel) { cancelled = true }
        pool.instance_variable_set(:@pinned_in_use, {conn => Fiber.current})
        pool.instance_variable_set(:@cancel_pinned_on_abort, true)

        pool.abort!
        expect(cancelled).to be(true)
        expect(pool.instance_variable_get(:@closing)).to be(true)
      end
    end
  end

  describe "replacement-driver ownership (P0.1)" do
    it "reconnects dead drivers on the Fiber.scheduler without a parent task tree" do
      Sync do
        pool = described_class.allocate
        pool.instance_variable_set(:@connection_args, nil)
        pool.instance_variable_set(:@reconnects, 0)
        pool.instance_variable_set(:@driver_backoff, [0.0])
        pool.instance_variable_set(:@driver_attempts, [0])
        pool.instance_variable_set(:@driver_last_health, [0.0])
        dead = instance_double(PgPipeline::ConnectionDriver, available?: false, dead?: true)
        live = instance_double(
          PgPipeline::ConnectionDriver,
          available?: true,
          dead?: false,
          stats: {available: true, load: 0, pending: 0, in_flight: 0, submitting: 0, needs_flush: false}
        )
        pool.instance_variable_set(:@drivers, [dead])

        expect(pool).to receive(:start_pipeline_driver).and_return(live)
        pool.send(:reap_and_replace)

        expect(pool.instance_variable_get(:@drivers)).to eq([live])
        expect(pool.reconnects).to eq(1)
      end
    end
  end

  describe "abort! vs pinned recycle race (P0.2)" do
    def pinned_state_pool
      pool = described_class.allocate
      pool.instance_variable_set(:@closing, false)
      pool.instance_variable_set(:@pinned_free, [])
      pool.instance_variable_set(:@pinned_active, 1)
      pool.instance_variable_set(:@pinned_owners, Hash.new(0))
      pool.instance_variable_set(:@pinned_idle, PgPipeline::Runtime::Notification.new)
      pool
    end

    it "closes a recycled connection when the pool started closing during recycle" do
      Sync do
        pool = pinned_state_pool
        owner = Fiber.current
        pool.instance_variable_get(:@pinned_owners)[owner] = 1

        conn = instance_double("PG::Connection")
        recycled = instance_double("PG::Connection")

        allow(pool).to receive(:recycle_pinned_connection) do
          pool.instance_variable_set(:@closing, true)
          recycled
        end

        expect(PgPipeline::PoolOps).to receive(:safe_close).with(recycled)

        pool.send(:release_pinned, owner, conn)

        expect(pool.instance_variable_get(:@pinned_free)).not_to include(recycled)
      end
    end
  end

  describe "cancellation during pinned recycle (P0.3)" do
    it "closes the connection and re-raises when cancelled mid-cleanup" do
      pool = described_class.allocate
      pool.instance_variable_set(:@connection_args, nil)

      conn = instance_double("PG::Connection", server_version: 90_400)
      allow(PgPipeline::PoolOps)
        .to receive(:sanitize_pinned_connection)
        .and_raise(described_class::CANCEL_SIGNAL.new("Task was cancelled"))

      expect(PgPipeline::PoolOps).to receive(:safe_close).with(conn)

      expect { pool.send(:recycle_pinned_connection, conn) }
        .to raise_error(described_class::CANCEL_SIGNAL)
    end

    it "uses a cancel signal that is NOT a StandardError (would bypass recovery)" do
      expect(described_class::CANCEL_SIGNAL.ancestors).to include(Exception)
      expect(described_class::CANCEL_SIGNAL.ancestors).not_to include(StandardError)
    end
  end

  describe "recursive pinned checkout (P0.4)" do
    it "rejects a nested checkout on the same fiber before touching the gate" do
      Sync do
        pool = described_class.allocate
        pool.instance_variable_set(:@started, true)
        pool.instance_variable_set(:@closing, false)
        pool.instance_variable_set(:@closed, false)
        pool.instance_variable_set(:@pinned_error, nil)
        pool.instance_variable_set(:@pinned_size, 1)

        owner = Fiber.current
        pool.instance_variable_set(:@pinned_owners, Hash.new(0).tap { |h| h[owner] = 1 })

        gate = instance_double(PgPipeline::Runtime::Semaphore)
        pool.instance_variable_set(:@pinned_gate, gate)
        expect(gate).not_to receive(:acquire)

        expect { pool.send(:with_pinned) { flunk("block must not run") } }
          .to raise_error(PgPipeline::RecursiveCheckoutError)
      end
    end

    it "allows a fresh (non-nested) checkout on a fiber that holds no slot" do
      Sync do
        pool = described_class.allocate
        pool.instance_variable_set(:@started, true)
        pool.instance_variable_set(:@closing, false)
        pool.instance_variable_set(:@closed, false)
        pool.instance_variable_set(:@pinned_error, nil)
        pool.instance_variable_set(:@pinned_size, 1)
        pool.instance_variable_set(:@pinned_owners, Hash.new(0))

        gate = instance_double(PgPipeline::Runtime::Semaphore)
        allow(gate).to receive(:acquire)
        pool.instance_variable_set(:@pinned_gate, gate)

        pool.send(:with_pinned) { :unused }

        expect(gate).to have_received(:acquire)
      end
    end
  end

  describe "restart of a closing or closed pool (P0.5)" do
    it "refuses to restart after close" do
      pool = described_class.allocate
      pool.instance_variable_set(:@started, false)
      pool.instance_variable_set(:@closing, true)
      pool.instance_variable_set(:@closed, true)

      expect { pool.start }
        .to raise_error(PgPipeline::ShutdownError, /cannot be restarted/)
    end

    it "refuses to restart while a previous close is still incomplete" do
      pool = described_class.allocate
      pool.instance_variable_set(:@started, false)
      pool.instance_variable_set(:@closing, true)
      pool.instance_variable_set(:@closed, false)

      expect { pool.start }
        .to raise_error(PgPipeline::ShutdownError, /cannot be restarted/)
    end
  end

  describe "availability error classification" do
    it "reports shutdown before the generic not-started state once closing begins" do
      pool = described_class.allocate
      pool.instance_variable_set(:@started, false)
      pool.instance_variable_set(:@closing, true)

      expect { pool.__send__(:ensure_available!) }
        .to raise_error(PgPipeline::ShutdownError, /closing/)
    end

    it "keeps the generic lifecycle error before the first start" do
      pool = described_class.allocate
      pool.instance_variable_set(:@started, false)
      pool.instance_variable_set(:@closing, false)
      pool.instance_variable_set(:@closed, false)

      expect { pool.__send__(:ensure_available!) }
        .to raise_error(PgPipeline::Error, /not started/)
    end

    it "reports shutdown for a terminally closed pool even if closing is false" do
      pool = described_class.allocate
      pool.instance_variable_set(:@started, false)
      pool.instance_variable_set(:@closing, false)
      pool.instance_variable_set(:@closed, true)

      expect { pool.__send__(:ensure_available!) }
        .to raise_error(PgPipeline::ShutdownError, /closed/)
    end
  end

  describe "partial start cleanup" do
    it "aborts created drivers when supervisor creation fails" do
      Sync do
        pool = described_class.new(nil, pipeline_size: 1, pinned_size: 0)
        driver = instance_double(PgPipeline::ConnectionDriver)

        allow(pool).to receive(:start_pipeline_driver).and_return(driver)
        allow(PgPipeline::Runtime).to receive(:spawn).and_raise(RuntimeError, "spawn failed")
        expect(driver).to receive(:abort!)

        expect { pool.start }.to raise_error(RuntimeError, "spawn failed")
        expect(pool.instance_variable_get(:@drivers)).to be_empty
        expect(pool.instance_variable_get(:@started)).to be(false)
        expect(pool.instance_variable_get(:@closing)).to be(false)
      end
    end
  end

  describe "timing option validation (P1)" do
    it "rejects negative timing values" do
      expect { described_class.new(nil, reconnect_interval: -1) }
        .to raise_error(ArgumentError, /reconnect_interval/)
    end

    it "rejects NaN timing values" do
      expect { described_class.new(nil, health_timeout: Float::NAN) }
        .to raise_error(ArgumentError, /health_timeout/)
    end

    it "rejects infinite timing values" do
      expect { described_class.new(nil, reconnect_backoff_max: Float::INFINITY) }
        .to raise_error(ArgumentError, /reconnect_backoff_max/)
    end

    it "allows health_interval: 0 (probe every cycle)" do
      expect { described_class.new(nil, health_interval: 0) }.not_to raise_error
    end
  end

  describe "supervisor cadence (P1)" do
    def cadence_pool(**overrides)
      pool = described_class.allocate
      defaults = {reconnect: true, reconnect_interval: 30.0, health_check: true, health_interval: 5.0}
      defaults.merge(overrides).each { |k, v| pool.instance_variable_set(:"@#{k}", v) }
      pool
    end

    it "wakes on the shorter health interval when reconnect interval is large" do
      pool = cadence_pool(reconnect_interval: 30.0, health_interval: 5.0)
      expect(pool.send(:supervisor_sleep_interval)).to eq(5.0)
    end

    it "keeps reconnect cadence when health_interval is 0 (probe every cycle)" do
      pool = cadence_pool(reconnect_interval: 30.0, health_interval: 0.0)
      expect(pool.send(:supervisor_sleep_interval)).to eq(30.0)
    end

    it "uses reconnect interval when health checks are disabled" do
      pool = cadence_pool(reconnect_interval: 12.0, health_check: false, health_interval: 1.0)
      expect(pool.send(:supervisor_sleep_interval)).to eq(12.0)
    end
  end

  describe "reconnect backoff (P1)" do
    def backoff_pool(interval:, max:)
      pool = described_class.allocate
      pool.instance_variable_set(:@reconnect_interval, interval)
      pool.instance_variable_set(:@reconnect_backoff_max, max)
      pool
    end

    it "caps very large attempt counts without constructing a giant integer" do
      pool = backoff_pool(interval: 0.5, max: 30.0)
      expect(pool.send(:next_backoff, 1_000)).to eq(30.0)
    end

    it "still reaches the configured ceiling for very small base intervals" do
      pool = backoff_pool(interval: 1e-9, max: 30.0)
      expect(pool.send(:next_backoff, 40)).to eq(30.0)
    end

    it "grows exponentially below the ceiling" do
      pool = backoff_pool(interval: 0.5, max: 1000.0)
      expect(pool.send(:next_backoff, 1)).to eq(0.5) # 0.5 * 2**0
      expect(pool.send(:next_backoff, 3)).to eq(2.0) # 0.5 * 2**2
    end
  end

  describe "driver shutdown cleanup" do
    it "attempts every driver even when one close raises" do
      pool = described_class.allocate
      first = instance_double(PgPipeline::ConnectionDriver)
      second = instance_double(PgPipeline::ConnectionDriver)
      pool.instance_variable_set(:@drivers, [first, second])

      allow(first).to receive(:abort!).and_raise(RuntimeError, "first failed")
      expect(second).to receive(:abort!)

      error = pool.send(:close_all_drivers, :abort!)
      expect(error).to be_a(RuntimeError)
      expect(error.message).to eq("first failed")
    end

    it "continues closing drivers and preserves cancellation over ordinary errors" do
      pool = described_class.allocate
      first = instance_double(PgPipeline::ConnectionDriver)
      second = instance_double(PgPipeline::ConnectionDriver)
      third = instance_double(PgPipeline::ConnectionDriver)
      cancellation = described_class::CANCEL_SIGNAL.new("Task was cancelled")
      pool.instance_variable_set(:@drivers, [first, second, third])

      allow(first).to receive(:abort!).and_raise(RuntimeError, "first failed")
      allow(second).to receive(:abort!).and_raise(cancellation)
      expect(third).to receive(:abort!)

      expect(pool.send(:close_all_drivers, :abort!)).to equal(cancellation)
    end
  end

  describe "least-loaded driver selection" do
    it "reads each available driver load only once" do
      first = instance_double(PgPipeline::ConnectionDriver, available?: true)
      second = instance_double(PgPipeline::ConnectionDriver, available?: true)
      expect(first).to receive(:load).once.and_return(2)
      expect(second).to receive(:load).once.and_return(1)

      driver, rr = PgPipeline::PoolOps.select_driver([first, second], 0)

      expect(driver).to equal(second)
      expect(rr).to eq(0)
    end

    it "continues after the selected driver when unequal loads become tied" do
      first_load = 2
      second_load = 1
      first = instance_double(PgPipeline::ConnectionDriver, available?: true)
      second = instance_double(PgPipeline::ConnectionDriver, available?: true)
      allow(first).to receive(:load) { first_load }
      allow(second).to receive(:load) { second_load }

      selected, rr = PgPipeline::PoolOps.select_driver([first, second], 0)
      second_load = 2
      next_selected, = PgPipeline::PoolOps.select_driver([first, second], rr)

      expect(selected).to equal(second)
      expect(next_selected).to equal(first)
    end

    it "rotates equal-load drivers from the round-robin cursor" do
      drivers = 3.times.map do
        instance_double(PgPipeline::ConnectionDriver, available?: true, load: 0)
      end

      first, rr = PgPipeline::PoolOps.select_driver(drivers, 0)
      second, rr = PgPipeline::PoolOps.select_driver(drivers, rr)
      third, = PgPipeline::PoolOps.select_driver(drivers, rr)

      expect([first, second, third]).to eq(drivers)
    end

    it "keeps select_driver_into equivalent to select_driver without allocating a result tuple" do
      fake = Struct.new(:available, :loadv) do
        def available? = available
        def load = loadv
      end

      srand(7)
      200.times do
        size = rand(0..5)
        drivers = Array.new(size) { fake.new([true, true, false].sample, rand(0..9)) }
        rr = size.zero? ? 0 : rand(0...size)

        expected_driver, expected_rr = PgPipeline::PoolOps.select_driver(drivers, rr)
        slot = [0]
        actual_driver = PgPipeline::PoolOps.select_driver_into(drivers, rr, slot)

        expect(actual_driver).to equal(expected_driver)
        expect(slot[0]).to eq(expected_rr)
      end
    end
  end

  describe "multiplexed prepared statements" do
    def prepared_pool(drivers)
      described_class.allocate.tap do |pool|
        pool.instance_variable_set(:@started, true)
        pool.instance_variable_set(:@closing, false)
        pool.instance_variable_set(:@closed, false)
        pool.instance_variable_set(:@drivers, drivers)
        pool.instance_variable_set(:@prepared_statements, {})
        pool.instance_variable_set(:@prepared_generation, 0)
        pool.instance_variable_set(:@statement_sequence, 0)
      end
    end

    def accepting_driver
      instance_double(PgPipeline::ConnectionDriver, available?: true).tap do |driver|
        allow(driver).to receive(:submit) do |request|
          result = instance_double("PG::Result")
          allow(result).to receive(:clear)
          request.queued!
          request.dispatched!
          request.accept_result(result)
          request.query_boundary!
          request.finish!
          request
        end
      end
    end

    it "prepares a statement on every current live driver before returning the handle" do
      drivers = [accepting_driver, accepting_driver]
      pool = prepared_pool(drivers)
      client = Object.new

      statement = pool.send(:prepare_statement, client, "by_id", "SELECT $1::int", [23])

      expect(statement).to be_a(PgPipeline::PreparedStatement)
      expect(statement.physical_name).to eq("pgp_1")
      drivers.each { |driver| expect(driver).to have_received(:submit).once }
      expect(pool.instance_variable_get(:@prepared_statements)).to include("by_id" => statement)
    end

    it "removes a failed registration from the reconnect catalog" do
      driver = instance_double(PgPipeline::ConnectionDriver, available?: true)
      allow(driver).to receive(:submit).and_raise(PgPipeline::NotDispatchedError, "driver died")
      pool = prepared_pool([driver])

      expect {
        pool.send(:prepare_statement, Object.new, "bad", "SELECT 1", nil)
      }.to raise_error(PgPipeline::NotDispatchedError)

      expect(pool.instance_variable_get(:@prepared_statements)).to be_empty
      expect(pool.instance_variable_get(:@prepared_generation)).to eq(2)
    end

    it "rejects duplicate logical names before dispatching another prepare" do
      driver = accepting_driver
      pool = prepared_pool([driver])

      pool.send(:prepare_statement, Object.new, "same", "SELECT 1", nil)

      expect {
        pool.send(:prepare_statement, Object.new, "same", "SELECT 2", nil)
      }.to raise_error(PgPipeline::Error, /already exists/)
      expect(driver).to have_received(:submit).once
    end

    it "prepares the complete current catalog before a replacement starts" do
      pool = prepared_pool([])
      first = PgPipeline::PreparedStatement.new(
        client: Object.new, name: "first", physical_name: "pgp_1", sql: "SELECT 1"
      )
      second = PgPipeline::PreparedStatement.new(
        client: Object.new, name: "second", physical_name: "pgp_2", sql: "SELECT 2"
      )
      pool.instance_variable_set(:@prepared_statements, {"first" => first})
      pool.instance_variable_set(:@prepared_generation, 1)

      first_result = instance_double("PG::Result", clear: nil)
      second_result = instance_double("PG::Result", clear: nil)
      connection = instance_double("PG::Connection")
      allow(connection).to receive(:prepare).with("pgp_1", "SELECT 1") do
        pool.instance_variable_get(:@prepared_statements)["second"] = second
        pool.instance_variable_set(:@prepared_generation, 2)
        first_result
      end
      allow(connection).to receive(:prepare).with("pgp_2", "SELECT 2").and_return(second_result)

      pool.send(:prepare_registered_statements, connection)

      expect(connection).to have_received(:prepare).with("pgp_1", "SELECT 1").once
      expect(connection).to have_received(:prepare).with("pgp_2", "SELECT 2").once
    end
  end
end
