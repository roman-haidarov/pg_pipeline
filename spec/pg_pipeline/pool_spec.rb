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
      pool.instance_variable_set(:@task_parent, nil)
      pool.instance_variable_set(:@pinned_free, [])
      pool.instance_variable_set(:@pinned_in_use, {})
      pool.instance_variable_set(:@pinned_gate, nil)
      pool.instance_variable_set(:@pinned_error, nil)
      pool.instance_variable_set(:@pinned_active, 0)
      pool.instance_variable_set(:@pinned_owners, Hash.new(0))
      pool.instance_variable_set(:@pinned_idle, Async::Notification.new)
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
      Sync do |task|
        pool = bare_pool
        dead = fake_driver(available: false, dead: true)
        live = fake_driver(available: true, dead: false)
        pool.instance_variable_set(:@drivers, [dead])

        expect(pool).to receive(:start_pipeline_driver).with(task).and_return(live)

        pool.send(:reap_and_replace, task)

        expect(pool.instance_variable_get(:@drivers)).to eq([live])
        expect(pool.reconnects).to eq(1)
        expect(pool.stats[:pipeline][:live]).to eq(1)
        expect(pool.stats[:reconnects]).to eq(1)
      end
    end

    it "applies exponential backoff when replacement fails" do
      Sync do |task|
        pool = bare_pool
        dead = fake_driver(available: false, dead: true)
        pool.instance_variable_set(:@drivers, [dead])
        pool.instance_variable_set(:@reconnect_interval, 0.5)

        expect(pool).to receive(:start_pipeline_driver).and_raise(RuntimeError, "pg down")

        before = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        pool.send(:reap_and_replace, task)

        expect(pool.reconnects).to eq(0)
        expect(pool.instance_variable_get(:@driver_attempts)).to eq([1])
        backoff_until = pool.instance_variable_get(:@driver_backoff).first
        expect(backoff_until).to be >= before + 0.5

        expect(pool).not_to receive(:start_pipeline_driver)
        pool.send(:reap_and_replace, task)
        expect(pool.instance_variable_get(:@driver_attempts)).to eq([1])
      end
    end

    it "skips drivers that are still draining (not dead yet)" do
      Sync do |task|
        pool = bare_pool
        draining = fake_driver(available: false, dead: false)
        pool.instance_variable_set(:@drivers, [draining])

        expect(pool).not_to receive(:start_pipeline_driver)
        pool.send(:reap_and_replace, task)
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
      Sync do |task|
        pool = bare_pool
        pool.instance_variable_set(:@task_parent, task)
        pool.instance_variable_set(:@reconnect, false)
        pool.instance_variable_set(:@health_check, true)
        pool.instance_variable_set(:@closing, false)

        expect(pool).not_to receive(:reap_and_replace)
        expect(pool).to receive(:health_probe).once
        allow(pool).to receive(:sleep) do
          pool.instance_variable_set(:@closing, true)
        end

        pool.send(:supervise)
      end
    end

    it "records supervisor errors and keeps looping" do
      Sync do |task|
        pool = bare_pool
        pool.instance_variable_set(:@task_parent, task)
        pool.instance_variable_set(:@drivers, [])
        pool.instance_variable_set(:@reconnect, true)
        pool.instance_variable_set(:@health_check, false)
        pool.instance_variable_set(:@closing, false)
        calls = 0

        allow(pool).to receive(:reap_and_replace) do
          calls += 1
          raise "boom" if calls == 1
        end
        allow(pool).to receive(:sleep) do
          pool.instance_variable_set(:@closing, true) if calls >= 2
        end

        pool.send(:supervise)

        expect(calls).to be >= 2
        # Second successful iteration clears the transient supervisor_error.
        expect(pool.stats[:supervisor_error]).to be_nil
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
        pool.instance_variable_set(:@pinned_in_use, {conn => Async::Task.current})
        pool.instance_variable_set(:@cancel_pinned_on_abort, true)

        pool.abort!
        expect(cancelled).to be(true)
        expect(pool.instance_variable_get(:@closing)).to be(true)
      end
    end
  end

  describe "replacement-driver ownership (P0.1)" do
    it "spawns replacements under the stable pool parent, not the supervisor task" do
      Sync do |pool_parent|
        pool = described_class.allocate
        pool.instance_variable_set(:@task_parent, pool_parent)
        pool.instance_variable_set(:@reconnect, true)
        pool.instance_variable_set(:@health_check, false)
        pool.instance_variable_set(:@reconnect_interval, 0.0)
        pool.instance_variable_set(:@supervisor_error, nil)
        pool.instance_variable_set(:@closing, false)

        seen_parent = nil
        allow(pool).to receive(:reap_and_replace) do |parent|
          seen_parent = parent
          pool.instance_variable_set(:@closing, true)
        end

        supervisor = pool_parent.async { pool.send(:supervise) }
        supervisor.wait

        expect(seen_parent).to equal(pool_parent)
        expect(seen_parent).not_to equal(supervisor)
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
      pool.instance_variable_set(:@pinned_idle, Async::Notification.new)
      pool
    end

    it "closes a recycled connection when the pool started closing during recycle" do
      Sync do
        pool = pinned_state_pool
        owner = Async::Task.current
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
    it "rejects a nested checkout on the same task before touching the gate" do
      Sync do
        pool = described_class.allocate
        pool.instance_variable_set(:@started, true)
        pool.instance_variable_set(:@closing, false)
        pool.instance_variable_set(:@closed, false)
        pool.instance_variable_set(:@pinned_error, nil)
        pool.instance_variable_set(:@pinned_size, 1)

        owner = Async::Task.current
        pool.instance_variable_set(:@pinned_owners, Hash.new(0).tap { |h| h[owner] = 1 })

        gate = instance_double(Async::Semaphore)
        pool.instance_variable_set(:@pinned_gate, gate)
        expect(gate).not_to receive(:acquire)

        expect { pool.send(:with_pinned) { flunk("block must not run") } }
          .to raise_error(PgPipeline::RecursiveCheckoutError)
      end
    end

    it "allows a fresh (non-nested) checkout on a task that holds no slot" do
      Sync do
        pool = described_class.allocate
        pool.instance_variable_set(:@started, true)
        pool.instance_variable_set(:@closing, false)
        pool.instance_variable_set(:@closed, false)
        pool.instance_variable_set(:@pinned_error, nil)
        pool.instance_variable_set(:@pinned_size, 1)
        pool.instance_variable_set(:@pinned_owners, Hash.new(0))

        gate = instance_double(Async::Semaphore)
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
      pool = described_class.new(nil, pipeline_size: 1, pinned_size: 0)
      driver = instance_double(PgPipeline::ConnectionDriver)
      parent = instance_double(Async::Task)

      allow(pool).to receive(:start_pipeline_driver).with(parent).and_return(driver)
      allow(parent).to receive(:async).and_raise(RuntimeError, "parent stopped")
      expect(driver).to receive(:abort!)

      expect { pool.start(parent: parent) }.to raise_error(RuntimeError, "parent stopped")
      expect(pool.instance_variable_get(:@drivers)).to be_empty
      expect(pool.instance_variable_get(:@task_parent)).to be_nil
      expect(pool.instance_variable_get(:@started)).to be(false)
      expect(pool.instance_variable_get(:@closing)).to be(false)
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
end
