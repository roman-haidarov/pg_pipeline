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
      pool.instance_variable_set(:@pinned_free, [])
      pool.instance_variable_set(:@pinned_in_use, {})
      pool.instance_variable_set(:@pinned_gate, nil)
      pool.instance_variable_set(:@pinned_error, nil)
      pool.instance_variable_set(:@pinned_active, 0)
      pool.instance_variable_set(:@pinned_owners, Hash.new(0))
      pool.instance_variable_set(:@pinned_idle, Async::Notification.new)
      pool.instance_variable_set(:@started, true)
      pool.instance_variable_set(:@closing, false)
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
      Async do |task|
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
      end.wait
    end

    it "applies exponential backoff when replacement fails" do
      Async do |task|
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
      end.wait
    end

    it "skips drivers that are still draining (not dead yet)" do
      Async do |task|
        pool = bare_pool
        draining = fake_driver(available: false, dead: false)
        pool.instance_variable_set(:@drivers, [draining])

        expect(pool).not_to receive(:start_pipeline_driver)
        pool.send(:reap_and_replace, task)
        expect(pool.reconnects).to eq(0)
      end.wait
    end

    it "reports no live drivers via select when all are dead" do
      dead = fake_driver(available: false, dead: true)
      driver, rr = PgPipeline::PoolOps.select_driver([dead], 0)
      expect(driver).to be_nil
      expect(rr).to eq(0)
    end

    it "does not reconnect dead drivers when reconnect is false but health checks are enabled" do
      Async do |task|
        pool = bare_pool
        pool.instance_variable_set(:@reconnect, false)
        pool.instance_variable_set(:@health_check, true)
        pool.instance_variable_set(:@closing, false)

        expect(pool).not_to receive(:reap_and_replace)
        expect(pool).to receive(:health_probe).once
        allow(task).to receive(:sleep) do
          pool.instance_variable_set(:@closing, true)
        end

        pool.send(:supervise)
      end.wait
    end

    it "records supervisor errors and keeps looping" do
      Async do |task|
        pool = bare_pool
        pool.instance_variable_set(:@drivers, [])
        pool.instance_variable_set(:@reconnect, true)
        pool.instance_variable_set(:@health_check, false)
        pool.instance_variable_set(:@closing, false)
        calls = 0

        allow(pool).to receive(:reap_and_replace) do
          calls += 1
          raise "boom" if calls == 1
        end
        allow(task).to receive(:sleep) do
          pool.instance_variable_set(:@closing, true) if calls >= 2
        end

        pool.send(:supervise)

        expect(calls).to be >= 2
        # Second successful iteration clears the transient supervisor_error.
        expect(pool.stats[:supervisor_error]).to be_nil
      end.wait
    end

    it "health_probe aborts an idle driver that fails its probe" do
      Async do
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
      end.wait
    end

    it "health_probe skips busy drivers" do
      Async do
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
      end.wait
    end

    it "abort! cancels checked-out pinned connections" do
      Async do
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
      end.wait
    end
  end
end
