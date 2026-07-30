# frozen_string_literal: true

require "pg"
require "async"
require "async/semaphore"
require "async/notification"

require_relative "errors"
require_relative "connection_driver"
require_relative "server_caps"

module PgPipeline
  class Pool
    DEFAULT_PIPELINE_SIZE = 4
    DEFAULT_PINNED_SIZE = 2
    DEFAULT_RECONNECT_INTERVAL = 0.5
    DEFAULT_RECONNECT_BACKOFF_MAX = 30.0
    DEFAULT_HEALTH_INTERVAL = 10.0
    DEFAULT_HEALTH_TIMEOUT = 5.0
    DISCARD_SEQUENCES_SERVER_VERSION = 90_400
    CANCEL_SIGNAL = Async::Cancel

    attr_reader :reconnects, :pipeline_size

    def initialize(connection_args,
                   pipeline_size: DEFAULT_PIPELINE_SIZE,
                   pinned_size: DEFAULT_PINNED_SIZE,
                   max_pending: ConnectionDriver::DEFAULT_MAX_PENDING,
                   max_in_flight: ConnectionDriver::DEFAULT_MAX_IN_FLIGHT,
                   reconnect: true,
                   reconnect_interval: DEFAULT_RECONNECT_INTERVAL,
                   reconnect_backoff_max: DEFAULT_RECONNECT_BACKOFF_MAX,
                   health_check: true,
                   health_interval: DEFAULT_HEALTH_INTERVAL,
                   health_timeout: DEFAULT_HEALTH_TIMEOUT,
                   cancel_pinned_on_abort: true)
      @connection_args = connection_args
      @pipeline_size = PoolOps.positive_integer!(pipeline_size, :pipeline_size)
      @pinned_size = PoolOps.nonnegative_integer!(pinned_size, :pinned_size)
      @max_pending = max_pending
      @max_in_flight = max_in_flight
      @reconnect = reconnect
      @reconnect_interval = PoolOps.finite_float!(reconnect_interval, :reconnect_interval)
      @reconnect_backoff_max = PoolOps.finite_float!(reconnect_backoff_max, :reconnect_backoff_max)
      @health_check = health_check
      @health_interval = PoolOps.finite_float!(health_interval, :health_interval, allow_zero: true)
      @health_timeout = PoolOps.finite_float!(health_timeout, :health_timeout)
      @cancel_pinned_on_abort = cancel_pinned_on_abort

      @drivers = []
      @driver_backoff = []
      @driver_attempts = []
      @driver_last_health = []
      @rr = 0
      @reconnects = 0
      @health_failures = 0
      @supervisor_error = nil
      @supervisor = nil
      @task_parent = nil

      @pinned_free = []
      @pinned_in_use = {}
      @pinned_gate = nil
      @pinned_error = nil
      @pinned_active = 0
      @pinned_owners = Hash.new(0)
      @pinned_idle = Async::Notification.new

      @started = false
      @closing = false
      @closed = false
    end

    def start(parent: nil)
      raise Error, "pool already started" if @started
      if @closing || @closed
        raise ShutdownError, "pool is closing or was closed and cannot be restarted; create a new Pool"
      end

      parent ||= Async::Task.current
      @task_parent = parent

      begin
        @pipeline_size.times { @drivers << start_pipeline_driver(parent) }
        @driver_backoff = Array.new(@drivers.size, 0.0)
        @driver_attempts = Array.new(@drivers.size, 0)
        @driver_last_health = Array.new(@drivers.size, monotonic)
        @pinned_gate = Async::Semaphore.new(@pinned_size) if @pinned_size.positive?
        @started = true
        @supervisor = parent.async { supervise } if @reconnect || @health_check
      rescue Exception
        cleanup_partial_start
        raise
      end

      self
    end

    def closing? = @closing

    def pipeline_driver
      ensure_available!

      driver, @rr = PoolOps.select_driver(@drivers, @rr)
      raise NotDispatchedError, "no live pipeline connections; request was not dispatched" unless driver

      driver
    end

    def with_pinned
      ensure_available!
      raise @pinned_error if @pinned_error
      raise Error, "pinned pool is disabled (pinned_size=0)" if @pinned_size.zero?

      if @pinned_owners[Async::Task.current].positive?
        raise RecursiveCheckoutError,
              "nested Client#session/transaction on the same task is not allowed; " \
              "use Transaction#savepoint for nested atomicity"
      end

      @pinned_gate.acquire do
        raise @pinned_error if @pinned_error
        raise ShutdownError, "pool is closing" if @closing

        owner = Async::Task.current
        conn = nil
        @pinned_active += 1
        @pinned_owners[owner] += 1

        begin
          conn = @pinned_free.pop || PoolOps.new_connection(@connection_args)
          raise @pinned_error if @pinned_error
          raise ShutdownError, "pool is closing" if @closing

          @pinned_in_use[conn] = owner
          yield conn
        ensure
          @pinned_in_use.delete(conn) if conn
          release_pinned(owner, conn)
        end
      end
    end

    def stats
      {
        pipeline: {
          size: @pipeline_size,
          live: @drivers.count(&:available?),
          drivers: @drivers.map(&:stats)
        },
        pinned: {
          size: @pinned_size,
          active: @pinned_active,
          free: @pinned_free.size,
          in_use: @pinned_in_use.size
        },
        reconnects: @reconnects,
        health_failures: @health_failures,
        supervisor_error: @supervisor_error&.message,
        closing: @closing,
        closed: @closed,
        pinned_error: @pinned_error&.message
      }
    end

    def graceful_close
      return unless @started

      if @pinned_owners[Async::Task.current].positive?
        raise Error, "cannot close the pool from inside Client#session/transaction"
      end

      @closing = true
      @started = false
      first_error = nil

      begin
        first_error = preferred_shutdown_error(first_error, stop_supervisor)
        first_error = preferred_shutdown_error(first_error, close_all_drivers(:graceful_close))

        begin
          wait_for_pinned_idle
        rescue CANCEL_SIGNAL, StandardError => e
          first_error = preferred_shutdown_error(first_error, e)
        end
      ensure
        close_free_pinned
        @closed = true
      end

      raise first_error if first_error

      nil
    end

    def abort!
      return unless @started || @closing

      @closing = true
      @started = false
      first_error = nil

      begin
        first_error = preferred_shutdown_error(first_error, stop_supervisor)
        if @cancel_pinned_on_abort
          first_error = preferred_shutdown_error(first_error, cancel_in_use_pinned)
        end
        first_error = preferred_shutdown_error(first_error, close_all_drivers(:abort!))
      ensure
        close_free_pinned
        @closed = true
      end

      raise first_error if first_error

      nil
    end

    private

    def supervise
      until @closing
        begin
          reap_and_replace(@task_parent) if @reconnect
          health_probe if @health_check
          @supervisor_error = nil
        rescue StandardError => e
          @supervisor_error = e
        end

        break if @closing

        sleep(supervisor_sleep_interval)
      end
    end

    def supervisor_sleep_interval
      candidates = []
      candidates << @reconnect_interval if @reconnect
      candidates << @health_interval if @health_check && @health_interval.positive?
      interval = candidates.min || @reconnect_interval
      interval.positive? ? interval : @reconnect_interval
    end

    def reap_and_replace(parent)
      now = monotonic
      ensure_driver_slots!

      @drivers.each_index do |index|
        driver = @drivers[index]
        next if driver.available?
        next unless driver.dead?
        next if now < (@driver_backoff[index] || 0.0)

        begin
          @drivers[index] = start_pipeline_driver(parent)
          @driver_backoff[index] = 0.0
          @driver_attempts[index] = 0
          @driver_last_health[index] = monotonic
          @reconnects += 1
        rescue StandardError
          @driver_attempts[index] = (@driver_attempts[index] || 0) + 1
          @driver_backoff[index] = now + next_backoff(@driver_attempts[index])
        end
      end
    end

    def health_probe
      now = monotonic
      ensure_driver_slots!

      @drivers.each_index do |index|
        driver = @drivers[index]
        next unless driver.available?
        next unless driver.load.zero?
        next if now - (@driver_last_health[index] || 0.0) < @health_interval

        @driver_last_health[index] = now
        next if driver.health_check(@health_timeout)

        @health_failures += 1
        driver.abort!
      end
    end

    def ensure_driver_slots!
      size = @drivers.size
      @driver_backoff = Array.new(size, 0.0) if @driver_backoff.nil? || @driver_backoff.size != size
      @driver_attempts = Array.new(size, 0) if @driver_attempts.nil? || @driver_attempts.size != size
      @driver_last_health = Array.new(size, 0.0) if @driver_last_health.nil? || @driver_last_health.size != size
    end

    def next_backoff(attempts)
      exponent = [Integer(attempts) - 1, 0].max
      delay = @reconnect_interval * (2.0**exponent)
      delay.finite? && delay < @reconnect_backoff_max ? delay : @reconnect_backoff_max
    end

    def cancel_in_use_pinned
      cancellation = nil

      @pinned_in_use.keys.each do |conn|
        conn.cancel if conn.respond_to?(:cancel)
      rescue CANCEL_SIGNAL => e
        cancellation ||= e
      rescue StandardError
        nil
      end

      cancellation
    end

    def stop_supervisor
      supervisor = @supervisor
      @supervisor = nil
      supervisor&.stop
      nil
    rescue CANCEL_SIGNAL => e
      e
    rescue StandardError
      nil
    end

    def close_all_drivers(method_name)
      first_error = nil
      cancellation = nil

      @drivers.each do |driver|
        begin
          driver.public_send(method_name)
        rescue CANCEL_SIGNAL => e
          cancellation ||= e
        rescue StandardError => e
          first_error ||= e
        end
      end

      cancellation || first_error
    end

    def preferred_shutdown_error(current, candidate)
      return current unless candidate
      return candidate if candidate.is_a?(CANCEL_SIGNAL)

      current || candidate
    end

    def ensure_available!
      raise ShutdownError, "pool is closing or closed" if @closing || @closed
      raise Error, "pool not started" unless @started
    end

    def start_pipeline_driver(parent)
      conn = PoolOps.new_connection(@connection_args)
      driver = ConnectionDriver.new(conn, max_pending: @max_pending, max_in_flight: @max_in_flight)
      driver.start(parent: parent)
    rescue Exception
      PoolOps.safe_close(conn) if conn
      raise
    end

    def release_pinned(owner, conn)
      begin
        if conn
          if @closing
            PoolOps.safe_close(conn)
          else
            recycled = recycle_pinned_connection(conn)

            if recycled
              if @closing
                PoolOps.safe_close(recycled)
              else
                @pinned_free << recycled
              end
            end
          end
        end
      ensure
        @pinned_active -= 1
        @pinned_owners[owner] -= 1
        @pinned_owners.delete(owner) if @pinned_owners[owner].zero?
        @pinned_idle.signal if @pinned_active.zero?
      end
    end

    def recycle_pinned_connection(conn)
      if conn.server_version < DISCARD_SEQUENCES_SERVER_VERSION
        PoolOps.safe_close(conn)
        return PoolOps.new_connection(@connection_args)
      end

      PoolOps.sanitize_pinned_connection(conn)
      conn
    rescue CANCEL_SIGNAL
      PoolOps.safe_close(conn)
      raise
    rescue StandardError => cleanup_error
      PoolOps.safe_close(conn)

      begin
        PoolOps.new_connection(@connection_args)
      rescue StandardError => replacement_error
        @pinned_error = ConnectionLostError.new(
          "pinned connection reset failed (#{cleanup_error.class}: #{cleanup_error.message}) " \
          "and replacement failed (#{replacement_error.class}: #{replacement_error.message})"
        )
        nil
      end
    end

    def wait_for_pinned_idle
      @pinned_idle.wait while @pinned_active.positive?
    end

    def cleanup_partial_start
      @drivers.each do |driver|
        driver.abort!
      rescue CANCEL_SIGNAL, StandardError
        nil
      end
      @drivers.clear
      @driver_backoff.clear
      @driver_attempts.clear
      @driver_last_health.clear
      close_free_pinned
      @pinned_gate = nil
      @supervisor = nil
      @task_parent = nil
      @started = false
      @closing = false
      @closed = false
    end

    def close_free_pinned
      @pinned_free.each { |conn| PoolOps.safe_close(conn) }
      @pinned_free.clear
    end

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  module PoolOps
    module_function

    def positive_integer!(value, name)
      integer = Integer(value)
      raise ArgumentError, "#{name} must be >= 1" if integer < 1

      integer
    rescue ArgumentError, TypeError
      raise ArgumentError, "#{name} must be an integer >= 1"
    end

    def finite_float!(value, name, allow_zero: false)
      number = Float(value)
      bound_ok = allow_zero ? number >= 0 : number.positive?
      raise ArgumentError unless number.finite? && bound_ok

      number
    rescue ArgumentError, TypeError
      requirement = allow_zero ? "non-negative finite" : "positive finite"
      raise ArgumentError, "#{name} must be a #{requirement} number (got #{value.inspect})"
    end

    def nonnegative_integer!(value, name)
      integer = Integer(value)
      raise ArgumentError, "#{name} must be >= 0" if integer.negative?

      integer
    rescue ArgumentError, TypeError
      raise ArgumentError, "#{name} must be an integer >= 0"
    end

    def select_driver(drivers, rr)
      min_load = nil
      count = 0
      drivers.each do |driver|
        next unless driver.available?

        load = driver.load
        if min_load.nil? || load < min_load
          min_load = load
          count = 1
        elsif load == min_load
          count += 1
        end
      end
      return [nil, rr] if count.zero?

      target = rr % count
      index = 0
      drivers.each do |driver|
        next unless driver.available?
        next unless driver.load == min_load

        return [driver, rr + 1] if index == target

        index += 1
      end
      [nil, rr]
    end

    def new_connection(connection_args)
      connection_args.nil? ? PG::Connection.new : PG::Connection.new(connection_args)
    end

    def sanitize_pinned_connection(conn)
      raise ConnectionLostError, "pinned connection is closed" if conn.finished?
      raise ConnectionLostError, "pinned connection is bad" unless conn.status == PG::CONNECTION_OK

      case conn.transaction_status
      when PG::PQTRANS_IDLE
        nil
      when PG::PQTRANS_INTRANS, PG::PQTRANS_INERROR
        conn.exec("ROLLBACK")
      else
        raise ConnectionLostError,
              "pinned connection returned in unsafe transaction state #{conn.transaction_status}"
      end

      conn.exec("DISCARD ALL")
      true
    end

    def safe_close(conn)
      conn.close unless conn.finished?
    rescue StandardError
      nil
    end
  end
end
