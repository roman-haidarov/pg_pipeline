# frozen_string_literal: true

require 'pg'

require_relative 'errors'
require_relative 'runtime'
require_relative 'connection_driver'
require_relative 'prepared_statement'
require_relative 'type_maps'
require_relative 'server_caps'

module PgPipeline
  PoolSettings = Data.define(
    :pipeline_size, :pinned_size, :max_pending, :max_in_flight, :reconnect,
    :reconnect_interval, :reconnect_backoff_max, :health_check, :health_interval,
    :health_timeout, :cancel_pinned_on_abort
  ) do
    def self.build(options)
      unknown = options.keys - members
      raise ArgumentError, "unknown keyword: #{unknown.first}" unless unknown.empty?

      new(**normalize_settings(defaults.merge(options)))
    end

    def self.defaults
      {
        pipeline_size: Pool::DEFAULT_PIPELINE_SIZE,
        pinned_size: Pool::DEFAULT_PINNED_SIZE,
        max_pending: ConnectionDriver::DEFAULT_MAX_PENDING,
        max_in_flight: ConnectionDriver::DEFAULT_MAX_IN_FLIGHT,
        reconnect: true,
        reconnect_interval: Pool::DEFAULT_RECONNECT_INTERVAL,
        reconnect_backoff_max: Pool::DEFAULT_RECONNECT_BACKOFF_MAX,
        health_check: true,
        health_interval: Pool::DEFAULT_HEALTH_INTERVAL,
        health_timeout: Pool::DEFAULT_HEALTH_TIMEOUT,
        cancel_pinned_on_abort: true
      }
    end

    def self.normalize_settings(values)
      {
        pipeline_size: PoolOps.positive_integer!(values.fetch(:pipeline_size), :pipeline_size),
        pinned_size: PoolOps.nonnegative_integer!(values.fetch(:pinned_size), :pinned_size),
        max_pending: values.fetch(:max_pending),
        max_in_flight: values.fetch(:max_in_flight),
        reconnect: values.fetch(:reconnect),
        reconnect_interval: PoolOps.finite_float!(values.fetch(:reconnect_interval), :reconnect_interval),
        reconnect_backoff_max: PoolOps.finite_float!(values.fetch(:reconnect_backoff_max), :reconnect_backoff_max),
        health_check: values.fetch(:health_check),
        health_interval: PoolOps.finite_float!(values.fetch(:health_interval), :health_interval, allow_zero: true),
        health_timeout: PoolOps.finite_float!(values.fetch(:health_timeout), :health_timeout),
        cancel_pinned_on_abort: values.fetch(:cancel_pinned_on_abort)
      }
    end

    private_class_method :defaults, :normalize_settings
  end
  private_constant :PoolSettings

  class Pool
    DEFAULT_PIPELINE_SIZE = 4
    DEFAULT_PINNED_SIZE = 2
    DEFAULT_RECONNECT_INTERVAL = 0.5
    DEFAULT_RECONNECT_BACKOFF_MAX = 30.0
    DEFAULT_HEALTH_INTERVAL = 10.0
    DEFAULT_HEALTH_TIMEOUT = 5.0
    DISCARD_SEQUENCES_SERVER_VERSION = 90_400
    CANCEL_SIGNAL = Runtime::Cancel
    SUPERVISOR_JOIN_TIMEOUT = 5.0
    MAX_PAUSE_FAILURES = 3

    attr_reader :reconnects, :pipeline_size

    def initialize(connection_args, **options)
      @connection_args = connection_args
      settings = PoolSettings.build(options)
      @pipeline_size = settings.pipeline_size
      @pinned_size = settings.pinned_size
      @max_pending = settings.max_pending
      @max_in_flight = settings.max_in_flight
      @reconnect = settings.reconnect
      @reconnect_interval = settings.reconnect_interval
      @reconnect_backoff_max = settings.reconnect_backoff_max
      @health_check = settings.health_check
      @health_interval = settings.health_interval
      @health_timeout = settings.health_timeout
      @cancel_pinned_on_abort = settings.cancel_pinned_on_abort

      @drivers = []
      @driver_backoff = []
      @driver_attempts = []
      @driver_last_health = []
      @rr = 0
      @rr_slot = [0]
      @reconnects = 0
      @health_failures = 0
      @supervisor_error = nil
      @supervisor = nil
      @supervisor_wake = Runtime::Notification.new
      @pause_failures = 0

      @prepared_statements = {}
      @prepared_generation = 0
      @statement_sequence = 0
      @type_maps = TypeMaps.new

      @pinned_free = []
      @pinned_in_use = {}
      @pinned_gate = nil
      @pinned_error = nil
      @pinned_active = 0
      @pinned_owners = Hash.new(0)
      @pinned_idle = Runtime::Notification.new

      @started = false
      @closing = false
      @closed = false
    end

    def start
      validate_start!

      begin
        start_runtime
      rescue Exception
        cleanup_partial_start
        raise
      end

      self
    end

    private

    def validate_start!
      raise Error, 'pool already started' if @started
      if @closing || @closed
        raise ShutdownError,
              'pool is closing or was closed and cannot be restarted; create a new Pool'
      end
      raise Error, 'pool start requires an active Fiber scheduler' unless Fiber.scheduler
    end

    def start_runtime
      @pipeline_size.times { @drivers << start_pipeline_driver }
      driver_count = @drivers.size
      @driver_backoff = Array.new(driver_count, 0.0)
      @driver_attempts = Array.new(driver_count, 0)
      @driver_last_health = Array.new(driver_count, monotonic)
      pinned_size = @pinned_size
      @pinned_gate = Runtime::Semaphore.new(pinned_size) if pinned_size.positive?
      @started = true
      start_supervisor if @reconnect || @health_check
    end

    def start_supervisor
      @supervisor = Runtime.spawn(name: :supervisor) { supervise }
    end

    public

    def closing? = @closing

    def pipeline_driver
      ensure_available!

      driver = PoolOps.select_driver_into(@drivers, @rr, @rr_slot)
      @rr = @rr_slot[0]
      raise NotDispatchedError, 'no live pipeline connections; request was not dispatched' unless driver

      driver
    end

    def bind_type_map(request, statement)
      return unless statement.typed?

      maps = @type_maps
      return unless maps

      maps.bind(request, statement)
    end

    def prepare_statement(client, name, sql, param_types, typed: false)
      ensure_available!
      logical_name = PreparedStatementOps.snapshot_name(name)
      if @prepared_statements.key?(logical_name)
        raise Error, "prepared statement #{logical_name.inspect} already exists"
      end

      statement = register_prepared_statement(client, logical_name, sql, param_types, typed)
      requests = []

      begin
        requests = dispatch_prepare(statement)
        await_prepares!(requests)
        statement
      rescue Exception
        drop_prepared_statement(logical_name, statement)
        raise
      ensure
        requests.each { |request| request.cancel! unless request.settled? }
      end
    end

    def register_prepared_statement(client, logical_name, sql, param_types, typed)
      @statement_sequence += 1
      statement = PreparedStatement.new(
        client: client,
        name: logical_name,
        physical_name: "pgp_#{@statement_sequence.to_s(36)}",
        sql: sql,
        param_types: param_types,
        typed: typed
      )

      @prepared_statements[logical_name] = statement
      register_typed_maps(statement) if statement.typed?
      @prepared_generation += 1
      statement
    end

    def register_typed_maps(statement)
      @type_maps ||= TypeMaps.new
      @type_maps.register(statement)
      @type_maps.ensure_bundle!(@connection_args)
    end

    def dispatch_prepare(statement)
      drivers = @drivers.select(&:available?)
      raise NotDispatchedError, 'no live pipeline connections; statement was not prepared' if drivers.empty?

      drivers.map { |driver| Request.prepare(statement).tap { |r| driver.submit(r) } }
    end

    def await_prepares!(requests)
      first_error = nil
      requests.each do |request|
        RequestOps.clear_result(request.wait)
      rescue StandardError => e
        if first_error
          e.clear_result! if e.respond_to?(:clear_result!)
        else
          first_error = e
        end
      end
      raise first_error if first_error
    end

    def drop_prepared_statement(logical_name, statement)
      return unless @prepared_statements.delete(logical_name)

      @type_maps.unregister(statement.physical_name) if statement&.typed? && @type_maps
      @prepared_generation += 1
    end

    def with_pinned(&block)
      ensure_available!
      raise @pinned_error if @pinned_error
      raise Error, 'pinned pool is disabled (pinned_size=0)' if @pinned_size.zero?

      owner = Fiber.current
      assert_not_nested_pinned!(owner)

      @pinned_gate.acquire do
        assert_pinned_open!
        run_pinned(owner, &block)
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
        prepared_statements: (@prepared_statements || {}).size,
        reconnects: @reconnects,
        health_failures: @health_failures,
        supervisor_error: @supervisor_error&.message,
        supervisor_alive: supervisor_alive?,
        closing: @closing,
        closed: @closed,
        pinned_error: @pinned_error&.message
      }
    end

    def graceful_close
      return unless @started

      if @pinned_owners[Fiber.current].positive?
        raise Error, 'cannot close the pool from inside Client#session/transaction'
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
        first_error = preferred_shutdown_error(first_error, cancel_in_use_pinned) if @cancel_pinned_on_abort
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
          reap_and_replace if @reconnect
          health_probe if @health_check
          @supervisor_error = nil
        rescue StandardError => e
          @supervisor_error = e
        end

        break if @closing
        break unless supervisor_pause
      end
    end

    def supervisor_pause
      @supervisor_wake.wait(supervisor_sleep_interval)
      @pause_failures = 0
      true
    rescue StandardError => e
      @supervisor_error = e
      @pause_failures = (@pause_failures || 0) + 1

      unless ENV['PG_PIPELINE_SILENCE_WARNINGS']
        warn("pg_pipeline: supervisor pause failed (#{e.class}: #{e.message}) " \
             "[#{@pause_failures}/#{MAX_PAUSE_FAILURES}]")
      end

      @pause_failures < MAX_PAUSE_FAILURES
    end

    def supervisor_alive?
      supervisor = @supervisor
      !supervisor.nil? && !supervisor.finished?
    end

    def supervisor_sleep_interval
      reconnect_interval = @reconnect_interval
      health_interval = @health_interval
      candidates = []
      candidates << reconnect_interval if @reconnect
      candidates << health_interval if @health_check && health_interval.positive?
      interval = candidates.min || reconnect_interval
      interval.positive? ? interval : reconnect_interval
    end

    def reap_and_replace
      now = monotonic
      ensure_driver_slots!

      @drivers.each_index do |index|
        replace_unavailable_driver(index, now)
      end
    end

    def replace_unavailable_driver(index, now)
      driver = @drivers[index]
      return if driver.available? || !driver.dead? || now < (@driver_backoff[index] || 0.0)

      begin
        @drivers[index] = start_pipeline_driver
        reset_driver_retry_state(index)
      rescue StandardError
        backoff_driver_retry(index, now)
      end
    end

    def reset_driver_retry_state(index)
      @driver_backoff[index] = 0.0
      @driver_attempts[index] = 0
      @driver_last_health[index] = monotonic
      @reconnects += 1
    end

    def backoff_driver_retry(index, now)
      attempts = (@driver_attempts[index] || 0) + 1
      @driver_attempts[index] = attempts
      @driver_backoff[index] = now + next_backoff(attempts)
    end

    def health_probe
      now = monotonic
      ensure_driver_slots!

      @drivers.each_index do |index|
        probe_driver_health(index, now)
      end
    end

    def probe_driver_health(index, now)
      driver = @drivers[index]
      return unless driver.available? && driver.load.zero?
      return if now - (@driver_last_health[index] || 0.0) < @health_interval

      @driver_last_health[index] = now
      run_driver_health_check(driver)
    end

    def run_driver_health_check(driver)
      return if driver.health_check(@health_timeout)

      @health_failures += 1
      driver.abort!
    rescue StandardError => e
      @health_failures += 1
      @supervisor_error = e
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
      maximum = @reconnect_backoff_max
      delay.finite? && delay < maximum ? delay : maximum
    end

    def cancel_in_use_pinned
      cancellation = nil

      @pinned_in_use.each_key do |conn|
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
      return nil unless supervisor

      @supervisor_wake.signal

      begin
        supervisor.wait(SUPERVISOR_JOIN_TIMEOUT)
      rescue Runtime::TimeoutError
        unless ENV['PG_PIPELINE_SILENCE_WARNINGS']
          warn("pg_pipeline: supervisor did not exit within #{SUPERVISOR_JOIN_TIMEOUT}s and has been leaked")
        end
      rescue CANCEL_SIGNAL, StandardError
        nil
      end

      nil
    rescue StandardError
      nil
    end

    def close_all_drivers(method_name)
      first_error = nil
      cancellation = nil

      @drivers.each do |driver|
        driver.public_send(method_name)
      rescue CANCEL_SIGNAL => e
        cancellation ||= e
      rescue StandardError => e
        first_error ||= e
      end

      cancellation || first_error
    end

    def preferred_shutdown_error(current, candidate)
      return current unless candidate
      return candidate if candidate.is_a?(CANCEL_SIGNAL)

      current || candidate
    end

    def ensure_available!
      raise ShutdownError, 'pool is closing or closed' if @closing || @closed
      raise Error, 'pool not started' unless @started
    end

    def start_pipeline_driver
      conn = PoolOps.new_connection(@connection_args)
      prepare_registered_statements(conn)
      driver = ConnectionDriver.new(conn, max_pending: @max_pending, max_in_flight: @max_in_flight,
                                          type_maps: @type_maps)
      driver.start
    rescue Exception
      PoolOps.safe_close(conn) if conn
      raise
    end

    def prepare_registered_statements(conn)
      prepared = {}

      loop do
        generation = @prepared_generation || 0
        statements = (@prepared_statements || {}).values.dup
        prepare_statements(conn, statements, prepared)

        break if generation == (@prepared_generation || 0)
      end
    end

    def prepare_statements(conn, statements, prepared)
      statements.each do |statement|
        prepare_statement_on_connection(conn, statement, prepared) unless prepared.key?(statement.physical_name)
      end
    end

    def prepare_statement_on_connection(conn, statement, prepared)
      param_types = statement.param_types
      name = statement.physical_name
      sql = statement.sql
      result = if param_types.nil?
                 conn.prepare(name, sql)
               else
                 conn.prepare(name, sql, param_types)
               end
      RequestOps.clear_result(result)
      prepared[name] = true
    end

    def assert_not_nested_pinned!(owner)
      return unless @pinned_owners[owner].positive?

      raise RecursiveCheckoutError,
            'nested Client#session/transaction on the same fiber is not allowed; ' \
            'use Transaction#savepoint for nested atomicity'
    end

    def assert_pinned_open!
      raise @pinned_error if @pinned_error
      raise ShutdownError, 'pool is closing' if @closing
    end

    def run_pinned(owner)
      conn = nil
      @pinned_active += 1
      @pinned_owners[owner] += 1

      begin
        conn = take_pinned_connection
        assert_pinned_open!
        @pinned_in_use[conn] = owner
        yield conn
      ensure
        @pinned_in_use.delete(conn) if conn
        release_pinned(owner, conn)
      end
    end

    def take_pinned_connection
      @pinned_free.pop || PoolOps.new_connection(@connection_args)
    end

    def release_pinned(owner, conn)
      return_pinned_connection(conn) if conn
    ensure
      @pinned_active -= 1
      owner_count = @pinned_owners[owner] - 1
      @pinned_owners[owner] = owner_count
      @pinned_owners.delete(owner) if owner_count.zero?
      @pinned_idle.signal if @pinned_active.zero?
    end

    def return_pinned_connection(conn)
      if @closing
        PoolOps.safe_close(conn)
        return
      end

      recycled = recycle_pinned_connection(conn)
      return unless recycled

      if @closing
        PoolOps.safe_close(recycled)
      else
        @pinned_free << recycled
      end
    end

    def recycle_pinned_connection(conn)
      return replace_pinned_connection(conn) if conn.server_version < DISCARD_SEQUENCES_SERVER_VERSION

      PoolOps.sanitize_pinned_connection(conn)
      conn
    rescue CANCEL_SIGNAL
      PoolOps.safe_close(conn)
      raise
    rescue StandardError => e
      begin
        replace_pinned_connection(conn)
      rescue StandardError => replacement_error
        @pinned_error = ConnectionLostError.new(
          "pinned connection reset failed (#{e.class}: #{e.message}) " \
          "and replacement failed (#{replacement_error.class}: #{replacement_error.message})"
        )
        nil
      end
    end

    def replace_pinned_connection(conn)
      PoolOps.safe_close(conn)
      PoolOps.new_connection(@connection_args)
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

    def positive_integer!(value, name) = DriverOps.positive_integer!(value, name)

    def finite_float!(value, name, allow_zero: false)
      number = Float(value)
      bound_ok = allow_zero ? number >= 0 : number.positive?
      raise ArgumentError unless number.finite? && bound_ok

      number
    rescue ArgumentError, TypeError
      requirement = allow_zero ? 'non-negative finite' : 'positive finite'
      raise ArgumentError, "#{name} must be a #{requirement} number (got #{value.inspect})"
    end

    def nonnegative_integer!(value, name)
      integer = Integer(value)
      raise ArgumentError, "#{name} must be >= 0" if integer.negative?

      integer
    rescue ArgumentError, TypeError
      raise ArgumentError, "#{name} must be an integer >= 0"
    end

    def select_driver_into(drivers, round_robin, slot)
      size = drivers.length
      if size.zero?
        slot[0] = round_robin
        return nil
      end

      start = round_robin % size
      best = nil
      best_index = 0
      best_load = 0
      offset = 0

      while offset < size
        index = start + offset
        index -= size if index >= size
        driver = drivers[index]
        offset += 1
        next unless driver.available?

        load = driver.load
        next unless best.nil? || load < best_load

        best = driver
        best_index = index
        best_load = load
      end

      slot[0] = best ? (best_index + 1) % size : round_robin
      best
    end

    def select_driver(drivers, round_robin)
      slot = [round_robin]
      driver = select_driver_into(drivers, round_robin, slot)
      [driver, slot[0]]
    end

    def new_connection(connection_args)
      connection_args.nil? ? PG::Connection.new : PG::Connection.new(connection_args)
    end

    def sanitize_pinned_connection(conn)
      raise ConnectionLostError, 'pinned connection is closed' if conn.finished?
      raise ConnectionLostError, 'pinned connection is bad' unless conn.status == PG::CONNECTION_OK

      transaction_status = conn.transaction_status
      case transaction_status
      when PG::PQTRANS_IDLE
        nil
      when PG::PQTRANS_INTRANS, PG::PQTRANS_INERROR
        conn.exec('ROLLBACK')
      else
        raise ConnectionLostError,
              "pinned connection returned in unsafe transaction state #{transaction_status}"
      end

      conn.exec('DISCARD ALL')
      true
    end

    def safe_close(conn)
      conn.close unless conn.finished?
    rescue StandardError
      nil
    end
  end
end
