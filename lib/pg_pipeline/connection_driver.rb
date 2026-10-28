# frozen_string_literal: true

require 'pg'

require_relative 'errors'
require_relative 'runtime'
require_relative 'bounded_queue'
require_relative 'server_caps'
require_relative 'request'

module PgPipeline
  class ConnectionDriver
    DEFAULT_MAX_PENDING = 256
    DEFAULT_MAX_IN_FLIGHT = 64

    attr_reader :conn, :caps, :max_pending, :max_in_flight, :type_maps
    attr_accessor :socket, :requests, :events, :reader_rearm, :writer_commands,
                  :inflight, :dispatching, :submitting, :accepting, :running,
                  :draining, :needs_flush, :writer_armed, :request_event_pending,
                  :owner_task, :reader_task, :writer_task

    attr_writer :readable_events, :results_read, :units_completed, :flush_calls,
                :flush_incomplete, :dispatches, :leaked_watchers

    def readable_events = @readable_events || 0
    def results_read = @results_read || 0
    def units_completed = @units_completed || 0
    def flush_calls = @flush_calls || 0
    def flush_incomplete = @flush_incomplete || 0
    def dispatches = @dispatches || 0
    def leaked_watchers = @leaked_watchers || 0

    def initialize(conn, max_pending: DEFAULT_MAX_PENDING, max_in_flight: DEFAULT_MAX_IN_FLIGHT,
                   type_maps: nil)
      @max_pending = DriverOps.positive_integer!(max_pending, :max_pending)
      @max_in_flight = DriverOps.positive_integer!(max_in_flight, :max_in_flight)

      @conn = conn
      @type_maps = type_maps
      @caps = ServerCaps.from_connection(conn)
      @caps.assert_supported!
      DriverOps.warn_flush_coupling_once(@caps)
      @requests = BoundedQueue.new(@max_pending)
      @events = Runtime::Queue.new
      @reader_rearm = Runtime::Queue.new
      @writer_commands = Runtime::Queue.new

      @accepting = @running = @draining = @needs_flush = @writer_armed = @request_event_pending = false
      @readable_events = @results_read = @units_completed = @flush_calls =
                                           @flush_incomplete = @dispatches = @leaked_watchers = @submitting = 0
      @socket = @owner_task = @reader_task = @writer_task = @dispatching = nil
      @inflight = []
    end

    def start = DriverOps.start(self)
    def submit(request) = DriverOps.submit(self, request)
    def load = @requests.size + @inflight.size + @submitting + (@dispatching ? 1 : 0)
    def available? = @accepting && @running
    def dead? = !@running && !@accepting

    def stats
      {
        available: available?,
        load: load,
        pending: @requests.size,
        in_flight: @inflight.size,
        submitting: @submitting,
        needs_flush: @needs_flush,
        fast_sync: @caps.fast_sync?
      }.merge(counter_stats)
    end

    private

    def counter_stats
      {
        readable_events: readable_events,
        results_read: results_read,
        units_completed: units_completed,
        flush_calls: flush_calls,
        flush_incomplete: flush_incomplete,
        dispatches: dispatches,
        units_per_readable: DriverOps.ratio(units_completed, readable_events),
        results_per_readable: DriverOps.ratio(results_read, readable_events),
        flush_calls_per_unit: DriverOps.ratio(flush_calls, units_completed),
        leaked_watchers: leaked_watchers
      }
    end

    public

    def health_check(timeout)
      return true unless available?

      probe = Request.build('SELECT 1', nil)
      submit(probe)
      Runtime.with_timeout(timeout) { probe.wait }
      true
    rescue Runtime::TimeoutError
      DriverOps.abort_timed_out_health_probe(self, probe)
    rescue QueryError, PipelineAbortedError
      true
    rescue ShutdownError, NotDispatchedError
      true
    rescue ConnectionLostError, PG::Error
      false
    ensure
      probe.cancel! if probe && !probe.settled?
    end

    def graceful_close = DriverOps.graceful_close(self)

    def abort!(error = ConnectionLostError.new('connection aborted'))
      DriverOps.abort!(self, error)
    end
  end

  module DriverOps
    module_function

    WATCHER_JOIN_TIMEOUT = 2.0
    OWNER_JOIN_TIMEOUT = 5.0
    WATCHER_POLL_INTERVAL = 0.25

    def ratio(numerator, denominator)
      return 0.0 if denominator.zero?

      (numerator.to_f / denominator).round(3)
    end

    def warn_flush_coupling_once(caps)
      return if @flush_coupling_warned
      return if caps.fast_sync?
      return if ENV['PG_PIPELINE_SILENCE_WARNINGS']

      @flush_coupling_warned = true
      warn(
        "pg_pipeline: libpq #{caps.libpq_version} couples pipeline Sync with flush " \
        '(no PQsendPipelineSync). Queries still pipeline and still amortise RTT, but ' \
        'one flush per unit caps local throughput; libpq >= 17 is recommended for ' \
        'maximum throughput. Set PG_PIPELINE_SILENCE_WARNINGS=1 to silence this.'
      )
    end

    def positive_integer!(value, name)
      integer = Integer(value)
      raise ArgumentError, "#{name} must be >= 1" if integer < 1

      integer
    rescue ArgumentError, TypeError
      raise ArgumentError, "#{name} must be an integer >= 1"
    end

    def start(driver)
      raise Error, 'driver already started' if driver.running
      raise Error, 'driver start requires an active Fiber scheduler' unless Fiber.scheduler

      conn = driver.conn
      conn.setnonblocking(true)
      conn.enter_pipeline_mode
      driver.socket = conn.socket_io
      driver.accepting = true
      driver.running = true
      start_tasks(driver)
      driver
    rescue Exception
      driver.accepting = false
      driver.running = false
      teardown_watchers(driver)
      join_owner(driver)
      raise
    end

    def start_tasks(driver)
      driver.reader_task = Runtime.spawn(name: :reader) { reader_watcher(driver) }
      driver.writer_task = Runtime.spawn(name: :writer) { writer_watcher(driver) }
      driver.owner_task = Runtime.spawn(name: :owner) { owner_loop(driver) }
    end

    def submit(driver, request)
      raise ShutdownError, 'driver is not accepting work' unless driver.accepting
      raise ProtocolError, 'request must be new before submit' unless request.state == :new

      driver.submitting += 1
      begin
        driver.requests.enqueue(request)
        request.queued!
        notify_requests(driver)
      ensure
        finish_submission(driver)
      end

      request
    end

    def finish_submission(driver)
      submitting = driver.submitting - 1
      driver.submitting = submitting
      return unless driver.draining && driver.running && submitting.zero?

      driver.events.enqueue(:submission_finished)
    end

    def graceful_close(driver)
      return unless driver.running

      driver.accepting = false
      driver.requests.close(ShutdownError.new('driver is closing'))
      driver.events.enqueue(:close)
      join_owner(driver)
      nil
    end

    def abort!(driver, error)
      return unless driver.running

      driver.accepting = false
      driver.requests.close(not_dispatched_error(error))
      driver.events.enqueue([:abort, error])
      join_owner(driver)
      nil
    end

    def abort_timed_out_health_probe(driver, probe)
      return true if probe.settled? || !driver.running
      return true unless exclusive_health_probe?(driver, probe)

      abort!(driver, ConnectionLostError.new('idle health check timed out'))
      false
    end

    def exclusive_health_probe?(driver, probe)
      inflight = driver.inflight
      driver.accepting &&
        driver.dispatching.nil? &&
        driver.submitting.zero? &&
        driver.requests.empty? &&
        inflight.length == 1 &&
        inflight.first.equal?(probe)
    end

    def owner_loop(driver)
      while driver.running
        event = driver.events.dequeue
        break if event.nil?

        process_event(driver, event)
      end
    rescue StandardError => e
      fatal_close(driver, ConnectionLostError.new("driver crashed: #{e.class}: #{e.message}"))
    ensure
      fatal_close(driver, ShutdownError.new('driver owner stopped before shutdown completed')) if driver.running
    end

    def process_event(driver, event)
      input_changed = handle_event(driver, event)
      return if input_changed == :stopped

      drain_results(driver) if input_changed
      pump_requests(driver) if driver.running
      finish_graceful_close(driver) if driver.running && driver.draining && drained?(driver)
    end

    def handle_event(driver, event)
      case event
      when :requests
        driver.request_event_pending = false
      when :submission_finished
        nil
      when :readable
        handle_readable_event(driver)
        return true
      when :writable
        handle_writable_event(driver)
      when :close
        driver.draining = true
      when Array
        tag, payload = event
        return abort_event(driver, tag, payload)
      end

      false
    end

    def handle_writable_event(driver)
      driver.writer_armed = false
      flush_output(driver)
    end

    def handle_readable_event(driver)
      driver.readable_events += 1
      read_available(driver)
      true
    ensure
      driver.reader_rearm.enqueue(:rearm) if driver.running
    end

    def abort_event(driver, tag, payload)
      return false unless tag == :abort

      fatal_close(driver, payload)
      :stopped
    end

    def notify_requests(driver)
      return if driver.request_event_pending
      return unless driver.running

      driver.request_event_pending = true
      driver.events.enqueue(:requests)
    end

    def pump_requests(driver)
      dispatched = false
      while driver.inflight.size < driver.max_in_flight && !driver.requests.empty?
        request = driver.requests.dequeue
        break unless request
        next if request.cancelled?

        dispatched = dispatch_request(driver, request) || dispatched
      end

      flush_output(driver) if dispatched && !driver.needs_flush
    end

    def dispatch_request(driver, request)
      driver.dispatching = request
      return false unless send_unit(driver, request)

      request.dispatched!
      driver.inflight << request
      driver.dispatching = nil
      driver.dispatches += 1
      true
    ensure
      driver.dispatching = nil if request&.settled?
    end

    def send_unit(driver, request)
      return false unless send_request(driver, request)

      place_sync(driver)
      true
    end

    def send_request(driver, request)
      send_command(driver.conn, request)
      true
    rescue PG::UnableToSend => e
      reject_send_error(driver, request, e, recoverable: reusable_after_send_rejection?(driver))
    rescue PG::Error => e
      reject_send_error(driver, request, e)
    rescue ProtocolError
      raise
    rescue StandardError => e
      request.reject!(e)
      false
    end

    def place_sync(driver)
      driver.caps.place_sync(driver.conn)
    rescue PG::Error => e
      raise ConnectionLostError, "dispatch Sync failed: #{e.class}: #{e.message}"
    end

    def reject_send_error(driver, request, error, recoverable: false)
      driver.dispatching = nil
      request.reject!(NotDispatchedError.new("query was rejected before libpq accepted it: #{error.class}: #{error.message}"))
      return false if recoverable

      raise ConnectionLostError, "dispatch failed: #{error.class}: #{error.message}"
    end

    def send_command(conn, request)
      operation = request.operation
      params = request.params
      case operation
      when :query
        conn.send_query_params(request.sql, params)
      when :prepare
        send_prepare(conn, request)
      when :prepared_query
        conn.send_query_prepared(request.statement_name, params)
      else
        raise ProtocolError, "unsupported request operation #{operation.inspect}"
      end
    end

    def send_prepare(conn, request)
      param_types = request.param_types
      statement_name = request.statement_name
      sql = request.sql
      if param_types.nil?
        conn.send_prepare(statement_name, sql)
      else
        conn.send_prepare(statement_name, sql, param_types)
      end
    end

    def reusable_after_send_rejection?(driver)
      conn = driver.conn
      !conn.finished? && conn.status == PG::CONNECTION_OK && conn.pipeline_status != PG::PQ_PIPELINE_OFF
    rescue PG::Error
      false
    end

    def flush_output(driver)
      return unless driver.running

      driver.flush_calls += 1
      if driver.conn.sync_flush
        driver.needs_flush = false
      else
        driver.needs_flush = true
        driver.flush_incomplete += 1
        arm_writer(driver)
      end
    rescue PG::Error => e
      raise ConnectionLostError, "flush failed: #{e.class}: #{e.message}"
    end

    def arm_writer(driver)
      return if driver.writer_armed
      return unless driver.running

      driver.writer_armed = true
      driver.writer_commands.enqueue(:wait_writable)
    end

    def read_available(driver)
      driver.conn.consume_input
    rescue PG::Error => e
      raise ConnectionLostError, "read failed: #{e.class}: #{e.message}"
    end

    def drain_results(driver)
      read = 0
      conn = driver.conn
      inflight = driver.inflight

      begin
        while !inflight.empty? && !conn.is_busy
          result = conn.sync_get_result
          read += 1
          request = inflight.first
          raise ProtocolError, 'result without an in-flight request' unless request

          handle_pipeline_result(driver, request, result)
        end
      ensure
        driver.results_read += read if read.positive?
      end
    end

    def handle_pipeline_result(driver, request, result)
      if result.nil?
        request.query_boundary!
        return
      end

      handle_pipeline_status(driver, request, result, result.result_status)
    end

    def handle_pipeline_status(driver, request, result, status)
      case status
      when PG::PGRES_TUPLES_OK
        accept_tuples_ok(driver, request, result)
      when PG::PGRES_PIPELINE_SYNC
        complete_pipeline_sync(driver, request, result)
      when PG::PGRES_COMMAND_OK, PG::PGRES_EMPTY_QUERY
        accept_command_result(request, result, status)
      when PG::PGRES_FATAL_ERROR
        record_fatal_result(request, result, status)
      when PG::PGRES_PIPELINE_ABORTED
        record_aborted_result(request, result, status)
      when PG::PGRES_BAD_RESPONSE
        reject_pipeline_status!(result, 'server response was not understood')
      when PG::PGRES_COPY_IN, PG::PGRES_COPY_OUT, PG::PGRES_COPY_BOTH
        reject_pipeline_status!(result, 'COPY is not supported on the multiplexed pipeline')
      else
        reject_pipeline_status!(result, "unexpected pipeline result status #{status}")
      end
    end

    def complete_pipeline_sync(driver, request, result)
      clear_result(result)
      complete_front(driver, request)
    end

    def accept_command_result(request, result, status)
      ensure_before_query_boundary!(request, status)
      request.accept_result(result)
    end

    def record_fatal_result(request, result, status)
      ensure_before_query_boundary!(request, status)
      request.record_error!(query_error(result), result: result)
    end

    def record_aborted_result(request, result, status)
      ensure_before_query_boundary!(request, status)
      clear_result(result)
      request.record_error!(PipelineAbortedError.new('pipeline unit aborted'))
    end

    def accept_tuples_ok(driver, request, result)
      ensure_before_query_boundary!(request, PG::PGRES_TUPLES_OK)
      begin
        apply_type_map(driver, request, result)
        request.accept_result(result)
      rescue StandardError => e
        detail = e.message
        message = e.is_a?(Error) ? detail : "typed result mapping failed: #{e.class}: #{detail}"
        request.record_error!(QueryError.new(message, cause_result: result), result: result)
      end
    end

    def reject_pipeline_status!(result, message)
      clear_result(result)
      raise ProtocolError, message
    end

    def ensure_before_query_boundary!(request, status)
      return unless request.query_boundary_seen?

      raise ProtocolError, "result status #{status} arrived after query boundary"
    end

    def complete_front(driver, request)
      inflight = driver.inflight
      raise ProtocolError, 'sync does not match FIFO front' unless request.equal?(inflight.first)

      inflight.shift
      driver.units_completed += 1
      request.finish!
    end

    def query_error(result)
      message = result.error_message.to_s.strip
      message = 'query failed' if message.empty?
      QueryError.new(message, cause_result: result)
    end

    def drained?(driver)
      driver.submitting.zero? && driver.requests.empty? && driver.inflight.empty? && !driver.needs_flush && driver.dispatching.nil?
    end

    def finish_graceful_close(driver)
      driver.accepting = false
      driver.running = false
      driver.conn.exit_pipeline_mode
    rescue PG::Error => e
      fail_all(driver, ConnectionLostError.new("failed to exit pipeline mode: #{e.message}"))
    ensure
      teardown_watchers(driver)
    end

    def fatal_close(driver, error)
      return unless driver.running || driver.accepting

      driver.accepting = false
      driver.running = false
      driver.requests.close(not_dispatched_error(error))
      fail_all(driver, error)

      teardown_watchers(driver)
    end

    def fail_all(driver, error)
      dispatching = driver.dispatching
      inflight = driver.inflight
      uncertain = []
      uncertain << dispatching if dispatching
      uncertain.concat(inflight)
      queued = driver.requests.drain

      reject_requests(uncertain.compact.uniq) { indeterminate_error(error) }
      reject_requests(queued) { not_dispatched_error(error) }
      driver.dispatching = nil
      inflight.clear
    end

    def reject_requests(requests)
      requests.each { |request| request.reject!(yield) unless request.settled? }
    end

    def not_dispatched_error(error)
      return error if error.is_a?(NotDispatchedError)

      NotDispatchedError.new("#{error.message}; request was not dispatched")
    end

    def indeterminate_error(error)
      return error if error.is_a?(IndeterminateResultError)

      IndeterminateResultError.new(
        "#{error.message}; request was dispatched but its Sync was not observed, " \
        'so execution/commit outcome is indeterminate'
      )
    end

    def watcher_wait_timeout
      scheduler = Fiber.scheduler
      return nil if scheduler&.respond_to?(:fiber_interrupt)

      WATCHER_POLL_INTERVAL
    end

    def wait_socket_readable(driver, timeout)
      loop do
        return false unless driver.running
        return true if driver.socket.wait_readable(timeout)
      end
    end

    def wait_socket_writable(driver, timeout)
      loop do
        return false unless driver.running
        return true if driver.socket.wait_writable(timeout)
      end
    end

    def reader_watcher(driver)
      timeout = watcher_wait_timeout

      while driver.running
        break unless wait_socket_readable(driver, timeout)

        driver.events.enqueue(:readable)
        command = driver.reader_rearm.dequeue
        break unless command == :rearm && driver.running
      end
    rescue StandardError => e
      if driver.running
        driver.events.enqueue([:abort, ConnectionLostError.new("reader watcher failed: #{e.class}: #{e.message}")])
      end
    end

    def writer_watcher(driver)
      timeout = watcher_wait_timeout

      while driver.running
        command = driver.writer_commands.dequeue
        break unless command == :wait_writable && driver.running
        break unless wait_socket_writable(driver, timeout)

        driver.events.enqueue(:writable) if driver.running
      end
    rescue StandardError => e
      if driver.running
        driver.events.enqueue([:abort, ConnectionLostError.new("writer watcher failed: #{e.class}: #{e.message}")])
      end
    end

    def teardown_watchers(driver)
      tasks = release_watchers(driver)
      close_wait_points(driver)
      begin
        driver.socket&.close
      rescue StandardError
        nil
      end
      stop_watchers(tasks)
      join_watchers(driver, tasks)
      driver.socket = nil
      safe_close_conn(driver)
      nil
    end

    def release_watchers(driver)
      tasks = [driver.reader_task, driver.writer_task].compact
      driver.reader_task = nil
      driver.writer_task = nil
      tasks
    end

    def close_wait_points(driver)
      [driver.reader_rearm, driver.writer_commands, driver.events].each do |queue|
        queue&.close
      rescue StandardError
        nil
      end
    end

    def stop_watchers(tasks)
      Array(tasks).each do |task|
        task.stop
      rescue Runtime::Cancel, StandardError
        nil
      end

      nil
    end

    def join_watchers(driver, tasks)
      Array(tasks).each do |task|
        task.wait(WATCHER_JOIN_TIMEOUT)
      rescue Runtime::TimeoutError
        driver.leaked_watchers += 1
        warn_leaked_task(driver, task, WATCHER_JOIN_TIMEOUT)
      rescue Runtime::Cancel, StandardError
        nil
      end

      nil
    end

    def join_owner(driver)
      owner = driver.owner_task
      return if owner.nil?
      return if Fiber.current.equal?(owner.fiber)

      begin
        owner.wait(OWNER_JOIN_TIMEOUT)
      rescue Runtime::TimeoutError
        driver.leaked_watchers += 1
        warn_leaked_task(driver, owner, OWNER_JOIN_TIMEOUT)
      rescue Runtime::Cancel, StandardError
        nil
      end

      nil
    end

    def warn_leaked_task(driver, task, timeout)
      return if ENV['PG_PIPELINE_SILENCE_WARNINGS']

      warn(
        "pg_pipeline: task #{task.name.inspect} did not exit within " \
        "#{timeout}s and has been leaked (total #{driver.leaked_watchers}). " \
        'Closing the socket and, on schedulers without #fiber_interrupt, the ' \
        "#{WATCHER_POLL_INTERVAL}s watcher poll did not release the fiber in time."
      )
    end

    def safe_close_conn(driver)
      driver.conn.close unless driver.conn.finished?
    rescue StandardError
      nil
    end

    def apply_type_map(driver, request, result)
      return unless request.type_map

      maps = driver.type_maps
      raise Error, 'typed statement has no type-map registry' unless maps

      maps.apply!(request, result, driver.conn)
    end

    def clear_result(result)
      result.clear if result.respond_to?(:clear)
    end
  end
end
