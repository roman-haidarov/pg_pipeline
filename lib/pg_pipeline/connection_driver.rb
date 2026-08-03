# frozen_string_literal: true

require "pg"
require "async"
require "async/queue"

require_relative "errors"
require_relative "bounded_queue"
require_relative "server_caps"
require_relative "request"

module PgPipeline
  class ConnectionDriver
    DEFAULT_MAX_PENDING = 256
    DEFAULT_MAX_IN_FLIGHT = 64

    attr_reader :conn, :caps, :max_pending, :max_in_flight
    attr_accessor :socket, :requests, :events, :reader_rearm, :writer_commands,
                  :inflight, :dispatching, :submitting, :accepting, :running,
                  :draining, :needs_flush, :writer_armed, :request_event_pending,
                  :owner_task, :reader_task, :writer_task

    attr_writer :readable_events, :results_read, :units_completed, :flush_calls,
                :flush_incomplete, :dispatches

    def readable_events = @readable_events || 0
    def results_read = @results_read || 0
    def units_completed = @units_completed || 0
    def flush_calls = @flush_calls || 0
    def flush_incomplete = @flush_incomplete || 0
    def dispatches = @dispatches || 0

    def initialize(conn, max_pending: DEFAULT_MAX_PENDING, max_in_flight: DEFAULT_MAX_IN_FLIGHT)
      @max_pending = DriverOps.positive_integer!(max_pending, :max_pending)
      @max_in_flight = DriverOps.positive_integer!(max_in_flight, :max_in_flight)

      @conn = conn
      @caps = ServerCaps.from_connection(conn)
      @caps.assert_supported!
      DriverOps.warn_flush_coupling_once(@caps)

      @requests = BoundedQueue.new(@max_pending)
      @events = Async::Queue.new
      @reader_rearm = Async::Queue.new
      @writer_commands = Async::Queue.new

      @inflight = []
      @dispatching = nil
      @submitting = 0

      @accepting = false
      @running = false
      @draining = false
      @needs_flush = false
      @writer_armed = false
      @request_event_pending = false

      @readable_events = 0
      @results_read = 0
      @units_completed = 0
      @flush_calls = 0
      @flush_incomplete = 0
      @dispatches = 0

      @socket = nil
      @owner_task = nil
      @reader_task = nil
      @writer_task = nil
    end

    def start(parent: Async::Task.current) = DriverOps.start(self, parent)
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
        fast_sync: @caps.fast_sync?,
        readable_events: readable_events,
        results_read: results_read,
        units_completed: units_completed,
        flush_calls: flush_calls,
        flush_incomplete: flush_incomplete,
        dispatches: dispatches,
        units_per_readable: DriverOps.ratio(units_completed, readable_events),
        results_per_readable: DriverOps.ratio(results_read, readable_events),
        flush_calls_per_unit: DriverOps.ratio(flush_calls, units_completed)
      }
    end

    def health_check(timeout)
      return true unless available?

      probe = Request.build("SELECT 1", nil)
      begin
        submit(probe)
        Async::Task.current.with_timeout(timeout) { probe.wait }
        true
      rescue Async::TimeoutError
        DriverOps.abort_timed_out_health_probe(self, probe)
      rescue QueryError, PipelineAbortedError
        true
      rescue ShutdownError, NotDispatchedError
        true
      rescue ConnectionLostError, PG::Error
        false
      ensure
        probe.cancel! unless probe.settled?
      end
    end

    def graceful_close = DriverOps.graceful_close(self)

    def abort!(error = ConnectionLostError.new("connection aborted"))
      DriverOps.abort!(self, error)
    end
  end

  module DriverOps
    module_function

    def ratio(numerator, denominator)
      return 0.0 if denominator.zero?

      (numerator.to_f / denominator).round(3)
    end

    def warn_flush_coupling_once(caps)
      return if @flush_coupling_warned
      return if caps.fast_sync?
      return if ENV["PG_PIPELINE_SILENCE_WARNINGS"]

      @flush_coupling_warned = true
      warn(
        "pg_pipeline: libpq #{caps.libpq_version} couples pipeline Sync with flush " \
        "(no PQsendPipelineSync). Queries still pipeline and still amortise RTT, but " \
        "one flush per unit caps local throughput; libpq >= 17 is recommended for " \
        "maximum throughput. Set PG_PIPELINE_SILENCE_WARNINGS=1 to silence this."
      )
    end

    def positive_integer!(value, name)
      integer = Integer(value)
      raise ArgumentError, "#{name} must be >= 1" if integer < 1

      integer
    rescue ArgumentError, TypeError
      raise ArgumentError, "#{name} must be an integer >= 1"
    end

    def start(d, parent)
      raise Error, "driver already started" if d.running

      d.conn.setnonblocking(true)
      d.conn.enter_pipeline_mode

      d.socket = d.conn.socket_io
      d.accepting = true
      d.running = true

      d.reader_task = parent.async { reader_watcher(d) }
      d.writer_task = parent.async { writer_watcher(d) }
      d.owner_task = parent.async { owner_loop(d) }
      d
    rescue Exception
      d.accepting = false
      d.running = false
      stop_watchers(d)
      safe_close_conn(d)
      raise
    end

    def submit(d, request)
      raise ShutdownError, "driver is not accepting work" unless d.accepting
      raise ProtocolError, "request must be new before submit" unless request.state == :new

      d.submitting += 1
      begin
        d.requests.enqueue(request)
        request.queued!
        notify_requests(d)
      ensure
        d.submitting -= 1
        d.events.enqueue(:submission_finished) if d.draining && d.running && d.submitting.zero?
      end

      request
    end

    def graceful_close(d)
      return unless d.running

      d.accepting = false
      d.requests.close(ShutdownError.new("driver is closing"))
      d.events.enqueue(:close)
      d.owner_task.wait
      nil
    end

    def abort!(d, error)
      return unless d.running

      d.accepting = false
      d.requests.close(not_dispatched_error(error))
      d.events.enqueue([:abort, error])
      d.owner_task.wait unless Async::Task.current.equal?(d.owner_task)
      nil
    end

    def abort_timed_out_health_probe(d, probe)
      return true if probe.settled? || !d.running
      return true unless exclusive_health_probe?(d, probe)

      abort!(d, ConnectionLostError.new("idle health check timed out"))
      false
    end

    def exclusive_health_probe?(d, probe)
      d.accepting &&
        d.dispatching.nil? &&
        d.submitting.zero? &&
        d.requests.empty? &&
        d.inflight.length == 1 &&
        d.inflight.first.equal?(probe)
    end

    def owner_loop(d)
      process_event(d, d.events.dequeue) while d.running
    rescue StandardError => e
      fatal_close(d, ConnectionLostError.new("driver crashed: #{e.class}: #{e.message}"))
    ensure
      fatal_close(d, ShutdownError.new("driver owner stopped before shutdown completed")) if d.running
    end

    def process_event(d, event)
      input_changed = false

      case event
      when :requests
        d.request_event_pending = false
      when :submission_finished
        nil
      when :readable
        begin
          d.readable_events += 1
          read_available(d)
          input_changed = true
        ensure
          d.reader_rearm.enqueue(:rearm) if d.running
        end
      when :writable
        d.writer_armed = false
        flush_output(d)
      when :close
        d.draining = true
      when Array
        tag, payload = event
        if tag == :abort
          fatal_close(d, payload)
          return
        end
      end

      drain_results(d) if input_changed
      pump_requests(d) if d.running
      finish_graceful_close(d) if d.running && d.draining && drained?(d)
    end

    def notify_requests(d)
      return if d.request_event_pending

      d.request_event_pending = true
      d.events.enqueue(:requests)
    end

    def pump_requests(d)
      dispatched = false

      while d.inflight.size < d.max_in_flight && !d.requests.empty?
        request = d.requests.dequeue
        break unless request
        next if request.cancelled?

        d.dispatching = request
        unless send_unit(d, request)
          d.dispatching = nil
          next
        end

        request.dispatched!
        d.inflight << request
        d.dispatching = nil
        d.dispatches += 1
        dispatched = true
      end

      flush_output(d) if dispatched && !d.needs_flush
    end

    def send_unit(d, request)
      begin
        send_command(d.conn, request)
      rescue PG::UnableToSend => e
        d.dispatching = nil
        request.reject!(
          NotDispatchedError.new("query was rejected before libpq accepted it: #{e.class}: #{e.message}")
        )
        return false if reusable_after_send_rejection?(d)

        raise ConnectionLostError, "dispatch failed: #{e.class}: #{e.message}"
      rescue PG::Error => e
        d.dispatching = nil
        request.reject!(
          NotDispatchedError.new("query was rejected before libpq accepted it: #{e.class}: #{e.message}")
        )
        raise ConnectionLostError, "dispatch failed: #{e.class}: #{e.message}"
      rescue ProtocolError
        raise
      rescue StandardError => e
        request.reject!(e)
        return false
      end

      d.caps.place_sync(d.conn)
      true
    rescue PG::Error => e
      raise ConnectionLostError, "dispatch Sync failed: #{e.class}: #{e.message}"
    end

    def send_command(conn, request)
      case request.operation
      when :query
        conn.send_query_params(request.sql, request.params)
      when :prepare
        if request.param_types.nil?
          conn.send_prepare(request.statement_name, request.sql)
        else
          conn.send_prepare(request.statement_name, request.sql, request.param_types)
        end
      when :prepared_query
        conn.send_query_prepared(request.statement_name, request.params)
      else
        raise ProtocolError, "unsupported request operation #{request.operation.inspect}"
      end
    end

    def reusable_after_send_rejection?(d)
      !d.conn.finished? &&
        d.conn.status == PG::CONNECTION_OK &&
        d.conn.pipeline_status != PG::PQ_PIPELINE_OFF
    rescue PG::Error
      false
    end

    def flush_output(d)
      return unless d.running

      d.flush_calls += 1

      if d.conn.sync_flush
        d.needs_flush = false
      else
        d.needs_flush = true
        d.flush_incomplete += 1
        arm_writer(d)
      end
    rescue PG::Error => e
      raise ConnectionLostError, "flush failed: #{e.class}: #{e.message}"
    end

    def arm_writer(d)
      return if d.writer_armed

      d.writer_armed = true
      d.writer_commands.enqueue(:wait_writable)
    end

    def read_available(d)
      d.conn.consume_input
    rescue PG::Error => e
      raise ConnectionLostError, "read failed: #{e.class}: #{e.message}"
    end

    def drain_results(d)
      read = 0

      begin
        while !d.inflight.empty? && !d.conn.is_busy
          result = d.conn.sync_get_result
          read += 1
          request = d.inflight.first
          raise ProtocolError, "result without an in-flight request" unless request

          if result.nil?
            request.query_boundary!
            next
          end

          status = result.result_status

          case status
          when PG::PGRES_TUPLES_OK
            ensure_before_query_boundary!(request, status)
            request.accept_result(result)
          when PG::PGRES_PIPELINE_SYNC
            clear_result(result)
            complete_front(d, request)
          when PG::PGRES_COMMAND_OK, PG::PGRES_EMPTY_QUERY
            ensure_before_query_boundary!(request, status)
            request.accept_result(result)
          when PG::PGRES_FATAL_ERROR
            ensure_before_query_boundary!(request, status)
            request.record_error!(query_error(result), result: result)
          when PG::PGRES_PIPELINE_ABORTED
            ensure_before_query_boundary!(request, status)
            clear_result(result)
            request.record_error!(PipelineAbortedError.new("pipeline unit aborted"))
          when PG::PGRES_BAD_RESPONSE
            clear_result(result)
            raise ProtocolError, "server response was not understood"
          when PG::PGRES_COPY_IN, PG::PGRES_COPY_OUT, PG::PGRES_COPY_BOTH
            clear_result(result)
            raise ProtocolError, "COPY is not supported on the multiplexed pipeline"
          else
            clear_result(result)
            raise ProtocolError, "unexpected pipeline result status #{status}"
          end
        end
      ensure
        d.results_read += read if read.positive?
      end
    end

    def ensure_before_query_boundary!(request, status)
      return unless request.query_boundary_seen?

      raise ProtocolError, "result status #{status} arrived after query boundary"
    end

    def complete_front(d, request)
      raise ProtocolError, "sync does not match FIFO front" unless request.equal?(d.inflight.first)

      d.inflight.shift
      d.units_completed += 1
      request.finish!
    end

    def query_error(result)
      message = result.error_message.to_s.strip
      message = "query failed" if message.empty?
      QueryError.new(message, cause_result: result)
    end

    def drained?(d)
      d.submitting.zero? && d.requests.empty? && d.inflight.empty? && !d.needs_flush && d.dispatching.nil?
    end

    def finish_graceful_close(d)
      d.accepting = false
      d.running = false

      begin
        d.conn.exit_pipeline_mode
      rescue PG::Error => e
        fail_all(d, ConnectionLostError.new("failed to exit pipeline mode: #{e.message}"))
      ensure
        stop_watchers(d)
        safe_close_conn(d)
      end
    end

    def fatal_close(d, error)
      return unless d.running || d.accepting

      d.accepting = false
      d.running = false
      d.requests.close(not_dispatched_error(error))
      fail_all(d, error)
      stop_watchers(d)
      safe_close_conn(d)
    end

    def fail_all(d, error)
      uncertain = []
      uncertain << d.dispatching if d.dispatching
      uncertain.concat(d.inflight)
      queued = d.requests.drain

      uncertain.compact.uniq.each do |request|
        request.reject!(indeterminate_error(error)) unless request.settled?
      end

      queued.each do |request|
        request.reject!(not_dispatched_error(error)) unless request.settled?
      end

      d.dispatching = nil
      d.inflight.clear
    end

    def not_dispatched_error(error)
      return error if error.is_a?(NotDispatchedError)

      NotDispatchedError.new("#{error.message}; request was not dispatched")
    end

    def indeterminate_error(error)
      return error if error.is_a?(IndeterminateResultError)

      IndeterminateResultError.new(
        "#{error.message}; request was dispatched but its Sync was not observed, " \
        "so execution/commit outcome is indeterminate"
      )
    end

    def reader_watcher(d)
      while d.running
        d.socket.wait_readable
        break unless d.running

        d.events.enqueue(:readable)
        command = d.reader_rearm.dequeue
        break unless command == :rearm && d.running
      end
    rescue StandardError => e
      if d.running
        d.events.enqueue([:abort, ConnectionLostError.new("reader watcher failed: #{e.class}: #{e.message}")])
      end
    end

    def writer_watcher(d)
      while d.running
        command = d.writer_commands.dequeue
        break unless command == :wait_writable && d.running

        d.socket.wait_writable
        d.events.enqueue(:writable) if d.running
      end
    rescue StandardError => e
      if d.running
        d.events.enqueue([:abort, ConnectionLostError.new("writer watcher failed: #{e.class}: #{e.message}")])
      end
    end

    def stop_watchers(d)
      reader = d.reader_task
      writer = d.writer_task
      d.reader_task = nil
      d.writer_task = nil

      [reader, writer].each do |task|
        task&.stop
      rescue Async::Cancel, StandardError
        nil
      end

      nil
    end

    def safe_close_conn(d)
      d.conn.close unless d.conn.finished?
    rescue StandardError
      nil
    end

    def clear_result(result)
      result.clear if result.respond_to?(:clear)
    end
  end
end
