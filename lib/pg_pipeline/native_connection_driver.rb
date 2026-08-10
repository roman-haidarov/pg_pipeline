# frozen_string_literal: true

require_relative "errors"
require_relative "runtime"
require_relative "bounded_queue"
require_relative "native"
require_relative "server_caps"
require_relative "request"

module PgPipeline
  class NativeConnectionDriver
    DEFAULT_MAX_PENDING = 256
    DEFAULT_MAX_IN_FLIGHT = 64
    FLUSH_THRESHOLD = 16

    attr_reader :max_pending, :max_in_flight, :core, :caps
    attr_accessor :socket, :requests, :events, :reader_rearm, :writer_commands,
                  :dispatching, :submitting, :pending, :accepting, :running, :draining,
                  :reader_draining, :needs_flush, :flush_pending, :flush_event_pending,
                  :unflushed, :writer_armed, :request_event_pending,
                  :owner_task, :reader_task, :writer_task

    attr_writer :leaked_watchers

    def core_counters = @core.stats
    def readable_events = @core.counter(:readable_events)
    def results_read = @core.counter(:results_read)
    def units_completed = @core.counter(:units_completed)
    def flush_calls = @core.counter(:flush_calls)
    def flush_incomplete = @core.counter(:flush_incomplete)
    def dispatches = @core.counter(:dispatches)
    def bytes_dispatched = @core.counter(:bytes_dispatched)
    def leaked_watchers = @leaked_watchers || 0

    def encoding_name
      @core.closed? ? nil : @core.encoding.name
    rescue Error
      nil
    end

    def initialize(connection_args,
                   max_pending: DEFAULT_MAX_PENDING,
                   max_in_flight: DEFAULT_MAX_IN_FLIGHT)
      @max_pending = NativeDriverOps.positive_integer!(max_pending, :max_pending)
      @max_in_flight = NativeDriverOps.positive_integer!(max_in_flight, :max_in_flight)

      @core = Native::Driver.new(connection_args, @max_in_flight)
      @caps = nil
      @requests = BoundedQueue.new(@max_pending)
      @events = Runtime::Queue.new
      @reader_rearm = Runtime::Queue.new
      @writer_commands = Runtime::Queue.new

      @dispatching = nil
      @submitting = 0
      @pending = 0
      @accepting = false
      @running = false
      @draining = false
      @reader_draining = false
      @needs_flush = false
      @flush_pending = false
      @flush_event_pending = false
      @unflushed = 0
      @writer_armed = false
      @request_event_pending = false
      @socket = nil
      @owner_task = nil
      @reader_task = nil
      @writer_task = nil
      @leaked_watchers = 0
    end

    def start = NativeDriverOps.start(self)
    def submit(request) = NativeDriverOps.submit(self, request)
    def available? = @accepting && @running
    def dead? = !@running && !@accepting
    def load
      extra = @pending + @submitting + (@dispatching ? 1 : 0)
      @core.inflight_plus(extra)
    end

    def stats
      counters = core_counters
      readable = counters.fetch(:readable_events)
      units = counters.fetch(:units_completed)

      {
        available: available?,
        load: load,
        pending: @requests.size,
        in_flight: counters.fetch(:in_flight),
        in_flight_peak: counters.fetch(:in_flight_peak),
        submitting: @submitting,
        needs_flush: @needs_flush,
        fast_sync: @caps&.fast_sync? || false,
        encoding: encoding_name,
        readable_events: readable,
        results_read: counters.fetch(:results_read),
        units_completed: units,
        flush_calls: counters.fetch(:flush_calls),
        flush_incomplete: counters.fetch(:flush_incomplete),
        dispatches: counters.fetch(:dispatches),
        bytes_dispatched: counters.fetch(:bytes_dispatched),
        units_per_readable: NativeDriverOps.ratio(units, readable),
        results_per_readable: NativeDriverOps.ratio(counters.fetch(:results_read), readable),
        flush_calls_per_unit: NativeDriverOps.ratio(counters.fetch(:flush_calls), units),
        leaked_watchers: leaked_watchers
      }
    end

    def health_check(timeout)
      return true unless available?

      probe = Request.build("SELECT 1", nil)
      submit(probe)
      Runtime.with_timeout(timeout) { probe.wait }
      true
    rescue Runtime::TimeoutError
      NativeDriverOps.abort_timed_out_health_probe(self, probe)
    rescue QueryError, PipelineAbortedError
      true
    rescue ShutdownError, NotDispatchedError
      true
    rescue ConnectionLostError
      false
    ensure
      probe.cancel! if probe && !probe.settled?
    end

    def graceful_close = NativeDriverOps.graceful_close(self)

    def abort!(error = ConnectionLostError.new("connection aborted"))
      NativeDriverOps.abort!(self, error)
    end
  end

  module NativeDriverOps
    WATCHER_JOIN_TIMEOUT = 2.0
    OWNER_JOIN_TIMEOUT = 5.0
    WATCHER_POLL_INTERVAL = 0.25

    module_function

    def ratio(numerator, denominator)
      return 0.0 if denominator.zero?

      (numerator.to_f / denominator).round(3)
    end

    def positive_integer!(value, name)
      integer = Integer(value)
      raise ArgumentError, "#{name} must be >= 1" if integer < 1

      integer
    rescue ArgumentError, TypeError
      raise ArgumentError, "#{name} must be an integer >= 1"
    end

    def warn_encoding_mismatch_once(d)
      return unless Native.seal_encoding_published?

      sealed = Native.seal_encoding
      negotiated = d.core.encoding
      return if negotiated == sealed
      return if @encoding_mismatch_warned
      return if ENV["PG_PIPELINE_SILENCE_WARNINGS"]

      @encoding_mismatch_warned = true
      warn(
        "pg_pipeline: this connection negotiated client_encoding #{negotiated} but request " \
        "payloads in this process are sealed for #{sealed} (fixed by the first connection). " \
        "All-ASCII queries are unaffected; any query carrying a non-ASCII byte will fail with " \
        "UnsupportedServerError. Use one client_encoding per process, or a separate process " \
        "per encoding. Set PG_PIPELINE_SILENCE_WARNINGS=1 to silence this."
      )
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

    def start(d)
      raise Error, "driver already started" if d.running
      raise Error, "driver start requires an active Fiber scheduler" unless Fiber.scheduler

      connect(d)
      d.core.enter_pipeline_mode
      d.accepting = true
      d.running = true

      d.reader_task = Runtime.spawn(name: :reader) { reader_watcher(d) }
      d.writer_task = Runtime.spawn(name: :writer) { writer_watcher(d) }
      d.owner_task = Runtime.spawn(name: :owner) { owner_loop(d) }
      d
    rescue Exception
      d.accepting = false
      d.running = false
      teardown_watchers(d)
      join_owner(d)
      raise
    end

    def connect(d)
      loop do
        status = d.core.connect_poll
        case status
        when :ok
          break
        when :reading
          socket_for(d).wait_readable
        when :writing
          socket_for(d).wait_writable
        when :active
          Fiber.scheduler&.yield
        when :failed
          raise ConnectionLostError, "native connection failed: #{d.core.error_message.to_s.strip}"
        else
          raise ProtocolError, "unknown native connect status #{status.inspect}"
        end
      end

      if d.core.protocol_version != ServerCaps::PROTOCOL_VERSION
        raise UnsupportedServerError,
              "pg_pipeline requires PostgreSQL protocol v3; protocol=#{d.core.protocol_version}"
      end

      libpq = Native.libpq_version
      d.instance_variable_set(
        :@caps, ServerCaps.new(
          libpq_version: libpq,
          protocol_version: d.core.protocol_version,
          pipeline_api: true,
          fast_sync_api: libpq >= ServerCaps::FAST_SYNC_LIBPQ_VERSION,
          raw_pipeline_sync_api: true
        )
      )
      d.caps.assert_supported!
      NativeDriverOps.warn_flush_coupling_once(d.caps)
      NativeDriverOps.warn_encoding_mismatch_once(d)
      Native.assert_libpq_compatible!
      d.socket = socket_for(d)
    end

    def socket_for(d)
      descriptor = d.core.socket
      socket = d.socket
      return socket if socket && socket.fileno == descriptor

      IO.for_fd(descriptor, autoclose: false)
    end

    def submit(d, request)
      raise ShutdownError, "driver is not accepting work" unless d.accepting
      raise ProtocolError, "request must be new before submit" unless request.state == :new

      if inline_dispatchable?(d)
        request.queued!
        d.dispatching = request
        begin
          ok = dispatch_unit(d, request)
          d.dispatching = nil
          maybe_flush_after_dispatch(d, false) if ok
        rescue ConnectionLostError => e
          fatal_close(d, e) if d.running || d.accepting
          raise_inline_submit_error!(request, e)
        rescue ProtocolError => e
          wrapped = ConnectionLostError.new("driver crashed: #{e.class}: #{e.message}")
          fatal_close(d, wrapped) if d.running || d.accepting
          raise_inline_submit_error!(request, wrapped)
        end
        return request
      end

      d.submitting += 1
      begin
        d.requests.enqueue(request)
        d.pending += 1
        request.queued!
        notify_requests(d)
      ensure
        d.submitting -= 1
        d.events.enqueue(:submission_finished) if d.draining && d.running && d.submitting.zero?
      end

      request
    end

    def inline_dispatchable?(d)
      d.running && !d.draining && !d.reader_draining &&
        d.submitting.zero? && d.dispatching.nil? &&
        !d.needs_flush && d.requests.empty? &&
        d.core.inflight_count < d.max_in_flight
    end

    # Explicit owner wake for an outstanding flush. Not used on the dispatch hot
    # path (see maybe_flush_after_dispatch); kept as the primitive for callers
    # that have no in-flight unit to ride on.
    def notify_flush(d)
      return if d.flush_event_pending || !d.running

      d.flush_event_pending = true
      d.flush_event_pending = false if d.events.enqueue(:flush).nil?
    end

    def raise_inline_submit_error!(request, error)
      request.reject!(indeterminate_error(error)) unless request.settled?
      settled = request.error
      raise settled if settled.is_a?(Exception)

      raise error
    end

    def graceful_close(d)
      return unless d.running

      d.accepting = false
      d.requests.close(ShutdownError.new("driver is closing"))
      d.events.enqueue(:close)
      join_owner(d)
      nil
    end

    def abort!(d, error)
      return unless d.running

      d.accepting = false
      d.requests.close(not_dispatched_error(error))
      d.events.enqueue([:abort, error])
      join_owner(d)
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
        d.core.inflight_count == 1 &&
        d.core.front_request.equal?(probe)
    end

    def owner_loop(d)
      while d.running
        flush_now(d) if d.flush_pending && d.events.empty?
        event = d.events.dequeue
        break if event.nil?

        process_event(d, event)
      end
    rescue StandardError => e
      fatal_close(d, ConnectionLostError.new("driver crashed: #{e.class}: #{e.message}"))
    ensure
      fatal_close(d, ShutdownError.new("driver owner stopped before shutdown completed")) if d.running
    end

    def process_event(d, event)
      case event
      when :requests
        d.request_event_pending = false
      when :flush
        d.flush_event_pending = false
      when :submission_finished, :drained
        nil
      when :writable
        d.writer_armed = false
        flush_now(d)
      when :close
        d.draining = true
      when Array
        tag, payload = event
        if tag == :abort
          fatal_close(d, payload)
          return
        end
      end

      pump_requests(d) if d.running
      finish_graceful_close(d) if d.running && d.draining && drained?(d)
    end

    def notify_requests(d)
      return if d.request_event_pending
      return unless d.running

      d.request_event_pending = true
      d.events.enqueue(:requests)
    end

    def pump_requests(d)
      while d.core.inflight_count < d.max_in_flight && !d.requests.empty?
        request = d.requests.dequeue
        break unless request

        d.pending -= 1 if d.pending.positive?
        next if request.cancelled?

        d.dispatching = request
        unless dispatch_unit(d, request)
          d.dispatching = nil
          next
        end

        d.dispatching = nil
        maybe_flush_after_dispatch(d, !d.requests.empty?)
      end

      flush_now(d) if d.flush_pending && d.events.empty?
    end

    def maybe_flush_after_dispatch(d, more_queued)
      d.unflushed += 1
      d.flush_pending = true
      in_flight = d.core.inflight_count

      alone = in_flight <= 1 && !more_queued
      nothing_on_wire = !more_queued && d.unflushed >= in_flight
      full_batch = d.unflushed >= NativeConnectionDriver::FLUSH_THRESHOLD

      flush_now(d) if alone || nothing_on_wire || full_batch
    end

    def flush_now(d)
      d.flush_pending = false
      d.unflushed = 0
      flush_output(d)
    end

    def dispatch_unit(d, request)
      d.core.dispatch(request)
      true
    rescue NotDispatchedError => e
      d.dispatching = nil
      request.reject!(e)
      return false if d.core.reusable?

      raise ConnectionLostError, "dispatch failed: #{e.message}"
    rescue UnsupportedServerError => e
      d.dispatching = nil
      request.reject!(e)
      false
    rescue ConnectionLostError
      raise
    rescue ProtocolError
      raise
    rescue StandardError => e
      request.reject!(e)
      false
    end

    def flush_output(d)
      return unless d.running

      if d.core.flush
        d.needs_flush = false
      else
        d.needs_flush = true
        arm_writer(d)
      end
    rescue ConnectionLostError
      raise
    rescue StandardError => e
      raise ConnectionLostError, "flush failed: #{e.class}: #{e.message}"
    end

    def arm_writer(d)
      return if d.writer_armed
      return unless d.running

      d.writer_armed = true
      d.writer_commands.enqueue(:wait_writable)
    end

    def drained?(d)
      d.submitting.zero? &&
        d.requests.empty? &&
        d.core.inflight_count.zero? &&
        !d.needs_flush &&
        !d.flush_pending &&
        d.dispatching.nil?
    end

    def finish_graceful_close(d)
      d.accepting = false
      d.running = false

      begin
        d.core.exit_pipeline_mode
      rescue StandardError => e
        fail_all(d, ConnectionLostError.new("failed to exit pipeline mode: #{e.message}"))
      ensure
        teardown_watchers(d)
      end
    end

    def fatal_close(d, error)
      return unless d.running || d.accepting

      d.accepting = false
      d.running = false
      d.requests.close(not_dispatched_error(error))
      fail_all(d, error)
      teardown_watchers(d)
    end

    def fail_all(d, error)
      uncertain = []
      uncertain << d.dispatching if d.dispatching
      uncertain.concat(d.core.take_inflight)
      queued = d.requests.drain
      d.pending = 0
      d.unflushed = 0
      d.flush_pending = false

      uncertain.compact.uniq.each do |request|
        request.reject!(indeterminate_error(error)) unless request.settled?
      end

      queued.each do |request|
        request.reject!(not_dispatched_error(error)) unless request.settled?
      end

      d.dispatching = nil
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

    def watcher_wait_timeout
      scheduler = Fiber.scheduler
      return nil if scheduler&.respond_to?(:fiber_interrupt)

      WATCHER_POLL_INTERVAL
    end

    def wait_socket_readable(d, timeout)
      loop do
        return false unless d.running
        return true if d.socket.wait_readable(timeout)
      end
    end

    def wait_socket_writable(d, timeout)
      loop do
        return false unless d.running
        return true if d.socket.wait_writable(timeout)
      end
    end

    def reader_watcher(d)
      timeout = watcher_wait_timeout

      while d.running
        break unless wait_socket_readable(d, timeout)

        begin
          d.reader_draining = true
          d.core.consume_and_drain
        rescue StandardError => e
          if d.running
            d.events.enqueue(
              [:abort, ConnectionLostError.new("reader drain failed: #{e.class}: #{e.message}")]
            )
          end
          break
        ensure
          d.reader_draining = false
        end

        if d.running && (!d.requests.empty? || d.flush_pending || d.draining)
          d.events.enqueue(:drained)
        end
      end
    rescue StandardError => e
      if d.running
        d.events.enqueue([:abort, ConnectionLostError.new("reader watcher failed: #{e.class}: #{e.message}")])
      end
    end

    def writer_watcher(d)
      timeout = watcher_wait_timeout

      while d.running
        command = d.writer_commands.dequeue
        break unless command == :wait_writable && d.running
        break unless wait_socket_writable(d, timeout)

        d.events.enqueue(:writable) if d.running
      end
    rescue StandardError => e
      if d.running
        d.events.enqueue([:abort, ConnectionLostError.new("writer watcher failed: #{e.class}: #{e.message}")])
      end
    end

    def teardown_watchers(d)
      tasks = release_watchers(d)
      close_wait_points(d)
      begin
        d.socket&.close
      rescue StandardError
        nil
      end
      stop_watchers(tasks)
      join_watchers(d, tasks)
      d.socket = nil
      safe_close_core(d)
      nil
    end

    def release_watchers(d)
      tasks = [d.reader_task, d.writer_task].compact
      d.reader_task = nil
      d.writer_task = nil
      tasks
    end

    def close_wait_points(d)
      [d.reader_rearm, d.writer_commands, d.events].each do |queue|
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

    def join_watchers(d, tasks)
      Array(tasks).each do |task|
        task.wait(WATCHER_JOIN_TIMEOUT)
      rescue Runtime::TimeoutError
        d.leaked_watchers += 1
        warn_leaked_task(d, task, WATCHER_JOIN_TIMEOUT)
      rescue Runtime::Cancel, StandardError
        nil
      end

      nil
    end

    def join_owner(d)
      owner = d.owner_task
      return if owner.nil?
      return if Fiber.current.equal?(owner.fiber)

      begin
        owner.wait(OWNER_JOIN_TIMEOUT)
      rescue Runtime::TimeoutError
        d.leaked_watchers += 1
        warn_leaked_task(d, owner, OWNER_JOIN_TIMEOUT)
      rescue Runtime::Cancel, StandardError
        nil
      end

      nil
    end

    def warn_leaked_task(d, task, timeout)
      return if ENV["PG_PIPELINE_SILENCE_WARNINGS"]

      warn(
        "pg_pipeline: task #{task.name.inspect} did not exit within " \
        "#{timeout}s and has been leaked (total #{d.leaked_watchers}). " \
        "Closing the socket and, on schedulers without #fiber_interrupt, the " \
        "#{WATCHER_POLL_INTERVAL}s watcher poll did not release the fiber in time."
      )
    end

    def safe_close_core(d)
      d.core.close unless d.core.closed?
    rescue StandardError
      nil
    end
  end
end
