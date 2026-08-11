# frozen_string_literal: true

require_relative "errors"
require_relative "pool"
require_relative "request"
require_relative "prepared_statement"
require_relative "session_guard"
require_relative "session"
require_relative "transaction"

module PgPipeline
  class Client
    attr_reader :guard

    def initialize(connection_args = nil, guard: :default, **pool_opts)
      @owner_thread, @scheduler = nil, nil

      @guard = SessionGuard.normalize_mode!(guard)
      @pool = Pool.new(connection_args, **pool_opts)
      @started = false
    end

    def self.open(connection_args = nil, **opts, &block)
      ClientOps.open(connection_args, opts, &block)
    end

    def start = ClientOps.start(self)
    def query(sql, params = RequestOps::EMPTY_PARAMS) = ClientOps.query(self, sql, params)
    def prepare(name, sql, param_types = nil) = ClientOps.prepare(self, name, sql, param_types)
    def stats = ClientOps.stats(self)

    def session(&block)
      ClientOps.session(self, &block)
    end

    def transaction(&block)
      ClientOps.transaction(self, &block)
    end

    def close = ClientOps.close(self)
    def abort! = ClientOps.abort!(self)

    private

    attr_reader :pool, :owner_thread, :scheduler
    attr_accessor :started
    attr_writer :owner_thread, :scheduler
  end

  module ClientOps
    module_function

    def open(connection_args, opts)
      client = Client.new(connection_args, **opts).start
      yield client
    ensure
      client.close if client
    end

    def start(client)
      raise Error, "client already started" if started?(client)
      raise Error, "client start requires an active Fiber scheduler" unless Fiber.scheduler

      pool(client).start
      client.__send__(:owner_thread=, Thread.current)
      client.__send__(:scheduler=, Fiber.scheduler)
      client.__send__(:started=, true)
      client
    end

    def query(client, sql, params)
      ensure_started!(client)
      sql = RequestOps.snapshot_sql(sql)
      SessionGuard.assert_multiplexable_normalized!(sql, mode: client.guard)

      wait_for_request do
        submit_with_failover(client, Request.build(sql, params, snapped_sql: true))
      end
    end

    def prepare(client, name, sql, param_types)
      ensure_started!(client)
      sql = RequestOps.snapshot_sql(sql)
      SessionGuard.assert_multiplexable_normalized!(sql, mode: client.guard)
      pool(client).__send__(:prepare_statement, client, name, sql, param_types)
    end

    def query_prepared(client, statement, params)
      ensure_started!(client)

      wait_for_request do
        submit_with_failover(client, Request.prepared_query(statement, params: params))
      end
    end

    def wait_for_request
      request = yield
      request.wait
    ensure
      request.cancel! if request && !request.settled?
    end

    def submit_with_failover(client, request)
      attempts = 0
      limit = [pool(client).pipeline_size, 1].max
      last_error = nil

      while attempts < limit
        begin
          pool(client).__send__(:pipeline_driver).submit(request)
          return request
        rescue NotDispatchedError => e
          last_error = e
          attempts += 1
        rescue ShutdownError => e
          last_error = e
          attempts += 1
          break unless request.state == :new && !request.settled?
        rescue IndeterminateResultError
          raise
        end

        request = request.respawn if attempts < limit
      end

      error = last_error || NotDispatchedError.new("no live pipeline connections; request was not dispatched")
      raise(error.is_a?(NotDispatchedError) ? error : NotDispatchedError.new(error.message))
    end

    def session(client)
      ensure_started!(client)

      pool(client).__send__(:with_pinned) do |conn|
        session = Session.new(conn)
        begin
          yield session
        ensure
          SessionOps.close!(session)
        end
      end
    end

    def transaction(client)
      ensure_started!(client)

      pool(client).__send__(:with_pinned) do |conn|
        tx = Transaction.new(conn)
        begin
          TransactionOps.run(tx) { |transaction| yield transaction }
        ensure
          SessionOps.close!(tx)
        end
      end
    end

    def stats(client)
      ensure_started!(client)
      pool(client).stats
    end

    def close(client)
      return unless started?(client)

      ensure_context!(client)
      begin
        pool(client).graceful_close
      ensure
        client.__send__(:started=, false) if pool(client).closing?
      end
    end

    def abort!(client)
      return unless started?(client)

      ensure_context!(client)
      begin
        pool(client).abort!
      ensure
        client.__send__(:started=, false) if pool(client).closing?
      end
    end

    def pool(client)
      client.__send__(:pool)
    end

    def ensure_started!(client)
      raise Error, "client not started; call #start or use Client.open" unless started?(client)

      ensure_context!(client)
    end

    def ensure_context!(client)
      return if Thread.current.equal?(client.__send__(:owner_thread)) &&
                Fiber.scheduler.equal?(client.__send__(:scheduler))

      raise Error, "client is reactor-local and cannot be used from another thread/scheduler"
    end

    def started?(client)
      client.__send__(:started)
    end
  end
end
