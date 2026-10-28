# frozen_string_literal: true

require_relative 'errors'
require_relative 'pool'
require_relative 'request'
require_relative 'prepared_statement'
require_relative 'session_guard'
require_relative 'session'
require_relative 'transaction'

module PgPipeline
  class Client
    attr_reader :guard

    def initialize(connection_args = nil, guard: :default, **pool_opts)
      @owner_thread = nil
      @scheduler = nil

      @guard = SessionGuard.normalize_mode!(guard)
      @pool = Pool.new(connection_args, **pool_opts)
      @started = false
    end

    def self.open(connection_args = nil, **opts, &block)
      ClientOps.open(connection_args, opts, &block)
    end

    def start = ClientOps.start(self)
    def query(sql, params = RequestOps::EMPTY_PARAMS) = ClientOps.query(self, sql, params)

    def prepare(name, sql, param_types = nil, typed: false)
      ClientOps.prepare(self, name, sql, param_types, typed: typed)
    end

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

    attr_accessor :owner_thread, :scheduler, :started
    attr_reader :pool
  end

  module ClientOps
    module_function

    def open(connection_args, opts)
      client = Client.new(connection_args, **opts).start
      yield client
    ensure
      client&.close
    end

    def start(client)
      raise Error, 'client already started' if started?(client)

      scheduler = Fiber.scheduler
      raise Error, 'client start requires an active Fiber scheduler' unless scheduler

      pool(client).start
      client.__send__(:owner_thread=, Thread.current)
      client.__send__(:scheduler=, scheduler)
      client.__send__(:started=, true)
      client
    end

    def query(client, sql, params)
      ensure_started!(client)
      sql = RequestOps.snapshot_sql(sql)
      SessionGuard.assert_multiplexable_normalized!(sql, mode: client.guard)

      wait_for_request do
        submit_with_failover(client) { Request.build(sql, params) }
      end
    end

    def prepare(client, name, sql, param_types, typed: false)
      ensure_started!(client)
      sql = RequestOps.snapshot_sql(sql)
      SessionGuard.assert_multiplexable_normalized!(sql, mode: client.guard)
      pool(client).__send__(:prepare_statement, client, name, sql, param_types, typed: typed)
    end

    def query_prepared(client, statement, params)
      ensure_started!(client)

      wait_for_request do
        submit_with_failover(client) do
          request = Request.prepared_query(statement, params: params)
          pool(client).bind_type_map(request, statement) if statement.typed?
          request
        end
      end
    end

    def wait_for_request
      request = yield
      request.wait
    ensure
      request.cancel! if request && !request.settled?
    end

    def submit_with_failover(client)
      attempts = 0
      client_pool = pool(client)
      limit = [client_pool.pipeline_size, 1].max
      last_error = nil

      while attempts < limit
        request = yield
        last_error = submit_attempt(client_pool, request)
        return request unless last_error

        attempts += 1
        break if stop_retrying?(request, last_error)
      end

      raise_submission_error(last_error)
    end

    def submit_attempt(client_pool, request)
      client_pool.__send__(:pipeline_driver).submit(request)
      nil
    rescue NotDispatchedError, ShutdownError => e
      e
    end

    def stop_retrying?(request, error)
      error.is_a?(ShutdownError) && (request.state != :new || request.settled?)
    end

    def raise_submission_error(error)
      error ||= NotDispatchedError.new('no live pipeline connections; request was not dispatched')
      raise error if error.is_a?(NotDispatchedError)

      raise NotDispatchedError, error.message
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

    def transaction(client, &block)
      ensure_started!(client)

      pool(client).__send__(:with_pinned) do |conn|
        tx = Transaction.new(conn)
        begin
          TransactionOps.run(tx, &block)
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
      client_pool = pool(client)
      begin
        client_pool.graceful_close
      ensure
        client.__send__(:started=, false) if client_pool.closing?
      end
    end

    def abort!(client)
      return unless started?(client)

      ensure_context!(client)
      client_pool = pool(client)
      begin
        client_pool.abort!
      ensure
        client.__send__(:started=, false) if client_pool.closing?
      end
    end

    def pool(client)
      client.__send__(:pool)
    end

    def ensure_started!(client)
      raise Error, 'client not started; call #start or use Client.open' unless started?(client)

      ensure_context!(client)
    end

    def ensure_context!(client)
      return if Thread.current.equal?(client.__send__(:owner_thread)) &&
                Fiber.scheduler.equal?(client.__send__(:scheduler))

      raise Error, 'client is reactor-local and cannot be used from another thread/scheduler'
    end

    def started?(client)
      client.__send__(:started)
    end
  end
end
