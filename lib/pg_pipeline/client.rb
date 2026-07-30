# frozen_string_literal: true

require "async"

require_relative "errors"
require_relative "pool"
require_relative "request"
require_relative "session_guard"
require_relative "session"
require_relative "transaction"

module PgPipeline
  class Client
    attr_reader :guard

    def initialize(connection_args = nil, guard: :default, **pool_opts)
      @guard = SessionGuard.normalize_mode!(guard)
      @pool = Pool.new(connection_args, **pool_opts)
      @started = false
      @owner_thread = nil
      @scheduler = nil
    end

    def self.open(connection_args = nil, **opts, &block)
      ClientOps.open(connection_args, opts, &block)
    end

    def start(parent: Async::Task.current) = ClientOps.start(self, parent)
    def query(sql, params = []) = ClientOps.query(self, sql, params)
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
      begin
        yield client
      ensure
        client.close
      end
    end

    def start(client, parent)
      raise Error, "client already started" if started?(client)

      pool(client).start(parent: parent)
      client.__send__(:owner_thread=, Thread.current)
      client.__send__(:scheduler=, Fiber.scheduler)
      client.__send__(:started=, true)
      client
    end

    def query(client, sql, params)
      ensure_started!(client)
      sql = sql.to_s
      SessionGuard.assert_multiplexable!(sql, mode: client.guard)

      request = submit_with_failover(client, sql, params)

      begin
        request.wait
      ensure
        request.cancel! unless request.settled?
      end
    end

    # NotDispatchedError is safe to retry on a fresh Request by contract.
    # ShutdownError is retried only while the current Request is still pre-dispatch.
    def submit_with_failover(client, sql, params)
      attempts = 0
      limit = [pool(client).pipeline_size, 1].max
      last_error = nil

      while attempts < limit
        request = Request.new(sql: sql, params: params)
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
        end
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
