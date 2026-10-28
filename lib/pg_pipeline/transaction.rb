# frozen_string_literal: true

require 'pg'

require_relative 'session'

module PgPipeline
  class Transaction < Session
    def initialize(conn)
      super
      @open = false
      @savepoint_seq = 0
    end

    def run(&block)
      TransactionOps.run(self, &block)
    end

    def savepoint(name = nil, &block)
      TransactionOps.savepoint(self, name, &block)
    end

    def open? = @open

    private

    attr_accessor :open, :savepoint_seq
  end

  module TransactionOps
    module_function

    def run(transaction)
      SessionOps.ensure_active!(transaction)
      raise Error, 'transaction is already open' if transaction.open?

      conn = SessionOps.connection(transaction)
      conn.exec('BEGIN')
      transaction.__send__(:open=, true)
      begin
        result = yield transaction
        commit(conn)
        transaction.__send__(:open=, false)
        result
      rescue Exception
        rollback_quietly(transaction)
        raise
      end
    end

    def commit(conn)
      conn.exec('COMMIT')
    rescue PG::Error => e
      raise unless indeterminate_commit_failure?(conn, e)

      raise IndeterminateCommitError.new(
        'COMMIT acknowledgement was not received; the transaction may have committed ' \
        "and must not be retried blindly (#{e.class}: #{e.message})"
      ), cause: e
    end

    def indeterminate_commit_failure?(conn, error)
      return true if defined?(PG::ConnectionBad) && error.is_a?(PG::ConnectionBad)
      return false unless conn.respond_to?(:finished?) && conn.respond_to?(:status)
      return true if conn.finished?

      conn.status != PG::CONNECTION_OK
    rescue PG::Error
      true
    end

    def savepoint(transaction, name)
      SessionOps.ensure_active!(transaction)
      raise Error, 'savepoint requires an open transaction' unless transaction.open?

      conn = SessionOps.connection(transaction)
      seq = transaction.__send__(:savepoint_seq) + 1
      transaction.__send__(:savepoint_seq=, seq)
      point = name || "pgp_sp_#{seq}"
      ident = conn.quote_ident(point)
      conn.exec("SAVEPOINT #{ident}")
      begin
        result = yield transaction
        conn.exec("RELEASE SAVEPOINT #{ident}")
        result
      rescue Exception
        rollback_to_savepoint(transaction, ident)
        raise
      end
    end

    def rollback_to_savepoint(transaction, ident)
      conn = SessionOps.connection(transaction)
      return unless conn

      conn.exec("ROLLBACK TO SAVEPOINT #{ident}")
      conn.exec("RELEASE SAVEPOINT #{ident}")
    rescue PG::Error
      nil
    end

    def rollback_quietly(transaction)
      return unless transaction.open?

      conn = SessionOps.connection(transaction)
      conn&.exec('ROLLBACK')
    rescue PG::Error
      nil
    ensure
      transaction.__send__(:open=, false)
    end
  end
end
