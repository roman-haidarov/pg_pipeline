# frozen_string_literal: true

require "pg"

require_relative "session"

module PgPipeline
  class Transaction < Session
    attr_accessor :open, :savepoint_seq

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
  end

  module TransactionOps
    module_function

    def run(tx)
      SessionOps.ensure_active!(tx)
      raise Error, "transaction is already open" if tx.open

      conn = SessionOps.connection(tx)
      conn.exec("BEGIN")
      tx.open = true
      begin
        result = yield tx
        conn.exec("COMMIT")
        tx.open = false
        result
      rescue Exception
        # Only roll back the transaction we opened. Early guard errors
        # ("already open", inactive handle) must not hit this path.
        rollback_quietly(tx)
        raise
      end
    end

    def savepoint(tx, name)
      SessionOps.ensure_active!(tx)
      raise Error, "savepoint requires an open transaction" unless tx.open

      conn = SessionOps.connection(tx)
      tx.savepoint_seq += 1
      point = name || "pgp_sp_#{tx.savepoint_seq}"
      ident = conn.quote_ident(point)

      conn.exec("SAVEPOINT #{ident}")
      begin
        result = yield tx
        conn.exec("RELEASE SAVEPOINT #{ident}")
        result
      rescue Exception
        rollback_to_savepoint(tx, ident)
        raise
      end
    end

    def rollback_to_savepoint(tx, ident)
      conn = SessionOps.connection(tx)
      return unless conn

      conn.exec("ROLLBACK TO SAVEPOINT #{ident}")
      conn.exec("RELEASE SAVEPOINT #{ident}")
    rescue PG::Error
      nil
    end

    def rollback_quietly(tx)
      return unless tx.open

      conn = SessionOps.connection(tx)
      conn&.exec("ROLLBACK")
    rescue PG::Error
      nil
    ensure
      tx.open = false
    end
  end
end
