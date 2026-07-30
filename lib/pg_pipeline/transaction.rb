# frozen_string_literal: true

require "pg"

require_relative "session"

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

    def run(tx)
      SessionOps.ensure_active!(tx)
      raise Error, "transaction is already open" if tx.open?

      conn = SessionOps.connection(tx)
      conn.exec("BEGIN")
      tx.__send__(:open=, true)
      begin
        result = yield tx
        conn.exec("COMMIT")
        tx.__send__(:open=, false)
        result
      rescue Exception
        rollback_quietly(tx)
        raise
      end
    end

    def savepoint(tx, name)
      SessionOps.ensure_active!(tx)
      raise Error, "savepoint requires an open transaction" unless tx.open?

      conn = SessionOps.connection(tx)
      seq = tx.__send__(:savepoint_seq) + 1
      tx.__send__(:savepoint_seq=, seq)
      point = name || "pgp_sp_#{seq}"
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
      return unless tx.open?

      conn = SessionOps.connection(tx)
      conn&.exec("ROLLBACK")
    rescue PG::Error
      nil
    ensure
      tx.__send__(:open=, false)
    end
  end
end
