# frozen_string_literal: true

require_relative "errors"

module PgPipeline
  class Session
    attr_reader :owner_fiber

    def initialize(conn)
      @conn = conn
      @active = true
      @owner_fiber = Fiber.current
    end

    def query(sql, params = []) = SessionOps.query(self, sql, params)
    def exec(sql, params = nil) = SessionOps.exec(self, sql, params)
    def prepare(name, sql, param_types = nil) = SessionOps.prepare(self, name, sql, param_types)
    def exec_prepared(name, params = []) = SessionOps.exec_prepared(self, name, params)
    def active? = @active

    private

    attr_accessor :conn, :active
  end

  module SessionOps
    module_function

    def ensure_active!(session)
      conn = connection(session)
      raise Error, "session handle is no longer active" unless session.active? && conn
      return if Fiber.current.equal?(session.owner_fiber)

      raise Error, "session handle is fiber-local and cannot be used from another fiber"
    end

    def query(session, sql, params)
      ensure_active!(session)
      connection(session).exec_params(sql, params)
    end

    def exec(session, sql, params = nil)
      ensure_active!(session)
      if params.nil?
        connection(session).exec(sql)
      else
        connection(session).exec_params(sql, params)
      end
    end

    def prepare(session, name, sql, param_types)
      ensure_active!(session)
      if param_types
        connection(session).prepare(name, sql, param_types)
      else
        connection(session).prepare(name, sql)
      end
    end

    def exec_prepared(session, name, params)
      ensure_active!(session)
      connection(session).exec_prepared(name, params)
    end

    def close!(session)
      session.__send__(:active=, false)
      session.__send__(:conn=, nil)
      nil
    end

    def connection(session)
      session.__send__(:conn)
    end
  end
end
