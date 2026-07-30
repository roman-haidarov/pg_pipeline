# frozen_string_literal: true

require "async/notification"

require_relative "errors"

module PgPipeline
  class Request
    attr_reader :sql, :params, :condition
    attr_accessor :state, :cancelled, :settled, :result_seen, :query_boundary_seen,
                  :result, :error

    def initialize(sql:, params: nil)
      @sql = RequestOps.snapshot_sql(sql)
      @params = RequestOps.snapshot_params(params)
      @state = :new
      @condition = Async::Notification.new
      @cancelled = false
      @settled = false
      @result_seen = false
      @query_boundary_seen = false
      @result = nil
      @error = nil
    end

    def self.prepare(statement) = PrepareRequest.new(statement)

    def self.prepared_query(statement, params: nil)
      PreparedQueryRequest.new(statement, params: params)
    end

    def operation = :query

    def queued! = RequestOps.transition!(self, :new, :queued)
    def dispatched! = RequestOps.transition!(self, :queued, :dispatched)
    def accept_result(result) = RequestOps.accept_result(self, result)
    def record_error!(error, result: nil) = RequestOps.record_error!(self, error, result)
    def query_boundary! = RequestOps.query_boundary!(self)
    def finish! = RequestOps.finish!(self)
    def reject!(error) = RequestOps.reject!(self, error)
    def cancel! = RequestOps.cancel!(self)
    def wait = RequestOps.wait(self)

    def query_boundary_seen? = @query_boundary_seen
    def cancelled? = @cancelled
    def settled? = @settled
  end

  class PrepareRequest < Request
    attr_reader :statement_name, :param_types

    def initialize(statement)
      super(sql: statement.sql)
      @statement_name = RequestOps.snapshot_name(statement.physical_name)
      @param_types = RequestOps.snapshot_param_types(statement.param_types)
    end

    def operation = :prepare
  end

  class PreparedQueryRequest < Request
    attr_reader :statement_name

    def initialize(statement, params: nil)
      super(sql: statement.sql, params: params)
      @statement_name = RequestOps.snapshot_name(statement.physical_name)
    end

    def operation = :prepared_query
  end

  module RequestOps
    module_function

    def snapshot_sql(sql)
      value = sql.to_s
      value.frozen? ? value : value.dup.freeze
    end

    def snapshot_params(params)
      values = params.nil? ? [] : params
      raise ArgumentError, "params must be an Array" unless values.is_a?(Array)

      values.map { |value| snapshot_value(value) }.freeze
    end

    def snapshot_value(value)
      case value
      when String
        value.frozen? ? value : value.dup.freeze
      when Hash
        value.each_with_object({}) do |(key, item), copy|
          copy[key] = item.is_a?(String) && !item.frozen? ? item.dup.freeze : item
        end.freeze
      else
        value
      end
    end

    def snapshot_name(name)
      value = name.to_s
      raise ArgumentError, "statement_name must not be empty" if value.empty?

      value.frozen? ? value : value.dup.freeze
    end

    def snapshot_param_types(param_types)
      return nil if param_types.nil?
      raise ArgumentError, "param_types must be an Array or nil" unless param_types.is_a?(Array)

      param_types.map { |oid| oid.nil? ? nil : Integer(oid) }.freeze
    rescue ArgumentError, TypeError
      raise ArgumentError, "param_types must contain only integer OIDs or nil"
    end

    def transition!(req, from, to)
      unless req.state == from
        raise ProtocolError, "invalid request transition #{req.state.inspect} -> #{to.inspect}"
      end

      req.state = to
    end

    def accept_result(req, result)
      assert_result_slot!(req)
      req.result_seen = true

      if req.cancelled
        clear_result(result)
      else
        req.result = result
      end
    end

    def record_error!(req, error, result)
      assert_result_slot!(req)
      req.result_seen = true

      if req.cancelled
        clear_result(result)
      else
        req.error ||= error
      end
    end

    def query_boundary!(req)
      raise ProtocolError, "query boundary before query result" unless req.result_seen
      raise ProtocolError, "duplicate query boundary" if req.query_boundary_seen

      req.query_boundary_seen = true
    end

    def finish!(req)
      raise ProtocolError, "sync arrived before query boundary" unless req.query_boundary_seen
      return if req.settled

      req.settled = true
      req.state = :done
      req.condition.signal unless req.cancelled
    end

    def reject!(req, error)
      return if req.settled

      clear_result(req.result)
      req.result = nil
      req.error ||= error
      req.settled = true
      req.state = :done
      req.condition.signal unless req.cancelled
    end

    def cancel!(req)
      return if req.cancelled

      req.cancelled = true
      clear_result(req.result)
      req.result = nil
      req.error.clear_result! if req.error.respond_to?(:clear_result!)
    end

    def wait(req)
      req.condition.wait unless req.settled
      raise req.error if req.error

      req.result
    end

    def assert_result_slot!(req)
      raise ProtocolError, "result arrived after query boundary" if req.query_boundary_seen
      raise ProtocolError, "multiple results for one pipeline unit" if req.result_seen
    end

    def clear_result(result)
      result.clear if result.respond_to?(:clear)
    end
  end
end
