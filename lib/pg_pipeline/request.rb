# frozen_string_literal: true

require_relative "errors"
require_relative "native"

module PgPipeline
  class Request < Native::RequestState
    attr_reader :sql

    alias native_transition! transition!
    alias native_accept_result accept_result
    alias native_record_error! record_error!
    alias native_query_boundary! query_boundary!
    alias native_finish! finish!
    alias native_reject! reject!
    alias native_cancel! cancel!
    alias native_wait wait
    alias native_seal! seal!
    alias native_adopt_payload! adopt_payload!

    def initialize(sql:, params: nil)
      super()
      build_sealed!(sql, params)
    end

    def self.build(sql, params, snapped_sql: false)
      request = allocate
      request.__send__(:build_sealed!, sql, params, snapped_sql: snapped_sql)
      request
    end

    def self.prepare(statement) = PrepareRequest.new(statement)

    def self.prepared_query(statement, params: nil)
      PreparedQueryRequest.build(statement, params)
    end

    def operation = :query
    def statement_name = nil
    def param_types = nil

    def respawn
      copy = self.class.allocate
      copy.__send__(:adopt_from, self)
      copy
    end

    def queued! = native_transition!(:new, :queued)
    def dispatched! = native_transition!(:queued, :dispatched)
    def accept_result(result) = native_accept_result(result)
    def record_error!(error, result: nil) = native_record_error!(error, result)
    def query_boundary! = native_query_boundary!
    def finish! = native_finish!(self)
    def reject!(error) = native_reject!(self, error)
    def cancel! = native_cancel!
    def wait = native_wait(self)

    def query_boundary_seen? = query_boundary_seen
    def cancelled? = cancelled
    def settled? = settled

    private :native_transition!, :native_accept_result, :native_record_error!,
            :native_query_boundary!, :native_finish!, :native_reject!,
            :native_cancel!, :native_wait, :native_seal!, :native_adopt_payload!

    private

    def build_sealed!(sql, params, snapped_sql: false)
      @sql = snapped_sql ? sql : RequestOps.snapshot_sql(sql)
      seal!(RequestOps.validate_params!(params))
    end

    def init_query(sql, params)
      @sql = RequestOps.snapshot_sql(sql)
      RequestOps.validate_params!(params)
    end

    def seal!(params)
      native_seal!(operation, @sql, params, statement_name, param_types)
      self
    end

    def adopt_from(origin)
      @sql = origin.sql
      native_adopt_payload!(origin)
      self
    end
  end

  class PrepareRequest < Request
    attr_reader :statement_name, :param_types

    def initialize(statement)
      @statement_name = RequestOps.snapshot_name(statement.physical_name)
      @param_types = RequestOps.snapshot_param_types(statement.param_types)
      super(sql: statement.sql)
    end

    def operation = :prepare

    private

    def adopt_from(origin)
      @statement_name = origin.statement_name
      @param_types = origin.param_types
      super
    end
  end

  class PreparedQueryRequest < Request
    attr_reader :statement_name

    def initialize(statement, params: nil)
      @statement_name = RequestOps.snapshot_name(statement.physical_name)
      super(sql: statement.sql, params: params)
    end

    def self.build(statement, params)
      request = allocate
      request.__send__(:build_prepared_sealed!, statement, params)
      request
    end

    def operation = :prepared_query

    private

    def build_prepared_sealed!(statement, params)
      @statement_name = RequestOps.snapshot_name(statement.physical_name)
      build_sealed!(statement.sql, params, snapped_sql: true)
    end

    def init_prepared(statement, params)
      @statement_name = RequestOps.snapshot_name(statement.physical_name)
      init_query(statement.sql, params)
    end

    def adopt_from(origin)
      @statement_name = origin.statement_name
      super
    end
  end

  module RequestOps
    module_function

    def snapshot_sql(sql)
      value = sql.to_s
      value.frozen? ? value : value.dup.freeze
    end

    EMPTY_PARAMS = [].freeze

    def validate_params!(params)
      return EMPTY_PARAMS if params.nil?
      raise ArgumentError, "params must be an Array" unless params.is_a?(Array)

      params
    end

    def snapshot_name(name)
      value = name.to_s
      raise ArgumentError, "statement_name must not be empty" if value.empty?

      value.frozen? ? value : value.dup.freeze
    end

    def snapshot_param_types(param_types)
      return nil if param_types.nil?
      raise ArgumentError, "param_types must be an Array or nil" unless param_types.is_a?(Array)

      param_types.map { |oid| snapshot_param_oid(oid) }.freeze
    end

    def snapshot_param_oid(oid)
      return if oid.nil?

      value = begin
        Integer(oid)
      rescue ArgumentError, TypeError
        raise ArgumentError, "param_types must contain only integer OIDs or nil"
      end

      unless value.between?(0, 0xffff_ffff)
        raise ArgumentError, "PostgreSQL OIDs must be between 0 and 4294967295"
      end

      value
    end

    def transition!(req, from, to) = req.__send__(:native_transition!, from, to)
    def accept_result(req, result) = req.__send__(:native_accept_result, result)
    def record_error!(req, error, result) = req.__send__(:native_record_error!, error, result)
    def query_boundary!(req) = req.__send__(:native_query_boundary!)
    def finish!(req) = req.__send__(:native_finish!, req)
    def reject!(req, error) = req.__send__(:native_reject!, req, error)
    def cancel!(req) = req.__send__(:native_cancel!)
    def wait(req) = req.__send__(:native_wait, req)

    def clear_result(result)
      result.clear if result.respond_to?(:clear)
    end
  end
end
