# frozen_string_literal: true

require_relative "errors"
require_relative "request"

module PgPipeline
  class PreparedStatement
    attr_reader :name, :sql, :param_types, :physical_name, :typed

    def initialize(client:, name:, physical_name:, sql:, param_types: nil, typed: false)
      @client = client
      @name = PreparedStatementOps.snapshot_name(name)
      @physical_name = PreparedStatementOps.snapshot_name(physical_name)
      @sql = RequestOps.snapshot_sql(sql)
      @param_types = RequestOps.snapshot_param_types(param_types)
      @typed = !!typed
      freeze
    end

    def typed? = @typed

    def query(params = RequestOps::EMPTY_PARAMS) = PreparedStatementOps.query(self, params)
    alias call query

    def inspect
      flag = @typed ? " typed" : ""
      "#<#{self.class} name=#{@name.inspect} sql=#{@sql.inspect}#{flag}>"
    end

    private

    attr_reader :client
  end

  module PreparedStatementOps
    module_function

    def query(statement, params)
      ClientOps.query_prepared(statement.__send__(:client), statement, params)
    end

    def snapshot_name(name)
      value = name.to_s
      raise ArgumentError, "prepared statement name must not be empty" if value.empty?
      raise ArgumentError, "prepared statement name must not contain NUL" if value.include?("\0")

      value.frozen? ? value : value.dup.freeze
    end
  end
end
