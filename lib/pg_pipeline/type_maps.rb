# frozen_string_literal: true

require 'pg'
require_relative 'errors'

module PgPipeline
  class TypeMaps
    PENDING = Object.new.freeze

    def initialize
      @cache = {}
      @wants_typed = {}
      @bundle = nil
      @text_map_for_results = nil
    end

    def register(statement)
      @wants_typed[statement.physical_name] = true if statement.typed?
    end

    def unregister(physical_name)
      @cache.delete(physical_name)
      @wants_typed.delete(physical_name)
    end

    def typed?(physical_name)
      @wants_typed[physical_name]
    end

    def cached(physical_name)
      @cache[physical_name]
    end

    def bind(request, statement)
      return unless statement.typed?

      request.type_map = @cache[statement.physical_name] || PENDING
    end

    def ensure_bundle!(connection_args)
      return if @bundle && @bundle != :failed

      build_bundle(connection_args)
    rescue StandardError => e
      @bundle = :failed
      @text_map_for_results = nil
      raise Error, "failed to build CoderMapsBundle (#{e.class}: #{e.message})"
    end

    private

    def build_bundle(connection_args)
      conn = open_connection(connection_args)
      @bundle = PG::BasicTypeRegistry::CoderMapsBundle.new(conn)
      @text_map_for_results = PG::BasicTypeMapForResults.new(@bundle)
    ensure
      close_connection(conn)
    end

    def open_connection(connection_args)
      return PG::Connection.new if connection_args.nil?

      PG::Connection.new(connection_args)
    end

    def close_connection(conn)
      conn&.close
    rescue StandardError
      nil
    end

    public

    def apply!(request, result, _conn)
      map = request.type_map
      return if map.nil?

      map = resolve_type_map(request, result, map)
      request.type_map = map if request.type_map.equal?(PENDING)

      result.type_map = map
    end

    private

    def resolve_type_map(request, result, map)
      return map unless map.equal?(PENDING)

      name = request.statement_name
      @cache[name] ||= build_column_map!(result)
    end

    def build_column_map!(result)
      unless result.result_status == PG::PGRES_TUPLES_OK && result.nfields.positive?
        raise Error, 'typed mapping requires a TUPLES_OK result with at least one column'
      end
      if @bundle == :failed || @text_map_for_results.nil?
        raise Error, 'typed result mapping is unavailable (CoderMapsBundle failed)'
      end

      @text_map_for_results.build_column_map(result)
    end
  end
end
