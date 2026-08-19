# frozen_string_literal: true

require "pg"
require_relative "errors"

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

      conn = nil
      conn = connection_args.nil? ? PG::Connection.new : PG::Connection.new(connection_args)
      @bundle = PG::BasicTypeRegistry::CoderMapsBundle.new(conn)
      @text_map_for_results = PG::BasicTypeMapForResults.new(@bundle)
    rescue StandardError => e
      @bundle = :failed
      @text_map_for_results = nil
      raise Error, "failed to build CoderMapsBundle (#{e.class}: #{e.message})"
    ensure
      begin
        conn&.close
      rescue StandardError
        nil
      end
    end

    def apply!(request, result, _conn)
      map = request.type_map
      return if map.nil?

      if map.equal?(PENDING)
        name = request.statement_name
        map = @cache[name]
        unless map
          map = build_column_map!(result)
          @cache[name] = map
        end
        request.type_map = map
      end

      result.type_map = map
    end

    private

    def build_column_map!(result)
      unless result.result_status == PG::PGRES_TUPLES_OK && result.nfields.positive?
        raise Error, "typed mapping requires a TUPLES_OK result with at least one column"
      end
      if @bundle == :failed || @text_map_for_results.nil?
        raise Error, "typed result mapping is unavailable (CoderMapsBundle failed)"
      end

      @text_map_for_results.build_column_map(result)
    end
  end
end
