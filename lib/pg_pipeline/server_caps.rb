# frozen_string_literal: true

require "pg"

require_relative "errors"

module PgPipeline
  class ServerCaps
    REQUIRED_LIBPQ_VERSION = 140_000
    FAST_SYNC_LIBPQ_VERSION = 170_000
    PROTOCOL_VERSION = 3

    attr_reader :libpq_version, :protocol_version, :pipeline_api,
                :fast_sync_api, :raw_pipeline_sync_api

    def initialize(libpq_version:, protocol_version:, pipeline_api: true,
                   fast_sync_api: false, raw_pipeline_sync_api: false)
      @libpq_version = Integer(libpq_version)
      @protocol_version = Integer(protocol_version)
      @pipeline_api = pipeline_api
      @fast_sync_api = fast_sync_api
      @raw_pipeline_sync_api = raw_pipeline_sync_api
    end

    def self.from_connection(conn) = Caps.from_connection(conn)
    def supported? = Caps.supported?(self)
    def assert_supported! = Caps.assert_supported!(self)
    def fast_sync? = Caps.fast_sync?(self)
    def place_sync(conn) = Caps.place_sync(self, conn)
  end

  module Caps
    module_function

    def from_connection(conn)
      ServerCaps.new(
        libpq_version: PG.library_version,
        protocol_version: conn.protocol_version,
        pipeline_api: pipeline_api?(conn),
        fast_sync_api: conn.respond_to?(:send_pipeline_sync),
        raw_pipeline_sync_api: conn.respond_to?(:sync_pipeline_sync)
      )
    end

    def pipeline_api?(conn)
      conn.respond_to?(:enter_pipeline_mode) &&
        conn.respond_to?(:exit_pipeline_mode) &&
        conn.respond_to?(:pipeline_status) &&
        (conn.respond_to?(:sync_pipeline_sync) || conn.respond_to?(:pipeline_sync))
    end

    def supported?(caps)
      caps.libpq_version >= ServerCaps::REQUIRED_LIBPQ_VERSION &&
        caps.protocol_version == ServerCaps::PROTOCOL_VERSION &&
        caps.pipeline_api
    end

    def assert_supported!(caps)
      return true if supported?(caps)

      raise UnsupportedServerError,
            "pg_pipeline requires libpq >= 14, PostgreSQL protocol v3, and " \
            "ruby-pg pipeline bindings; libpq=#{caps.libpq_version}, " \
            "protocol=#{caps.protocol_version}, pipeline_api=#{caps.pipeline_api}"
    end

    def fast_sync?(caps)
      caps.libpq_version >= ServerCaps::FAST_SYNC_LIBPQ_VERSION && caps.fast_sync_api
    end

    def place_sync(caps, conn)
      if fast_sync?(caps)
        conn.send_pipeline_sync
      elsif caps.raw_pipeline_sync_api
        conn.sync_pipeline_sync
      else
        conn.pipeline_sync
      end

      nil
    end
  end
end
