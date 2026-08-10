# frozen_string_literal: true

require "pg"
require "rbconfig"

require_relative "errors"
require_relative "result"

# Prefer whatever is on the load path -- that is the extension RubyGems or
# Bundler built for *this* Ruby and this platform. Only fall back to a build
# sitting next to the checked-out sources, and never silently prefer it: this
# repository once shipped a committed macOS arm64 `.bundle`, and preferring
# `ext/` meant a clone on any Apple Silicon machine loaded that stale binary
# instead of the one it had just compiled.
begin
  require "pg_pipeline/pg_pipeline_native"
rescue LoadError => e
  local = File.expand_path("../../ext/pg_pipeline_native/pg_pipeline_native", __dir__)
  raise e unless File.file?("#{local}.#{RbConfig::CONFIG.fetch("DLEXT")}")

  require local
end

module PgPipeline
  module Native
    REQUIRED_LIBPQ_VERSION = 140_000

    module_function

    def libpq_versions
      {native: libpq_version, ruby_pg: PG.library_version}.freeze
    end

    def assert_libpq_supported!
      versions = libpq_versions
      unsupported = versions.select { |_name, version| version < REQUIRED_LIBPQ_VERSION }
      return true if unsupported.empty?

      details = unsupported.map { |name, version| "#{name}=#{version}" }.join(", ")
      raise UnsupportedServerError,
            "pg_pipeline requires libpq >= 14 for both the native core and ruby-pg " \
            "(used for pinned sessions); #{details}"
    end

    def assert_libpq_compatible!
      assert_libpq_supported!
    end
  end
end

PgPipeline::Native.assert_libpq_supported!
