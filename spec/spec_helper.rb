# frozen_string_literal: true

begin
  require "async"
rescue LoadError
  warn "async not installed; specs tagged :async will be skipped"
end

require "pg_pipeline"

Dir[File.expand_path("support/**/*.rb", __dir__)].sort.each { |file| require file }

module SpecSupport
  module_function

  def database_url = ENV.fetch("PG_PIPELINE_URL")

  def async_available? = defined?(::Async)
end

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = :expect }
  config.disable_monkey_patching!
  config.order = :random

  live = ENV["PG_PIPELINE_URL"] || ENV["PG_PIPELINE_INTEGRATION"]
  config.filter_run_excluding(:integration => true) unless live
  config.filter_run_excluding(:async => true) unless SpecSupport.async_available?

  # Specs that touch PgPipeline::Native are tagged :native_only. Skip them if
  # the extension failed to load rather than erroring mid-example.
  config.filter_run_excluding(:native_only => true) unless defined?(PgPipeline::Native)

  config.before(:suite) do
    if defined?(PgPipeline::Native)
      RSpec.configuration.reporter.message(
        "pg_pipeline=#{PgPipeline::VERSION} native libpq=#{PgPipeline::Native.libpq_version} " \
        "ruby-pg libpq=#{PG.library_version}"
      )
    else
      RSpec.configuration.reporter.message(
        "pg_pipeline=#{PgPipeline::VERSION} (no native core); ruby-pg libpq=#{PG.library_version}"
      )
    end
  end
end
