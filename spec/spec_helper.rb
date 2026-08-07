# frozen_string_literal: true

require "async"
require "pg_pipeline"

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = :expect }
  config.disable_monkey_patching!
  config.order = :random

  live = ENV["PG_PIPELINE_URL"] || ENV["PG_PIPELINE_INTEGRATION"]
  config.filter_run_excluding(:integration => true) unless live
end
