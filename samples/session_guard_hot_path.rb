# frozen_string_literal: true

require_relative "_sample_helper"

sample_name = "pg_pipeline_session_guard_hot_path"
mode = ENV.fetch("MODE", "default").to_sym
unique = ENV.fetch("UNIQUE", "0") != "0"
sql_seed = ENV.fetch("SQL", "SELECT id, name FROM users WHERE active = true").freeze
preheat = PgPipelineSample.preheat_iterations(500)

mode = PgPipeline::SessionGuard.normalize_mode!(mode)

preheat.times do |i|
  sql = unique ? "#{sql_seed} /* #{i} */" : sql_seed
  PgPipeline::SessionGuard.assert_multiplexable!(sql, mode: mode)
end

PgPipelineSample.print_banner(
  sample_name: sample_name,
  call: "SessionGuard.assert_multiplexable!(sql, mode: :#{mode})",
  native_grep: "SessionGuard|session_guard|needs_mask|unsafe|mask_|match_|GUARD|cache",
  extra: {
    mode: mode,
    unique: unique,
    sql_bytes: sql_seed.bytesize,
    preheat_iterations: preheat,
    cache_size_after_preheat: (PgPipeline::SessionGuard.cache_size if PgPipeline::SessionGuard.respond_to?(:cache_size))
  },
  expected: [
    "assert_multiplexable_normalized_c! / unsafe_reason_normalized_c",
    "needs_mask → mask path only when quotes/comments/dollar-tags present",
    "forbidden-call scan (set_config, dblink, large-object, …)",
    "two-generation verdict cache (UNIQUE=0 → hits; UNIQUE=1 → churn)"
  ]
)

counter = 0
PgPipelineSample.run_hot_loop do
  sql = unique ? "#{sql_seed} /* #{counter} */" : sql_seed
  counter += 1
  PgPipeline::SessionGuard.assert_multiplexable!(sql, mode: mode)
  true
end

if PgPipeline::SessionGuard.respond_to?(:cache_size)
  puts "cache_size=#{PgPipeline::SessionGuard.cache_size}"
end
