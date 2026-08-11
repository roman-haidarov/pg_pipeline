# frozen_string_literal: true

require_relative "_sample_helper"

sample_name = "pg_pipeline_prepared_query_hot_path"
sql = ENV.fetch("SQL", "SELECT $1::int AS n").freeze
preheat = PgPipelineSample.preheat_iterations(50)

PgPipelineSample.with_client do |client|
  statement = client.prepare("sample_prepared_hot", sql)
  preheat.times { |i| statement.query([i]).clear }

  PgPipelineSample.print_banner(
    sample_name: sample_name,
    call: "PreparedStatement#query([n]) for #{sql.inspect}",
    extra: {
      url: PgPipelineSample.redact_url(PgPipelineSample.database_url!),
      preheat_iterations: preheat
    },
    expected: [
      "Request.prepared_query → seal as OP_PREPARED_QUERY",
      "dispatch: PQsendQueryPrepared + pipeline Sync",
      "same owner/wait/drain path as ad-hoc query",
      "no re-PREPARE on the hot loop (statement already registered on drivers)"
    ]
  )

  n = 0
  PgPipelineSample.run_hot_loop_in_scheduler do
    result = statement.query([n])
    n = (n + 1) & 0xFFFF
    result.clear
    result
  end

  puts "stats=#{client.stats.inspect}"
end
