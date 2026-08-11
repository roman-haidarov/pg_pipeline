# frozen_string_literal: true

require_relative "_sample_helper"

sample_name = "pg_pipeline_client_query_params_hot_path"
width = Integer(ENV.fetch("PARAMS", "2"))
raise "PARAMS must be >= 1" if width < 1

placeholders = Array.new(width) { |i| "$#{i + 1}::text" }.join(", ")
sql = "SELECT #{placeholders}".freeze
ring = PgPipelineSample.params_ring(width)
preheat = PgPipelineSample.preheat_iterations(50)
mask = ring.length - 1

PgPipelineSample.with_client do |client|
  preheat.times { |i| client.query(sql, ring[i & mask]).clear }

  PgPipelineSample.print_banner(
    sample_name: sample_name,
    call: "client.query(#{sql.inspect}, params[#{width}])",
    extra: {
      url: PgPipelineSample.redact_url(PgPipelineSample.database_url!),
      params: width,
      param_bytes: ring[0].sum(&:bytesize),
      preheat_iterations: preheat
    },
    expected: [
      "seal!: pp_params_describe / pp_payload_seal arena",
      "dispatch: PQsendQueryParams from sealed payload only",
      "same wait/drain path as client_query_hot_path"
    ]
  )

  PgPipelineSample.run_hot_loop_in_scheduler do |i|
    result = client.query(sql, ring[i & mask])
    result.clear
    result
  end

  puts "stats=#{client.stats.inspect}"
end
