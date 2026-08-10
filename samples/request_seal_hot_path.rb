# frozen_string_literal: true

require_relative "_sample_helper"

sample_name = "pg_pipeline_request_seal_hot_path"
width = Integer(ENV.fetch("PARAMS", "2"))
raise "PARAMS must be >= 0" if width.negative?

sql =
  if width.zero?
    "SELECT 1".freeze
  else
    placeholders = Array.new(width) { |i| "$#{i + 1}::text" }.join(", ")
    "SELECT #{placeholders}".freeze
  end

ring = PgPipelineSample.params_ring(width)
preheat = PgPipelineSample.preheat_iterations(200)

preheat.times do |i|
  params = ring ? ring[i & (ring.length - 1)] : nil
  PgPipeline::Request.build(sql, params)
end

PgPipelineSample.print_banner(
  sample_name: sample_name,
  call: "PgPipeline::Request.build(sql, params)  # seal only",
  native_grep: "PgPipeline|seal|pp_payload|pp_params|pp_export|Request|RequestState",
  extra: {
    params: width,
    sql_bytes: sql.bytesize,
    param_bytes: ring ? ring[0].sum(&:bytesize) : 0,
    preheat_iterations: preheat
  },
  expected: [
    "Request.build → seal! → pp_request_state_seal",
    "pp_params_describe / pp_payload_seal arena",
    "no PQsend*, no scheduler, no Result"
  ]
)

mask = ring ? ring.length - 1 : 0
PgPipelineSample.run_hot_loop do |i|
  params = ring ? ring[i & mask] : nil
  PgPipeline::Request.build(sql, params)
end
