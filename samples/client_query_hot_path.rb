# frozen_string_literal: true

require_relative "_sample_helper"

sample_name = "pg_pipeline_client_query_hot_path"
sql = ENV.fetch("SQL", "SELECT 1").freeze
preheat = PgPipelineSample.preheat_iterations(50)

PgPipelineSample.with_client do |client|
  preheat.times { client.query(sql).clear }

  PgPipelineSample.print_banner(
    sample_name: sample_name,
    call: "client.query(#{sql.inspect})",
    extra: {
      url: PgPipelineSample.redact_url(PgPipelineSample.database_url!),
      pipeline_size: Integer(ENV.fetch("PIPELINE_SIZE", "1")),
      preheat_iterations: preheat
    },
    expected: [
      "ClientOps.query → SessionGuard.assert_multiplexable_normalized_c!",
      "Request.build / Native::RequestState#seal!",
      "PoolOps.select_driver → BoundedQueue#enqueue → request wait (scheduler block)",
      "NativeDriverOps.owner_loop / pump_requests / dispatch_unit",
      "Native::Driver#dispatch → PQsendQueryParams + PQsendPipelineSync|PQpipelineSync",
      "Native::Driver#flush → PQflush; io_wait when incomplete",
      "reader → consume_and_drain → PQconsumeInput / PQisBusy / PQgetResult",
      "RequestState#wait unblock; Native::Result wrap"
    ]
  )

  PgPipelineSample.run_hot_loop_in_scheduler do
    result = client.query(sql)
    result.clear
    result
  end

  puts "stats=#{client.stats.inspect}"
end
