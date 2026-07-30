# frozen_string_literal: true

require "pg_pipeline"

CLIENTS = {}

def client_for_reactor
  key = [Thread.current.object_id, Fiber.scheduler.object_id]
  CLIENTS[key] ||= PgPipeline::Client.new(
    { host: ENV.fetch("PGHOST", "localhost"), dbname: ENV.fetch("PGDATABASE", "postgres") },
    pipeline_size: Integer(ENV.fetch("PGP_PIPELINE_SIZE", "4")),
    pinned_size: Integer(ENV.fetch("PGP_PINNED_SIZE", "2"))
  ).start
end

run lambda { |_env|
  db = client_for_reactor
  row = db.query("SELECT now() AS at, $1::int AS n", [42]).first
  [200, { "content-type" => "text/plain" }, ["at=#{row['at']} n=#{row['n']}\n"]]
}
