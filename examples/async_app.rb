# frozen_string_literal: true

require "pg_pipeline"

CONN = { host: ENV.fetch("PGHOST", "localhost"), dbname: ENV.fetch("PGDATABASE", "postgres") }

Async do |task|
  PgPipeline::Client.open(CONN, pipeline_size: 4, pinned_size: 2) do |db|
    ids = (1..20).to_a
    rows = ids.map do |id|
      task.async do
        res = db.query("SELECT $1::int AS id, now() AS at", [id])
        res && res.first
      end
    end.map(&:wait)

    puts "fetched #{rows.compact.size} rows via pipeline"

    begin
      db.query("SET search_path = other")
    rescue PgPipeline::UnsafeMultiplexError => e
      puts "correctly refused: #{e.message.lines.first.strip}"
    end

    db.transaction do |tx|
      tx.exec("CREATE TEMP TABLE IF NOT EXISTS demo(x int)")
      tx.query("INSERT INTO demo(x) VALUES ($1)", [42])
      got = tx.query("SELECT x FROM demo")
      puts "in-txn temp table row: #{got.first["x"]}"
    end
  end
end.wait
