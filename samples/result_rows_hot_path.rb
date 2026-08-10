# frozen_string_literal: true

require_relative "_sample_helper"

sample_name = "pg_pipeline_result_rows_hot_path"
rows = Integer(ENV.fetch("ROWS", "50"))
cols = Integer(ENV.fetch("COLS", "8"))
raise "ROWS/COLS must be >= 1" if rows < 1 || cols < 1

select_list = Array.new(cols) { |i| "#{i} AS c#{i}" }.join(", ")
sql = "SELECT * FROM generate_series(1, #{rows}) g, LATERAL (SELECT #{select_list}) s".freeze
preheat = PgPipelineSample.preheat_iterations(10)
materialize = ENV.fetch("MATERIALIZE", "to_a")

PgPipelineSample.with_client do |client|
  preheat.times do
    result = client.query(sql)
    case materialize
    when "to_a" then result.to_a
    when "first" then result.first
    when "values" then result.values
    when "each_row" then result.each_row { |row| row }
    else raise "MATERIALIZE must be to_a, first, values, or each_row"
    end
    result.clear
  end

  PgPipelineSample.print_banner(
    sample_name: sample_name,
    call: "client.query(generate_series #{rows}x#{cols}).#{materialize}",
    extra: {
      url: PgPipelineSample.redact_url(PgPipelineSample.database_url!),
      rows: rows,
      cols: cols,
      materialize: materialize,
      preheat_iterations: preheat
    },
    expected: [
      "same dispatch/drain path as client_query_hot_path",
      "Native::Result#to_a / #first / #values / #each_row materializing Ruby strings from PGresult",
      "PQgetvalue / field name cache paths under result accessors",
      "GC pressure if DISABLE_GC=0 (row Strings)"
    ]
  )

  PgPipelineSample.run_hot_loop_in_scheduler do
    result = client.query(sql)
    case materialize
    when "to_a" then result.to_a
    when "first" then result.first
    when "values" then result.values
    when "each_row" then result.each_row { |row| row }
    end
    ntuples = result.ntuples
    result.clear
    ntuples
  end

  puts "stats=#{client.stats.inspect}"
end
