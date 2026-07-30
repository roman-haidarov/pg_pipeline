# frozen_string_literal: true

# Shared helpers for all bench_kit scripts. Loaded via `require_relative "common"`.

require "fileutils"
require "bundler/setup"
require "pg_pipeline"

module BenchKit
  module_function

  ROOT = File.expand_path("..", __dir__)
  OUT_DIR = ENV.fetch("OUT_DIR", File.join(ROOT, "tmp", "bench_kit"))

  def now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def pct(sorted, p)
    return 0.0 if sorted.empty?

    rank = (p / 100.0) * (sorted.length - 1)
    low = rank.floor
    high = rank.ceil
    return sorted[low] if low == high

    sorted[low] + ((sorted[high] - sorted[low]) * (rank - low))
  end

  def require_url!
    ENV.fetch("PG_PIPELINE_URL") do
      abort <<~MSG
        set PG_PIPELINE_URL

        examples:
          # direct (localhost — low RTT, hides pipelining wins)
          PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:5417/postgres

          # via latency proxy (realistic RTT)
          #   bundle exec rake bench:proxy RTT_MS=10 UPSTREAM=127.0.0.1:5417
          PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:6432/postgres
      MSG
    end
  end

  def url_with_app(url, app)
    sep = url.include?("?") ? "&" : "?"
    "#{url}#{sep}application_name=#{app}"
  end

  def ensure_out_dir!
    FileUtils.mkdir_p(OUT_DIR)
    OUT_DIR
  end

  def stamp
    Time.now.strftime("%Y%m%d_%H%M%S")
  end

  def redact_url(url)
    url.to_s.sub(%r{//[^/]*@}, "//***@")
  end

  def app_conn_count(url, app)
    c = nil
    c = PG::Connection.new(url)
    c.exec_params(
      "SELECT count(*)::int AS c FROM pg_stat_activity WHERE application_name = $1",
      [app]
    ).first["c"].to_i
  rescue StandardError
    -1
  ensure
    begin
      c&.close
    rescue StandardError
      nil
    end
  end

  def report_latencies(label, latencies_ms, wall_s, extra = {})
    sorted = latencies_ms.compact.sort
    n = sorted.length
    thrpt = wall_s.positive? ? (n / wall_s) : 0.0
    bits = [
      format("%-12s", label),
      format("reqs=%d", n),
      format("wall=%.2fs", wall_s),
      format("thrpt=%.0f/s", thrpt),
      format("p50=%.1f", pct(sorted, 50)),
      format("p95=%.1f", pct(sorted, 95)),
      format("p99=%.1f", pct(sorted, 99)),
      format("max=%.1f ms", sorted.last || 0)
    ]
    extra.each { |k, v| bits << "#{k}=#{v}" }
    puts bits.join("  ")
  end
end
