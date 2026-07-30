# bench_kit

One place for **benchmarks** and **profiles** of `pg_pipeline`.

| Script | Needs PG? | What it proves |
|---|---|---|
| `rtt_amortization_demo.rb` | no | pipelining only wins when RTT > 0 |
| `latency_proxy.rb` | no* | inject RTT in front of real PG |
| `pipeline_throughput.rb` | yes | thrpt / p50–p99 under N fibers |
| `pipeline_vs_baseline.rb` | yes | A/B vs naive pool + server_conns |
| `multiworker_smoke.rb` | yes | multi-process connection occupancy |
| `profile_ci_scenario.rb` | yes | RubyProf call paths (CI-shaped load) |

\*proxy needs an upstream PG port.

## Quick start

```bash
# 1) Postgres
docker compose up -d pg17

# 2) Optional: realistic RTT (other terminal)
bundle exec rake bench:proxy RTT_MS=10 UPSTREAM=127.0.0.1:5417 LISTEN=127.0.0.1:6432

# 3) Point URL at PG or at the proxy
export PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:5417/postgres
# or via proxy:
# export PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:6432/postgres

# 4) Run what you need
bundle exec rake bench:list
bundle exec rake bench:rtt_demo          # no PG
bundle exec rake bench:throughput
bundle exec rake bench:ab               # pipeline vs baseline
bundle exec rake bench:smoke
bundle exec rake bench:profile          # ruby-prof → tmp/bench_kit/
```

All rake tasks use `bundle exec ruby bench_kit/<script>.rb` so load paths and gems are consistent.

## Outputs

| Task | Output |
|---|---|
| throughput / ab / smoke / rtt_demo | STDOUT |
| profile | `tmp/bench_kit/ruby_prof_STAMP.txt` — aggregated flat (all fibers, one pass) |
| | `tmp/bench_kit/ruby_prof_STAMP_topN.txt` — per-fiber flat for top-N fibers |
| | `tmp/bench_kit/ruby_prof_STAMP.html` — full call graph (all fibers, drill-down) |

Override: `OUT_DIR=...`.

## Why localhost alone is not enough

Pipelining amortizes **round-trips**. On ~0 RTT (socket to local docker) the win is
tiny by design. Use `bench:proxy` + `bench:ab` to see real speedups as RTT grows.

RubyProf is for **where CPU/time goes in the control plane**, not for absolute
throughput claims (it hooks every call and perturbs Async). Prefer:

- **throughput / ab** for numbers
- **profile** for call graphs (`MEASURE=process` for Ruby CPU; `wall` includes waits)

## Common env

| Env | Used by | Default |
|---|---|---|
| `PG_PIPELINE_URL` | all PG scripts | required |
| `PIPELINE_SIZE` | thrpt, ab, smoke, profile | 2–4 |
| `PINNED_SIZE` | smoke, profile | 2 |
| `FIBERS` / `CONCURRENCY` | thrpt / ab | 2000 / 500 |
| `BASELINE_POOL` | ab | 32 |
| `QUERIES` | ab (per fiber) | 5 |
| `PREPARED` | throughput, ab | `0`; set `1` to prepare both A/B clients |
| `RTT_MS` / `UPSTREAM` / `LISTEN` | proxy | 10 / 5432 / 6432 |
| `MEASURE` | profile | wall |
| `ROUNDS` / `MIN_PERCENT` / `TOP_FIBERS` | profile | 3 / 1 / 8 |
| `OUT_DIR` | profile | `tmp/bench_kit` |

## Suggested realism sweep

```bash
# terminal 1
bundle exec rake bench:proxy RTT_MS=10 UPSTREAM=127.0.0.1:5417

# terminal 2
export PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:6432/postgres
for rtt in 0 1 10 50; do
  echo "=== RTT_MS=$rtt (restart proxy with this RTT) ==="
  CONCURRENCY=500 PIPELINE_SIZE=4 BASELINE_POOL=32 bundle exec rake bench:ab
done
```

## Full metrics (no more doubt)

`rake bench:metrics` runs the complete picture in one shot and writes to
`tmp/bench_kit/`:

- **allocations / query** and GC pressure (GC.stat delta) — the concrete
  per-query churn number.
- **RubyProf process_time** — CPU attribution (not wall, which is IO wait).
- **RubyProf allocations** — which methods allocate.
- **RubyProf wall** — kept only as a sanity check.
- **StackProf cpu + wall** — sampled hot paths, no per-call perturbation; the
  trustworthy CPU view (deterministic RubyProf inflates call-heavy code).

Correctness assertions run once, **outside** every profiler, and warmup runs
before measurement, so harness bookkeeping and connection setup never pollute
the numbers.

Profiling is not benchmarking: throughput / p50–p99 come from `bench:ab`
(ideally through `bench:proxy` for realistic RTT), never from a profiled run.

To isolate repeated Parse/Describe overhead without giving either side an
unfair advantage, run the same A/B with prepared statements enabled for both
clients:

```bash
PREPARED=1 CONCURRENCY=500 PIPELINE_SIZE=4 BASELINE_POOL=32 \
  bundle exec rake bench:ab
```
