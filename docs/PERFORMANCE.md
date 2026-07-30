# Performance tests

Version under test: **`pg_pipeline 0.2.3`**  
Results collected: **2026-07-30 to 2026-07-31**

This document consolidates the performance work performed for `pg_pipeline 0.2.3`.
It includes the in-process benchmark kit, fixed-arrival-rate HTTP SLA tests, raw
closed-loop HTTP throughput, connection-count experiments, RTT amortization, and
post-optimization profiling.

These numbers are a practical reference, not a universal capacity guarantee.
Results depend on CPU, PostgreSQL version and configuration, query cost, network
RTT, Falcon/Ruby versions, connection counts, and workload shape.

## Executive summary

The strongest externally observed result was the closed-loop HTTP saturation test:

| Variant | PostgreSQL connections | Requests/sec | Average | p50 | p95 | p99 | Success |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pg_pipeline` | 16 | **19,002.7** | **52.6 ms** | **49.8 ms** | **86.7 ms** | **108.6 ms** | 100% |
| Direct `pg` pool | 32 | 14,541.1 | 68.7 ms | 68.6 ms | 111.1 ms | 141.8 ms | 100% |

In that single run, `pg_pipeline` delivered about **30.7% more RPS**, with lower
latency, while using half as many PostgreSQL connections. Throughput per server
connection was approximately **1,188 RPS/connection** for the pipeline path versus
**454 RPS/connection** for the direct pool, or about **2.6x greater connection
efficiency**.

The fixed-arrival-rate tests tell a complementary story. At a target of 15,000 RPS,
the pipeline path came very close to the selected SLA while the direct pool was
already heavily queued. At 17,000 and 20,000 target RPS both variants were overloaded,
but the pipeline path consistently completed more work, dropped fewer iterations,
and had lower latency.

## Test environment

The HTTP comparisons used the same local test shape for both variants:

- macOS MacBook Pro host
- load generator running on the same host as Docker Desktop
- Ruby 3.3 container based on `ruby:3.3-slim-bookworm`
- Falcon with 4 workers
- PostgreSQL 16 Alpine in Docker
- identical HTTP route, SQL, row lookup, JSON serialization, and response body
- 100,000 seeded users; each request selected a user by ID
- unprepared SQL for the HTTP comparisons in this document
- `pg_pipeline`: 4 multiplexed connections per worker, 16 active server connections
- direct `pg`: usually 8 ordinary connections per worker, 32 active server connections

Because the database, application, and load generator shared one physical machine,
these tests include contention from Docker Desktop and the load generator itself.
The local database RTT was close to zero, which is the least favorable environment
for demonstrating round-trip amortization.

## 1. Closed-loop maximum HTTP throughput (`oha`)

This test asked each application to run as fast as possible rather than imposing a
preselected arrival rate.

Common command shape:

```bash
oha \
  -z 60s \
  -c 1000 \
  -w \
  -t 10s \
  --no-tui \
  --rand-regex-url \
  'http://127.0.0.1:3000/users/[1-9][0-9]{0,4}'
```

| Metric | `pg_pipeline` (16 conn) | Direct `pg` (32 conn) | Pipeline difference |
|---|---:|---:|---:|
| Requests/sec | **19,002.7** | 14,541.1 | **+30.7%** |
| Completed responses | **1,141,335** | 874,195 | +267,140 |
| Average | **52.6 ms** | 68.7 ms | **-23.4%** |
| p50 | **49.8 ms** | 68.6 ms | **-27.4%** |
| p95 | **86.7 ms** | 111.1 ms | **-22.0%** |
| p99 | **108.6 ms** | 141.8 ms | **-23.4%** |
| Slowest | 1.272 s | **574.1 ms** | worse extreme outlier |
| HTTP 200 | 100% | 100% | equal |

Interpretation: the pipeline path produced a clear raw-throughput gain and better
central/tail latency through p99, but it also recorded a worse single maximum outlier.
This was one run per variant. The pipeline run had an explicit 10-second warmup in
the captured terminal log; the direct run's explicit warmup was not captured. The
database was restarted between variants, so this result should be treated as a
strong indicative measurement rather than a publication-grade multi-run median.

## 2. Fixed-arrival-rate HTTP SLA (`k6`)

The fixed-rate tests used an open-loop workload. Unlike `oha`, k6 attempted to inject
a specified RPS even when the server was already queued.

The selected SLA was:

- HTTP success checks greater than 99.9%
- HTTP error rate below 0.1%
- p95 below 100 ms
- p99 below 250 ms
- dropped iterations below 0.1% of the target arrival rate

All rows below had 0% HTTP failures and 100% successful status checks. A failed SLA
therefore means excessive latency and/or dropped iterations, not incorrect responses.

### Consolidated results

| Target | Variant | Conn | Actual RPS | Dropped | Drop rate | p50 | p95 | p99 | SLA |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| 15,000 | `pg_pipeline` | 16 | **14,979.3** | **1,020** | **0.113%** | **7.87 ms** | **48.37 ms** | **83.23 ms** | Near pass: drops only |
| 15,000 | Direct `pg` | 32 | 14,762.4 | 13,440 | 1.493% | 29.86 ms | 272.37 ms | 439.59 ms | Fail |
| 17,000 | `pg_pipeline` | 16 | **15,855.9** | **33,549** | **3.289%** | **137.05 ms** | **488.08 ms** | **807.52 ms** | Fail / overloaded |
| 17,000 | Direct `pg` | 32 | 15,530.6 | 77,944 | 7.641% | 236.98 ms | 772.08 ms | ~1.10 s | Fail / overloaded |
| 20,000 | `pg_pipeline` | 16 | **17,569.1** | **133,964** | **11.163%** | **232.38 ms** | **391.03 ms** | **910.70 ms** | Fail / overloaded |
| 20,000 | Direct `pg` | 32 | 14,990.2 | 288,248 | 24.021% | 283.85 ms | 617.23 ms | 964.72 ms | Fail / overloaded |

### Pipeline advantage at each offered load

| Target | RPS advantage | Fewer drops | p50 reduction | p95 reduction | p99 reduction |
|---:|---:|---:|---:|---:|---:|
| 15,000 | +1.5% | **92.4%** | **73.6%** | **82.2%** | **81.1%** |
| 17,000 | +2.1% | **57.0%** | **42.2%** | **36.8%** | **~26.6%** |
| 20,000 | **+17.2%** | **53.5%** | **18.1%** | **36.6%** | **5.6%** |

The 15,000 RPS pipeline result missed only the dropped-iteration criterion: the
threshold was below 15 drops/second, while the observed rate was approximately
17 drops/second. The direct pool failed both latency and drop-rate criteria.

At 17,000 and 20,000 RPS, both systems reached the k6 virtual-user ceiling and were
well beyond their sustainable SLA capacity. Those rows measure overload behavior,
not sustainable production throughput.

Each consolidated row is a single run. During the 17,000 RPS run, the direct pool
reached 5,000 active VUs within roughly 3 seconds; the pipeline path reached that
ceiling only around 42 seconds, illustrating slower queue collapse under overload.

## 3. Low-load HTTP sanity check

At 1,000 fixed requests/sec, neither variant was meaningfully saturated:

| Variant | Conn | Actual RPS | Average | p50 | p95 | p99 | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pg_pipeline` | 16 | 999.96 | **1.01 ms** | 0.70 ms | 1.54 ms | **7.71 ms** | 0% |
| Direct `pg` | 32 | 999.94 | 1.07 ms | **0.67 ms** | **1.52 ms** | 9.06 ms | 0% |

At low load the paths are effectively tied, as expected: there is little queueing and
almost no local RTT to amortize.

## 4. In-process A/B benchmark kit

The repository benchmark compares the pipeline client with a naive direct `pg` pool
without HTTP/Rack/Falcon overhead. Workload: 500 concurrent fibers, 5 queries each,
2,500 requests total, pipeline size 4, baseline pool 32.

### Unprepared SQL

| Variant | Conn | RPS | p50 | p95 | p99 | Max |
|---|---:|---:|---:|---:|---:|---:|
| `pg_pipeline` | 4 | **28,012** | 1.0 ms | **5.5 ms** | **8.5 ms** | **9.4 ms** |
| Direct `pg` pool | 32 | 25,908 | 1.0 ms | 30.4 ms | 57.0 ms | 92.3 ms |

Pipeline difference: **+8.1% RPS**, approximately **82% lower p95**, and
approximately **85% lower p99**, using one eighth as many server connections.

### Prepared SQL on both sides

| Variant | Conn | RPS | p50 | p95 | p99 | Max |
|---|---:|---:|---:|---:|---:|---:|
| `pg_pipeline` | 4 | **28,175** | 1.0 ms | **4.1 ms** | **8.2 ms** | **8.5 ms** |
| Direct `pg` pool | 32 | 27,685 | **0.9 ms** | 14.2 ms | 35.5 ms | 46.5 ms |

Pipeline difference: **+1.8% RPS**, approximately **71% lower p95**, and
approximately **77% lower p99**. Prepared statements improved both clients and
narrowed the raw-RPS gap, while the pipeline path retained much lower tail latency
with one eighth as many connections.

### Pipeline-only throughput sanity run

With 2,000 fibers and pipeline size 4:

| Requests | Wall time | Throughput | p50 | p95 | p99 | Max |
|---:|---:|---:|---:|---:|---:|---:|
| 2,000 | 0.15 s | 13,410/s | 0.6 ms | 3.3 ms | 5.5 ms | 6.5 ms |

## 5. RTT amortization

A real-socket demonstration used 64 requests per connection and simulated 0.5 ms of
serial server work per request. It compares a request/reply loop with sending all
requests before waiting for responses.

| RTT | Serial | Pipelined | Speedup |
|---:|---:|---:|---:|
| 0 ms | 53.7 ms | 42.5 ms | 1.3x |
| 2 ms | 217.1 ms | 48.5 ms | 4.5x |
| 10 ms | 845.6 ms | 68.5 ms | 12.3x |
| 30 ms | 2,376.4 ms | 106.4 ms | 22.3x |

This is a protocol-level demonstration, not an HTTP application benchmark. It shows
why localhost testing understates the architectural benefit: a pipeline connection
can hide repeated round trips, but it does not make one PostgreSQL backend execute
multiple statements in parallel.

## 6. Connection-count experiment (historical diagnostic run)

Before the final `0.2.3` harness, a 15,000 RPS fixed-rate test compared one pipeline
configuration with two direct-pool sizes:

| Variant | Conn | Actual RPS | Dropped | Average | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pg_pipeline` | 16 | **14,980.8** | **1,014** | **12.54 ms** | **6.49 ms** | **45.61 ms** | **79.61 ms** |
| Direct `pg` | 64 | 14,943.7 | 3,318 | 20.92 ms | 13.66 ms | 61.46 ms | 103.76 ms |
| Direct `pg` | 32 | 14,629.5 | 21,078 | 61.71 ms | 33.82 ms | 204.67 ms | 341.61 ms |

The direct path improved substantially when its connection count increased from 32
to 64, but the pipeline path still had lower latency and fewer drops with 16
connections. This was a preliminary harness and is retained here as connection-scaling
evidence, not as the primary release benchmark.

A separate earlier 20,000 RPS overload run with a lower 2,500-VU ceiling measured
17,934.6 RPS for the 16-connection pipeline path versus 16,516.5 RPS for the
32-connection direct path. It was superseded by the later 5,000-VU harness shown in
the main SLA table.

## 7. Multi-worker connection occupancy

The four-worker smoke test used pipeline size 4 and pinned size 2. It expected 16 to
24 PostgreSQL connections depending on whether pinned connections had been opened,
and observed exactly 16 active connections. No reconnects or health failures were
reported during the benchmark/profile runs.

## 8. Profiling after the `0.2.3` drain-loop optimization

The `0.2.3` optimization removed redundant result-drain checks after events that had
not consumed new socket input.

In the earlier CPU profile, `PG::Connection#is_busy` accounted for approximately
19.3% of samples. In the post-fix profile it accounted for approximately 7.8%.
The post-fix profile also showed:

- `send_query_params`: 15.7% of CPU samples
- `sync_flush`: 11.2%
- `consume_input`: 8.0%
- `sync_get_result`: 1.7%
- approximately 45.7 allocated Ruby objects per query in the measured scenario

This profiling supports the local control-plane optimization, but sampled percentages
are not throughput benchmarks and should not be compared as absolute performance
between machines.

## What these results do and do not show

They support the following practical claims:

- `pg_pipeline` can deliver higher maximum HTTP throughput than a direct `pg` pool
  in a highly concurrent Falcon workload.
- It can do so with materially fewer PostgreSQL connections.
- Its strongest and most consistent advantage is connection efficiency and lower
  queue/tail latency under concurrency and overload.
- The advantage grows as network RTT increases because more round-trip wait can be
  amortized.

They do **not** show that:

- every application will gain 30% RPS;
- pipeline mode makes a single PostgreSQL backend execute queries concurrently;
- the local Mac/Docker figures are production capacity numbers;
- one-run results are statistically stable medians;
- Ruby/Falcon can match the absolute throughput of native Rust HTTP stacks.

For application-specific decisions, reproduce the benchmark with representative SQL,
result sizes, network RTT, PostgreSQL configuration, and a load generator on a
separate machine.

## Reproducing repository benchmarks

```bash
docker compose up -d pg17
export PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:5417/postgres

bundle exec rake bench:rtt_demo
bundle exec rake bench:throughput
bundle exec rake bench:ab
PREPARED=1 bundle exec rake bench:ab
bundle exec rake bench:smoke
bundle exec rake bench:metrics
```

See [`bench_kit/README.md`](../bench_kit/README.md) for environment variables,
latency injection, profiling outputs, and interpretation guidance.
