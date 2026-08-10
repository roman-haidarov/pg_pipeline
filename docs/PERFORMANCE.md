# Performance tests

Current release under test: **`pg_pipeline 0.4.0`** — section 9 only.

Sections 1-8 are the HTTP A/B of **`0.2.4`** vs **`0.2.3`**, collected
**2026-07-30 to 2026-07-31**. They predate the native data plane and are kept
because the control-plane and connection-efficiency conclusions still hold; do
not read them as 0.4.0 numbers.

This document consolidates the performance work performed for `pg_pipeline 0.2.3`
and the final release A/B for `0.2.4`. The latest HTTP matrices below use the final
60-second measurements for `0.2.3`, `0.2.4`, and the direct `pg` baseline. Older
in-process, RTT, connection-count, and profiling results are retained and explicitly
marked where they were collected before `0.2.4`.

These numbers are a practical reference, not a universal capacity guarantee.
Results depend on CPU, PostgreSQL version and configuration, query cost, network
RTT, Falcon/Ruby versions, connection counts, and workload shape.

## Executive summary

The final closed-loop HTTP saturation comparison used two 60-second `oha` runs per
variant and reports the arithmetic mean:

| Variant | PostgreSQL connections | Requests/sec | Average | p50 | p95 | p99 | Success |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pg_pipeline 0.2.4` | 16 | **21,432.2** | **46.65 ms** | **43.00 ms** | **80.40 ms** | 102.85 ms | 100% |
| `pg_pipeline 0.2.3` | 16 | 20,336.2 | 49.15 ms | 45.80 ms | 83.85 ms | 105.85 ms | 100% |
| Direct `pg` pool | 32 | 16,287.8 | 61.36 ms | 59.16 ms | 85.73 ms | **102.10 ms** | 100% |

Relative to `0.2.3`, the `0.2.4` request-completion primitive delivered about
**5.4% more maximum RPS**, **5.1% lower average latency**, **6.1% lower median
latency**, and **4.1% lower p95**. Relative to the direct pool, `0.2.4` delivered
about **31.6% more RPS** while using half as many PostgreSQL connections.
Throughput per server connection was approximately **1,340 RPS/connection** for
`0.2.4`, **1,271 RPS/connection** for `0.2.3`, and **509 RPS/connection** for the
direct pool. The latest pipeline path therefore achieved about **2.6x greater
connection efficiency** than the direct pool in this workload.

The fixed-arrival-rate tests show the more operationally important result. At a
target of 15,000 RPS, `0.2.4` was the only variant that passed the selected latency
and dropped-iteration thresholds. At 17,000 RPS it still missed the drop threshold,
but kept p95 below 60 ms and required roughly half as many virtual users as `0.2.3`.
The direct pool was already severely queued at 15,000 RPS.

## Test environment

The HTTP comparisons used the same local test shape for all variants:

- macOS MacBook Pro host
- load generator running on the same host as Docker Desktop
- Ruby 3.3 container based on `ruby:3.3-slim-bookworm`
- Falcon with 4 workers
- PostgreSQL 16 Alpine in Docker
- identical HTTP route, SQL, row lookup, JSON serialization, and response body
- 100,000 seeded users; each request selected a user by ID
- unprepared SQL for the HTTP comparisons in this document
- `pg_pipeline`: 4 multiplexed connections per worker, 16 active server connections
- direct `pg`: 8 ordinary connections per worker, 32 active server connections

Because the database, application, and load generator shared one physical machine,
these tests include contention from Docker Desktop and the load generator itself.
The local database RTT was close to zero, which is the least favorable environment
for demonstrating round-trip amortization.

For the final release comparison, each variant was rebuilt and started in its own
Docker Compose run. The `oha` table uses two runs per variant. Each k6 row is one
60-second run, so the direction of the result is stronger evidence than the exact
magnitude of every overload percentage.

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

### Raw runs and two-run means

| Variant | Run 1 RPS | Run 2 RPS | Mean RPS | Mean average | Mean p50 | Mean p95 | Mean p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pg_pipeline 0.2.4` | 21,402.6 | 21,461.8 | **21,432.2** | **46.65 ms** | **43.00 ms** | **80.40 ms** | 102.85 ms |
| `pg_pipeline 0.2.3` | 20,542.4 | 20,130.0 | 20,336.2 | 49.15 ms | 45.80 ms | 83.85 ms | 105.85 ms |
| Direct `pg` pool | 16,364.8 | 16,210.8 | 16,287.8 | 61.36 ms | 59.16 ms | 85.73 ms | **102.10 ms** |

### `0.2.4` change relative to `0.2.3`

| Metric | `0.2.3` mean | `0.2.4` mean | Change |
|---|---:|---:|---:|
| Requests/sec | 20,336.2 | **21,432.2** | **+5.39%** |
| Completed responses/run | 1,221,224 | **1,286,987** | **+5.38%** |
| Average | 49.15 ms | **46.65 ms** | **-5.09%** |
| p10 | 28.35 ms | **26.90 ms** | **-5.11%** |
| p25 | 35.55 ms | **33.25 ms** | **-6.47%** |
| p50 | 45.80 ms | **43.00 ms** | **-6.11%** |
| p75 | 58.50 ms | **55.60 ms** | **-4.96%** |
| p90 | 73.45 ms | **70.15 ms** | **-4.49%** |
| p95 | 83.85 ms | **80.40 ms** | **-4.11%** |
| p99 | 105.85 ms | **102.85 ms** | **-2.83%** |
| p99.9 | **146.35 ms** | 146.80 ms | +0.31% worse |
| p99.99 | **955.5 ms** | 1,199.9 ms | +25.58% worse |
| Slowest | **1.270 s** | 1.609 s | +26.73% worse |

`0.2.4` improved every central latency percentile through p99 and increased raw
throughput. The extreme `oha` tail was worse: p99.99 and the single slowest response
increased. This tail represents roughly one request in ten thousand and did not
repeat in the fixed-rate k6 comparison, where maximum latency improved at both
15,000 and 17,000 target RPS. It remains an observation to monitor rather than a
reason to hide or discard the release result.

Relative to the direct pool, `0.2.4` produced **31.6% more RPS**, **24.0% lower
average latency**, **27.3% lower median latency**, and **6.2% lower p95**, while
using 16 rather than 32 PostgreSQL connections. Direct `pg` had a marginally lower
mean p99 in this particular closed-loop comparison, but materially lower throughput
and worse latency across the rest of the distribution.

## 2. Fixed-arrival-rate HTTP SLA (`k6`)

The fixed-rate tests used an open-loop workload. Unlike `oha`, k6 attempted to inject
a specified RPS even when the server was already queued. The final comparison used
60-second runs at 15,000 and 17,000 target RPS.

The selected SLA was:

- HTTP success checks greater than 99.9%
- HTTP error rate below 0.1%
- p95 below 100 ms
- p99 below 250 ms
- dropped iterations below 0.1% of the target arrival rate

All rows below had 0% HTTP failures and 100% successful status checks. A failed SLA
therefore means excessive latency and/or dropped iterations, not incorrect responses.

### Consolidated final results

| Target | Variant | Conn | Actual RPS | Dropped | Drop rate | Average | p50 | p95 | p99 | Max VUs | SLA |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 15,000 | `pg_pipeline 0.2.4` | 16 | **14,984.7** | **847** | **0.094%** | **12.44 ms** | **6.44 ms** | **44.37 ms** | **80.66 ms** | **1,033** | **Pass** |
| 15,000 | `pg_pipeline 0.2.3` | 16 | 14,961.4 | 1,835 | 0.204% | 14.66 ms | 7.93 ms | 51.34 ms | 82.62 ms | 1,110 | Fail: drops |
| 15,000 | Direct `pg` | 32 | 14,439.6 | 31,259 | 3.473% | 158.08 ms | 103.94 ms | 440.34 ms | 660.35 ms | 5,000 | Fail: drops and latency |
| 17,000 | `pg_pipeline 0.2.4` | 16 | **16,942.1** | **3,300** | **0.324%** | **21.58 ms** | **15.02 ms** | **59.94 ms** | **92.67 ms** | **1,370** | Fail: drops only |
| 17,000 | `pg_pipeline 0.2.3` | 16 | 16,831.7 | 9,638 | 0.945% | 50.14 ms | 36.80 ms | 129.20 ms | 203.65 ms | 2,718 | Fail: drops and p95 |
| 17,000 | Direct `pg` | 32 | 14,088.5 | 158,696 | 15.558% | 334.33 ms | 308.36 ms | 600.62 ms | 850.50 ms | 5,000 | Fail: drops and latency |

### `0.2.4` change relative to `0.2.3`

| Target | RPS change | Fewer drops | Average reduction | p50 reduction | p95 reduction | p99 reduction | Max-VU reduction |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 15,000 | +0.16% | **53.8%** | **15.1%** | **18.8%** | **13.6%** | 2.4% | 6.9% |
| 17,000 | +0.66% | **65.8%** | **57.0%** | **59.2%** | **53.6%** | **54.5%** | **49.6%** |

At 15,000 RPS, `0.2.4` reduced dropped iterations below the selected 0.1% limit and
was the only tested variant to pass the complete SLA. At 17,000 RPS, `0.2.4` still
missed the drop-rate target, but retained acceptable p95 and p99 while `0.2.3` had
already crossed its p95 limit. The direct pool reached the 5,000-VU ceiling at both
targets and showed severe queue collapse.

The open-loop result is the strongest practical evidence for the `0.2.4` completion
change: the raw-RPS improvement is modest, but reducing the fixed per-request
scheduler cost materially delayed backlog growth near saturation. Because each k6
row is one run, the exact 53.8% and 65.8% drop reductions should be treated as the
measured results of this harness, not universal guarantees.

Earlier 20,000-RPS rows are no longer used in the primary release matrix because the
final `0.2.4` A/B focused on longer, directly comparable 15,000- and 17,000-RPS runs.

## 3. Low-load HTTP sanity check (historical `0.2.3`)

At 1,000 fixed requests/sec, neither variant was meaningfully saturated:

| Variant | Conn | Actual RPS | Average | p50 | p95 | p99 | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| `pg_pipeline` | 16 | 999.96 | **1.01 ms** | 0.70 ms | 1.54 ms | **7.71 ms** | 0% |
| Direct `pg` | 32 | 999.94 | 1.07 ms | **0.67 ms** | **1.52 ms** | 9.06 ms | 0% |

At low load the paths are effectively tied, as expected: there is little queueing and
almost no local RTT to amortize.

## 4. In-process A/B benchmark kit (historical `0.2.3`)

These repository benchmark-kit results were collected for `0.2.3` and are retained
as historical in-process evidence. They compare the pipeline client with a naive
direct `pg` pool without HTTP/Rack/Falcon overhead. Workload: 500 concurrent
fibers, 5 queries each, 2,500 requests total, pipeline size 4, baseline pool 32.

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

## 9. Sealed dispatch payloads (0.4)

An earlier iteration of the 0.4 data plane moved send/drain into C but still read
`operation`, `sql`, `params`, `statement_name` and `param_types` back out of the
Ruby `Request` on **every** dispatch, and re-encoded the parameters each time.
The shipped version seals that work into a single arena when the request is
built, so `dispatch` touches no Ruby object at all.

Measured with `bench_kit/dispatch_cost.rb` (`rake bench:dispatch`): 50,000
dispatches, so the number is request encoding + `PQsend*` + Sync and nothing
else. Local PostgreSQL 16 over a Unix socket, shared CI-class machine --
absolute values move between runs, the ratios did not. Collected on Ruby 3.2;
the gem requires >= 3.3, so treat the absolute microseconds as indicative and
the ratios as the result.

> **Methodology note.** The table below was collected with the original version
> of that bench, which sized the in-flight FIFO at `COUNT + 16` and never
> flushed or drained. All 50,000 units therefore piled up in libpq's output
> buffer, and the per-dispatch figure includes memcpy into a buffer growing into
> the tens of megabytes, its reallocations, and a FIFO depth no real driver
> reaches. The bench now flushes and drains every 64 units and times only the
> dispatch window. Both columns moved by the same mechanism, so the **ratios**
> below still stand; the **absolute microseconds do not** and will be lower on
> the current bench. Re-collect before quoting them anywhere.

| parameters | per-dispatch encode | sealed payload | change | objects/dispatch |
| --- | --- | --- | --- | --- |
| 0 | 14.79 us | 12.32 us | -16.8% | 1.00 -> 0.00 |
| 1 | 15.74 us | 14.06 us | -10.7% | 1.00 -> 0.00 |
| 2 | 16.74 us | 14.52 us | -13.2% | 1.00 -> 0.00 |
| 8 | 37.51 us | 23.40 us | -37.6% | 1.00 -> 0.00 |

A second run on a quieter machine showed -11.0%, -3.1% and -45.5% at 1, 2 and 8
parameters. The pattern is what the change predicts: a fixed saving of five
`rb_funcall`s and four `ALLOC_N`s per dispatch, plus a saving proportional to
parameter count from not re-encoding.

### What this does *not* show

End-to-end throughput barely moves. Over a Unix socket with a two-parameter
`SELECT`, 64 concurrent fibers and 20,000 queries:

| | per-dispatch encode | sealed payload | sealed, no Ruby snapshots |
| --- | --- | --- | --- |
| queries/sec | 25,465 | 26,787 | 43,881 |
| allocated objects/query | 13.37 | 13.33 | 11.34 |
| retained malloc bytes/query | 166 | 348 | 348 |

Read `allocated objects/query` as **per query, end to end** -- the `Request`, its
sealed arena, the drained `Result`, the scheduler's bookkeeping and the fiber.
It is not the cost of `#seal!`, which is about three objects, and it is not the
cost of `#dispatch`, which is zero. Those three numbers get conflated easily;
`rake bench:dispatch` now prints the dispatch-only and end-to-end figures in
separate columns for exactly that reason.

Sealing alone is worth roughly +3-5% across runs, which on a single-run
comparison is close to noise. The 43,881 column is **not** an isolated
measurement of snapshot removal: it changes two things at once, on a shared
machine, minutes apart, with a within-build spread of 35k-45k qps. Only the
allocation counts in that table are exact. The third column adds the removal of the pure-Ruby
driver: with nothing left to read `request.params` at dispatch time, the frozen
snapshot layer became dead weight and was deleted, taking two object
allocations per query with it. Absolute throughput between columns is not
directly comparable -- these runs are minutes apart on a shared machine, and the
qps spread within a single build was as wide as 35k-45k -- but the allocation
counts are exact and reproducible.
PostgreSQL itself dominates a local trivial query, so the send path is a small
share of the total. The honest summary:

- the send path itself got 11-46% cheaper, and stopped allocating;
- one arena per request replaces four transient allocations per dispatch, which
  is why retained bytes per query go **up** while allocation *count* goes down;
- expect the end-to-end effect to grow with parameter count, statement size and
  RTT, and to stay small for trivial queries on a local socket;
- if your workload is trivial queries over a Unix socket, the native data plane
  is not the reason to upgrade. The reason is that there is now one
  implementation of the protocol invariants instead of two.

Reproduce both with:

```bash
PG_PIPELINE_URL=postgres://... bundle exec rake bench:dispatch
PG_PIPELINE_URL=postgres://... bundle exec rake bench:throughput
```

## What these results do and do not show

They support the following practical claims:

- `pg_pipeline 0.2.4` delivered higher maximum HTTP throughput than both `0.2.3`
  and a direct `pg` pool in the measured highly concurrent Falcon workload.
- It can do so with materially fewer PostgreSQL connections.
- Its strongest and most consistent advantage is connection efficiency and lower
  queue/tail latency under concurrency and overload.
- The advantage grows as network RTT increases because more round-trip wait can be
  amortized.

They do **not** show that:

- every application will reproduce the observed 31.6% RPS advantage over direct `pg`;
- pipeline mode makes a single PostgreSQL backend execute queries concurrently;
- the local Mac/Docker figures are production capacity numbers;
- the two-run `oha` means or single-run k6 rows are publication-grade statistical medians;
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
bundle exec rake bench:dispatch
bundle exec rake bench:metrics
```

See [`bench_kit/README.md`](../bench_kit/README.md) for environment variables,
latency injection, profiling outputs, and interpretation guidance.
