# pg_pipeline

`pg_pipeline` is an experimental Async/Falcon-oriented PostgreSQL client layer
built on top of `ruby-pg` and libpq pipeline mode.

Its job is deliberately narrow:

```text
many Async fibers -> a few PostgreSQL connections -> libpq pipeline mode
```

It does **not** implement the PostgreSQL protocol, replace `pg`, or make one
PostgreSQL backend execute queries in parallel. libpq/`pg` remain the data plane;
this gem is the Ruby control plane that queues requests, sends them without
waiting for prior results, and routes FIFO results back to the right fibers.

## Status

`0.2.1` is pre-release/experimental. The core state machine is implemented and
the live integration suite passes against libpq 14, 16, and 17. It should not
be used in production without first running the integration suite against the
libpq/PostgreSQL combination you intend to support.

**Known limitation:** `Client#session` and `Client#transaction` use a blocking
(non-pipelined) PG connection. Queries inside a session/transaction block will
block the Async fiber scheduler for their duration. For short OLTP transactions
this is acceptable; avoid long analytical queries inside pinned blocks.

## Requirements

- Ruby `>= 3.3`
- `async ~> 2.42`
- `pg >= 1.5, < 2`
- **libpq >= 14**
- PostgreSQL server using protocol v3 (there is no PostgreSQL-server-14 floor)

Pipeline mode was added to **libpq 14** and is a client-side feature. On libpq
17+, `PQsendPipelineSync` is used as an optimization; libpq 14-16 use the older
`PQpipelineSync` path.

## API

```ruby
require "pg_pipeline"

Async do |task|
  PgPipeline::Client.open(ENV.fetch("DATABASE_URL")) do |client|
    a = task.async { client.query("SELECT $1::int AS n", [1]) }
    b = task.async { client.query("SELECT $1::int AS n", [2]) }

    p a.wait[0] # {"n"=>"1"}
    p b.wait[0] # {"n"=>"2"}
  end
end.wait
```

`Client.open` is deliberately structured: call it from an existing Async/Falcon
task. It does not start a hidden reactor and returns the block value directly. A
`Client` is reactor-local: create one per scheduler/thread and share it only among
fibers running on that same reactor.

There are intentionally three execution paths.

### `Client#query`

Multiplexed shared-session path:

```ruby
result = client.query("SELECT * FROM users WHERE id = $1", [42])
```

Only **independent, session-neutral** operations belong here. Every logical unit
gets its own pipeline `Sync`, so an SQL error in one caller does not share an
implicit transaction/error-recovery segment with the next caller.

`SessionGuard` rejects obvious foot-guns, but it is **not a SQL security parser**.
A user-defined function can mutate session state while looking like a `SELECT`.
The caller is still responsible for the session-neutrality contract.

The guard also rejects reads such as `currval()`/`lastval()` because those expose
sequence state belonging to the shared PostgreSQL session.

### `Client#session`

Exclusive non-pipelined physical session, without an implicit transaction.
Pinned connections are created lazily, so an application that only uses the
multiplexed path does not pay extra PostgreSQL connections at startup:

```ruby
client.session do |session|
  session.exec("SET application_name = 'maintenance'")
  session.prepare("by_id", "SELECT * FROM users WHERE id = $1")
  session.exec_prepared("by_id", [42])
end
```

Use this for session state, DDL/maintenance commands, prepared statements,
LISTEN, or commands that cannot run inside a transaction. Before this physical
connection is returned to another caller, the pool rolls back an accidentally
left-open transaction if necessary. On PostgreSQL 9.4+ it then runs `DISCARD ALL`;
on PostgreSQL 9.3 it reconnects the physical connection because pre-9.4
`DISCARD ALL` does not clear sequence `currval`/`lastval` state.

The `Session` object is valid only inside the block and only from the fiber that
received it. Keeping it after the block, or passing it to another Async task for
concurrent use, raises before touching the physical connection. Re-entering
`Client#session` or `Client#transaction` from the same pinned block is also
rejected instead of deadlocking on `pinned_size: 1` or silently checking out a
second independent session; use `tx.savepoint` for nested transaction scope.

### `Client#transaction`

Exclusive pinned connection with explicit transaction semantics:

```ruby
client.transaction do |tx|
  tx.query("UPDATE accounts SET balance = balance - $1 WHERE id = $2", [10, 1])
  tx.query("UPDATE accounts SET balance = balance + $1 WHERE id = $2", [10, 2])
end
```

Transactions are never multiplexed across callers. The `tx` handle is fiber-local
like `Session`: another fiber cannot call `tx.run` / `tx.savepoint` / `tx.query`.
Nested `tx.run` is rejected (`transaction is already open`); use `tx.savepoint`
for nested atomicity.

A pinned transaction still has the ordinary PostgreSQL **commit-acknowledgement
ambiguity**: if the connection fails while `COMMIT` is being sent or while its
result is being received, the client cannot know from the transport error alone
whether the server committed. Do not blindly retry a non-idempotent transaction
after such a failure; use application-level idempotency/reconciliation when that
outcome matters.

## Cancellation

Cancelling/timing out the waiting Ruby fiber does **not** send PostgreSQL
`CancelRequest`. On a shared backend that would cancel whichever statement is
currently executing, which may belong to another caller.

Instead, the request is marked abandoned. If it has already been dispatched,
the driver still consumes its result and `PGRES_PIPELINE_SYNC` so FIFO routing
remains synchronized, then discards the result. **For mutations this means a
caller timeout does not imply database cancellation: the statement may still
execute and commit.**

Connection loss is split deliberately:

- `NotDispatchedError` means the unit was still on the Ruby side and never
  entered the pipeline;
- `IndeterminateResultError` means it was dispatched but its `Sync` was not
  observed. For writes, do not blindly retry this case: the server may already
  have committed the unit.

## Backpressure

Each pipeline connection has:

- `max_pending` — submitted but not dispatched requests;
- `max_in_flight` — requests already represented in the libpq pipeline.

The pending queue is bounded and closeable so producers blocked by backpressure
are woken when the driver shuts down. libpq still materializes normal results as
whole `PG::Result` objects; row-streaming backpressure is outside the v1 scope.

## Design references

The implementation intentionally uses two references for different layers:

- `tokio-postgres` for the single-owner connection architecture, FIFO response
  ownership and lifecycle ideas;
- PostgreSQL's `src/test/modules/libpq_pipeline/libpq_pipeline.c` and official
  libpq docs for `PQconsumeInput` / `PQisBusy` / `PQgetResult` / flush / sync
  mechanics.

See [DESIGN.md](DESIGN.md) for the invariants and failure model.

## Sizing (read before deploying under Falcon)

Total server connections a deployment opens:

```
max_conns = workers × (pipeline_size + pinned_size)
```

PostgreSQL's `max_connections` (often 100) is a hard ceiling. With the defaults
(`pipeline_size: 4`, `pinned_size: 2`) each worker opens **6** connections, so 16
Falcon workers already need 96. Start small and measure:

| Deployment | pipeline_size | pinned_size | per worker |
|---|---:|---:|---:|
| small web app | 2 | 1 | 3 |
| default | 4 | 2 | 6 |
| heavy pinned/tx use | 4 | 4 | 8 |

If `workers × (pipeline_size + pinned_size)` approaches `max_connections`, put
pgbouncer (transaction pooling) in front, or lower the per-worker sizes. Pipeline
multiplexing lets a **small** `pipeline_size` carry high RPS, so prefer 2–4.

Tunable backpressure defaults (lowered for fiber-storm safety): `max_in_flight: 64`,
`max_pending: 256`. Raise `max_in_flight` only if results are small; wide result
sets multiply memory by in-flight count.

## Falcon: one Client per worker

A `Client` is **reactor-local**: bound to the thread + `Fiber.scheduler` that
called `#start`, and it fails fast if used from another. Build one Client per
Falcon worker (after fork), never in the master, and share it across that
worker's request fibers. See `examples/falcon_config.ru`.

## Reconnect

Dead pipeline drivers are replaced automatically by a per-pool supervisor
(`reconnect: true` by default) with exponential backoff up to
`reconnect_backoff_max`. While a slot is down the pool routes to the surviving
drivers; if all are down, `#query` raises `NotDispatchedError` (safe to retry)
until a replacement connects. `client.stats[:reconnects]` counts replacements.
Pinned-connection failures are handled per-checkout (reset, else replace, else
mark the pinned pool errored).

## Observability

```ruby
db.stats
# => { pipeline: { size:, live:, drivers: [{ available:, load:, pending:,
#                                            in_flight:, submitting:, needs_flush: }, ...] },
#      pinned: { size:, active:, free:, in_use: },
#      reconnects:, health_failures:, closing:, pinned_error: }
```

`load` (pending + in_flight + submitting) per driver is your head-of-line signal.

## Head-of-line blocking

A single slow query occupies its connection's FIFO until its Sync arrives,
delaying units queued behind it on that connection. Keep long/reporting queries
off the multiplexed path (use `#session` on a dedicated connection), watch
per-driver `load`, and size `pipeline_size` so one slow unit cannot starve RPS.

## Guard modes

`#query` accepts only session-neutral SQL. `SessionGuard` is a foot-gun guard,
**not** a security boundary — a user-defined function can still call `SET`,
create temp tables, etc. Known holes: UDFs, dynamic SQL. For stricter filtering:

```ruby
PgPipeline::Client.new(url, guard: :strict)
```

`:strict` additionally rejects sequence mutation and a small denylist
(`nextval`/`setval`/`pg_export_snapshot`, …) without broad `pg_*(` / `set*(`
false positives — `pg_typeof`, `setup(`, and transaction-scoped
`pg_advisory_xact_lock` stay allowed. Anything stateful belongs on `#session` /
`#transaction`.

## Failure model (do not auto-retry blindly)

- `NotDispatchedError` — request never reached the wire. Retry is protocol-safe.
- `IndeterminateResultError` — request was dispatched but its Sync was not seen.
  A mutating statement may or may not have committed. **Do not** auto-retry
  non-idempotent work; reconcile instead.

## Transactions and savepoints

```ruby
db.transaction do |tx|
  tx.query("UPDATE ...", [...])
  tx.savepoint do |sp|
    sp.query("INSERT ...", [...])
  end
end
```

`#query` (multiplexed) forbids `BEGIN`; use `#transaction` (pinned). COPY and
protocol-level query cancellation are not offered on the multiplexed path.

## Idle health checks

Long-lived Falcon workers can accumulate half-open TCP connections (NAT/firewall
idle drops, a PostgreSQL restart while a driver was idle) that produce no socket
event and would otherwise sit "available" but wedged. The pool supervisor
actively probes **idle** drivers with a short `SELECT 1` (`health_check: true`,
`health_interval: 10s`, `health_timeout: 5s`).

A timed-out probe aborts the driver only while that probe is still the driver's
**sole** work. If user work arrived behind the probe, the check becomes
inconclusive and is abandoned without aborting the shared connection; a
background liveness check must never make another caller's write indeterminate.
`client.stats[:health_failures]` counts probes that safely proved the driver
unhealthy and triggered its shutdown.

`reconnect: false` is respected independently from health checking: probes may
still detect/close an unhealthy driver, but the supervisor will not replace it.
Unexpected supervisor-loop errors are recorded in `client.stats[:supervisor_error]`;
one bad iteration does not silently disable future health checks/reconnect
attempts.

## Submit failover

If the pipeline driver chosen for a request dies in the window between selection
and submit — a definitely-not-dispatched failure — `#query` builds a **fresh**
Request and retries on a live sibling (bounded by `pipeline_size`).
`NotDispatchedError` is retryable by definition even if the failed Request was
already settled; the old Request itself is never re-used. Once every driver is
down it still raises `NotDispatchedError` (safe to retry) until reconnect
restores a slot.

A `PG::UnableToSend` raised before libpq accepts a unit is also surfaced as
`NotDispatchedError`. If the physical connection and pipeline are still healthy,
that rejection is request-local and does not make already in-flight sibling
writes indeterminate. Other `PG::Error` send failures remain driver-fatal.

## Abort and pinned work

`#abort!` cancels in-flight pinned work: it sends `CancelRequest` to every
checked-out pinned connection (`cancel_pinned_on_abort: true`) so a fiber blocked
on a long query or `pg_sleep` unwinds instead of hanging the shutdown. The
multiplexed path never issues `CancelRequest` (it would hit whatever the backend
is currently running, i.e. possibly another caller's query); a cancelled
multiplexed waiter is only a future-cancel that still drains its Sync.

## Benchmarks and profiling

Everything lives in **`bench_kit/`** (see `bench_kit/README.md`).

```bash
bundle exec rake bench:list

# no PG — shows why RTT matters for pipelining
bundle exec rake bench:rtt_demo

# with PG
docker compose up -d pg17
export PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:5417/postgres
bundle exec rake bench:throughput   # thrpt / p99
bundle exec rake bench:smoke        # multi-worker conns
bundle exec rake bench:profile      # RubyProf → tmp/bench_kit/

# realistic RTT A/B (two terminals)
bundle exec rake bench:proxy RTT_MS=10 UPSTREAM=127.0.0.1:5417
PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:6432/postgres \
  bundle exec rake bench:ab
```

| Task | Role |
|---|---|
| `bench:rtt_demo` | mechanism demo (no PG) |
| `bench:proxy` | inject RTT in front of PG |
| `bench:throughput` | fiber thrpt / latency |
| `bench:ab` | pipeline vs naive pool + `server_conns` |
| `bench:smoke` | multi-process occupancy |
| `bench:profile` | RubyProf CI-shaped paths → `tmp/bench_kit/` |

Localhost (~0 RTT) hides pipelining wins; use `bench:proxy` for realism.
RubyProf is for call graphs, not absolute thrpt (it perturbs Async).

## Upstream ruby-pg note

As of July 2026 the latest released `pg` is 1.6.3. Upstream issue #719 reports a
heap-buffer-overflow while encoding extremely large bind-parameter sets in that
release; the fix was merged to `ruby-pg` master in June 2026 but has not yet
shipped as a newer RubyGems release. Until a fixed release is available, do not
allow untrusted callers to construct unbounded multi-gigabyte parameter arrays;
deployments with that exposure should build `pg` from an upstream revision that
contains the fix.

## libpq 14–16 sync note

On libpq 17+ the per-unit sync is `send_pipeline_sync` (queue Sync without an
immediate flush) followed by one explicit `sync_flush`. On libpq 14–16 there is
no flush-decoupled sync primitive; `sync_pipeline_sync` (PQpipelineSync) flushes
internally, so the model there is "sync (with its own flush) + our flush" — the
extra flush is a cheap no-op. This is a libpq-version limitation, not a design
choice, and it does not change sync-per-unit semantics.
