# pg_pipeline

PostgreSQL pipeline multiplexing for Ruby — inspired by [tokio-postgres](https://github.com/sfackler/rust-postgres).

## What is PostgreSQL pipeline mode?

Normally every query follows a request/reply cycle:

```
fiber → SEND query → wait RTT → RECV result → done
```

With [libpq pipeline mode](https://www.postgresql.org/docs/current/libpq-pipeline-mode.html) (libpq ≥ 14)
the client can send multiple queries without waiting for each result first:

```
fiber A → SEND query A ─┐
fiber B → SEND query B ─┤─── one RTT ───┐
fiber C → SEND query C ─┘               ├─ RECV A, B, C
                                         └─ route back to fibers
```

At 10 ms RTT a naïve pool does ~100 queries/s per connection.  
A pipelined connection can do thousands — the wire stays full instead of sitting idle.

`pg_pipeline` is the fiber control plane that does exactly this: it multiplexes
independent queries from many fibers onto a small pool of libpq connections,
routes FIFO results back to the right fiber, and keeps transactional/session work
on separate pinned connections. Wire protocol work stays in libpq. Version 0.4
runs the multiplexed data plane (request state, send/flush/drain, `PGresult`
ownership) in a C extension while the owner/watcher tasks and pool stay on
`PgPipeline::Runtime` + any `Fiber::Scheduler`. Pinned sessions and explicit
transactions continue to use ruby-pg.

The single-owner connection model, FIFO result ownership, and lifecycle approach
are directly inspired by tokio-postgres.

## Any Fiber scheduler, not just Async

Up to 0.2.x the control plane was built on the `async` gem: `Async::Task`,
`Async::Queue`, `Async::Semaphore`, and a task tree rooted in Async's reactor.
That made Falcon the only realistic host.

0.3.0 removes that. The control plane is built on Ruby's `Fiber::Scheduler`
interface — `Fiber.schedule` plus the scheduler's `block` / `unblock` — and on
nothing else. The host installs whichever scheduler it likes; the gem never
installs one and never calls a scheduler hook directly. `async` is now a
development dependency only, and the sole runtime dependency is `pg`.

```ruby
# Falcon / any Async host — unchanged, still works
Async do
  client = PgPipeline::Client.open(ENV["DATABASE_URL"])
  client.query("SELECT * FROM users WHERE id = $1", [id]).first
end

# Itsi, with its own scheduler — no Async anywhere
# Itsi.rb:
#   fiber_scheduler "Itsi::Scheduler"
client = PgPipeline::Client.open(ENV["DATABASE_URL"])
client.query("SELECT * FROM users WHERE id = $1", [id]).first
```

The call site does not change: `query` blocks the *fiber*, not the thread, so
application code reads synchronously with no `await` and no coloured functions.
The only hard requirement is that some scheduler is installed on the current
thread — under a web server running requests in `Fiber.schedule` that is free,
while a plain script or rake task must set one up itself.

### What it buys

On our benchmark stand (4 workers, `oha -z 60s -c 1000`, single-row lookup by
primary key, local PostgreSQL 16):

| gem | server | scheduler | req/s |
|---|---|---|---:|
| 0.2.5 | Falcon | Async | 23254 |
| 0.3.0 | Falcon | Async | 23007 |
| 0.3.0 | Itsi | Async | 29206 |
| 0.3.0 | Itsi | Itsi::Scheduler | 29350 |

Falcon throughput is unchanged — this was a portability change, not a
Falcon optimisation. The gain comes from being *able* to move: an Itsi host is
roughly **26% faster than the 0.2.5 Falcon baseline**, and that configuration
simply could not run before, because 0.2.5 required an Async reactor.

Numbers from one stand on one machine; treat them as a direction, not a
guarantee. Your own ratio depends on payload size, RTT, and how much of the
request is spent outside the database.

## Installation

```ruby
# Gemfile
gem "pg_pipeline"
```

Requires Ruby ≥ 3.3, `pg ≥ 1.5`, and **libpq ≥ 14** at runtime. `async` is a
development dependency: the gem needs *some* `Fiber::Scheduler` installed on the
thread, and does not care which. Pick one yourself if your host does not provide
one.

There is no precompiled platform gem. `pg_pipeline` ships a C extension, so
installing it builds from source and needs a C compiler and the libpq
development headers (`libpq-dev` on Debian/Ubuntu, `libpq` from Homebrew on
macOS, `postgresql-devel` on RHEL). Point `PG_CONFIG` at a specific `pg_config`
if you have more than one libpq installed. Plan for this in image builds and in
any environment that installs gems without a toolchain.

**libpq ≥ 17 is recommended for maximum local throughput.** Below 17 there is no
`PQsendPipelineSync`, so `PQpipelineSync` couples Sync with a flush and the driver
cannot batch writes across a dispatch burst. Correctness is identical and RTT
amortisation — the main reason to pipeline — still works on libpq 14; only local
throughput is capped. The driver detects this at connect time and warns once per
process; set `PG_PIPELINE_SILENCE_WARNINGS=1` to suppress it, and check
`db.stats[:pipeline][:drivers].first[:fast_sync]` to see which path is active.

## Usage

### Multiplexed queries

```ruby
require "pg_pipeline"

Sync do |task|
  PgPipeline::Client.open(ENV["DATABASE_URL"]) do |db|
    # 20 queries fly out in parallel over a handful of connections
    rows = (1..20).map do |id|
      task.async do
        result = db.query("SELECT $1::int AS id, now() AS at", [id])
        begin
          result.first
        ensure
          result.clear
        end
      end
    end.map(&:wait)

    p rows.first # => {"id"=>"1", "at"=>"..."}
  end
end
```

`Client.open` manages the pool lifecycle. `db.query` is fiber-safe and multiplexed —
every fiber yields while waiting, the event loop stays unblocked.

### Results are yours to clear

`query` and `PreparedStatement#query` return a `PgPipeline::Result` that owns a
libpq `PGresult` living outside the Ruby heap. The GC is told how big that is and
will free it eventually, but "eventually" is the wrong schedule for a result set
of any size: call `#clear` as soon as you have taken what you need, ideally in an
`ensure`. `#clear` is idempotent, and reading a cleared result raises
`ProtocolError` rather than returning stale rows.

The row and metadata surface is the one you already know — `#first`, `#each`,
`#each_row`, `#to_a`, `#values`, `#ntuples`/`#num_tuples`, `#nfields`/`#num_fields`,
`#fields`, `#getvalue`, `#cmd_tuples`, `#error_message`, `#error_field`. Two
things do **not** carry over from `PG::Result`, see the 0.4.0 CHANGELOG:
`is_a?(PG::Result)` is false, and ruby-pg's type maps (`map_types!`,
`PG::BasicTypeMapForResults`) are not available — the multiplexed path always
requests text-format results, so values arrive as strings.

### Multiplexed prepared statements

For repeated SQL, prepare it once on every pipeline connection and execute the
returned immutable handle:

```ruby
by_id = db.prepare(
  "user_by_id",
  "SELECT id, email, name FROM users WHERE id = $1",
  [23] # optional PostgreSQL parameter OIDs
)

result = by_id.query([42])
begin
  p result.first
ensure
  result.clear
end
```

`Client#prepare` returns only after the statement is ready on every currently
live pipeline connection. Replacement connections automatically prepare the
registered catalog before they begin accepting requests. The logical name is
client-local; the gem uses private generated server names on its owned connections.

Prepared handles are intentionally explicit rather than an unbounded automatic
SQL cache. The SQL guard runs once at preparation time, while each execution
still snapshots its bind values before submission.

### Transactions

```ruby
db.transaction do |tx|
  tx.exec("INSERT INTO orders(user_id, total) VALUES ($1, $2)", [user_id, total])
  tx.exec("UPDATE balances SET reserved = reserved + $1 WHERE user_id = $2", [total, user_id])
end
# COMMIT on success, ROLLBACK on any exception
```

Nested atomicity with savepoints:

```ruby
db.transaction do |tx|
  tx.exec("INSERT INTO events(kind) VALUES ('started')", [])
  tx.savepoint do |sp|
    sp.exec("INSERT INTO risky_table(x) VALUES ($1)", [val])
    # raises → rolls back to savepoint only, outer tx survives
  end
end
```

### Session (DDL, LISTEN, SET, connection-local prepared statements)

```ruby
db.session do |s|
  s.exec("SET application_name = 'worker-1'")
  s.exec("PREPARE by_id AS SELECT * FROM users WHERE id = $1")
  rows = s.exec_prepared("by_id", [42])
end
```

The session gets an exclusive pinned connection. It's automatically cleaned up
(`DISCARD ALL`) before the connection returns to the pool.

### Pool configuration

```ruby
PgPipeline::Client.open(
  ENV["DATABASE_URL"],
  pipeline_size: 4,   # pipelined connections per client
  pinned_size:   2,   # exclusive connections for session/transaction
  guard:         :strict
) do |db|
  # ...
end
```

Total PostgreSQL connections per process: `pipeline_size + pinned_size`.

### Observability

```ruby
db.stats
# => {
#   pipeline: { size: 4, live: 4, drivers: [{load: 3, in_flight: 2, ...}, ...] },
#   pinned:   { size: 2, active: 1, free: 1 },
#   prepared_statements: 1,
#   reconnects: 0
# }
```

`load` (pending + in-flight + submitting + dispatching) per driver is the routing/head-of-line signal.

Each driver also reports how well the pipeline is filling:

```ruby
db.stats[:pipeline][:drivers].first
# => { ..., fast_sync: true,
#      units_per_readable: 7.9, results_per_readable: 22.2,
#      flush_calls_per_unit: 0.17, flush_incomplete: 0 }
```

`units_per_readable` is the number to watch. Around `1.0` means every query pays a
full socket wait plus scheduler round trip and the pipeline is not filling — that is
the expected shape for a single fiber issuing one query at a time, and no amount of
Ruby-side optimisation will change it. Values well above `1.0` mean one reactor
wakeup is amortised over many queries, which is the regime pipelining is for. Check
this before attributing a throughput number to control-plane cost.

The numbers above are from a saturated HTTP benchmark (4 workers, 4 connections
each, ~20k requests/second against a local server): roughly eight queries per
reactor wakeup and one flush per six queries. `results_per_readable` runs at
three times `units_per_readable` because a completed unit yields three protocol
results -- the data, the query boundary and the Sync.

**That 7.9 is not what you will see from `rake bench:throughput`.** A fiber storm
against a local socket typically sits just above `1.0`: with no round-trip to
wait through, each query is answered before the next one is submitted, so there
is nothing to batch and the pipeline never fills. Both numbers are real; they
measure different regimes. If you are evaluating this gem, the question is which
regime your production traffic is in, and the answer is set by your RTT and your
concurrency, not by the driver. Inject latency (`rake bench:proxy`) before
concluding anything from a localhost throughput run.

## Head-of-line blocking

Results on a pipelined connection arrive in FIFO order, and PostgreSQL offers no
safe way to cancel one request out of a multiplexed pipeline (see `DESIGN.md` §7).
Two consequences worth designing around:

- **A slow query delays everything behind it on the same connection.** With the
  default `max_in_flight: 64`, one multi-second query can hold up to 63 unrelated
  queries on that driver. Driver selection balances by queue depth, not by expected
  cost, so a slow query counts the same as a fast one.
- **A timeout is not a cancellation.** Wrapping `db.query` in `with_timeout` returns
  control to your fiber, but the unit stays in the pipeline until the server answers
  it, and the requests behind it still wait.

If your workload mixes fast and slow queries, prefer one of:

- lower `max_in_flight` so a stall cannot capture a deep queue;
- a second `Client` with its own connections for the slow queries;
- `Client#session` for anything long-running, which uses an exclusive pinned
  connection and cannot block multiplexed traffic.

## What the session guard does and does not catch

Multiplexed queries share a connection, so anything that mutates session state
would leak between unrelated fibers. `SessionGuard` refuses those before they
reach the wire. It is a **policy scanner over masked SQL, not a parser**, and the
distinction matters when you decide how much to lean on it.

It masks string literals, dollar-quoted bodies, `E''` escapes and both comment
forms before scanning, so the checks cannot be fooled by hiding a keyword in a
literal or by splitting a call across a comment. It then refuses:

| Reason | Example |
|---|---|
| `leading:<kw>` | anything not starting `SELECT`/`INSERT`/`UPDATE`/`DELETE`/`MERGE`/`VALUES`/`WITH` — so `SET`, `SHOW`, `DISCARD`, `PREPARE`, `DECLARE`, `COPY`, DDL |
| `multiple-statements` | `SELECT 1; SET application_name = 'x'` |
| `set_config`, `setseed`, `currval`, `lastval` | including `pg_catalog.set_config(…)` and `"set_config"(…)` |
| `session-advisory-lock` / `-unlock` | the session-scoped `pg_advisory_*` family (transaction-scoped `pg_advisory_xact_*` is allowed — it releases at the unit's Sync) |
| `dblink-session` | `dblink_connect` / `dblink_disconnect`: named connections outlive the unit |
| `large-object` | `lo_open`/`lo_import`/`loread`/… : descriptors are session- and transaction-scoped |
| `select-into-temp`, `select-into-pg-temp` | `SELECT … INTO TEMP t` |
| `unicode-escaped-identifier`, `uescape` | `U&"pg_advisory_\006Cock"(1)` — the escaped form resolves to a name the scanner cannot see, so it is refused rather than guessed at |
| `strict:nextval`, `strict:setval`, `strict:pg_export_snapshot` | `guard: :strict` only |

What it **cannot** see, by construction:

- **Side effects inside a function you call.** `SELECT my_report(1)` is
  session-neutral as far as the scanner is concerned; if `my_report` does a `SET`
  or takes an advisory lock internally, that lands on a shared connection. No
  scanner can resolve this without the catalog.
- **Sequence advances reached indirectly.** `INSERT INTO t(name) VALUES ($1)`
  against a `serial` column advances the sequence and sets `currval` on whichever
  connection ran it. The blast radius is contained — `currval` and `lastval` are
  themselves refused on the multiplexed path, so no fiber can read the wrong one
  — but use `RETURNING id` rather than reasoning about sequence state.
- **Cost.** `SELECT pg_sleep(30)` is perfectly session-neutral and will block
  everything queued behind it on that connection. See *Head-of-line blocking*.

Treat the guard as a guard rail against a mistake in your own code, not as a
security boundary: if an attacker controls the SQL string rather than the bind
parameters, you have already lost. When you legitimately need session state, use
`Client#session` — that is what the exclusive pinned connections are for.

## Failure model

| Error | Meaning | Retry safe? |
|---|---|---|
| `NotDispatchedError` | query never reached the wire | ✅ yes |
| `IndeterminateResultError` | query was sent, Sync not observed | ⚠️ only if idempotent |
| `IndeterminateCommitError` | `Client#transaction`'s `COMMIT` acknowledgement was lost while the connection itself was gone/broken | ⚠️ never — the transaction may have committed |
| `UnsafeMultiplexError` | session-mutating SQL on multiplexed path | — fix the call site |

`IndeterminateCommitError < IndeterminateResultError`. It is raised only when the
pinned connection looks dead (`PG::ConnectionBad`, `finished?`, or a non-OK
status) at the moment `COMMIT` fails. If `COMMIT` fails while the connection is
still healthy, that's the server giving a complete, unambiguous answer — the
original `PG::Error` is raised as-is, and the transaction did not commit.

Cancelling a fiber does **not** send `CancelRequest` to PostgreSQL — the query may
still execute. For mutations, do not retry blindly after a timeout.

## Falcon integration

One `Client` per worker, created after fork:

```ruby
# falcon.rb / config.ru
app = Rack::Builder.new do
  use MyMiddleware
  run MyApp
end

Async do
  db = PgPipeline::Client.new(ENV["DATABASE_URL"]).start
  run app  # fibers in this worker share db
end
```

See `examples/falcon_config.ru` for a complete setup.

## Benchmark

[Performance test results and methodology](docs/PERFORMANCE.md).

```bash
docker compose up -d pg17
export PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:5417/postgres

bundle exec rake bench:throughput   # fibers/s, p99
bundle exec rake bench:ab           # pipeline vs naive pool
```

For realistic numbers inject latency first:

```bash
# terminal 1
bundle exec rake bench:proxy RTT_MS=10 UPSTREAM=127.0.0.1:5417

# terminal 2
PG_PIPELINE_URL=postgres://postgres:postgres@127.0.0.1:6432/postgres \
  bundle exec rake bench:ab
```

At 10 ms RTT, pipeline mode delivers ~7× better p99 with 8× fewer server connections.

## License

MIT
