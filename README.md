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

`pg_pipeline` is the Ruby control plane that does exactly this: it multiplexes
independent queries from many Async fibers onto a small pool of libpq connections,
routes FIFO results back to the right fiber, and keeps transactional/session work
on separate pinned connections. All wire protocol work stays in libpq; zero C code here.

The single-owner connection model, FIFO result ownership, and lifecycle approach
are directly inspired by tokio-postgres.

## Installation

```ruby
# Gemfile
gem "pg_pipeline"
```

Requires Ruby ≥ 3.3, `async ~> 2.42`, `pg ≥ 1.5`, and **libpq ≥ 14** at runtime.

## Usage

### Multiplexed queries

```ruby
require "pg_pipeline"

Sync do |task|
  PgPipeline::Client.open(ENV["DATABASE_URL"]) do |db|
    # 20 queries fly out in parallel over a handful of connections
    results = (1..20).map do |id|
      task.async { db.query("SELECT $1::int AS id, now() AS at", [id]) }
    end.map(&:wait)

    p results.first # => [{"id"=>"1", "at"=>"..."}]
  end
end
```

`Client.open` manages the pool lifecycle. `db.query` is fiber-safe and multiplexed —
every fiber yields while waiting, the event loop stays unblocked.

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

## Failure model

| Error | Meaning | Retry safe? |
|---|---|---|
| `NotDispatchedError` | query never reached the wire | ✅ yes |
| `IndeterminateResultError` | query was sent, Sync not observed | ⚠️ only if idempotent |
| `UnsafeMultiplexError` | session-mutating SQL on multiplexed path | — fix the call site |

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
