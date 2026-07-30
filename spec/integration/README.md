# Live integration suite

These specs are **not** run by the default `rspec` task. They require a real
PostgreSQL server and the `pg` + `async` gems installed.

## Run locally

```
docker compose up -d pg17          # or pg14 / pg16
PG_PIPELINE_URL=postgres://postgres:postgres@localhost:5417/postgres \
  bundle exec rspec spec/integration
```

Set `PG_PIPELINE_URL` to point at each server in the matrix (14 / 16 / 17+) to
prove the libpq 14–16 `pipeline_sync` path and the 17+ `send_pipeline_sync`
path both behave identically.

## Covered by `integration_spec.rb`

- N fibers × K connections: result order preserved
- isolation of a mid-load SQL error to its own unit
- cancel / stop of a waiter still drains its Sync (next query succeeds)
- backpressure: `max_pending` full → `abort!` wakes blocked producers
- pinned: concurrent transactions limited to `pinned_size`
- driver abort mid-flight → `IndeterminateResultError`
- reconnect: dead driver replaced, queries recover (`reconnect: true`)
- backend terminate + reconnect recovery
- `abort!` cancels in-flight pinned `pg_sleep` quickly
- savepoints on pinned transactions
- `stats` shape (`reconnects`, `health_failures`, pinned in_use); SessionGuard rejects `SET`

## Still manual / future

- `benchmarks/multiworker_smoke.rb` — multi-process connection occupancy
- client **libpq** 14 vs 17 path (CI matrices **server** majors; runner libpq is one)
- dedicated live health-probe failure (half-open idle) without manual fault injection
