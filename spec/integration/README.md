# Live integration suite

These specs are **not** run by the default `rspec` task. They require a real
PostgreSQL server and the `pg` + `async` gems installed.

## Run locally

```
docker compose up -d pg18          # or pg14 / pg16 / pg17
PG_PIPELINE_URL=postgres://postgres:postgres@localhost:5418/postgres \
  bundle exec rspec spec/integration
```

Set `PG_PIPELINE_URL` to point at each server in the matrix (14 / 16 / 17 / 18)
to exercise supported server generations. The Sync implementation path is chosen
from the linked **client libpq**, not the server; `.github/workflows/ci.yml` has a
separate source-built client-libpq 14/16/17 matrix that also runs this live suite
for capability coverage.

## Covered by `integration_spec.rb`

- N fibers × K connections: result order preserved
- isolation of a mid-load SQL error to its own unit
- cancel / stop of a waiter still drains its Sync (next query succeeds)
- backpressure: `max_pending` full → `abort!` wakes blocked producers
- pinned: concurrent transactions limited to `pinned_size`
- driver abort mid-flight → `IndeterminateResultError`
- reconnect: dead driver replaced, queries recover (`reconnect: true`)
- explicit prepared statements execute across every pipeline driver
- replacement drivers re-prepare the client catalog before accepting work
- backend terminate + reconnect recovery
- `abort!` cancels in-flight pinned `pg_sleep` quickly
- savepoints on pinned transactions
- `stats` shape (`reconnects`, `health_failures`, pinned in_use); SessionGuard rejects `SET`

## Still manual / future

- `bench_kit/multiworker_smoke.rb` — multi-process connection occupancy
- dedicated live health-probe failure (half-open idle) without manual fault injection
