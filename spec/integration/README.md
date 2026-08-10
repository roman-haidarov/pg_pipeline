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
to exercise supported server generations. CI uses the runner's distro **client**
libpq for all jobs; the Sync path (fast `PQsendPipelineSync` vs flush-coupled
`PQpipelineSync`) follows whatever that client is, not the server major.

Specs that reach into `PgPipeline::Native` are tagged `:native_only` and live in
`native_data_plane_spec.rb` (and native-only unit files). Keep shared behavioural
coverage in `integration_spec.rb` so both paths stay readable.

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

## Covered by `native_data_plane_spec.rb` (`:native_only`)

- bound / non-ASCII / binary parameters round-trip through the sealed arena
- server-side error stays request-local
- dispatch reads no Ruby accessor on the request
- many completed units per socket wakeup (`units_per_readable > 1`)
- hot-path counters come straight from the C driver
- `PGresult` size is reported to the GC and given back on `#clear`
- ruby-pg row spellings (`each_row`, `num_tuples`, `num_fields`) on the native result
- per-driver `:encoding` against the process-wide `:seal_encoding`

## Still manual / future

- `bench_kit/multiworker_smoke.rb` — multi-process connection occupancy
- dedicated live health-probe failure (half-open idle) without manual fault injection
- fault injection beyond `pg_terminate_backend`: half-open TCP, packet loss and a
  server restart mid-burst are not exercised. `bench_kit/latency_proxy.rb` is the
  obvious place to grow this from, since it already sits in the connection path.
- multi-encoding: the mismatch warning and the dispatch-time
  `UnsupportedServerError` are unit-covered, but no live job runs two servers
  with different `client_encoding` in one process.
