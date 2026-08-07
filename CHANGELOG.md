# Changelog

## [0.3.0] - 2026-08-07

Scheduler-agnostic control plane: the gem no longer depends on the `async` gem
at runtime. Any `Fiber::Scheduler` host works (Async::Scheduler, Itsi::Scheduler,
or another `Fiber.set_scheduler` implementation).

### Changed

- New `PgPipeline::Runtime` primitives (`Notification`, `Queue`, `Semaphore`,
  `Task`, `spawn`, `with_timeout`, `Cancel`) built on `Fiber.scheduler`.
- Driver/pool background work uses `Runtime.spawn` instead of `parent.async`.
- Pinned-connection ownership keys on `Fiber.current` (not `Async::Task.current`).
- Watcher shutdown closes queues + connection, then joins tasks with a timeout.
- Timeouts go through `Timeout.timeout` (correct `timeout_after` arity), never a
  direct one-arg `scheduler.timeout_after` call.
- Closed `Runtime::Queue#enqueue` is a no-op; supervisor sleep errors are recorded
  and `stats` exposes `supervisor_alive`.
- `Client#start` / `Pool#start` / `ConnectionDriver#start` no longer take
  `parent:` (no Async task-tree ownership; use `Runtime.spawn` on the active
  scheduler). Call sites that passed `start(parent: task)` should use `#start`.
- Runtime dependency: only `pg`. `async` is a development dependency for the
  existing Async-based test harness.

### Migration

Hosts must install a Fiber scheduler before `Client#start`. Under Async:

```ruby
require "async"
Sync { client.start; ... }
```

Under Itsi, the server installs `Itsi::Scheduler` for you — just `client.start`.

Apps that previously relied on `pg_pipeline` pulling in `async` transitively
should add `gem "async"` themselves if they still use Async.

## [0.2.5] - 2026-08-03

Control-plane cleanup on the multiplexed query path: fewer per-query allocations,
a cheaper session-guard fast path, slightly tighter result drain, and driver
fill metrics. Wire behaviour and the public API are unchanged.

### Performance

- `SessionGuard` skips comment/literal masking when the SQL has no quotes,
  comments or dollar-quotes; skips the forbidden-pattern scan when the SQL has
  neither `(` nor `into`; and uses a byte scan for multi-statement detection.
- `Request.build` / `PreparedQueryRequest.build` avoid keyword `Class#new` hash
  allocation on the hot path.
- `RequestOps.snapshot_params` reuses a shared frozen empty array and skips
  copying frozen immutable param arrays.
- Default query params use that empty frozen array instead of allocating `[]`
  per call (`Client`, `Session`, `PreparedStatement`).
- `PoolOps.select_driver_into` writes the round-robin cursor into a pool-owned
  slot instead of allocating a `[driver, cursor]` pair per selection.
- `drain_results` matches hot statuses (`TUPLES_OK`, `PIPELINE_SYNC`) first and
  accumulates `results_read` once per drain.

### Observability

- Driver stats add `fast_sync`, `units_per_readable`, `results_per_readable`,
  `flush_calls`, `flush_incomplete`, `dispatches`, and `flush_calls_per_unit`.
- One process-level warning on libpq older than 17 when Sync is coupled to flush
  (`PG_PIPELINE_SILENCE_WARNINGS=1` to suppress).

### Reliability

- Round-robin cursor is normalised with `rr % size` when past the pool length.
- Driver metric readers default to zero if ivars are unset.

### Tests

- Guard fast-path / masking / cache eviction coverage.
- `Request.build` equivalence and prepared-query build coverage.
- Randomised driver-selection vs reference algorithm.
- Drain counter write-back when the loop raises.

## [0.2.4] - 2026-07-31

Request-completion allocation patch. It replaces the general-purpose
`Async::Notification` attached to every multiplexed request with a direct
single-waiter fiber handoff while preserving deferred reactor wakeups and the
existing SQL, Sync, result and failure semantics.

### Performance

- A request now stores only the waiting fiber and its originating scheduler.
  `Scheduler#block` parks that fiber and `Scheduler#unblock` schedules only that
  waiter on a later reactor turn.
- Removed the per-request `Async::Notification` and its initial
  `Thread::Queue`; when a waiter is present, completion also avoids the
  replacement queue and `Async::Notification::Signal` allocation.
- The optimization is limited to one-shot request completion. Bounded-queue and
  pinned-pool notifications retain `Async::Notification` because they require
  multi-waiter coordination.

### Reliability

- Completion-before-wait still returns immediately through the settled guard, so
  no wakeup can be lost.
- Timeout or task cancellation clears the stored waiter in an `ensure`, preventing
  a late query completion from retaining or waking an abandoned fiber.
- A second concurrent waiter is rejected explicitly instead of silently replacing
  the first waiter.
- The scheduler associated with the waiting fiber is stored and used for the
  matching unblock rather than looking up an implicit current scheduler at
  completion time.
- Added focused coverage for wait-before-completion, completion after an
  interrupted wait, the single-waiter invariant and already-settled waits outside
  an active scheduler.

## [0.2.3] - 2026-07-30

Performance-focused release based on CPU profiles from the live pipeline
workload. It removes redundant libpq polling, adds an explicit prepared-
statement hot path, and trims several per-query control-plane costs without
changing the failure model.

### Added

- `Client#prepare(name, sql, param_types = nil)` prepares immutable SQL on every
  currently live pipeline connection and returns a `PreparedStatement` handle.
- `PreparedStatement#query(params = [])` executes through
  `send_query_prepared`, avoiding repeated Parse/Describe work for hot SQL.
- Replacement pipeline connections replay the registered prepared-statement
  catalog before becoming available, including registrations that race with a
  reconnect.
- Prepared-statement unit and live integration coverage, including execution
  across multiple drivers and automatic re-prepare after reconnect.

### Performance

- The connection owner drains results only after `consume_input` actually read
  a readable socket event. Removed unconditional pre/post-dispatch drain passes
  that repeatedly called `PQisBusy` without new input.
- A blocked `PQflush` is retried by the writer watcher instead of again on every
  unrelated owner event. Newly queued output remains buffered in libpq until the
  socket becomes writable.
- Driver selection is now one circular least-load pass: each available driver's
  `load` is read once and equal-load ties rotate from the selected slot.
- The normalized SQL guard mode is reused on the query hot path instead of
  coercing and validating it for every request.
- Already-frozen SQL strings and frozen string bind values are reused rather
  than duplicated; mutable caller input is still snapshotted before submission.
- Prepared-only metadata lives on internal request subclasses, so ordinary
  query requests do not grow extra instance variables for the new API.
- The throughput and A/B harnesses explicitly clear every result and support
  `PREPARED=1`, preparing both pipeline and baseline clients for a fair hot-path
  comparison.

### Semantics

- Prepared statements are explicit client-scoped handles, not an automatic
  unbounded SQL cache. The SQL guard runs once during registration.
- Failed registration is removed from the reconnect catalog. Successfully
  prepared but unreachable internal names may remain on already-prepared
  physical connections until those connections close.
- Multiplexed prepared handles live for the lifetime of the client; this release
  intentionally does not add a concurrent `DEALLOCATE` API.

## [0.2.2] - 2026-07-30

Lifecycle-hardening release. Fixes five pool/task-ownership defects surfaced by
review, adds input validation, and ships deterministic regression coverage for
each fix.

### Fixed (pool & task lifecycle)

- Replacement pipeline drivers are now owned by the pool's stable parent task,
  not the supervisor. Stopping the supervisor during shutdown can no longer
  cascade-cancel a live replacement driver before it is gracefully drained.
- Returning a pinned connection re-checks the closing state after the yielding
  recycle step (`ROLLBACK`/`DISCARD ALL`/reconnect); a connection recycled while
  a concurrent `abort!` runs is closed instead of leaked into the free list.
- Task cancellation during pinned recycle (`Async::Cancel`/`Async::Stop` are
  `Exception`s, not `StandardError`s) is caught solely to close the connection
  and then re-raised, so cancellation semantics are preserved and the connection
  is not leaked.
- Nested `Client#session`/`Client#transaction` on the same task is rejected with
  `RecursiveCheckoutError` before the pinned semaphore is acquired, preventing a
  self-deadlock (`pinned_size: 1`) or a silently different connection
  (`pinned_size > 1`). Use `Transaction#savepoint` for nested atomicity.
- A closing or closed pool is terminal: `Pool#start` raises rather than
  half-restarting onto stale driver state, including after an interrupted close.
  Create a new pool instead.
- Partial startup now also cleans up if spawning the supervisor task fails after
  drivers were created.
- Once shutdown begins, availability checks report `ShutdownError` before the
  generic not-started state, preserving the definitely-not-dispatched failure
  classification for concurrent submitters.

### Changed (hardening)

- Timing options (`reconnect_interval`, `reconnect_backoff_max`,
  `health_interval`, `health_timeout`) reject negative, `NaN`, and infinite
  values; `health_interval` additionally allows `0` ("probe every cycle").
- The supervisor wakes on the shorter of the enabled reconnect/health cadences,
  so a short `health_interval` is honored independently of a large
  `reconnect_interval`, using scheduler-aware `Kernel#sleep` rather than the
  deprecated `Async::Task#sleep` wrapper.
- Reconnect backoff uses floating-point exponentiation and caps non-finite or
  over-limit delays, avoiding giant integers without prematurely flattening the
  backoff for very small base intervals.
- `Transaction#open`/`#savepoint_seq` are no longer publicly writable; control
  state is read via `#open?` and mutated only internally.
- `Session#exec` accepts optional bind parameters (`exec(sql, params)`),
  matching the documented API; any explicitly supplied params value, including
  an empty array, uses `exec_params`, while omitted params keep simple-query mode.
- Driver shutdown attempts every driver even if one close raises or the closing
  task is cancelled, then re-raises cancellation after best-effort cleanup;
  watcher cleanup still stops the writer when stopping the reader fails.
- `Client#close`/`#abort!` now clear client lifecycle state even when terminal pool
  cleanup reports an error, while a rejected re-entrant close leaves the live
  client started.

### Compatibility

- Shutdown cleanup handles `Async::Cancel` explicitly. The supported dependency
  floor (`async 2.42.0`) already defines that class, so no dead compatibility
  fallback is required.
- CI now runs the unit suite against exact `async 2.42.0`, builds ruby-pg against
  source-built client libpq 14.23/16.14/17.10 and runs each build against a live
  PostgreSQL server, plus a separate server integration matrix for 14/16/17/18.

## [0.2.1]

- SessionGuard: bounded per-SQL memo cache for unsafe_reason. Repeated SQL
  (parameterized queries) now costs ~0 allocations instead of ~330 objects/call
  (measured); eliminates the code_only/byteslice line that showed up in profiles.
- Client#query: drop redundant sql dup (Request owns the one immutable copy).
- PoolOps.select_driver: allocation-free least-loaded scan (was 2-3 arrays/call);
  behavior verified identical across 20k random cases.
- bench_kit/metrics.rb + rake bench:metrics: full picture in one run —
  allocations/query + GC pressure, RubyProf process_time (CPU) and allocations,
  RubyProf wall (sanity), and StackProf cpu/wall sampling. Asserts run outside
  every profiler; warmup keeps connection setup out of frame.
- profile_ci_scenario.rb: correctness asserts moved out of the profiled block;
  GC.start before profiling.
- Dev dependency: stackprof (~> 0.2).

## [0.2.0]

Initial experimental implementation.

- Single-owner `ConnectionDriver` with separate readiness watchers.
- Raw nonblocking libpq state machine over ruby-pg.
- FIFO demultiplexing with request completion only at `PGRES_PIPELINE_SYNC`.
- Per-unit Sync to isolate independent implicit transactions/error recovery.
- Real bounded pending queue with shutdown wakeup semantics.
- Future cancellation drains abandoned protocol slots without backend cancel.
- Session-neutral multiplexed path plus separate pinned `session` and
  `transaction` APIs.
- Pinned connections sanitized with rollback-if-needed + `DISCARD ALL`.
- Capabilities keyed from `PG.library_version`/protocol version, not server
  version; libpq 14 minimum, libpq 17 optional fast Sync path.
- Async-native: Ruby >= 3.3, async ~> 2.42, pg >= 1.5 (chosen for what
  maximally simplifies the implementation; not held back to an older floor).
- Opportunistic result draining mirrors libpq's aborted-pipeline readiness
  corner case instead of relying solely on socket-readable notifications.
- Strict result-status whitelist; unsupported streaming/COPY modes fail closed.
- Structured `Client.open` runs inside the caller's Async task instead of
  creating a hidden task lifetime.
- Graceful close rejects re-entrant shutdown from inside a pinned block.
- Deferred `Async::Notification` wakeups avoid re-entrant owner deadlocks on
  the connection owner; CI now exercises the exact async 2.42.0 dependency floor
  in addition to the current lockfile-resolved 2.x release.
- Pinned PostgreSQL 9.3 sessions are physically reconnected after use because
  pre-9.4 `DISCARD ALL` does not clear sequence `currval`/`lastval` state.
- Client use is rejected across thread/scheduler boundaries; shared clients are
  explicitly reactor-local.
- Connection-loss semantics distinguish requests that were never dispatched from
  dispatched units whose commit outcome is indeterminate until Sync is observed.
- String bind values (including the string `:value` in ruby-pg parameter hashes)
  are snapshotted before asynchronous dispatch to avoid caller-side mutation races
  while a request is queued; arbitrary custom mutable encoder objects remain the
  caller's responsibility.
- Graceful shutdown now counts pinned connection establishment as active work,
  closing a race where a yielding connect could outlive pool shutdown.
- Pinned `Session`/`Transaction` handles are fiber-local and expire at block exit,
  so they cannot race from child tasks or use a recycled connection later.
- Queue ownership now starts only after `BoundedQueue` actually accepts a Request;
  a close while a producer is blocked remains definitely pre-dispatch and can
  fail over to a sibling driver.
- `NotDispatchedError` retries always use a fresh Request, including when the
  failed Request was already settled by the losing driver.
- Timed-out idle health probes can abort only while the probe is still the sole
  driver work; a concurrent user request makes the probe inconclusive instead
  of risking an unrelated `IndeterminateResultError`.
- Client-side bind/encoding exceptions raised before `PQsendQueryParams` are
  request-local and no longer tear down the shared connection. `PG::UnableToSend`
  is `NotDispatched`; when the connection remains healthy in pipeline mode that
  rejection is request-local, while other PG send failures stay driver-fatal.
  Failure after the query was accepted but before Sync remains indeterminate.
- CI now separates server-version coverage from source-built client-libpq
  coverage and exercises the libpq 14 and 16 fallback path explicitly.
- `reconnect: false` now remains authoritative even when health checks keep the
  supervisor running; health probing no longer accidentally re-enables reconnect.
- Unexpected supervisor-loop failures are recorded in `stats[:supervisor_error]`
  and the loop keeps running (no longer permanently disables reconnect/health).
- `Transaction#run` / `#savepoint` enforce fiber-local ownership via
  `SessionOps.ensure_active!`; nested `run` is rejected (`already open`).
- Nested-run guard errors no longer trigger the outer transaction's
  `rescue Exception` rollback path (would have ROLLBACK'd the outer BEGIN).
- `TransactionOps` accesses the physical connection only through
  `SessionOps.connection` (private `conn` after Session encapsulation).

### Style

- Refactored to a moderate functional-procedural shape: classes are thin state
  wrappers holding the public API; behavior lives in `*Ops`/module_function
  modules that receive state explicitly (Caps, DriverOps, PoolOps, RequestOps,
  SessionOps, TransactionOps, ClientOps, SessionGuard).
- Implementation comments are kept only where they capture a non-obvious protocol or
  ownership invariant; routine explanatory noise is omitted.
- `loop` retained only where idiomatic (BoundedQueue wait-retry); no `while true`.

### Review response (hardening)

- Reconnect: pool supervisor replaces dead pipeline drivers (backoff); reconnect count in stats.
- BoundedQueue wake-one (no thundering herd under many blocked producers).
- BoundedQueue transfers a wake when a blocked waiter is cancelled after being
  signalled, so free capacity cannot strand behind a dead fiber.
- Observability: client/pool/driver `stats`.
- Safer defaults: max_in_flight 64, max_pending 256, pinned_size 2.
- SessionGuard `:strict` mode + documented holes.
- Transaction savepoints.
- place_sync stays raw-first (send_pipeline_sync on 17+, sync_pipeline_sync on 14-16) + explicit flush.
- Falcon per-worker contract, sizing formula, honest example naming (async_app.rb, falcon_config.ru).
- Packaging: gemspec metadata; CI (unit + PG 14/16/17 matrix); docker-compose.
- Live integration suite under `spec/integration` (env-gated): multiplex, error
  isolation, cancel-drain, savepoints, stats, backpressure-on-close, pinned
  concurrency, driver abort → IndeterminateResultError, reconnect recovery.
- Unit suite aligned with Ops modules; reconnect/backoff and queue cancel-leak covered.

### Second-pass reliability

- Idle health checks: pool probes idle drivers (SELECT 1 + timeout); timeout
  aborts only when the probe is still the sole driver work; health_failures in stats.
- abort! cancels in-flight pinned work via CancelRequest.
- Submit failover: fresh Request per attempt; `NotDispatchedError` can retry
  even when the losing Request was already settled.
- Strict guard precise denylist (no pg_typeof/setup/xact-advisory false positives).
- CI separates server-major coverage from source-built client-libpq 14/16 coverage.
- `bench_kit/` (fiber-storm latency, multi-worker connection-count smoke).
- Pool driver slot arrays self-heal size (reconnect/health safe under partial init).
- Unit coverage: failover, health_probe, pinned cancel on abort.

- Pinned recycle rechecks shutdown after yielding reset/reconnect work so a
  connection cannot be returned to the free list after pool shutdown.
- Replacement pipeline drivers are children of the pool's original parent task,
  not the supervisor; ownership transfer is explicit so cancellation cannot orphan
  a just-created replacement.
- `Session` no longer exposes its raw `PG::Connection`; Ruby-side typemap/callback
  mutations cannot leak through a recycled pinned handle.
- `Transaction` lifecycle (`BEGIN`/`COMMIT`/`ROLLBACK`) is internal; callers cannot
  recursively invoke the outer transaction runner or mutate its open state.
- `Client` lifecycle/reactor ownership state is private, preventing callers from
  disabling close or bypassing the thread/scheduler guard.
- Timing options reject negative, NaN and infinite values; a closed Pool cannot be
  restarted; unknown guard modes fail closed.
- CI YAML is valid, Linux platforms are present in the lockfile, and source-built
  jobs assert the actual linked libpq 14/16 versions rather than confusing server
  version coverage with client capability coverage.
- Documented upstream ruby-pg 1.6.3 parameter-encoding overflow (#719); its fix is
  merged upstream but no newer RubyGems release is available as of July 2026.
- Documented the ordinary pinned-transaction commit-acknowledgement ambiguity: a
  transport failure around `COMMIT` is not proof that the transaction rolled back.
- Re-entrant pinned checkout from the same task now fails immediately instead of
  deadlocking at `pinned_size: 1` or silently using a second independent session.
- Supervisor wake cadence now respects a shorter health interval independently of
  reconnect cadence; `health_interval: 0` keeps probe-on-each-supervisor-cycle
  semantics without introducing a busy loop.
- Driver load accounting includes the narrow dispatching state so routing and
  health checks cannot observe a transient false-idle driver.
- Pinned recycle cleanup now treats `Async::Cancel < Exception` as a cleanup
  boundary: cancellation during rollback/sanitize/reconnect closes the physical
  connection instead of orphaning it outside the pool. Graceful close also closes
  already-idle pinned sockets before waiting for active checkouts.
