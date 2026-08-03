# pg_pipeline design

## 1. Scope

`pg_pipeline` is a driver-adjacent Ruby control plane over `ruby-pg`. It does not
parse the wire protocol and does not replace libpq.

The target workload is many concurrent Async/Falcon fibers issuing independent
small/medium PostgreSQL operations where RTT and/or connection count matter.
Pipeline mode does not parallelize execution inside one PostgreSQL backend.

## 2. Non-negotiable ownership invariant

```text
one ConnectionDriver == one PG::Connection == one task that calls it
```

The owner task is the only code allowed to call `PG::Connection` methods.
Reader/writer watcher tasks only wait on the memoized socket IO and enqueue
readiness events.

This avoids concurrent mutation of libpq protocol state while still giving the
owner three wakeup sources:

```text
submitted request ----\
socket readable -------+--> event queue --> OWNER --> PG::Connection
socket writable -------/
```

## 3. Raw nonblocking mode

At startup the driver calls:

```ruby
conn.setnonblocking(true)
conn.enter_pipeline_mode
```

In ruby-pg this means the application takes responsibility for send/flush
blocking states. The owner uses raw primitives where the distinction matters:

```text
send_query_params
sync_flush
consume_input
is_busy
sync_get_result
```

`is_busy` is only a readiness predicate: false means the next
`sync_get_result` will not block. It is not a request boundary.

The owner calls `consume_input` on a socket-readable event and then drains until
`is_busy` becomes true or the in-flight FIFO is empty. A drain pass exhausts all
currently buffered results, including locally represented pipeline-aborted and
pipeline-sync statuses. Request, writable and bookkeeping events therefore do
not poll `is_busy` again: without a new `consume_input` they cannot reveal new
server input, and the redundant Ruby-to-C calls were the largest control-plane
CPU hot spot in profiling.

## 4. Protocol/FIFO model

Each multiplexed unit is encoded as:

```text
extended-protocol query
Sync
```

The request is appended to the in-flight FIFO only after both operations have
been queued successfully.

Results are associated with `@inflight.first`:

- ordinary/error `PG::Result` -> belongs to the FIFO front;
- `nil` -> end of that query's results;
- `PGRES_PIPELINE_SYNC` -> end of that logical unit; only here is FIFO front
  removed and its waiter resolved;
- `PGRES_PIPELINE_ABORTED` -> record an error for the front, but do **not** pop
  it until its Sync.

A server-side SQL error is therefore request-local. A client-side send/flush/
protocol error is connection-fatal because the driver can no longer prove how
its local FIFO maps to libpq's internal command queue.

The v1 driver accepts only the normal whole-result statuses
`EMPTY_QUERY`/`COMMAND_OK`/`TUPLES_OK`. COPY, single-row/chunked-row statuses and
unknown statuses are protocol errors because the public API does not enable
those result modes.

## 5. Why Sync is per unit

Without an explicit transaction, commands between synchronization points form an
implicit transaction/error-recovery segment. Sharing one trailing Sync across
independent callers would couple their commit/error semantics.

Therefore v1 always uses one Sync per independent unit. On libpq 17+ the driver
uses `PQsendPipelineSync`; on older libpq it uses `PQpipelineSync`. In both cases
it explicitly checks `PQflush` afterwards because nonblocking output can remain
buffered.

## 6. Shared-session contract

Autocommit alone is not sufficient. Multiplexed callers share a PostgreSQL
session. `SET`, `SET ROLE`, prepared statements, temp objects, LISTEN state and
session advisory locks can leak across callers.

`Client#query` therefore has a **session-neutral SQL contract**. `SessionGuard`
is intentionally conservative and catches common hazards, but it cannot prove
that arbitrary SQL or user-defined functions are session-neutral.

Anything stateful goes through an exclusive pinned connection:

- `Client#session` — no implicit BEGIN;
- `Client#transaction` — BEGIN/COMMIT/ROLLBACK.

The one deliberate exception is `Client#prepare`, which is not an arbitrary
session mutation exposed to callers. It registers immutable SQL in a
client-owned catalog and prepares the same generated physical name on every
pipeline connection. The returned `PreparedStatement` handle can then use
`send_query_prepared` through the normal FIFO protocol. A replacement connection
replays the full catalog before it becomes available, so routing never selects a
driver that lacks a successfully registered handle.

Registration is explicit: there is no unbounded automatic SQL cache. A failed
registration is removed from the reconnect catalog; statements that had already
been prepared on another connection may remain there under an unreachable
generated name until that connection is replaced or closed. Version 0.2.3 and later do
not expose `DEALLOCATE` for multiplexed handles. Server-side plan invalidation is
reported as the ordinary request-local `QueryError`.

Pinned connections are opened lazily up to `pinned_size`; a query-only process
does not reserve an otherwise idle second pool at startup. Before a pinned
physical connection is reused, the pool makes sure it is outside an explicit
transaction. PostgreSQL 9.4+ is sanitized with `DISCARD ALL`; PostgreSQL 9.3 is
physically reconnected because its `DISCARD ALL` cannot clear sequence state.

`Session`/`Transaction` handles are fiber-local scoped capabilities. They may
only be used by the fiber that received them and are invalidated when their block
exits. An escaped object therefore cannot race another fiber or keep using a
physical connection after the pool has handed it to another caller.

Calling `Client#close` from inside its own `session`/`transaction` block is
rejected instead of deadlocking while graceful shutdown waits for that same
pinned connection to be returned. A task that already owns a pinned checkout
also cannot recursively call `Client#session`/`Client#transaction`; that would
deadlock with `pinned_size: 1` or create a misleading second session with a
larger pool. Nested transactional work uses `Transaction#savepoint` instead.

## 7. Cancellation

There are two unrelated concepts:

1. **waiter cancellation** — caller stops caring about its Future/result;
2. **backend cancellation** — PostgreSQL CancelRequest cancels the statement
   currently executing on that backend.

Only (1) exists on the multiplexed API. An abandoned request already sent to the
server remains in the in-flight FIFO and is fully drained to Sync. Backend query
cancellation would be unsafe without a dedicated/pinned connection or stronger
knowledge that the target is the currently executing FIFO head. A cancelled
waiter therefore does **not** mean a mutating statement was cancelled; it can
still execute and commit.

A connection failure before dispatch is reported as `NotDispatchedError`. A
`PG::UnableToSend` from `PQsendQueryParams` is also definitely pre-dispatch; if
libpq still reports a healthy connection in pipeline mode, that rejection stays
request-local instead of poisoning unrelated in-flight units. Other `PG::Error`
send failures are treated as driver-fatal. Once a request has entered
dispatch/in-flight state, losing the connection before its `PGRES_PIPELINE_SYNC`
is observed is reported as `IndeterminateResultError`: for mutations the
server-side commit outcome is unknowable and automatic retry is unsafe without
an application-level idempotency strategy.

Pinned `Client#transaction` has the standard PostgreSQL commit-acknowledgement
ambiguity as well: a transport failure during `COMMIT` does not prove rollback
or commit. That path intentionally preserves the underlying `PG::Error`; callers
that retry non-idempotent transactions need application-level idempotency or
reconciliation rather than assuming the transaction did not commit.

## 8. Backpressure and shutdown

`max_pending` is enforced by a small reactor-local bounded queue. On async 2.42
`Async::LimitedQueue` (with `#close`) would cover plain bounded backpressure; the
in-tree `BoundedQueue` is kept only for the two things it adds and the failure
model needs: a **typed** close (a producer blocked on backpressure during an
abort is woken with `NotDispatchedError`, not a generic closed error) and
`#drain` (hand back queued-but-undispatched requests to reject in one shot).

`max_in_flight` bounds the number of dispatched pipeline units.

Graceful shutdown:

```text
stop accepting -> reject/wake blocked producers -> drain queued/in-flight work
-> flush -> exit pipeline mode -> stop watchers -> close socket
```

Fatal shutdown:

```text
stop accepting -> fail every owned request -> stop watchers -> close socket
```

## 9. Pinned connection hygiene

Pinned callers are allowed to mutate session state. Reusing such a connection
without reset would leak that state to the next caller. After each pinned block:

1. if transaction status is `INTRANS`/`INERROR`, issue `ROLLBACK`;
2. require an idle connection;
3. on PostgreSQL 9.4+, run `DISCARD ALL`;
4. on PostgreSQL 9.3, reconnect instead because pre-9.4 `DISCARD ALL` does not
   clear sequence `currval`/`lastval` state;
5. if cleanup fails, close and replace the physical connection;
6. if replacement also fails, mark the pinned pool broken rather than reusing a
   contaminated connection.

## 10. Deferred fiber wakeups

Immediately resuming a waiter from inside the connection owner is unsafe: a
resumed caller could re-enter the driver (submit, or close the client and wait on
the owner) while the owner is still completing the current pipeline unit.

A `Request` has exactly one consumer, so request completion stores that waiter
fiber and its scheduler directly. `Scheduler#block` parks it, and
`Scheduler#unblock` pushes it onto the selector for a later reactor turn. This
preserves deferred wakeup without allocating a general-purpose
`Async::Notification` and its queue objects for every query. The settled flag
handles completion-before-wait, and an `ensure` removes a waiter interrupted by
timeout or task cancellation before a late result can try to wake it.

Multi-waiter coordination points still use `Async::Notification`: bounded-queue
backpressure and pinned-pool idle notifications need queue/broadcast semantics
that the request-specific one-shot waiter deliberately does not provide.

> Note: the supported Async range starts at 2.42 and stays below 3. The lockfile
> exercises the current compatible 2.x release, while CI separately runs the unit
> suite against the exact 2.42.0 floor so `Scheduler#block`/`#unblock` behavior is
> not merely assumed from a broad pessimistic dependency range.

## 10a. Guard evaluation shape

`SessionGuard` validates the session-neutral SQL contract of §6 before a query is
allowed onto the shared path, so its cost lands on the reactor thread and its
stalls are visible in every other fiber's latency, not just the caller's.

Evaluation is therefore staged so that the expensive stage runs only when it can
change the answer:

1. **Masking is skipped when it is provably an identity transform.** `code_only`
   blanks string literals, quoted identifiers, comments and dollar-quoted bodies.
   If none of `'`, `"`, `--`, `/*` or a `$tag$` delimiter appears in the SQL,
   there is nothing to blank and the raw SQL is validated directly.
2. **The forbidden-pattern scan is skipped when no pattern can match.** Every
   pattern in `FORBIDDEN_PATTERNS` and `STRICT_FORBIDDEN` is anchored on either a
   parenthesis or the word `into`, so SQL containing neither cannot match any of
   them.
3. **Verdicts are cached by SQL string**, and the cache evicts a single oldest
   entry at its limit. Clearing the whole cache would turn one insertion into a
   full recompute for every subsequent statement.

Stage 1 and stage 2 are correctness-preserving only as long as their premises
hold. Both are asserted in `spec/pg_pipeline/session_guard_fast_path_spec.rb`;
**adding a pattern that is not anchored on `(` or `into` requires updating the
prefilter in the same change.**

Masking is deliberately monotone in the wrong direction to be reordered: blanking
a comment can *create* a match (`nextval/* c */('s')` becomes `nextval      ('s')`),
so the raw SQL can never be used as a negative filter for stage 2. Only the
absence of maskable syntax justifies skipping stage 1.

## 10b. Fill observability

Pipelining pays off only when one reactor wakeup is amortised over several units.
`ConnectionDriver#stats` therefore reports `units_per_readable` (and related
counters) alongside the queue depths. A value near 1.0 means each query costs a
full socket wait and a scheduler round trip, which bounds throughput
independently of control-plane work; values well above 1.0 mean the pipeline is
filling. Read this number before attributing throughput to Ruby-side cost.

## 11. Version policy

| Component | Minimum | Reason |
|---|---:|---|
| Ruby | 3.3 | our dev-env floor; required by the current async line |
| async | ~> 2.42 | Async-native; floor 2.42.0 is exercised separately in CI |
| pg | >= 1.5, < 2 | pipeline bindings + raw result/flush + setnonblocking |
| libpq | 14 | pipeline API introduced here (client-side) |
| server | protocol v3 | pipeline is client-side; NO server-14 requirement |

We go Async-native and depend on the current async line rather than reinventing
its coordination primitives to hold an older Ruby floor. Ruby/async are OUR
dev-env floors — chosen freely for what maximally simplifies the implementation.
libpq/server are the USER's floor and are NOT raised without cause: libpq 14 is
the true minimum (no pipeline mode below it); the server only needs protocol v3.
libpq 17 is an optional runtime-detected fast path (`PQsendPipelineSync`,
chunked-rows, non-blocking cancel), never a floor.

> The server floor is the user's environment; revisit annually against
> PostgreSQL's EOL calendar. Capability gating is by `PG.library_version`
> (the client libpq), never by `conn.server_version`.

## 12. Known limitations before production

- Pipeline driver death is recovered by the pool supervisor (`reconnect: true`
  default) with exponential backoff; while a slot is down traffic goes to live
  drivers, and with zero live drivers `#query` raises `NotDispatchedError`.
  Idle drivers are also probed (`health_check: true`) with `SELECT 1`. A
  timed-out probe may abort the connection only while it remains the sole
  driver work; concurrent user work makes the probe inconclusive instead of
  allowing a background check to make that work indeterminate.
- No PostgreSQL query cancellation on the **multiplexed** path (would target
  whichever statement is currently executing on the shared backend).
- `abort!` sends `CancelRequest` to checked-out **pinned** connections
  (`cancel_pinned_on_abort: true`); it does not force-close the socket if the
  backend ignores cancel.
- No streaming/single-row/chunked-row result API.
- No multiplexed prepared-statement deallocation API; registered handles live
  for the lifetime of the client.
- No cross-thread/reactor sharing; `Client` rejects use from a different thread or Fiber scheduler.
- `SessionGuard` is policy/ergonomics, not a security boundary. `:strict` adds a
  precise denylist (`nextval`/`setval`/`pg_export_snapshot`, …) without the old
  broad `pg_*(` / `set*(` false positives; UDFs can still mutate session state.
- Live integration covers **server** PG 14/16/17/18. A separate CI matrix
  builds ruby-pg against source-built **client** libpq 14/16/17, asserts the
  linked major, and runs both unit and live integration suites because pipeline
  capability is determined by client libpq rather than server version.
- On libpq 14–16, `place_sync` uses `PQpipelineSync` / ruby-pg `sync_pipeline_sync`
  (flush coupled); libpq 17+ uses `send_pipeline_sync` + explicit `sync_flush`.
- Pinned recycle still runs full `DISCARD ALL` (no lighter reset profile / recycle
  latency metric yet).
- The latest released ruby-pg (1.6.3 as of July 2026) has upstream issue #719
  in very large query-parameter encoding. The fix is merged upstream but no newer
  RubyGems release exists yet; untrusted multi-gigabyte parameter sets must be
  bounded or deployments should build ruby-pg from a revision containing the fix.

## 13. Roadmap status (review response)

Implemented:

- Automatic reconnect / replacement of dead pipeline drivers (pool supervisor
  with exponential backoff); `client.stats[:reconnects]`.
- Idle health checks on zero-load drivers; `client.stats[:health_failures]`.
- BoundedQueue wake-one + cancel wake-transfer (no thundering herd / stranded slots).
- Observability: public `client.stats` plus internal per-driver `stats`.
- Safer fiber-storm defaults: `max_in_flight: 64`, `max_pending: 256`,
  `pinned_size: 2`.
- `SessionGuard` `:strict` precise denylist + documented holes.
- Savepoints (`Transaction#savepoint`).
- Submit failover: `NotDispatchedError` retries on a **new** Request against a
  live sibling; the failed Request object itself is never re-used.
- `abort!` CancelRequest on in-use pinned connections.
- Falcon per-worker contract + sizing formula; reactor-local Client fail-fast.
- Packaging: gemspec metadata, CI (unit + PG server 14/16/17/18 + client-libpq
  14/16/17 + exact async floor), docker-compose,
  examples, `bench_kit/pipeline_throughput.rb` + `bench_kit/multiworker_smoke.rb`.
- Live integration suite (multiplex, isolation, cancel-drain, savepoints, stats,
  backpressure, pinned concurrency, indeterminate on abort, reconnect, backend
  terminate recovery, pinned abort cancel).

Still open:

- Lighter pinned reset / recycle-latency metrics under high-TPS tx.
- Force socket close if pinned cancel is ignored.
- Published bench numbers in README; tighten multiworker smoke to peak occupancy.
