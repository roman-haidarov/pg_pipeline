# Changelog

## [0.4.0] - 2026-08-11

Native multiplexed data plane. Public SQL, Sync-per-unit, failure model, and
`Fiber::Scheduler` control plane match 0.3.1; the implementation of the hot path
moves into C. Read *Breaking changes* first: the return type of `Client#query`
changed, and that is not something a 0.3 caller can discover from a green suite.

### Breaking changes

- **`Client#query` and `PreparedStatement#query` return a `PgPipeline::Result`,
  not a `PG::Result`.** The native driver owns the `PGresult` and never builds a
  ruby-pg object for it. The row and metadata surface is deliberately
  spelled the same -- `#first`, `#each`, `#each_row`, `#to_a`, `#values`,
  `#ntuples`/`#num_tuples`, `#nfields`/`#num_fields`, `#fields`, `#fname`,
  `#fnumber`, `#ftype`, `#fmod`, `#getvalue`, `#getisnull`, `#getlength`,
  `#tuple_values`, `#column_values`, `#field_values`, `#cmd_tuples`,
  `#error_message`, `#error_field`, `#result_status`, `#clear` -- so ordinary row
  handling needs no change. Two things cannot be carried over:
  - `result.is_a?(PG::Result)` is now false. Introspection or dispatch written
    against that check silently takes the other branch. Test for
    `PgPipeline::Result`, or for the methods you actually call.
  - ruby-pg's type-map machinery (`#map_types!`, `PG::BasicTypeMapForResults`)
    is not available, and the multiplexed path always requests text-format
    results. Values arrive as strings, which is what 0.3.1 also returned in
    practice, but there is no longer a hook to change that.

  Explicit transactions and `Client#session` are unaffected: they run on
  ruby-pg and still hand back `PG::Result`.
- **The `backend:` pool option is gone** along with the pure-Ruby multiplexed
  driver. Passing it now raises an `ArgumentError` that says where it went and
  what to pin instead, rather than Ruby's bare "unknown keyword: :backend".
- **`Request#params` and `RequestOps.snapshot_params` / `snapshot_value` /
  `immutable_values?` are gone.** Parameter bytes are copied into the sealed
  arena at build time and are not retained as Ruby objects.
- **Installing requires a C compiler and libpq >= 14 headers.** There is no
  precompiled platform gem and no compiler-free fallback backend; pin
  `~> 0.3.1` if you need one.

### Added

- C `SessionGuard` (masking, forbidden-call scan, two-generation verdict cache)
  and C `BoundedQueue` (typed close, drain, fiber `block`/`unblock` waiters) on
  the multiplexed submit path — justified hot-path work that previously
  allocated Ruby strings/regex/Notification objects per query under load.
- C extension (`ext/pg_pipeline_native`) owning multiplexed `PGconn` objects,
  nonblocking connect/poll, pipeline dispatch/flush, full drain while
  `!PQisBusy`, the fixed-capacity in-flight FIFO, and request completion.
- `Native::RequestState` (TypedData) for request lifecycle and one-shot
  fiber waiter `block`/`unblock` via the **stored** scheduler.
- `Native::RequestState#seal!`: a request's dispatch payload is built once, when
  the `Request` is constructed -- one arena allocation holding the side tables
  plus NUL-terminated SQL, statement name and every parameter body. `dispatch`
  reads only that arena and performs **zero** `rb_funcall`.
- `Native::RequestState#adopt_payload!` and `Request#respawn`:
  `NotDispatchedError` failover copies the sealed arena instead of re-encoding
  SQL and parameters. The failed `Request` object is still never re-submitted.
- `Native::Result` owning `PGresult*` with lazy row materialization and
  idempotent `#clear`.
- `Native::Driver#stats` exposing the hot-path counters, which live in the C
  driver struct, plus `in_flight_peak` and `bytes_dispatched`, and
  `Native::Driver#counter(name)` for reading a single counter without building
  the Hash.
- `Native::Driver#pipeline_aborted?`, so callers can tell an aborted pipeline
  from an unusable connection instead of inferring it from `#reusable?`.
- `SessionGuard.clear_cache!` / `.cache_size`, and the mode-normalizing
  `unsafe_reason_c` / `assert_multiplexable_c!` entry points.
- Load-time check that both native and ruby-pg libpq runtimes are >= 14.
- `spec/support/reference_scheduler.rb`: a dependency-free `Fiber::Scheduler`
  built on `IO.select`. The native specs run on it, so a failure points at
  pg_pipeline rather than at Async or Itsi.
- Live specs for the native data plane (`spec/integration/native_data_plane_spec.rb`),
  sealed-payload unit specs, and specs for the reference scheduler itself.
- Regression specs for seal-time reentrancy, delimited-identifier guard
  evasion, and `BoundedQueue` close/wake behaviour.
- CI (`.github/workflows/ci.yml`) tests **only the current tree** against the
  runner's distro libpq: `rake compile` in every job that needs the extension,
  a Ruby 3.3/3.4 unit matrix, a PostgreSQL **server** matrix 14/16/17/18, a gem
  build/install job that loads the just-built gem from outside the checkout and
  checks its version against `PgPipeline::VERSION`, and a hygiene job
  (`rake verify_no_build_products`). No Valgrind job: memcheck does not model
  CRuby fiber stacks and only produces false positives on this control plane.
  libpq ≥ 14 remains a runtime requirement; the optional fast-Sync path
  (libpq ≥ 17) is selected at compile/runtime from the linked client.
- `bench_kit/dispatch_cost.rb` (`rake bench:dispatch`): isolated send-path cost
  per parameter width.
- `Native::Result#external_bytes` and `Native::Driver#encoding`, plus
  `:encoding` per driver and `:seal_encoding` for the pipeline in
  `Client#stats`, so an encoding mismatch is visible before it costs a request.
- `SessionGuard` refuses `dblink_connect` / `dblink_disconnect`
  (`dblink-session`), the large-object function family (`large-object`), and
  Unicode-escaped delimited identifiers and the `UESCAPE` clause
  (`unicode-escaped-identifier`, `uescape`).

### Changed

- Multiplexed send/drain no longer loops through ruby-pg per
  `consume_input` / `is_busy` / `get_result` step.
- `Request` is backed by `Native::RequestState`.
- Query parameters are no longer snapshotted, frozen or retained on the Ruby
  side. `#seal!` copies their bytes at build time, so the immutable-snapshot
  layer that protected the Ruby data plane from later mutation is gone; the
  equivalent guarantee is now provided inside `#seal!` itself (see *Fixed*).
- Parameter encoding happens once per request instead of once per dispatch, and
  parameter bodies are copied rather than borrowed from Ruby strings -- GC
  compaction can relocate string bodies reachable from a marked array, so
  borrowed pointers would have needed per-element pinning in the mark function.
- `consume_and_drain` returns the completed-unit count as a Fixnum rather than a
  freshly allocated Hash; `dispatches`, `flush_calls`, `flush_incomplete`,
  `readable_events`, `results_read` and `units_completed` are incremented in C.
  The owner loop does no bookkeeping.
- Sealing validates SQL, statement names and parameters at `Request` build time,
  so malformed input raises before the request is queued rather than in the
  middle of the owner loop's dispatch. A failed seal cannot leave a partially
  initialised payload: every Ruby call that can raise happens before the arena
  is allocated.
- The dispatch-byte counter is named `bytes_dispatched` rather than
  `bytes_sealed`: it is incremented per dispatch, so a respawned request counts
  twice, and the old name described something the number never measured.
- The sealed-payload encoding is published by the **first** successful connect
  and then left alone, instead of being overwritten by every connect. A payload
  that carries non-ASCII text records the encoding it was sealed for, and
  dispatch refuses to send it over a connection that negotiated a different
  `client_encoding` (`UnsupportedServerError`, request-local and permanent)
  rather than shipping mis-encoded bytes. All-ASCII payloads are unaffected.
- Building the gem requires a C compiler and libpq development headers >= 14
  (`PG_CONFIG` / Homebrew `libpq` discovery in `extconf.rb`). The extension is
  built with `-std=gnu99`; set `PG_PIPELINE_STRICT_BUILD=1` to add `-Werror`.

### Fixed

- `Native::Result` tells the GC how much memory it is holding. A `PGresult`
  lives entirely outside the Ruby heap and libpq exposes no size accessor, so
  the collector saw a ~40 byte object and had no reason to run while hundreds of
  megabytes of rows accumulated behind uncleared results. The size is estimated
  once at wrap time (all columns, rows sampled and extrapolated past 64 to keep
  the estimate O(1) on large results), reported through the TypedData `dsize`
  hook and handed to `rb_gc_adjust_memory_usage`, and given back on `#clear`.
  Calling `#clear` promptly is still the right thing to do -- this makes
  forgetting it a latency problem rather than an unbounded-RSS one.
- `SessionGuard` no longer accepts a Unicode-escaped delimited identifier.
  `SELECT U&"pg_advisory_\006Cock"(1)` resolves to `pg_advisory_lock` on the
  server, but masking hid the escaped form from the forbidden-call scan, so it
  was classified session-neutral; the same held for every name the scanner
  checks. Resolving the escapes here would mean reimplementing them including
  the `UESCAPE` clause's configurable escape character, so the guard refuses
  instead: a `U&"…"` identifier that is a plain run of identifier bytes has no
  escapes to resolve and is still scanned as code, anything else is reported,
  and a `UESCAPE` clause attached to a `U&` literal is reported on its own.
- A connection whose negotiated `client_encoding` differs from the process-wide
  seal encoding warns once at connect, instead of leaving the mismatch invisible
  until the first query that happens to carry a non-ASCII byte.
- `bench_kit/dispatch_cost.rb` flushes and drains every 64 units instead of
  sizing the in-flight FIFO at `COUNT + 16` and never flushing. The old shape
  accumulated 50,000 units in libpq's output buffer, so it timed memcpy into a
  buffer growing into the tens of megabytes -- reallocations included -- at a
  FIFO depth no driver reaches. It also now reports `#dispatch` allocations in
  isolation separately from end-to-end allocations per unit, which are not the
  same number and were easy to read as if they were.
- `#seal!` no longer walks the caller's parameter array while running caller
  Ruby on it. Converting a parameter calls `#to_s` (and, for the Hash form,
  `#hash`/`#eql?`/`#to_int`); a conversion that shrank the array made the C walk
  read past its end -- a segfault reachable from ordinary Ruby -- and a
  conversion that replaced elements had its replacements sealed and sent.
  Parameters are now snapshotted into a private array before any conversion
  runs, and every converted body is frozen before the arena is sized, so the
  length measured and the bytes copied cannot disagree.
- `SessionGuard` no longer treats a delimited identifier as an opaque literal.
  `SELECT "set_config"('a','b',false)` is an ordinary call and was classified
  session-neutral; the same held for `"currval"`, `"lastval"`, `"setseed"` and
  the advisory-lock family, schema-qualified or not. A quoted identifier whose
  body is a single run of identifier bytes is now scanned as code. Anything
  else -- `"a; b"`, `"into temp"`, `"weird(col"`, `"a""b"` -- stays fully
  masked, so an identifier can never inject a statement separator or a call
  site into the code being scanned.
- The drain loop is no longer reentrant. `Scheduler#unblock`, `Result#clear` and
  `QueryError`'s constructor are all caller Ruby that can re-enter the driver
  while the loop holds a raw `PGconn*`; `#close` taken from that Ruby is now
  deferred to the end of the drain instead of `PQfinish`-ing under the loop, the
  connection is revalidated after every such call, and a nested drain raises
  `ProtocolError` instead of corrupting the FIFO. The completed request is
  popped before its waiter is woken, so a scheduler that resumes inline never
  sees a settled request at the head of the queue.
- `BoundedQueue#close` re-raises the exact exception object it was closed with.
  It used to rebuild it from `(class, message)`, which dropped the backtrace and
  `cause` and raised `ArgumentError` outright for any error class whose
  `#initialize` is not a single String.
- `BoundedQueue` wakes waiters from a detached copy of the waiter list. Waking
  runs caller Ruby, so a waiter that queued during the walk used to be dropped
  by the trailing length reset, and the walk itself could index a list that had
  been reallocated.
- `Request#respawn` assigns the same instance variables in the same order as the
  constructor of its class, so a respawned request shares the object shape of a
  freshly built one.
- Text parameters, SQL and statement names are exported to the connection's
  `client_encoding` when sealed, matching what ruby-pg does. A String in another
  encoding used to be shipped as raw bytes, which PostgreSQL rejected
  (`invalid byte sequence for encoding "UTF8"`). Binary-format parameters are
  still copied byte for byte.
- `#adopt_payload!` rebinds the copied arena from stored offsets instead of
  subtracting pointers into two different allocations.
- Result values from a binary-format column are tagged `ASCII-8BIT` rather than
  the connection's `client_encoding`.
- A native connection installs a notice processor, so `NOTICE`/`WARNING` no
  longer go to the process's stderr.
- Building the conninfo arrays from a Hash validates every string before
  allocating, instead of leaking two `ALLOC_N` arrays when a value with an
  embedded NUL raised between the allocation and the fill.
- The `SessionGuard` verdict cache rotates two generations instead of calling
  `Hash#keys` on every miss once full, which allocated a 2048-element Array
  exactly when the cache had become useful.
- `code_only` initialises its output buffer before masking, so a future missed
  byte cannot leak uninitialised heap into a Ruby String.
- `rake compile` regenerates the Makefile when the one on disk was produced for
  a different platform, instead of failing to link a foreign object file, and
  `rake integration` now depends on `:compile` like `rake spec` does.
- Four `strict:` reason tags (`strict:set_config`, `strict:setseed`,
  `strict:session-advisory-lock`, `strict:session-advisory-unlock`) were
  unreachable by construction, because default mode returns first. Removed;
  strict mode adds exactly `strict:nextval`, `strict:setval` and
  `strict:pg_export_snapshot`, which is the behaviour the specs already pinned.
- `SessionGuard.unsafe_reason_c` / `.assert_multiplexable_c!` were compiled and
  then hidden behind a `(void)` cast. They are registered now.

### Removed

- `PgPipeline::ConnectionDriver`, the pure-Ruby multiplexed driver, and the
  `backend:` pool option. libpq is now driven only from C on the multiplexed
  path. Two implementations of the same protocol invariants were the largest
  standing cost in this gem, and the option bought nothing: it was opt-in rather
  than an automatic fallback, it could not help an install without a compiler
  (the gemspec declares an extension), and callers who need a compiler-free
  multiplexed driver already have one in the released 0.3.1. Behavioural
  coverage for the current tree is the live integration suite plus the native
  data-plane specs; CI does not reinstall older gem releases.
- `RequestOps.snapshot_params` / `snapshot_value` / `immutable_values?` and
  `Request#params`, all of which existed only to hand safe objects to the Ruby
  driver at dispatch time.
- The committed macOS arm64 build products under `ext/pg_pipeline_native/`.
  They were tracked despite `.gitignore`, and because `native.rb` preferred a
  build sitting next to the sources, a clone on Apple Silicon loaded that binary
  instead of the one it had just compiled. `native.rb` now prefers the extension
  on the load path and only falls back to `ext/`.

### Unchanged

- Control plane stays on `PgPipeline::Runtime` (any `Fiber::Scheduler`).
- ruby-pg remains a runtime dependency: pinned sessions and explicit
  transactions still run on `PG::Connection`.
- Owner/reader/writer task model, BoundedQueue, pool supervisor, SessionGuard,
  pinned sessions/transactions on ruby-pg.
- Drain only after socket-readable + `consume_input` (no archive-style
  completion budget / continue_drain by default).

### Known limitations

- All multiplexed connections in one process must share a `client_encoding`.
  Payloads are sealed before a driver is chosen, so the seal target is
  process-wide; a mismatch is detected and reported rather than silently
  mis-encoded, but it is not resolved.
- The multiplexed path always requests text-format results. Binary result
  format is not reachable from the native driver.
- COPY is not supported on the multiplexed pipeline.
- `SessionGuard` scans masked SQL; it does not resolve the catalog. Session
  side effects inside a called function (`SELECT my_udf(1)` where `my_udf` does
  a `SET`) and sequence advances reached indirectly (an `INSERT` into a `serial`
  column setting `currval`) are not detectable and are not detected. The second
  is contained by `currval`/`lastval` being refused on the multiplexed path.
  See the README section on what the guard does and does not catch.
- End-to-end throughput gain from the native data plane is small on a local
  socket with trivial queries (see `docs/PERFORMANCE.md` §9); the send path
  itself is 11-46% cheaper and no longer allocates.

## [0.3.1] - 2026-08-09

Reliability and correctness fixes on top of 0.3.0. No public API changes other
than the new error class below; no changes to wire behaviour.

### Fixed

- `SessionGuard` dollar-quote masking now recognises tags containing non-ASCII
  identifier bytes (e.g. `$тег$...$тег$`). Previously an unrecognised tag left
  the quoted body unmasked, so its raw text was scanned by the forbidden-pattern
  checks instead of being treated as an opaque literal.
- `Client#transaction` now distinguishes a lost `COMMIT` acknowledgement from an
  ordinary server-side rejection. If `COMMIT` fails while the connection is
  still healthy (`status == CONNECTION_OK`, not `finished?`), the original
  `PG::Error` is raised unchanged -- the server gave a complete answer and the
  transaction did not commit. If the connection itself is gone or broken
  (`PG::ConnectionBad`, `finished?`, or a non-OK status), the outcome is
  genuinely unknown, and this is now raised as the new
  `PgPipeline::IndeterminateCommitError < IndeterminateResultError` instead of
  a bare `PG::Error`, so callers can tell "definitely rolled back" apart from
  "may have committed, do not retry blindly" without inspecting connection
  internals themselves. See the updated "Failure model" table in the README.

### Reverted

- The reader/writer watcher shutdown mechanism briefly introduced in this cycle
  (self-pipe `Runtime::Wakeup` + `IO.select` on every socket wait) has been
  removed before release. It fixed a real gap -- on schedulers with no way to
  interrupt a fiber blocked in `wait_readable`/`wait_writable`, closing the
  socket alone does not reliably wake it pre-Ruby-4.0 -- but at an unacceptable
  cost: it moved the connection driver's hottest loop from `io_wait` (native,
  zero extra threads) onto `IO.select`, and both reference schedulers implement
  that hook expensively. `Async::Scheduler#io_select` spawns a new OS thread per
  call; `Itsi::Scheduler#io_select` with more than one IO (our case: socket +
  wakeup pipe) falls onto Itsi's bounded `blocking_operation_wait` worker pool
  and holds a worker for the full duration of every idle wait, which can
  exhaust that pool under a modest number of pipeline connections.
- Root-caused instead: `Async::Scheduler` and `Itsi::Scheduler` both already
  implement `#fiber_interrupt` as an ordinary library method, independent of
  Ruby core's own hook of the same name (Ruby >= 4.0). `Task#stop` already
  tries `#fiber_interrupt` first, so on both of this gem's reference schedulers
  a blocked watcher is woken immediately via `fiber.raise`, on the same
  `wait_readable`/`wait_writable` fast path as 0.3.0 -- no self-pipe needed.
  `ConnectionDriver` now only falls back to a bounded
  `wait_readable(timeout)`/`wait_writable(timeout)` poll
  (`DriverOps::WATCHER_POLL_INTERVAL`, 0.25s) when the active scheduler does
  *not* respond to `#fiber_interrupt`, guaranteeing shutdown within one poll
  interval instead of depending on socket-close propagation. Async and Itsi
  never pay this poll; an unknown/minimal scheduler does, bounded and cheap.
- `teardown_watchers` also reverts to closing the socket/connection
  unconditionally regardless of whether the watcher tasks joined in time. The
  self-pipe version returned early when a watcher missed
  `WATCHER_JOIN_TIMEOUT`, before closing the wakeup pipes, the socket, or the
  `PG::Connection` -- turning a leaked *task* into a leaked live DB connection
  and file descriptors. Resources are now always closed; only the task's own
  fiber can still leak (tracked via `stats[:leaked_watchers]`, unchanged).

## [0.3.0] - 2026-08-07

Scheduler-agnostic control plane: the gem no longer depends on the `async` gem
at runtime. Any `Fiber::Scheduler` host works (Async::Scheduler, Itsi::Scheduler,
or another `Fiber.set_scheduler` implementation).

This unlocks hosts that were previously impossible. On our stand, Itsi with
0.3.0 serves ~26% more req/s than the 0.2.5 Falcon baseline (29350 vs 23254,
4 workers, `oha -z 60s -c 1000`) — a configuration 0.2.5 could not run at all,
since it required an Async reactor. Falcon throughput itself is unchanged
(23007 vs 23254, inside run-to-run spread): this is a portability change, and
the speedup comes from being free to pick the host.

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
