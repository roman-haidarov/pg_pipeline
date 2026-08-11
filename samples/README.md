# Hot-path samples

Diagnostic loops for the **native multiplexed data plane**.
They are **not** release benchmarks (`bench_kit/` is).

**Metrics vs profiles (do not mix in one number table):**

- `PROFILE=0` (default for scripts) → `ops_per_sec_median` / `alloc_per_op` over `REPS` short windows
- `PROFILE=1` → attach `sample`/`perf`; scripts print `ops_per_sec_unreliable=...` only

No env is required for a normal run (DB URL auto-discovered from docker compose ports).

## Setup

```bash
bundle install
bundle exec rake compile
docker compose up -d pg17
```

## Reliable numbers

```bash
PROFILE=0 REPS=7 WINDOW=3 DISABLE_GC=1 \
  bundle exec ruby samples/request_seal_hot_path.rb
# → ops_per_sec_median, ops_per_sec_mad_pct, alloc_per_op, anchor_ns_*

PROFILE=0 ./samples/run_all_profiles.sh
```

## Call-tree profiles (macOS)

```bash
PROFILE=1 ./samples/run_all_profiles.sh
ONLY=client_query_hot_path PROFILE=1 ./samples/run_all_profiles.sh
```

Profiler attaches only after `HOT_LOOP_START` (not during sleep/preheat).

## Scripts

| Script | What it stresses |
| --- | --- |
| `request_seal_hot_path.rb` | seal arena only (no PG) |
| `session_guard_hot_path.rb` | C SessionGuard + cache (no PG) |
| `client_query_hot_path.rb` | e2e `Client#query` |
| `client_query_params_hot_path.rb` | e2e query with bind params |
| `client_query_multiplex_hot_path.rb` | many fibers / pipeline fill |
| `prepared_query_hot_path.rb` | prepared statement path |
| `result_rows_hot_path.rb` | drain + row materialization |
| `native_dispatch_hot_path.rb` | `Driver#dispatch` only |
| `native_drain_hot_path.rb` | `consume_and_drain` only |

## Bench_kit aggregate

```bash
bundle exec ruby samples/run_all_benchmark_task.rb
# → samples/results/all_benchmarks.txt
```

## Env

| Env | Default | Meaning |
| --- | --- | --- |
| `PROFILE` | `0` (scripts) / `1` (`run_all_profiles.sh`) | profile attach vs metrics |
| `REPS` | `7` (`1` if PROFILE=1) | timed windows kept after warm-up |
| `WINDOW` | `3` | seconds per rep (`DURATION` overrides when PROFILE=1) |
| `BATCH` | `64` | ops between deadline clock reads |
| `DISABLE_GC` | `1` | `1` → trust `alloc_per_op`; `0` → user-visible ops |
| `SLEEP_BEFORE_HOT_LOOP` | `0.5` / `2` if PROFILE | settle before hot loop |
| `PG_PIPELINE_URL` | auto (5417…) | override DB |
| `SHUFFLE` | `0` | shuffle sample order in `run_all_profiles.sh` |
| `SLEEP_BEFORE_HOT_LOOP` | `7` | attach window |
| `PREHEAT_ITERATIONS` | per script | warmup |
| `DISABLE_GC` | `1` | GC off in timed loop |
| `FIBERS` / `PIPELINE_SIZE` | `64` / `2` | multiplex only |

## How to read a profile

1. Confirm `call=` matches the path you wanted.
2. Focused section: seal / `PQsend*` / Sync / flush / drain / `owner_loop` /
   scheduler `block`/`unblock`.
3. Compare e2e samples with isolated seal/dispatch/drain.
