#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SAMPLES_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${SAMPLES_DIR}/results"
mkdir -p "${RESULTS_DIR}"

PROFILE="${PROFILE:-1}"
export PROFILE

DEFAULT_GREP='PgPipeline|pg_pipeline|Native|native_|pp_|PQsend|PQpipeline|PQflush|PQconsume|PQisBusy|PQgetResult|seal|dispatch|drain|SessionGuard|BoundedQueue|owner_loop|pump_requests|process_event|select_driver|block|unblock|io_wait|fiber'

SAMPLES=(
  request_seal_hot_path
  session_guard_hot_path
  client_query_hot_path
  client_query_params_hot_path
  client_query_multiplex_hot_path
  prepared_query_hot_path
  result_rows_hot_path
  native_dispatch_hot_path
  native_drain_hot_path
  run_all_benchmark_task
)

if [[ -n "${ONLY:-}" ]]; then
  IFS=',' read -r -a SAMPLES <<< "${ONLY}"
fi

if [[ "${PROFILE}" == "1" ]]; then
  if ! command -v sample >/dev/null 2>&1 || ! command -v filtercalltree >/dev/null 2>&1; then
    echo "need macOS 'sample' and 'filtercalltree' (Xcode Command Line Tools), or PROFILE=0" >&2
    exit 1
  fi
fi

cd "${ROOT}"

banner_value() {
  local file="$1" key="$2"
  grep -E "^${key}=" "${file}" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '\r' || true
}

run_benchmark_task() {
  local script="${SAMPLES_DIR}/run_all_benchmark_task.rb"
  local status=0

  echo
  echo "======== run_all_benchmark_task ========"
  if [[ ! -f "${script}" ]]; then
    echo "skip missing ${script}" >&2
    return 0
  fi

  set +e
  bundle exec ruby "${script}"
  status=$?
  set -e
  echo "ruby exit=${status}  out=${RESULTS_DIR}/all_benchmarks.txt"
  return "${status}"
}

run_one() {
  local name="$1"
  local script="${SAMPLES_DIR}/${name}.rb"
  local banner log pid sample_seconds sample_file txt_file grep_pat
  local ruby_pid status=0

  if [[ "${name}" == "run_all_benchmark_task" ]]; then
    run_benchmark_task
    return $?
  fi

  if [[ ! -f "${script}" ]]; then
    echo "skip missing ${script}" >&2
    return 0
  fi

  banner="$(mktemp -t "pgp_banner_${name}.XXXXXX")"
  log="$(mktemp -t "pgp_log_${name}.XXXXXX")"
  echo
  echo "======== ${name} ========"

  set +e
  bundle exec ruby "${script}" >"${log}" 2>&1 &
  ruby_pid=$!
  set -e

  tail -n +1 -f "${log}" &
  local tail_pid=$!

  pid=""
  local i
  for i in $(seq 1 200); do
    if grep -q '^pid=' "${log}" 2>/dev/null; then
      cp "${log}" "${banner}"
      pid="$(banner_value "${banner}" pid | tr -d '[:space:]')"
      break
    fi
    if ! kill -0 "${ruby_pid}" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done

  if [[ -z "${pid}" ]]; then
    kill "${tail_pid}" 2>/dev/null || true
    wait "${tail_pid}" 2>/dev/null || true
    echo "failed to parse pid= from ${name}" >&2
    wait "${ruby_pid}" || true
    echo "----- sample output -----"
    cat "${log}" || true
    rm -f "${banner}" "${log}"
    return 1
  fi

  for i in $(seq 1 400); do
    grep -q '^HOT_LOOP_START' "${log}" 2>/dev/null && break
    if ! kill -0 "${ruby_pid}" 2>/dev/null; then
      break
    fi
    sleep 0.05
  done
  cp "${log}" "${banner}"

  sample_seconds="$(banner_value "${banner}" sample_seconds | tr -d '[:space:]')"
  sample_file="$(banner_value "${banner}" sample_file)"
  txt_file="$(banner_value "${banner}" txt_file)"
  sample_seconds="${sample_seconds:-27}"
  sample_file="${sample_file:-/tmp/pg_pipeline_${name}.sample}"
  txt_file="${txt_file:-${RESULTS_DIR}/pg_pipeline_${name}.txt}"
  grep_pat="${NATIVE_GREP:-${DEFAULT_GREP}}"

  mkdir -p "$(dirname "${txt_file}")"

  if [[ "${PROFILE}" == "1" ]]; then
    echo "capturing pid=${pid} for ${sample_seconds}s -> ${txt_file}"
    {
      sample "${pid}" "${sample_seconds}" -f "${sample_file}"
      echo
      echo "===== focused pg_pipeline/native symbols ====="
      filtercalltree "${sample_file}" | grep -E "${grep_pat}" | head -320 || true
      echo
      echo "===== filtercalltree head -320 ====="
      filtercalltree "${sample_file}" | head -320 || true
    } >"${txt_file}" 2>&1
    echo "----- profile written (${txt_file}) -----"
    tail -n 30 "${txt_file}" || true
  else
    echo "PROFILE=0 metrics-only -> ${txt_file}"
    : >"${txt_file}"
  fi

  set +e
  wait "${ruby_pid}"
  status=$?
  set -e

  kill "${tail_pid}" 2>/dev/null || true
  wait "${tail_pid}" 2>/dev/null || true

  {
    echo
    echo "===== sample process stdout (metrics) ====="
    tail -n 60 "${log}"
  } >>"${txt_file}"

  echo "ruby exit=${status}  profile=${txt_file}"
  rm -f "${banner}" "${log}"

  if [[ "${status}" -ne 0 ]]; then
    return 1
  fi
  return 0
}

if [[ "${SHUFFLE:-0}" == "1" ]]; then
  # shellcheck disable=SC2207
  SAMPLES=($(printf '%s\n' "${SAMPLES[@]}" | awk 'BEGIN{srand()} {print rand() "\t" $0}' | sort -n | cut -f2-))
fi

failed=0
for s in "${SAMPLES[@]}"; do
  if ! run_one "${s}"; then
    failed=1
  fi
done

echo
if [[ "${failed}" -eq 0 ]]; then
  echo "done. profiles in ${RESULTS_DIR} (PROFILE=${PROFILE})"
  exit 0
fi
echo "done with failures. profiles in ${RESULTS_DIR} (PROFILE=${PROFILE})" >&2
exit 1
