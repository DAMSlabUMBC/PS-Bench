#!/usr/bin/env bash
set -euo pipefail

# Run all single-node DDS scenarios (scalability + QoS) using the DDS compose stack.

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PS_BENCH_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)
SCALABILITY_ROOT="${PS_BENCH_DIR}/configs/builtin-test-suites/testcases/scalability/1-node"
QOS_ROOT="${PS_BENCH_DIR}/configs/builtin-test-suites/testcases/qos-variation/1-node"
RESULTS_ROOT="${PS_BENCH_DIR}/results"
DDS_RESULTS_ROOT="${RESULTS_ROOT}/dds"
COMPOSE_FILE="docker-compose.single.dds.yml"
REPEAT_COUNT=${REPEAT_COUNT:-3}

if ! command -v docker >/dev/null 2>&1; then
  echo "docker CLI is required but was not found" >&2
  exit 1
fi

if [ ! -d "${SCALABILITY_ROOT}" ] && [ ! -d "${QOS_ROOT}" ]; then
  echo "DDS scenario roots not found under ${PS_BENCH_DIR}/configs" >&2
  exit 1
fi

mkdir -p "${RESULTS_ROOT}" "${DDS_RESULTS_ROOT}" 2>/dev/null || true

mapfile -t SCALABILITY_SCENARIOS < <(find "${SCALABILITY_ROOT}" -type f -name 'scalabilitysuite_*_dds*_1_node.scenario' 2>/dev/null | sort || true)
mapfile -t QOS_SCENARIOS < <(find "${QOS_ROOT}" -type f -name 'qossuite_*_dds*_1_node.scenario' 2>/dev/null | sort || true)

SCENARIO_FILES=()
if [ ${#SCALABILITY_SCENARIOS[@]} -gt 0 ]; then
  SCENARIO_FILES+=("${SCALABILITY_SCENARIOS[@]}")
fi
if [ ${#QOS_SCENARIOS[@]} -gt 0 ]; then
  SCENARIO_FILES+=("${QOS_SCENARIOS[@]}")
fi

if [ ${#SCENARIO_FILES[@]} -eq 0 ]; then
  echo "No single-node DDS scenarios found under ${SCALABILITY_ROOT} or ${QOS_ROOT}" >&2
  exit 1
fi

SCEN_FILTER=${SCEN_FILTER:-}

is_reserved_dir() {
  case "$1" in
    dds) return 0 ;;
  esac
  return 1
}

move_new_entries() {
  local search_root="$1"
  local destination_root="$2"
  local marker="$3"

  [ -d "${search_root}" ] || return 0
  mkdir -p "${destination_root}"

  find "${search_root}" -mindepth 1 -maxdepth 1 -type d -newer "${marker}" -print0 2>/dev/null |
  while IFS= read -r -d '' candidate; do
    local base
    base=$(basename "${candidate}")
    if is_reserved_dir "${base}"; then
      continue
    fi
    case "${base}" in
      emqx|mosquitto|nanomq|vernemq|mochi)
        continue
        ;;
    esac
    if [[ "${base}" != *dds_run* ]]; then
      continue
    fi
    mv "${candidate}" "${destination_root}/${base}"
  done
}

current_compose=""
cleanup() {
  if [ -n "${current_compose}" ]; then
    (cd "${PS_BENCH_DIR}" && docker compose -f "${current_compose}" down --remove-orphans >/dev/null 2>&1) || true
  fi
}
trap cleanup EXIT

for scenario_file in "${SCENARIO_FILES[@]}"; do
  scenario_name=$(basename "${scenario_file}" .scenario)

  if [ -n "${SCEN_FILTER}" ] && [[ "${scenario_name}" != *"${SCEN_FILTER}"* ]]; then
    continue
  fi

  scenario_dir=$(dirname "${scenario_file}")
  rel_dir="${scenario_dir#${PS_BENCH_DIR}}"
  container_dir="/app${rel_dir}"

  suite_dir="scalability"
  if [[ "${scenario_file}" == *"/qos-variation/"* ]]; then
    suite_dir="qos-variation"
  fi

  if [ ! -f "${PS_BENCH_DIR}/${COMPOSE_FILE}" ]; then
    echo "Skipping: missing ${COMPOSE_FILE} in ${PS_BENCH_DIR}" >&2
    break
  fi

  for repeat in $(seq 1 "${REPEAT_COUNT}"); do
    marker=$(mktemp)
    touch "${marker}"

    run_tag="${scenario_name}_dds_run${repeat}"
    echo
    echo "=== dds: ${scenario_name} (run ${repeat}/${REPEAT_COUNT}) ==="

    current_compose="${COMPOSE_FILE}"
    if ! (cd "${PS_BENCH_DIR}" && \
          SCEN_DIR="${container_dir}" \
          SCENARIO="${scenario_name}" \
          RUN_TAG="${run_tag}" \
          docker compose -f "${COMPOSE_FILE}" up --build --force-recreate --abort-on-container-exit)
    then
      echo "Run failed for scenario ${scenario_name}" >&2
      rm -f "${marker}"
      exit 1
    fi

    (cd "${PS_BENCH_DIR}" && docker compose -f "${COMPOSE_FILE}" down --remove-orphans >/dev/null)
    current_compose=""

    move_new_entries "${RESULTS_ROOT}" "${DDS_RESULTS_ROOT}/${suite_dir}" "${marker}"

    rm -f "${marker}"
  done
done
