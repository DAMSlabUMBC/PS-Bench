#!/usr/bin/env bash
set -euo pipefail

# Run all single-node QoS variation scenarios against each MQTT broker compose stack.

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PS_BENCH_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)
SCEN_ROOT="${PS_BENCH_DIR}/configs/builtin-test-suites/testcases/qos-variation/1-node"
RESULTS_ROOT="${PS_BENCH_DIR}/results"
REPEAT_COUNT=${REPEAT_COUNT:-3}
BROKER_LIST=${BROKER_LIST:-emqx,mosquitto,nanomq,vernemq,mochi}
IFS=',' read -r -a BROKERS <<<"${BROKER_LIST}"
SCEN_FILTER=${SCEN_FILTER:-}

if ! command -v docker >/dev/null 2>&1; then
  echo "docker CLI is required but was not found" >&2
  exit 1
fi

if [ ! -d "${SCEN_ROOT}" ]; then
  echo "Scenario root not found: ${SCEN_ROOT}" >&2
  exit 1
fi

mkdir -p "${RESULTS_ROOT}" 2>/dev/null || true

mapfile -t SCENARIO_FILES < <(find "${SCEN_ROOT}" -type f -name 'qossuite_*_mqttv5*_1_node.scenario' | sort)
if [ ${#SCENARIO_FILES[@]} -eq 0 ]; then
  echo "No single-node MQTT QoS scenarios found under ${SCEN_ROOT}" >&2
  exit 1
fi

is_broker_name() {
  local candidate="$1"
  for known in "${BROKERS[@]}"; do
    if [ "${candidate}" = "${known}" ]; then
      return 0
    fi
  done
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
    if is_broker_name "${base}"; then
      continue
    fi
    if [ "${base}" = "dds" ]; then
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

  for broker in "${BROKERS[@]}"; do
    compose_file="docker-compose.single.${broker}.yml"
    if [ ! -f "${PS_BENCH_DIR}/${compose_file}" ]; then
      echo "Skipping broker ${broker}: missing ${compose_file}" >&2
      continue
    fi

    for repeat in $(seq 1 "${REPEAT_COUNT}"); do
      marker=$(mktemp)
      touch "${marker}"

      run_tag="${scenario_name}_${broker}_run${repeat}"
      echo
      echo "=== ${broker}: ${scenario_name} (run ${repeat}/${REPEAT_COUNT}) ==="

      current_compose="${compose_file}"
      if ! (cd "${PS_BENCH_DIR}" && \
            SCEN_DIR="${container_dir}" \
            SCENARIO="${scenario_name}" \
            RUN_TAG="${run_tag}" \
            docker compose -f "${compose_file}" up --build --force-recreate --abort-on-container-exit)
      then
        echo "Run failed for broker ${broker} and scenario ${scenario_name}" >&2
        rm -f "${marker}"
        exit 1
      fi

      (cd "${PS_BENCH_DIR}" && docker compose -f "${compose_file}" down --remove-orphans >/dev/null)
      current_compose=""

      move_new_entries "${RESULTS_ROOT}" "${RESULTS_ROOT}/${broker}" "${marker}"

      rm -f "${marker}"
    done
  done
done
