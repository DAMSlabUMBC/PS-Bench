#!/bin/sh
set -eu

# Config (overridable via env)
OUT_DIR="${OUT_DIR:-/app/out}"
SCEN_DIR="${SCEN_DIR:-/app/configs/scenarios}"
CONFIG_EXS="${CONFIG_EXS:-/app/configs/config.exs}"

BROKER_HOST="${BROKER_HOST:-mosq1}"
BROKER_PORT="${BROKER_PORT:-1883}"
SELECTED_SCENARIO="${SELECTED_SCENARIO:-test_scenario}"

# 8-char container id suffix for uniqueness
CID_SUFFIX="${CLIENT_ID_SUFFIX:-$(hostname | tr -cd 'A-Za-z0-9' | cut -c1-8)}"
NODE_SNAME="runner_${CID_SUFFIX}"

# Erlang release paths
REL_ROOT="$(ls -d /app/_build/*/rel/ps_bench 2>/dev/null | head -n1 || true)"
[ -z "${REL_ROOT}" ] && REL_ROOT="/app/_build/default/rel/ps_bench"
BIN="${REL_ROOT}/bin/ps_bench"

mkdir -p "${OUT_DIR}"

log() { printf '%s %s\n' "[entrypoint]" "$*"; }


# Force a unique Erlang node name across containers
force_unique_sname() {
  if [ -d "${REL_ROOT}/releases" ]; then
    find "${REL_ROOT}/releases" -type f -name 'vm.args' | while read -r f; do
      sed -i -r '/^[[:space:]]*-[sn]name[[:space:]]+.*/d' "$f" || true
      printf '%s\n' "-sname ${NODE_SNAME}" >> "$f"
      log "vm.args patched: $f => -sname ${NODE_SNAME}"
    done
  else
    log "WARNING: releases dir not found at ${REL_ROOT}/releases; cannot patch vm.args"
  fi

  # Ensure our -sname is appended at the end of erl args
  if [ -n "${ERL_ZFLAGS:-}" ]; then
    export ERL_ZFLAGS="${ERL_ZFLAGS} -sname ${NODE_SNAME}"
  else
    export ERL_ZFLAGS="-sname ${NODE_SNAME}"
  fi
  log "ERL_ZFLAGS set to append -sname ${NODE_SNAME}"
}


# Patch broker host/port inside scenario files (BusyBox-safe sed)
patch_broker_in_scenarios() {
  if [ -d "${SCEN_DIR}" ]; then
    find "${SCEN_DIR}" -type f -name '*.scenario' | while read -r f; do
      sed -i -r "s|^[[:space:]]*host[[:space:]]*=[[:space:]]*\"[^\"]*\"|host = \"${BROKER_HOST}\"|g" "$f" || true
      sed -i -r "s|^[[:space:]]*hostname[[:space:]]*=[[:space:]]*\"[^\"]*\"|hostname = \"${BROKER_HOST}\"|g" "$f" || true
      sed -i -r "s|^[[:space:]]*port[[:space:]]*=[[:space:]]*[0-9]+|port = ${BROKER_PORT}|g" "$f" || true
    done
    log "Broker patched in scenarios: host=${BROKER_HOST}, port=${BROKER_PORT}."
  else
    log "WARNING: Scenario dir ${SCEN_DIR} not found; skipping broker patch."
  fi
}


# suffixing of any explicit client IDs in scenarios
suffix_client_ids() {
  [ -d "${SCEN_DIR}" ] || return 0
  find "${SCEN_DIR}" -type f -name '*.scenario' | while read -r f; do
    # Handle '=' or '=>'; only add if this exact suffix not already present
    if ! grep -q "_${CID_SUFFIX}\"" "$f"; then
      sed -i -r \
        -e "s|([[:space:]]client_name[[:space:]]*(=|=>)[[:space:]]*\"[^\"]*)\"|\1_${CID_SUFFIX}\"|g" \
        -e "s|([[:space:]]name[[:space:]]*(=|=>)[[:space:]]*\"[^\"]*)\"|\1_${CID_SUFFIX}\"|g" \
        -e "s|([[:space:]]client_id[[:space:]]*(=|=>)[[:space:]]*\"[^\"]*)\"|\1_${CID_SUFFIX}\"|g" \
        "$f" || true
    fi
  done
  log "Scenario client IDs suffixed with _${CID_SUFFIX} (best-effort)."
}

patch_selected_scenario() {
  if [ -f "${CONFIG_EXS}" ]; then
    sed -i -r "s|^[[:space:]]*selected_scenario[[:space:]]*=[[:space:]]*\"[^\"]*\"|selected_scenario = \"${SELECTED_SCENARIO}\"|g" "${CONFIG_EXS}" || true
    sed -i -r "s|\{[[:space:]]*selected_scenario[[:space:]]*,[[:space:]]*\"[^\"]*\"[[:space:]]*\}|{selected_scenario,\"${SELECTED_SCENARIO}\"}|g" "${CONFIG_EXS}" || true
    log "Selected scenario set to: ${SELECTED_SCENARIO}"
  fi
}

# CSV exporters
to_csv() {
  src="$1"
  dir="$(dirname "$src")"
  base="$(basename "$src")"

  case "$base" in
    metrics.out)   dest="${dir}/metrics_out.csv" ;;
    metrics.out.*) dest="${dir}/$(echo "$base" | sed 's/^metrics\.out\./metrics_out./').csv" ;;
    *)             dest="${src}.csv" ;;
  esac

  if [ ! -s "$src" ]; then
    printf "metric,value\n" > "$dest"
    return 0
  fi

  awk '
    function ltrim(s){sub(/^[ \t\r\n]+/,"",s);return s}
    function rtrim(s){sub(/[ \t\r\n]+$/,"",s);return s}
    function trim(s){return rtrim(ltrim(s))}
    BEGIN { OFS=","; print "metric","value" }
    {
      line=$0
      c=index(line,":"); e=index(line,"=")
      sep=0
      if (c>0 && e>0)      sep = (c<e ? c : e)
      else if (c>0)        sep = c
      else if (e>0)        sep = e

      if (sep>0) {
        key=trim(substr(line,1,sep-1))
        val=trim(substr(line,sep+1))
        gsub(/"/,"\"\"",val)
        print key,"\"" val "\""
      } else {
        gsub(/"/,"\"\"",line)
        print "raw","\"" line "\""
      }
    }
  ' "$src" > "$dest"
}

convert_all_metrics() {
  # Aggregate index
  index_csv="${OUT_DIR}/metrics_index.csv"
  printf "source,metric,value\n" > "$index_csv"

  # Find every metrics.out or metrics.out.*
  find "${OUT_DIR}" -type f \( -name 'metrics.out' -o -name 'metrics.out.*' \) | while read -r src; do
    to_csv "$src"

    # Append to index (skip per-file header)
    dir="$(dirname "$src")"
    base="$(basename "$src")"
    case "$base" in
      metrics.out)   dest="${dir}/metrics_out.csv" ;;
      metrics.out.*) dest="${dir}/$(echo "$base" | sed 's/^metrics\.out\./metrics_out./').csv" ;;
      *)             dest="${src}.csv" ;;
    esac

    # Use a path relative to OUT_DIR for the "source" column
    rel="${src#${OUT_DIR}/}"
    if [ -f "$dest" ]; then
      # Skip header line from dest and prefix the source path
      tail -n +2 "$dest" | awk -v s="$rel" -F, 'BEGIN{OFS=","} {print s,$1,$2}' >> "$index_csv"
    fi
  done

  log "CSV export complete. See: ${index_csv} and per-file metrics_out*.csv."
}

###############################################################################
# F) Run
###############################################################################
force_unique_sname
patch_broker_in_scenarios
suffix_client_ids
patch_selected_scenario

if [ ! -x "${BIN}" ]; then
  log "ERROR: Release binary not found at ${BIN}"
  exit 1
fi

log "Starting ps_bench with Erlang short name: ${NODE_SNAME} (CID_SUFFIX=${CID_SUFFIX})"
set +e
"${BIN}" foreground &
APP_PID="$!"

trap 'log "SIGTERM received, stopping ps_bench…"; kill -TERM "${APP_PID}" 2>/dev/null; wait "${APP_PID}" 2>/dev/null || true' TERM
trap 'log "SIGINT received, stopping ps_bench…"; kill -INT  "${APP_PID}" 2>/dev/null; wait "${APP_PID}" 2>/dev/null || true' INT

wait "${APP_PID}"
APP_RC="$?"
set -e

log "ps_bench exited with code ${APP_RC}; converting metrics to CSV…"
convert_all_metrics

log "Done."
exit "${APP_RC}"
