#!/usr/bin/env sh
set -eu

cd /app 2>/dev/null || true

RUN_TAG="${RUN_TAG:-$(hostname | tr -cd 'a-zA-Z0-9' | tail -c 8)}"
RUN_TS="${RUN_TS:-$(date +%Y%m%d_%H%M%S)}"

OUT_DIR="${OUT_DIR:-/app/out}"
OUT_DIR="${OUT_DIR%/}/${RUN_TAG}_${RUN_TS}"
export RUN_TAG RUN_TS OUT_DIR

SCEN_DIR="${SCEN_DIR:-/app/configs/initial_tests/scenarios}"
CONFIG_EXS="${CONFIG_EXS:-/app/configs/config.exs}"
BROKER_HOST="${BROKER_HOST:-mqtt}"
BROKER_PORT="${BROKER_PORT:-1883}"
DIST_MODE="${DIST_MODE:-sname}"
export ERLANG_DIST_MODE="$DIST_MODE"


DEFAULT_SCEN="mqtt_multi_node_light"
if [ -n "${SELECTED_SCENARIO:-}" ]
then
  SCEN_CHOSEN="$SELECTED_SCENARIO"
else
  if [ -n "${SCENARIO:-}" ]
  then
    SCEN_CHOSEN="$SCENARIO"
  else
    SCEN_CHOSEN="$DEFAULT_SCEN"
  fi
fi

NODE_BASE="${NODE_BASE:-runner1}"
# Unique, valid short name
_node_base_default="runner_${RUN_TAG:-$(hostname | tr -cd 'a-zA-Z0-9' | tail -c 8)}"
NODE_BASE="${NODE_BASE:-${_node_base_default}}"
# sanitize for erl
NODE_BASE="$(printf '%s' "$NODE_BASE" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g')"
case "$NODE_BASE" in
  [a-z]* ) : ;;
  * ) NODE_BASE="r_${NODE_BASE}" ;;
esac

# Make sure it's not empty
if [ -z "$NODE_BASE" ]; then
    NODE_BASE="runner1"
fi

RELEASE_COOKIE="${RELEASE_COOKIE:-ps_bench_cookie}"
export NODE_BASE RELEASE_COOKIE

REL_ROOT="$(ls -d /app/_build/*/rel/ps_bench 2>/dev/null | head -n1 || true)"
if [ -z "$REL_ROOT" ]
then
  REL_ROOT="/app/_build/default/rel/ps_bench"
fi
BIN="$REL_ROOT/bin/ps_bench"

# log helper
log() {
  printf '[entrypoint] %s\n' "$*"
}

# escape for sed replacement
esc_sed_repl() {
  printf '%s' "$1" | sed 's/[\/&|]/\\&/g'
}

strip_name_flags() {
  printf '%s' "${1:-}" | sed -E 's/(^|[[:space:]])-(sname|name)[[:space:]]+[^[:space:]]+//g'
}

# replace REPLACE_HOSTNAME in scenarios with actual hostname
patch_hostnames_in_scenarios() {
  if [ -d "$SCEN_DIR" ]; then
    # Get full hostname with domain
    FULL_HOSTNAME="$(hostname -f)"
    
    for f in "$SCEN_DIR"/*.scenario; do
      [ -f "$f" ] || continue
      
      # Replace 'REPLACE_HOSTNAME' atoms with full hostname
      sed -i "s/'REPLACE_HOSTNAME'/'${FULL_HOSTNAME}'/g" "$f" || true
      
      # For multi-node scenarios with specific runner hostnames
      sed -i "s/'REPLACE_RUNNER1_HOSTNAME'/'runner01.benchnet'/g" "$f" || true
      sed -i "s/'REPLACE_RUNNER2_HOSTNAME'/'runner02.benchnet'/g" "$f" || true
      sed -i "s/'REPLACE_RUNNER3_HOSTNAME'/'runner03.benchnet'/g" "$f" || true
      sed -i "s/'REPLACE_RUNNER4_HOSTNAME'/'runner04.benchnet'/g" "$f" || true
      sed -i "s/'REPLACE_RUNNER5_HOSTNAME'/'runner05.benchnet'/g" "$f" || true
      
      # Also handle quoted strings if any
      sed -i "s/\"REPLACE_HOSTNAME\"/\"${FULL_HOSTNAME}\"/g" "$f" || true
      sed -i "s/\"REPLACE_BROKER_IP\"/\"${BROKER_HOST}\"/g" "$f" || true
    done
    
    log "Hostnames patched in scenarios to ${FULL_HOSTNAME}"
  fi
}

# This forces the use of -name or -sname based on DIST_MODE
configure_distribution() {
  if [ -d "$REL_ROOT/releases" ]; then
    find "$REL_ROOT/releases" -type f -name vm.args | while read -r f; do
      sed -i -r '/^[[:space:]]*-[sn]name([[:space:]]+|$).*/d' "$f" || true
      
      if [ "$DIST_MODE" = "name" ] || [ "$DIST_MODE" = "longnames" ]; then
        # Use full hostname for -name
        FULL_HOSTNAME="$(hostname -f)"
        printf '%s\n' "-name ${NODE_BASE}@${FULL_HOSTNAME}" >> "$f"
        log "vm.args patched: $f => -name ${NODE_BASE}@${FULL_HOSTNAME}"
        export RELEASE_DISTRIBUTION="name"
        export RELEASE_NODE="${NODE_BASE}@${FULL_HOSTNAME}"
      else
        # Use short hostname for -sname
        SHORT_HOSTNAME="$(hostname -s)"
        printf '%s\n' "-sname ${NODE_BASE}@${SHORT_HOSTNAME}" >> "$f"
        log "vm.args patched: $f => -sname ${NODE_BASE}@${SHORT_HOSTNAME}"
        export RELEASE_DISTRIBUTION="sname"
        export RELEASE_NODE="${NODE_BASE}@${SHORT_HOSTNAME}"
      fi
    done
  else
    log "WARNING: ${REL_ROOT}/releases not found"
  fi
  
  export RELEASE_COOKIE="${RELEASE_COOKIE:-ps_bench_cookie}"
}

# write broker host and port into scenario files
patch_broker_in_scenarios() {
  if [ -d "$SCEN_DIR" ]
  then
    bh="$(esc_sed_repl "$BROKER_HOST")"
    for f in "$SCEN_DIR"/*.scenario
    do
      if [ -f "$f" ]
      then
        sed -i -r "s|^[[:space:]]*host[[:space:]]*=[[:space:]]*\"[^\"]*\"|host = \"${bh}\"|g" "$f" || true
        sed -i -r "s|^[[:space:]]*hostname[[:space:]]*=[[:space:]]*\"[^\"]*\"|hostname = \"${bh}\"|g" "$f" || true
        sed -i -r "s|^[[:space:]]*port[[:space:]]*=[[:space:]]*[0-9]+|port = ${BROKER_PORT}|g" "$f" || true
      fi
    done
    log "Broker patched in scenarios host=${BROKER_HOST} port=${BROKER_PORT}"
  else
    log "WARNING: scenario dir $SCEN_DIR not found"
  fi
}

# Rewrite any hard-coded 'runner1' bases to the unique NODE_BASE
patch_node_base_everywhere() {
  local nb="$NODE_BASE"
  if [ -f "$CONFIG_EXS" ]; then
    sed -i -E \
      -e 's/(node[_-]?base[[:space:]]*=[[:space:]]*")[^"]*"/\1'"$nb"'"/g' \
      -e 's/(\{[[:space:]]*node[_-]?base[[:space:]]*,[[:space:]]*")[^"]*"/\1'"$nb"'"/g' \
      "$CONFIG_EXS" || true
    sed -i -E 's/"runner1_([^"]+)"/"'"$nb"'_\1"/g' "$CONFIG_EXS" || true
    sed -i -E 's/runner1@/'"$nb"'@/g' "$CONFIG_EXS" || true
  fi

  if [ -d "$SCEN_DIR" ]; then
    for f in "$SCEN_DIR"/*.scenario; do
      [ -f "$f" ] || continue
      sed -i -E \
        -e 's/(node[_-]?base[[:space:]]*(=|:)[[:space:]]*")[^"]*"/\1'"$nb"'"/g' \
        -e 's/"runner1_([^"]+)"/"'"$nb"'_\1"/g' \
        -e 's/runner1@/'"$nb"'@/g' \
        "$f" || true
    done
  fi
  log "Node base set to ${nb} in configs/scenarios"
}

# select the scenario in config.exs if present
patch_selected_scenario() {
  if [ -f "$CONFIG_EXS" ]
  then
    ss="$(esc_sed_repl "$SCEN_CHOSEN")"
    sed -i -r "s|(^[[:space:]]*selected_scenario[[:space:]]*=[[:space:]]*)\"[^\"]*\"|\1\"${ss}\"|g" "$CONFIG_EXS" || true
    sed -i -r "s|\{[[:space:]]*selected_scenario[[:space:]]*,[[:space:]]*\"[^\"]*\"[[:space:]]*\}|{selected_scenario,\"${ss}\"}|g" "$CONFIG_EXS" || true
    log "Selected scenario set to ${SCEN_CHOSEN}"
  else
    log "WARNING: config.exs not found at $CONFIG_EXS"
  fi
}

# add a stable suffix to explicit client ids to avoid collisions
suffix_client_ids() {
  if [ "${ADD_NODE_TO_SUFFIX:-true}" != "true" ]; then
    log "Client ID suffixing disabled"
    return 0
  fi
  [ -d "$SCEN_DIR" ] || return 0

  SUF="_${RUN_TAG:-$(hostname | tr -cd 'a-zA-Z0-9' | tail -c 8)}"

  for f in "$SCEN_DIR"/*.scenario; do
    [ -f "$f" ] || continue
    awk -v suf="$SUF" '
      function add_suf(v) { return (v ~ (suf"$")) ? v : v suf }

      {
        line=$0

        # 1) Handle client_* keys anywhere on the line
        #    client_id | clientId | client-name | client_name
        while (match(line, /(^|[^[:alnum:]_])(client_id|clientId|client-name|client_name)[[:space:]]*(=|=>)[[:space:]]*"([^"]+)"/, m)) {
          pre=substr(line,1,RSTART-1); post=substr(line,RSTART+RLENGTH)
          pfx=m[1]; key=m[2]; sep=m[3]; val=m[4]
          line=pre pfx key " " sep " \"" add_suf(val) "\"" post
        }

        # 2) Handle a bare key named "name" (possibly nested),
        #    but NOT parts of larger identifiers like hostname, username, topic_name, device_name, etc.
        #    We ensure the char before 'name' is not [A-Za-z0-9_].
        while (match(line, /(^|[^[:alnum:]_])(name)[[:space:]]*(=|=>)[[:space:]]*"([^"]+)"/, n)) {
          pre=substr(line,1,RSTART-1); post=substr(line,RSTART+RLENGTH)
          pfx=n[1]; key=n[2]; sep=n[3]; val=n[4]
          line=pre pfx key " " sep " \"" add_suf(val) "\"" post
        }

        print line
      }
    ' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
  done

  log "Scenario client IDs/names suffixed with ${SUF}"

  grep -RIn "^[[:space:]]*\(name\|client_id\|clientId\|client-name\|client_name\)[[:space:]]*=" "$SCEN_DIR" | head -n 60 || true
}


# convert a metrics.out style file to csv
to_csv() {
  src="$1"
  dir="$(dirname "$src")"
  base="$(basename "$src")"
  if [ "$base" = "metrics.out" ]
  then
    dest="${dir}/metrics_out.csv"
  else
    case_prefix="$(printf '%s' "$base" | sed 's/^metrics\.out\./metrics_out./')"
    if printf '%s' "$base" | grep -q '^metrics\.out\.'
    then
      dest="${dir}/${case_prefix}.csv"
    else
      dest="${src}.csv"
    fi
  fi
  if [ ! -s "$src" ]
  then
    printf "metric,value\n" > "$dest"
    return 0
  fi
  awk '
    function ltrim(s){
      sub(/^[ \t\r\n]+/,"",s)
      return s
    }
    function rtrim(s){
      sub(/[ \t\r\n]+$/,"",s)
      return s
    }
    function trim(s){
      return rtrim(ltrim(s))
    }
    BEGIN {
      OFS=","
      print "source_metric","value"
    }
    {
      line=$0
      c=index(line,":")
      e=index(line,"=")
      sep=0
      if (c>0 && e>0) {
        if (c<e) {
          sep=c
        } else {
          sep=e
        }
      } else {
        if (c>0) {
          sep=c
        } else {
          if (e>0) {
            sep=e
          }
        }
      }
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

# build a global csv index over all metrics files
convert_all_metrics() {
  index_csv="${OUT_DIR}/metrics_index.csv"
  mkdir -p "$OUT_DIR"
  printf "source,metric,value\n" > "$index_csv"
  find "$OUT_DIR" -type f \( -name 'metrics.out' -o -name 'metrics.out.*' \) 2>/dev/null | while read -r src
  do
    to_csv "$src"
    dir="$(dirname "$src")"
    base="$(basename "$src")"
    if [ "$base" = "metrics.out" ]
    then
      dest="${dir}/metrics_out.csv"
    else
      dest_guess="${dir}/$(printf '%s' "$base" | sed 's/^metrics\.out\./metrics_out./').csv"
      if [ -f "$dest_guess" ]
      then
        dest="$dest_guess"
      else
        dest="${src}.csv"
      fi
    fi
    rel="${src#${OUT_DIR}/}"
    if [ -f "$dest" ]
    then
      tail -n +2 "$dest" | awk -v s="$rel" -F, 'BEGIN{OFS=","} {print s,$1,$2}' >> "$index_csv"
    fi
  done
  log "CSV export complete ${index_csv}"
}

# handle SIGTERM
on_term() {
  log "SIGTERM received stopping"
  if kill -TERM "$APP_PID" 2>/dev/null
  then
    true
  fi
  if wait "$APP_PID" 2>/dev/null
  then
    true
  fi
  convert_all_metrics
  exit 0
}

# handle SIGINT
on_int() {
  log "SIGINT received stopping"
  if kill -INT "$APP_PID" 2>/dev/null
  then
    true
  fi
  if wait "$APP_PID" 2>/dev/null
  then
    true
  fi
  convert_all_metrics
  exit 130
}

# main launcher
main() {
  mkdir -p "$OUT_DIR"
  log "OUT_DIR=${OUT_DIR}"
  : "${ERL_AFLAGS:=}"
  : "${ELIXIR_ERL_OPTIONS:=}"
  ERL_AFLAGS="$(strip_name_flags "$ERL_AFLAGS")"
  ELIXIR_ERL_OPTIONS="$(strip_name_flags "$ELIXIR_ERL_OPTIONS")"
  export ERL_AFLAGS ELIXIR_ERL_OPTIONS

  unset SNAME NAME RELEASE_SNAME RELEASE_NAME
  force_name
  find "$REL_ROOT/releases" -type f -name vm.args -exec sh -c 'echo "--- {} ---"; head -n 20 "{}"' \; || true
  suffix_client_ids
  patch_broker_in_scenarios
  patch_hostnames_in_scenarios
  grep -RIn '^\s*\(host\|hostname\|port\)\s*=' "$SCEN_DIR" || true
  grep -RIn '^\s*\(name\|client_id\|clientId\|client-name\|client_name\)\s*=' "$SCEN_DIR" | head -n 40 || true  
  patch_node_base_everywhere  
  patch_selected_scenario
  export NODE_HOST_OVERRIDE="$(hostname)"
  if [ ! -x "$BIN" ]
  then
    log "ERROR release binary not found at $BIN"
    exit 1
  fi
  log "StartinFg ps_bench as ${NODE_BASE}@$(hostname) broker=${BROKER_HOST}:${BROKER_PORT} scenario=${SCEN_CHOSEN}"
  set +e
  "$BIN" foreground &
  APP_PID="$!"
  trap on_term TERM
  trap on_int INT
  wait "$APP_PID"
  APP_RC="$?"
  set -e
  log "ps_bench exited with code ${APP_RC} converting metrics"
  convert_all_metrics
  log "Done"
  exit "$APP_RC"
}

main "$@"
