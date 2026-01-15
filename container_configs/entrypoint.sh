#!/usr/bin/env bash
set -eu

cd /app 2>/dev/null || true

# For recursive searching
shopt -s globstar

RUN_TAG="${RUN_TAG:-$(hostname | tr -cd 'a-zA-Z0-9' | tail -c 8)}"
RUN_TS="${RUN_TS:-$(date +%Y%m%d_%H%M%S)}"

OUT_DIR="${OUT_DIR:-/app/out}"
OUT_DIR="${OUT_DIR%/}/${RUN_TAG}_${RUN_TS}"
export RUN_TAG RUN_TS OUT_DIR

CONFIG_EXS="${CONFIG_EXS:-/app/configs/config.exs}"
BROKER_HOST="${BROKER_HOST:-mqtt}"
BROKER_PORT="${BROKER_PORT:-1883}"
DIST_MODE="${DIST_MODE:-sname}"
export ERLANG_DIST_MODE="$DIST_MODE"

DEFAULT_SCEN="scalabilitysuite_smart_home_mqttv5"

# Use SCENARIO for the scenario file name when provided
if [ -n "${SCENARIO:-}" ]; then
  SCEN_CHOSEN="${SCENARIO}"
elif [ -n "${SELECTED_SCENARIO:-}" ]; then
  SCEN_CHOSEN="${SELECTED_SCENARIO}"
else
  SCEN_CHOSEN="${DEFAULT_SCEN}"
fi

# Detect DDS by scenario name (dds_*)
IS_DDS="false"
if [[ $SCEN_CHOSEN == *"dds"* ]]; then
  IS_DDS="true"
fi

# Node base (erl short name)
_node_base_default="runner_${RUN_TAG:-$(hostname | tr -cd 'a-zA-Z0-9' | tail -c 8)}"
NODE_BASE="${NODE_BASE:-${_node_base_default}}"
NODE_BASE="$(printf '%s' "$NODE_BASE" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g')"
case "$NODE_BASE" in [a-z]* ) : ;; * ) NODE_BASE="r_${NODE_BASE}" ;; esac
[ -z "$NODE_BASE" ] && NODE_BASE="runner1"

RELEASE_COOKIE="${RELEASE_COOKIE:-ps_bench_cookie}"
export NODE_BASE RELEASE_COOKIE

REL_ROOT="$(ls -d /app/_build/*/rel/ps_bench 2>/dev/null | head -n1 || true)"
[ -z "$REL_ROOT" ] && REL_ROOT="/app/_build/default/rel/ps_bench"
BIN="$REL_ROOT/bin/ps_bench"

log() {
  if [ "$IS_DDS" = "true" ]; then
    printf '[entrypoint-dds] %s\n' "$*"
  else
    printf '[entrypoint] %s\n' "$*"
  fi
}

# Escape for sed replacement
esc_sed_repl() { printf '%s' "$1" | sed 's/[\/&|]/\\&/g'; }

strip_name_flags() {
  printf '%s' "${1:-}" | sed -E 's/(^|[[:space:]])-(sname|name)[[:space:]]+[^[:space:]]+//g'
}

stage_configs() {
  local work_root="/tmp/cfg_${NODE_BASE}_${RUN_TAG}"

  rm -rf "$work_root"
  mkdir -p "$work_root/devices" "$work_root/deployments" "$work_root/scenarios"

  cp -a "${DEVICE_DIR}/."     "$work_root/devices/"      2>/dev/null || true
  cp -a "${DEPLOY_DIR}/." "$work_root/deployments/"  2>/dev/null || true
  cp -a "${SCEN_DIR}/."   "$work_root/scenarios/"    2>/dev/null || true

  SCEN_DIR="$work_root/scenarios"
  export SCEN_DIR
  log "Working SCEN_DIR=${SCEN_DIR}"
}

# Compute host prefix mapping: runnerMQTT{N} / runnerDDS{N} 
host_prefix_autodetect() {
  # Allow override
  [ -n "${HOST_PREFIX:-}" ] && { printf '%s' "$HOST_PREFIX"; return; }
  # Default: strip trailing digits from container hostname
  hp="$(hostname | sed 's/[0-9][0-9]*$//')"
  # Fallbacks if ran outside compose
  if [ -z "$hp" ]; then
    if [ "$IS_DDS" = "true" ]; then hp="runnerDDS"; else hp="runnerMQTT"; fi
  fi
  printf '%s' "$hp"
}

# Replace hostnames per runner, whitespace tolerant, and correct prefix 
patch_hostnames_in_scenarios() {
  [ -d "$SCEN_DIR" ] || return 0

  # Domain suffix only for -name (longnames)
  DOMAIN_PART="$(hostname -d 2>/dev/null || true)"
  [ -z "$DOMAIN_PART" ] && DOMAIN_PART="${DOMAINNAME:-benchnet}"
  case "$DIST_MODE" in
    name|longnames) FQDN_SUFFIX=".${DOMAIN_PART}";;
    *)              FQDN_SUFFIX="";;
  esac

  HOST_PREFIX="$(host_prefix_autodetect)"
  export FQDN_SUFFIX HOST_PREFIX

  for f in "$SCEN_DIR"/**/*.scenario; do
    [ -f "$f" ] || continue

    # 1) Explicit placeholders: REPLACE_RUNNER{n}_HOSTNAME → runner{MQTT/DDS}{n}[.domain]
    perl -0777 -pe '
      my $suf = $ENV{FQDN_SUFFIX} // "";
      my $pfx = $ENV{HOST_PREFIX} // "runner";
      for my $n (1..8) {
        my $host = $pfx.$n.$suf;
        s/(\x27)REPLACE_RUNNER${n}_HOSTNAME\1/$1$host$1/g;  # single-quoted
        s/(")REPLACE_RUNNER${n}_HOSTNAME\1/$1$host$1/g;     # double-quoted
      }
    ' "$f" > "$f.tmp" && mv "$f.tmp" "$f"

    # 2) Scoped REPLACE_HOSTNAME inside runner blocks → runner{MQTT/DDS}{n}[.domain]
    perl -0777 -pe '
      my $suf = $ENV{FQDN_SUFFIX} // "";
      my $pfx = $ENV{HOST_PREFIX} // "runner";

      my @lines = split /\n/;
      my $runner = 0;
      my $brace_depth = 0;
      my $runner_depth = -1;

      sub host_for { my $n = shift; return $pfx.$n.$suf; }

      for (@lines) {
        # Update depth approximately (good enough for these files)
        my $opens  = () = /\{/g;
        my $closes = () = /\}/g;

        # Detect entering a runner block (e.g., "{ runner3 ,", "{runner2,", "runner4 =>", "\"runner1\"", etc.)
        if (!$runner) {
          if (/\{\s*runner([1-8])\s*,/ || /\brunner([1-8])\b\s*=>/ || /"runner([1-8])"/ || /\brunner([1-8])\b\s*:/) {
            $runner = $1;
            $runner_depth = $brace_depth + $opens - $closes; # depth at this line
          }
        }

        if ($runner) {
          my $h = host_for($runner);
          s/(\x27)REPLACE_HOSTNAME\1/$1$h$1/g;   # 'REPLACE_HOSTNAME'
          s/(")REPLACE_HOSTNAME\1/$1$h$1/g;      # "REPLACE_HOSTNAME"
          s/\bREPLACE_HOSTNAME\b/$h/g;           # bare
        }

        # Move depth after replacements
        $brace_depth += $opens - $closes;

        # If we left the runner block, clear
        if ($runner && $brace_depth < $runner_depth) {
          $runner = 0; $runner_depth = -1;
        }
      }
      $_ = join("\n", @lines);
    ' "$f" > "$f.tmp" && mv "$f.tmp" "$f"

    # 3) MQTT only: patch broker ip placeholders if any
    if [ "$IS_DDS" = "false" ]; then
      sed -i "s/\"REPLACE_BROKER_IP\"/\"${BROKER_HOST}\"/g; s/'REPLACE_BROKER_IP'/'${BROKER_HOST}'/g" "$f" || true
    fi
  done

  # Guardrail: fail if any placeholders remain (but only check relevant ones)
  if [ "$IS_DDS" = "true" ]; then
    targets=$(ls "$SCEN_DIR"/dds_*.scenario 2>/dev/null || true)
    if [ -n "$targets" ] && grep -R -n -E "REPLACE_(HOSTNAME|RUNNER[1-8]_HOSTNAME)" $targets >/dev/null; then
      log "ERROR: hostname placeholder(s) still present in DDS scenarios:"
      grep -R -n -E "REPLACE_(HOSTNAME|RUNNER[1-8]_HOSTNAME)" $targets || true
      exit 1
    fi
  else
    # MQTT: check hostname placeholders and broker placeholder in mqtt_* files
    targets=$(ls "$SCEN_DIR"/mqtt_*.scenario 2>/dev/null || true)
    if [ -n "$targets" ] && grep -R -n -E "REPLACE_(HOSTNAME|RUNNER[1-8]_HOSTNAME|BROKER_IP)" $targets >/dev/null; then
      log "ERROR: placeholder(s) still present in MQTT scenarios:"
      grep -R -n -E "REPLACE_(HOSTNAME|RUNNER[1-8]_HOSTNAME|BROKER_IP)" $targets || true
      exit 1
    fi
  fi
  log "Hostnames patched in scenarios"
}

# Replace hostnames per runner, whitespace tolerant, and correct prefix 
patch_hostnames_in_dds_config() {

  FQDN="$(hostname -f 2>/dev/null || true)"
  sed -i -e "s/REPLACE_LOCAL_HOSTNAME/${FQDN}/g" /app/configs/dds_configs/ps_bench_default_dds_interface.ini || true
 
  log "Hostnames patched in DDS config"
}

# If scenario lists specific runners, idle if not listed
maybe_become_idle_if_not_listed() {
  # Scenario file after staging/patching:
  local scen_file="${SCEN_DIR}/${SCEN_CHOSEN}.scenario"
  [ -f "$scen_file" ] || { log "No scenario file at ${scen_file}; will run."; return 0; }

  # Quick override: keep only the first N runners (by numeric suffix)
  if [ -n "${MAX_RUNNERS:-}" ]; then
    local idx; idx="$(printf '%s' "$NODE_BASE" | sed -nE 's/.*([0-9]+)$/\1/p')"
    [ -z "$idx" ] && idx=1
    if [ "$idx" -gt "$MAX_RUNNERS" ]; then
      log "MAX_RUNNERS=$MAX_RUNNERS and my index=$idx -> idling."
      tail -f /dev/null & wait
    fi
  fi

  local my_short_host my_id_short my_id_long
  my_short_host="$(hostname -s)"
  my_id_short="${NODE_BASE}@${my_short_host}"
  my_id_long="$my_id_short"
  if [ "${RELEASE_DISTRIBUTION}" = "name" ] && [ -n "${FULL_HOSTNAME:-}" ]; then
    my_id_long="${NODE_BASE}@${FULL_HOSTNAME}"
  fi

  # Treat scenario as "explicit" if it names any runner1..runner5 anywhere:
  if grep -Eq "runner[1-8]@|\"runner[1-8]\"|'runner[1-8]'|(^|[^[:alnum:]_])runner[1-8]([^[:alnum:]_]|$)" "$scen_file"; then
    # Participate only if THIS runner is referenced (long, short, quoted, or bare)
    if grep -Fq "$my_id_long"  "$scen_file" || \
       grep -Fq "$my_id_short" "$scen_file" || \
       grep -Fq "\"${NODE_BASE}\"" "$scen_file" || \
       grep -Fq "'${NODE_BASE}'"  "$scen_file" || \
       grep -Eq "(^|[^[:alnum:]_])${NODE_BASE}([^[:alnum:]_]|$)" "$scen_file"
    then
      log "Scenario references me; participating."
      return 0
    else
      log "Scenario lists specific runners but not me; idling."
      tail -f /dev/null & wait
    fi
  else
    log "No specific runners referenced in ${SCEN_CHOSEN}; will run."
  fi
}

# Only patch broker for MQTT
patch_broker_in_scenarios() {
  if [ -d "$SCEN_DIR" ]; then
    bh="$(esc_sed_repl "$BROKER_HOST")"
    for f in "$SCEN_DIR"/**/*.scenario; do
      [ -f "$f" ] || continue
      sed -i -r "s|^[[:space:]]*host[[:space:]]*=[[:space:]]*\"[^\"]*\"|host = \"${bh}\"|g" "$f" || true
      sed -i -r "s|^[[:space:]]*hostname[[:space:]]*=[[:space:]]*\"[^\"]*\"|hostname = \"${bh}\"|g" "$f" || true
      sed -i -r "s|^[[:space:]]*port[[:space:]]*=[[:space:]]*[0-9]+|port = ${BROKER_PORT}|g" "$f" || true
      # In case files use the old placeholder style:
      sed -i "s/\"REPLACE_BROKER_IP\"/\"${BROKER_HOST}\"/g; s/'REPLACE_BROKER_IP'/'${BROKER_HOST}'/g" "$f" || true
    done
    log "Broker patched in mqtt_* scenarios host=${BROKER_HOST} port=${BROKER_PORT}"
  else
    log "WARNING: scenario dir $SCEN_DIR not found"
  fi
}

# Suffix client IDs/names in scenarios to avoid clashes
suffix_client_ids() {
  if [ "${ADD_NODE_TO_SUFFIX:-true}" != "true" ]; then
    log "Client ID suffixing disabled"
    return 0
  fi
  [ -d "$SCEN_DIR" ] || return 0
  SUF="_${RUN_TAG:-$(hostname | tr -cd 'a-zA-Z0-9' | tail -c 8)}"
  SUF="$SUF" perl -0777 -pe '
    my $s = $ENV{SUF} // "";
    sub add { my $v = shift; return ($s ne "" && $v !~ /\Q$s\E$/) ? ($v.$s) : $v; }

    # 1) client_* keys
    s{(?<![A-Za-z0-9_])(client_id|clientId|client-name|client_name)\s*(=|=>)\s*"([^"]+)"}{
      my ($k,$eq,$val)=($1,$2,$3);
      "$k $eq \"" . add($val) . "\""
    }ge;

    # 2) bare "name" key (not hostname/username/etc.)
    s{(?<![A-Za-z0-9_])(name)\s*(=|=>)\s*"([^"]+)"}{
      my ($k,$eq,$val)=($1,$2,$3);
      "$k $eq \"" . add($val) . "\""
    }ge;
  ' "$SCEN_DIR"/*.scenario
  log "Scenario client IDs/names suffixed with ${SUF}"
  grep -REn '^\s*(name|client_id|clientId|client-name|client_name)\s*=' "$SCEN_DIR" | head -n 40 || true
}

# Distribution config (-sname / -name)
configure_distribution() {
  log "Starting configure_distribution with DIST_MODE=$DIST_MODE"
  SHORT_HOSTNAME="$(hostname -s)"

  if [ "$DIST_MODE" = "name" ] || [ "$DIST_MODE" = "longnames" ]; then
    DOMAIN_PART="$(hostname -d 2>/dev/null || true)"
    [ -z "$DOMAIN_PART" ] && DOMAIN_PART="${DOMAINNAME:-benchnet}"
    FULL_HOSTNAME="${SHORT_HOSTNAME}.${DOMAIN_PART}"
    export RELEASE_DISTRIBUTION="name"
    export NAME="${NODE_BASE}@${FULL_HOSTNAME}"
    export RELEASE_NODE="${NODE_BASE}@${FULL_HOSTNAME}"
    unset SNAME 2>/dev/null || true
    log "Configured for -name mode: NAME=$NAME"
    log "Full hostname: $FULL_HOSTNAME"
  else
    export RELEASE_DISTRIBUTION="sname"
    export SNAME="$NODE_BASE"
    export RELEASE_NODE="${NODE_BASE}@${SHORT_HOSTNAME}"
    unset NAME 2>/dev/null || true
    log "Configured for -sname mode: SNAME=$SNAME"
    log "Full node will be: $RELEASE_NODE"
  fi

  export RELEASE_COOKIE="${RELEASE_COOKIE:-ps_bench_cookie}"
  export SHORT_HOSTNAME
  if [ "$RELEASE_DISTRIBUTION" = "name" ]; then export FULL_HOSTNAME; fi
  log "RELEASE_COOKIE=$RELEASE_COOKIE"
  log "RELEASE_DISTRIBUTION=$RELEASE_DISTRIBUTION"
  log "RELEASE_NODE=$RELEASE_NODE"
}

patch_node_base_everywhere() {
  local nb="$NODE_BASE"
  if [ -f "$CONFIG_EXS" ]; then
    sed -i -E \
      -e 's/(node[_-]?base[[:space:]]*=[[:space:]]*")[^"]*"/\1'"$nb"'"/g' \
      -e 's/(\{[[:space:]]*node[_-]?base[[:space:]]*,[[:space:]]*")[^"]*"/\1'"$nb"'"/g' \
      "$CONFIG_EXS" || true
    sed -i -E 's/"runner1_([^"]+)"/"'"$nb"'_\1"/g' "$CONFIG_EXS" || true
  fi

  if [ -d "$SCEN_DIR" ]; then
    for f in "$SCEN_DIR"/**/*.scenario; do
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

patch_selected_scenario() {
  if [ -f "$CONFIG_EXS" ]; then
    ss="$(esc_sed_repl "$SCEN_CHOSEN")"
    sed -i -r "s|(^[[:space:]]*selected_scenario[[:space:]]*=[[:space:]]*)\"[^\"]*\"|\1\"${ss}\"|g" "$CONFIG_EXS" || true
    sed -i -r "s|\{[[:space:]]*selected_scenario[[:space:]]*,[[:space:]]*\"[^\"]*\"[[:space:]]*\}|{selected_scenario,\"${ss}\"}|g" "$CONFIG_EXS" || true
    log "Selected scenario set to ${SCEN_CHOSEN}"
  else
    log "WARNING: config.exs not found at $CONFIG_EXS"
  fi
}

create_app_config() {
  local config_path="${REL_ROOT}/releases/${REL_VSN}/sys.config"
  local CFG_ROOT
  CFG_ROOT="$(dirname "$SCEN_DIR")"

  cat > "$config_path" << EOF
[
  {ps_bench, [
    {node_name, ${NODE_BASE}},
    {device_definitions_directory, "$CFG_ROOT/devices"},
    {deployment_definitions_directory, "$CFG_ROOT/deployments"},
    {scenario_definitions_directory, "$SCEN_DIR"},
    {selected_scenario, ${SCEN_CHOSEN}}
  ]}
].
EOF
  log "Created sys.config with selected_scenario=${SCEN_CHOSEN}"
}

build_dds_nif() {
    # Do required replaces
    ERL_PATH=$(realpath /usr/local/lib/erlang/erts*)
    sed -i -e "s@REPLACE_ERTS_ROOT@${ERL_PATH}@g" /app/priv/dds_cplusplus/build.sh || true
    sed -i -e 's@REPLACE_XERCES_ROOT@/usr/lib@g' /app/priv/dds_cplusplus/build.sh || true
    
    source /opt/OpenDDS-3.33.0/setenv.sh

    cd /app/priv/dds_cplusplus 
    chmod 777 build.sh 
    bash build.sh
}

# metrics helpers 
to_csv() {
  src="$1"
  dir="$(dirname "$src")"
  base="$(basename "$src")"
  if [ "$base" = "metrics.out" ]; then
    dest="${dir}/metrics_out.csv"
  else
    case_prefix="$(printf '%s' "$base" | sed 's/^metrics\.out\./metrics_out./')"
    if printf '%s' "$base" | grep -q '^metrics\.out\.'; then
      dest="${dir}/${case_prefix}.csv"
    else
      dest="${src}.csv"
    fi
  fi
  if [ ! -s "$src" ]; then
    printf "metric,value\n" > "$dest"
    return 0
  fi
  awk '
    function ltrim(s){sub(/^[ \t\r\n]+/,"",s);return s}
    function rtrim(s){sub(/[ \t\r\n]+$/,"",s);return s}
    function trim(s){return rtrim(ltrim(s))}
    BEGIN{OFS=",";print "source_metric","value"}
    {
      line=$0; c=index(line,":"); e=index(line,"="); sep=0
      if(c>0 && e>0){sep=(c<e)?c:e}else if(c>0){sep=c}else if(e>0){sep=e}
      if(sep>0){ key=trim(substr(line,1,sep-1)); val=trim(substr(line,sep+1)); gsub(/"/,"\"\"",val); print key,"\"" val "\"" }
      else { gsub(/"/,"\"\"",line); print "raw","\"" line "\"" }
    }' "$src" > "$dest"
}

convert_all_metrics() {
  index_csv="${OUT_DIR}/metrics_index.csv"
  mkdir -p "$OUT_DIR"
  printf "source,metric,value\n" > "$index_csv"
  find "$OUT_DIR" -type f \( -name 'metrics.out' -o -name 'metrics.out.*' \) 2>/dev/null | while read -r src
  do
    to_csv "$src"
    dir="$(dirname "$src")"
    base="$(basename "$src")"
    if [ "$base" = "metrics.out" ]; then
      dest="${dir}/metrics_out.csv"
    else
      dest_guess="${dir}/$(printf '%s' "$base" | sed 's/^metrics\.out\./metrics_out./').csv"
      if [ -f "$dest_guess" ]; then dest="$dest_guess"; else dest="${src}.csv"; fi
    fi
    rel="${src#${OUT_DIR}/}"
    if [ -f "$dest" ]; then
      tail -n +2 "$dest" | awk -v s="$rel" -F, 'BEGIN{OFS=","} {print s,$1,$2}' >> "$index_csv"
    fi
  done
  log "CSV export complete ${index_csv}"
}

on_term() {
  log "SIGTERM received stopping"
  kill -TERM "$APP_PID" 2>/dev/null || true
  wait "$APP_PID" 2>/dev/null || true
  convert_all_metrics
  exit 0
}
on_int() {
  log "SIGINT received stopping"
  kill -INT "$APP_PID" 2>/dev/null || true
  wait "$APP_PID" 2>/dev/null || true
  convert_all_metrics
  exit 130
}

main() {
  mkdir -p "$OUT_DIR"
  log "OUT_DIR=${OUT_DIR}"

  if [ -x /usr/local/bin/node_exporter ]; then
    log "Starting node_exporter for metrics..."
    /usr/local/bin/node_exporter --collector.disable-defaults --collector.cpu --collector.meminfo > /dev/null 2>&1 &
    NODE_EXPORTER_PID=$!
    log "Node exporter started with PID $NODE_EXPORTER_PID"
  else
    log "WARNING: node_exporter not found at /usr/local/bin/node_exporter"
  fi

  stage_configs

  : "${ERL_AFLAGS:=}"; : "${ELIXIR_ERL_OPTIONS:=}"
  ERL_AFLAGS="$(strip_name_flags "$ERL_AFLAGS")"
  ELIXIR_ERL_OPTIONS="$(strip_name_flags "$ELIXIR_ERL_OPTIONS")"
  export ERL_AFLAGS ELIXIR_ERL_OPTIONS

  unset RELEASE_SNAME RELEASE_NAME 2>/dev/null || true

  log "Before configure_distribution: SNAME=${SNAME:-not_set} NAME=${NAME:-not_set}"
  configure_distribution
  log "After configure_distribution: SNAME=${SNAME:-not_set} NAME=${NAME:-not_set}"
  log "RELEASE_NODE=${RELEASE_NODE:-not_set} RELEASE_DISTRIBUTION=${RELEASE_DISTRIBUTION:-not_set}"

  REL_VSN="$(basename "$(ls -d "${REL_ROOT}/releases"/*/ 2>/dev/null | sort | tail -n1)")"
  export REL_VSN
  log "Found release version: ${REL_VSN}"

  find "$REL_ROOT/releases" -type f -name vm.args -exec sh -c 'echo "--- {} ---"; head -n 20 "{}"' \; || true

  suffix_client_ids
  patch_broker_in_scenarios
  patch_hostnames_in_scenarios
  maybe_become_idle_if_not_listed

  patch_node_base_everywhere
  patch_selected_scenario
  create_app_config

  if [ "$IS_DDS" = "true" ]; then
    # Make logging dir
    mkdir -p "/app/logs"
    
    patch_hostnames_in_dds_config
    build_dds_nif
  fi

  if [ -z "${NODE_HOST_OVERRIDE:-}" ]; then
    if [ "$RELEASE_DISTRIBUTION" = "name" ]; then
      export NODE_HOST_OVERRIDE="${FULL_HOSTNAME}"
    else
      export NODE_HOST_OVERRIDE="$(hostname -s)"
    fi
    log "NODE_HOST_OVERRIDE auto-set to: ${NODE_HOST_OVERRIDE}"
  else
    log "NODE_HOST_OVERRIDE manually set to: ${NODE_HOST_OVERRIDE}"
  fi

  if [ ! -x "$BIN" ]; then
    log "ERROR release binary not found at $BIN"
    exit 1
  fi

  if [ "$IS_DDS" = "true" ]; then
    log "Starting ps_bench (DDS) as ${NODE_BASE}@$(hostname) scenario=${SCEN_CHOSEN} pwd=`pwd`"
  else
    log "Starting ps_bench (MQTT) as ${NODE_BASE}@$(hostname) broker=${BROKER_HOST}:${BROKER_PORT} scenario=${SCEN_CHOSEN} pwd=`pwd`"
  fi

  log "Environment check before running:"
  log "  SNAME=${SNAME:-not_set}"
  log "  NAME=${NAME:-not_set}"
  log "  RELEASE_NODE=${RELEASE_NODE:-not_set}"

  set +e
  export RELX_REPLACE_OS_VARS=true

  REL_VSN="$(basename "$(ls -d "${REL_ROOT}/releases"/*/ 2>/dev/null | sort | tail -n1)")"
  VM_ARGS_PATH="${REL_ROOT}/releases/${REL_VSN}/vm.args"
  VM_ARGS_SRC="${REL_ROOT}/releases/${REL_VSN}/vm.args.src"
  mkdir -p "$(dirname "$VM_ARGS_PATH")"

  {
    echo "-kernel inet_dist_listen_min 15000"
    echo "-kernel inet_dist_listen_max 15010"
    echo "+Bd"
    echo "-setcookie ${RELEASE_COOKIE}"
    if [ "${RELEASE_DISTRIBUTION}" = "name" ]; then
      echo "-name ${NAME}"
    else
      echo "-sname ${SNAME}"
    fi
  } > "$VM_ARGS_PATH"
  cp -f "$VM_ARGS_PATH" "$VM_ARGS_SRC" 2>/dev/null || true

  log "Created vm.args with content:"
  while IFS= read -r line; do log "  $line"; done < "$VM_ARGS_PATH"

  log "Running: $BIN foreground"
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
  if [ -n "${NODE_EXPORTER_PID:-}" ]; then
    kill "$NODE_EXPORTER_PID" 2>/dev/null || true
    log "Stopped node_exporter"
  fi
  exit "$APP_RC"
}


main "$@"