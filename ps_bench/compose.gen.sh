#!/usr/bin/env bash
set -euo pipefail

# Load .env if present
if [ -f .env ]; then
  set -a; . ./.env; set +a
fi

RUNNERS="${RUNNERS:-3}"
SCENARIO="${SCENARIO:-mqtt_single_node_light.scenario}"
RUN_TAG="${RUN_TAG:-run1}"
ADD_NODE_TO_SUFFIX="${ADD_NODE_TO_SUFFIX:-false}"
NODE_HOST_OVERRIDE="${NODE_HOST_OVERRIDE:-}"

out="docker-compose.override.yml"
{
  echo 'version: "3.9"'
  echo 'services:'
  for i in $(seq 1 "$RUNNERS"); do
    cat <<YAML
  runner$i:
    image: yourorg/ps-bench:latest
    networks: [benchnet]
    environment:
      SELECTED_SCENARIO: "${SCENARIO}"
      NODE_NAME: "runner$i"
      RUN_TAG: "${RUN_TAG}"
      ADD_NODE_TO_SUFFIX: "${ADD_NODE_TO_SUFFIX}"
      NODE_HOST_OVERRIDE: "${NODE_HOST_OVERRIDE}"
YAML
  done
} > "$out"

echo "Generated $out with $RUNNERS runners for scenario $SCENARIO (tag=$RUN_TAG)"
