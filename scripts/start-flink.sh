#!/usr/bin/env bash
set -euo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
source scripts/env.sh

if [[ ! -d "$FLINK_HOME" ]]; then
  echo "FLINK_HOME=$FLINK_HOME not found. Run ./scripts/setup-flink.sh first."
  exit 1
fi

# Patch flink-conf.yaml on first start to bind to all interfaces and bump slots.
CONF="$FLINK_HOME/conf/flink-conf.yaml"
if [[ ! -f "$CONF.patched" ]]; then
  sed -i.bak \
    -e 's|^rest.bind-address:.*|rest.bind-address: 0.0.0.0|' \
    -e 's|^jobmanager.bind-host:.*|jobmanager.bind-host: 0.0.0.0|' \
    -e 's|^taskmanager.bind-host:.*|taskmanager.bind-host: 0.0.0.0|' \
    -e 's|^taskmanager.numberOfTaskSlots:.*|taskmanager.numberOfTaskSlots: 4|' \
    "$CONF"
  touch "$CONF.patched"
fi

export FLINK_LOG_DIR
"$FLINK_HOME/bin/start-cluster.sh"

# Sanity: REST API responds
for i in {1..30}; do
  if curl -fsS http://localhost:8081/overview >/dev/null 2>&1; then
    echo "Flink is up. Web UI: http://localhost:8081"
    exit 0
  fi
  sleep 1
done
echo "Flink did not become ready in 30s. See $FLINK_LOG_DIR"
exit 1
