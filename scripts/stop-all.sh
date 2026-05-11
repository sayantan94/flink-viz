#!/usr/bin/env bash
set -uo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
source scripts/env.sh

for svc in ui backend generator; do
  pidfile="$PID_DIR/$svc.pid"
  if [[ -f "$pidfile" ]]; then
    pid="$(cat "$pidfile")"
    if kill -0 "$pid" 2>/dev/null; then
      echo "Stopping $svc (pid $pid)"
      kill "$pid" 2>/dev/null || true
      # also try process-group kill so child npm/uvicorn workers go down too
      kill -- -"$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  fi
done

if [[ -d "$FLINK_HOME" ]]; then
  echo "Stopping Flink cluster"
  "$FLINK_HOME/bin/stop-cluster.sh" || true
fi

if [[ -f "$PID_DIR/kafka.pid" ]]; then
  pid="$(cat "$PID_DIR/kafka.pid")"
  if kill -0 "$pid" 2>/dev/null; then
    echo "Stopping Kafka (pid $pid)"
    kill "$pid" 2>/dev/null || true
    sleep 2
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$PID_DIR/kafka.pid"
fi

echo "Done."
