#!/usr/bin/env bash
set -euo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
source scripts/env.sh

cd backend
if [[ ! -d node_modules ]]; then
  npm install
fi

if [[ -f "$PID_DIR/backend.pid" ]] && kill -0 "$(cat "$PID_DIR/backend.pid")" 2>/dev/null; then
  echo "Backend already running."
  exit 0
fi

echo "Starting backend on :3000"
KAFKA_BROKERS=localhost:9092 TOPK_TOPIC=results.topk PORT=3000 \
  nohup npx tsx src/server.ts > "$FV_STATE/backend.out" 2>&1 &
echo $! > "$PID_DIR/backend.pid"

for i in {1..30}; do
  if curl -fsS http://localhost:3000/health >/dev/null 2>&1; then
    echo "Backend is up."
    exit 0
  fi
  sleep 0.5
done
echo "Backend did not become ready. See $FV_STATE/backend.out"
exit 1
