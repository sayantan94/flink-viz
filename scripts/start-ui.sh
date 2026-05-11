#!/usr/bin/env bash
set -euo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
source scripts/env.sh

cd ui
if [[ ! -d node_modules ]]; then
  npm install
fi

if [[ -f "$PID_DIR/ui.pid" ]] && kill -0 "$(cat "$PID_DIR/ui.pid")" 2>/dev/null; then
  echo "UI already running."
  exit 0
fi

echo "Starting UI dev server on :5273"
nohup npm run dev > "$FV_STATE/ui.out" 2>&1 &
echo $! > "$PID_DIR/ui.pid"

for i in {1..30}; do
  if curl -fsS http://localhost:5273 >/dev/null 2>&1; then
    echo "UI is up: http://localhost:5273"
    exit 0
  fi
  sleep 0.5
done
echo "UI did not become ready. See $FV_STATE/ui.out"
exit 1
