#!/usr/bin/env bash
set -euo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
source scripts/env.sh

cd event-generator
if [[ ! -d .venv ]]; then
  python3.11 -m venv .venv
  .venv/bin/pip install -e ".[dev]"
fi

if [[ -f "$PID_DIR/generator.pid" ]] && kill -0 "$(cat "$PID_DIR/generator.pid")" 2>/dev/null; then
  echo "Generator already running."
  exit 0
fi

echo "Starting event generator on :8000"
KAFKA_BOOTSTRAP=localhost:9092 \
EVENTS_TOPIC=events.page_views \
NUM_PRODUCTS=200 \
ZIPF_S=1.2 \
nohup .venv/bin/uvicorn src.main:app --host 0.0.0.0 --port 8000 \
  > "$FV_STATE/generator.out" 2>&1 &
echo $! > "$PID_DIR/generator.pid"

for i in {1..20}; do
  if curl -fsS http://localhost:8000/health >/dev/null 2>&1; then
    echo "Generator is up."
    exit 0
  fi
  sleep 0.5
done
echo "Generator did not become ready. See $FV_STATE/generator.out"
exit 1
