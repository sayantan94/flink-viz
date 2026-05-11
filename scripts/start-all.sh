#!/usr/bin/env bash
set -euo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

./scripts/start-kafka.sh
./scripts/start-flink.sh
./scripts/start-generator.sh
./scripts/start-backend.sh
./scripts/start-ui.sh

echo ""
echo "=========================="
echo "  flink-viz is running."
echo "  UI:    http://localhost:5273"
echo "  Flink: http://localhost:8081"
echo "  Backend health: http://localhost:3000/health"
echo "  Generator health: http://localhost:8000/health"
echo "=========================="
echo ""
echo "Submit the Top-K job:"
echo "  ./scripts/submit-job.sh"
echo "Then start the event flow:"
echo "  curl -s -X POST http://localhost:8000/rate -H 'content-type: application/json' -d '{\"events_per_sec\": 200}'"
