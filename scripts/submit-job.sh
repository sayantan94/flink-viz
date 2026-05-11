#!/usr/bin/env bash
set -euo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
source scripts/env.sh

echo "Packaging topk-products jar..."
( cd flink-jobs && mvn -pl topk-products -am package -DskipTests -q )

JAR="$FV_ROOT/flink-jobs/topk-products/target/topk-products-0.1.0.jar"
if [[ ! -f "$JAR" ]]; then
  echo "Jar not found at $JAR"
  exit 1
fi

echo "Submitting job to Flink at http://localhost:8081 ..."
KAFKA_BOOTSTRAP=localhost:9092 \
INPUT_TOPIC=events.page_views \
OUTPUT_TOPIC=results.topk \
TOPK_K=10 \
WINDOW_MS=10000 \
"$FLINK_HOME/bin/flink" run -d "$JAR"

echo "Submitted. Verify: curl -s http://localhost:8081/jobs/overview | python3 -m json.tool"
