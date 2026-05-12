#!/usr/bin/env bash
set -euo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
source scripts/env.sh

# Ensure topics exist
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic events.page_views --partitions 4 --replication-factor 1 2>/dev/null
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic events.purchases --partitions 4 --replication-factor 1 2>/dev/null
for t in results.topk results.windows.tumbling results.windows.sliding results.windows.sessions \
         results.joins results.fraud; do
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$t" --partitions 1 --replication-factor 1 2>/dev/null
done

echo "Building all jobs..."
( cd flink-jobs && mvn -DskipTests package -q )

submit() {
  local name="$1"
  local jar="flink-jobs/$name/target/$name-0.1.0.jar"
  if [[ ! -f "$jar" ]]; then
    echo "Missing $jar" >&2; return 1
  fi
  # Skip only if a RUNNING job with the same name exists.
  if curl -fsS http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import sys, json
name = '$name'
for j in json.load(sys.stdin)['jobs']:
    if j['name'] == name and j['state'] == 'RUNNING':
        sys.exit(0)
sys.exit(1)
"; then
    echo "$name already RUNNING, skipping"
    return 0
  fi
  echo "Submitting $name..."
  KAFKA_BOOTSTRAP=localhost:9092 \
    "$FLINK_HOME/bin/flink" run -d "$jar"
}

submit topk-products
submit window-stats
submit view-purchase-join
submit fraud-cep

echo "Done. Jobs:"
curl -s http://localhost:8081/jobs/overview | python3 -c "
import sys, json
for j in json.load(sys.stdin)['jobs']:
    print(f\"  {j['name']:<25} {j['state']}\")
"
