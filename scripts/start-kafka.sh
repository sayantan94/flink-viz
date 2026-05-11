#!/usr/bin/env bash
set -euo pipefail
cd "$( dirname "${BASH_SOURCE[0]}" )/.."
source scripts/env.sh

KAFKA_CFG="$FV_STATE/kafka.properties"

# Generate a KRaft (no-zookeeper) standalone config the first time.
if [[ ! -f "$KAFKA_CFG" ]]; then
  echo "Generating KRaft config at $KAFKA_CFG"
  cat > "$KAFKA_CFG" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$KAFKA_LOG_DIR/data
num.partitions=4
auto.create.topics.enable=true
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF
  CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  echo "Formatting storage with cluster id $CLUSTER_ID"
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_CFG"
fi

if [[ -f "$PID_DIR/kafka.pid" ]] && kill -0 "$(cat "$PID_DIR/kafka.pid")" 2>/dev/null; then
  echo "Kafka already running with pid $(cat "$PID_DIR/kafka.pid")"
  exit 0
fi

echo "Starting Kafka on localhost:9092 ..."
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CFG" \
  > "$KAFKA_LOG_DIR/kafka.out" 2>&1 &
echo $! > "$PID_DIR/kafka.pid"
echo "Kafka pid: $(cat "$PID_DIR/kafka.pid"). Logs: $KAFKA_LOG_DIR/kafka.out"

# Wait for broker to accept connections.
for i in {1..30}; do
  if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "Kafka is up. Ensuring topics exist..."
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
      --create --topic events.page_views --partitions 4 --replication-factor 1 --if-not-exists 2>/dev/null
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
      --create --topic results.topk --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null
    exit 0
  fi
  sleep 1
done
echo "Kafka did not become ready in 30s. See $KAFKA_LOG_DIR/kafka.out"
exit 1
