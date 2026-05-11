#!/usr/bin/env bash
# Shared env for all lifecycle scripts. Source me.

# Repo root
export FV_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# Java 17 (Flink 1.18 + topk-products are built for 17)
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

# Tools (brew-managed Kafka + locally-downloaded Flink 1.18)
export KAFKA_HOME="$(brew --prefix kafka)/libexec"
export FLINK_HOME="$FV_ROOT/infra/local/flink-1.18.1"

# Runtime state lives outside FLINK_HOME so we can wipe it freely
export FV_STATE="$FV_ROOT/infra/local/state"
export KAFKA_LOG_DIR="$FV_STATE/kafka"
export FLINK_LOG_DIR="$FV_STATE/flink-logs"
export PID_DIR="$FV_STATE/pids"

mkdir -p "$KAFKA_LOG_DIR" "$FLINK_LOG_DIR" "$PID_DIR"
