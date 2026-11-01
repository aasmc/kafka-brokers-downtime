#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TOPIC="downtime-experiment"
ALL_BROKERS="kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092,kafka-5:9092,kafka-6:9092"
SURVIVING_BROKERS="kafka-4:9092,kafka-5:9092,kafka-6:9092"
BASELINE_SECONDS="${BASELINE_SECONDS:-15}"
OUTAGE_SECONDS="${OUTAGE_SECONDS:-45}"
RECOVERY_SECONDS="${RECOVERY_SECONDS:-20}"
REPLICA_ASSIGNMENT="${REPLICA_ASSIGNMENT:-1:2:3}"
RESULTS_ROOT="${RESULTS_ROOT:-results}"

wait_for_cluster() {
  for attempt in $(seq 1 60); do
    if docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$ALL_BROKERS" --list >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done
  echo "Kafka cluster did not become ready" >&2
  exit 1
}

wait_for_producer() {
  for attempt in $(seq 1 90); do
    if docker compose logs --no-color app 2>/dev/null | grep "PRODUCER_ACK" >/dev/null; then
      return
    fi
    sleep 2
  done
  echo "Producer did not receive an acknowledgement" >&2
  docker compose logs --no-color app >&2
  exit 1
}

describe_topic() {
  local server="$1"
  docker exec kafka-4 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$server" --describe --topic "$TOPIC"
}

save_logs() {
  local output="$1"
  for broker in kafka-1 kafka-2 kafka-3 kafka-4 kafka-5 kafka-6; do
    docker compose logs --no-color "$broker" > "$output/$broker.log"
  done
  docker compose logs --no-color zookeeper > "$output/zookeeper.log"
  docker compose logs --no-color app > "$output/application.log"
  grep -E "PRODUCER_(ACK|ERROR|SEND_ERROR)" "$output/application.log" > "$output/producer-events.log" || true
  grep "CONSUMER_RECORD" "$output/application.log" > "$output/consumer-events.log" || true
  docker compose ps -a > "$output/containers-final.txt"
}

run_variant() {
  local variant="$1"
  local output="$ROOT_DIR/$RESULTS_ROOT/$variant"
  mkdir -p "$output"

  echo "=== unclean.leader.election.enable=$variant ==="
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose up -d --build zookeeper kafka-1 kafka-2 kafka-3 kafka-4 kafka-5 kafka-6
  wait_for_cluster

  docker exec kafka-1 /opt/kafka/bin/zookeeper-shell.sh zookeeper:2181 ls /brokers/ids \
    > "$output/registered-brokers.txt" 2>&1
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$ALL_BROKERS" \
    --create --topic "$TOPIC" \
    --replica-assignment "$REPLICA_ASSIGNMENT" \
    --config min.insync.replicas=2 \
    --config unclean.leader.election.enable="$variant" \
    > "$output/topic-create.txt" 2>&1

  describe_topic "$ALL_BROKERS" > "$output/topic-before.txt" 2>&1
  docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "$ALL_BROKERS" \
    --entity-type topics --entity-name "$TOPIC" --describe --all \
    > "$output/topic-config.txt" 2>&1

  docker compose up -d --build app
  wait_for_producer
  sleep "$BASELINE_SECONDS"

  {
    echo "baseline_finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "kill_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$output/timestamps.txt"
  docker kill kafka-1 kafka-2 kafka-3 > "$output/killed-containers.txt"
  docker compose ps -a > "$output/containers-during.txt"
  sleep 5
  describe_topic "$SURVIVING_BROKERS" > "$output/topic-during-5s.txt" 2>&1

  sleep "$OUTAGE_SECONDS"
  describe_topic "$SURVIVING_BROKERS" > "$output/topic-during-final.txt" 2>&1
  {
    echo "outage_finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "restart_started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >> "$output/timestamps.txt"

  docker compose start kafka-1 kafka-2 kafka-3 > "$output/restarted-containers.txt"
  sleep "$RECOVERY_SECONDS"
  describe_topic "$ALL_BROKERS" > "$output/topic-after.txt" 2>&1
  echo "recovery_finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$output/timestamps.txt"

  save_logs "$output"
  echo "Results saved to $RESULTS_ROOT/$variant"
}

run_variant false
run_variant true

echo "Both experiment variants completed."
