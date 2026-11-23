#!/usr/bin/env bash
set -euo pipefail

: "${KAFKA_BOOTSTRAP_SERVERS:?Set KAFKA_BOOTSTRAP_SERVERS}"
: "${KAFKA_TOPIC:?Set KAFKA_TOPIC}"
input=${REPLAY_INPUT:-/opt/meridian/tracking-events.txt}

producer=(
  /opt/kafka/bin/kafka-console-producer.sh
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS"
  --topic "$KAFKA_TOPIC"
  --property parse.key=true
  --property 'key.separator=|'
  --producer-property acks=all
)
if [[ -n ${KAFKA_CLIENT_PROPERTIES:-} ]]; then
  producer+=(--producer.config "$KAFKA_CLIENT_PROPERTIES")
fi
if [[ ${REPLAY_CREATE_TOPIC:-false} == true ]]; then
  topic_config=()
  if [[ -n ${KAFKA_CLIENT_PROPERTIES:-} ]]; then
    topic_config=(--command-config "$KAFKA_CLIENT_PROPERTIES")
  fi
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create --if-not-exists --topic "$KAFKA_TOPIC" --partitions 1 --replication-factor 1 "${topic_config[@]}"
fi

tr -d '\r' < "$input" | "${producer[@]}"
printf 'Replayed %s fixture records into %s\n' "$(wc -l < "$input")" "$KAFKA_TOPIC"

# Keep the completed replay pod available for inspection. A new pod replays the fixture.
if [[ ${REPLAY_KEEP_ALIVE:-false} == true ]]; then
  touch /tmp/replay-complete
  exec sleep infinity
fi
