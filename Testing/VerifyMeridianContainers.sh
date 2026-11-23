#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
compose() {
  docker compose -f Development/Docker/compose.yaml "$@"
}
cleanup() {
  local status=$?
  local cleanup_status=0
  compose down || cleanup_status=$?
  if [[ $status -eq 0 ]]; then
    status=$cleanup_status
  fi
  exit "$status"
}
trap cleanup EXIT
compose build
compose up -d --wait kafka
compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 \
  --create --topic meridian-tracking-events --partitions 1 --replication-factor 1
compose run --rm producer
offset=$(compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 \
  --topic meridian-tracking-events --time -1)
[[ $offset == meridian-tracking-events:0:43 ]]

compose run --rm spark --master 'local[2]' --conf spark.sql.shuffle.partitions=4 \
  --class meridian.BatchJob /opt/meridian/meridian.jar /opt/meridian/batch-input --check

compose run --rm -T --entrypoint /bin/bash spark -euo pipefail -c '
  config=(--master "local[2]"
    --properties-file /opt/meridian/meridian-spark-defaults.conf
    --conf spark.sql.catalog.meridian.warehouse=file:///tmp/stream-iceberg)
  spark=(/opt/spark/bin/spark-submit "${config[@]}" --class MeridianStreaming /opt/meridian/meridian.jar
    kafka kafka:29092 /tmp/checkpoint /tmp/ledger meridian-tracking-events meridian.stream.hub_windows)
  "${spark[@]}" | tee /tmp/replay-result
  awk -F "|" '\''/^[|](accepted|duplicate|late|rejected) / { counts[$2] = $3; batch_rows++ }
    END { for (disposition in counts) total += counts[disposition]; exit !(batch_rows && total == 43) }'\'' /tmp/replay-result
  [[ $(find /tmp/checkpoint/commits -maxdepth 1 -type f -name "[0-9]*" | wc -l) -eq 4 ]]
  "${spark[@]}"
  [[ $(find /tmp/checkpoint/commits -maxdepth 1 -type f -name "[0-9]*" | wc -l) -eq 4 ]]

  cd /tmp
  sql=(/opt/spark/bin/spark-sql "${config[@]}" -S)
  "${sql[@]}" -e "SELECT assert_true(count(*) = 43) FROM delta.\`/tmp/ledger\`;
    SELECT assert_true((SELECT coalesce(sum(event_count), 0) FROM meridian.stream.hub_windows)
      = (SELECT count(*) FROM delta.\`/tmp/ledger\` WHERE disposition = '\''accepted'\''));
    SELECT count(*) AS snapshot_count FROM meridian.stream.hub_windows.snapshots;
    SELECT assert_true(count(*) > 1) FROM meridian.stream.hub_windows.snapshots;"
  first_snapshot=$("${sql[@]}" -e "SELECT snapshot_id FROM meridian.stream.hub_windows.snapshots ORDER BY committed_at LIMIT 1" |
    grep -E "^[0-9]+$" | tail -n 1)
  printf "Earliest Iceberg snapshot: %s\n" "$first_snapshot"
  if [[ ! $first_snapshot =~ ^[0-9]+$ ]]; then
    printf "Expected a numeric Iceberg snapshot ID, got: %s\n" "$first_snapshot" >&2
    exit 1
  fi
  "${sql[@]}" -e "SELECT * FROM meridian.stream.hub_windows VERSION AS OF $first_snapshot;"
'
