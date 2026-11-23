param(
    [string]$Container = 'meridian-kafka',
    [string]$Topic = 'meridian-tracking-events',
    [string]$InputPath = (Join-Path $PSScriptRoot '..\..\Datasets\streaming\tracking_events_kafka_keyed.txt')
)

$ErrorActionPreference = 'Stop'
$records = @(Get-Content -LiteralPath $InputPath -Encoding UTF8)
if (-not $records.Count -or ($records | Where-Object { $_ -notmatch '^[^|]+\|\{' })) {
    throw 'Expected a replay file containing Kafka keys separated from JSON payloads by |.'
}

function Invoke-Kafka([string]$Tool, [string[]]$Options) {
    & docker exec $Container "/opt/kafka/bin/$Tool" --bootstrap-server localhost:9092 @Options
    if ($LASTEXITCODE -ne 0) { throw "$Tool failed with exit code $LASTEXITCODE." }
}

Invoke-Kafka 'kafka-topics.sh' @('--create', '--if-not-exists', '--topic', $Topic,
    '--partitions', '1', '--replication-factor', '1')

$before = @(Invoke-Kafka 'kafka-get-offsets.sh' @('--topic', $Topic, '--time', '-1'))
if ($before.Count -ne 1 -or $before[0] -ne "${Topic}:0:0") {
    throw "Use an empty single-partition topic for a repeatable replay. Current offsets: $before"
}

$records | & docker exec -i $Container /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $Topic --property parse.key=true --property 'key.separator=|' --producer-property acks=all
if ($LASTEXITCODE -ne 0) { throw "Kafka replay failed with exit code $LASTEXITCODE." }

$after = @(Invoke-Kafka 'kafka-get-offsets.sh' @('--topic', $Topic, '--time', '-1'))
if ($after.Count -ne 1 -or $after[0] -ne "${Topic}:0:$($records.Count)") {
    throw "Replay did not write the expected $($records.Count) records. Current offsets: $after"
}
"Verified $($records.Count) records in $Topic."
