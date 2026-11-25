param(
    [Parameter(Mandatory = $true)]
    [string]$EventJson
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Set-Location -LiteralPath $repoRoot

try {
    $event = $EventJson | ConvertFrom-Json -ErrorAction Stop
} catch {
    throw 'EventJson must contain one valid JSON object.'
}

$key = if ($event.shipment_id) { [string]$event.shipment_id } elseif ($event.event_id) { [string]$event.event_id } else { 'unknown' }
$line = "$key|$($EventJson.Trim())"
$line | docker compose -f Development/Docker/compose.realtime.yaml exec -T kafka `
    /opt/kafka/bin/kafka-console-producer.sh `
    --bootstrap-server kafka:9092 `
    --topic shipment-tracking-live `
    --reader-property parse.key=true `
    --reader-property 'key.separator=|'
if ($LASTEXITCODE -ne 0) {
    throw 'Kafka did not accept the tracking event. Confirm the real-time services are running.'
}

Write-Output "Published event $($event.event_id) for shipment $key."
