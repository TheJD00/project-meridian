$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath (Join-Path $PSScriptRoot '..')

docker build -f Development/Docker/meridian-producer.Dockerfile -t meridian-producer:ci .
if ($LASTEXITCODE -ne 0) { throw 'Producer image build failed.' }

$containerUser = docker run --rm --entrypoint id meridian-producer:ci -u
if ($LASTEXITCODE -ne 0 -or [int]$containerUser -le 0) {
    throw 'Producer image did not run as a non-root user.'
}
$eventCount = docker run --rm --entrypoint grep meridian-producer:ci -c . /opt/kafka/meridian-events.txt
if ($LASTEXITCODE -ne 0 -or [int]$eventCount -ne 43) {
    throw 'Producer image did not contain 43 replay events.'
}
Write-Output 'PASS: producer image builds, runs as non-root, and contains 43 replay events.'

$batch = Get-Content -LiteralPath Logging/meridian-batch-kubernetes.log -Raw
if ($batch -notmatch 'Batch verified: raw=141 latest=133 curated=122 rejected=11') {
    throw 'Batch Job evidence does not show the expected reconciliation.'
}
Write-Output 'PASS: saved Kubernetes batch log reconciles 141 raw, 133 latest, 122 curated, and 11 rejected.'

$before = @(Get-Content -LiteralPath Logging/meridian-stream-before-restart.log)
$after = @(Get-Content -LiteralPath Logging/meridian-stream-after-restart.log)
if (($before -join "`n") -notmatch 'New Kafka records processed = 43' -or
    ($after -join "`n") -notmatch 'New Kafka records processed = 0' -or
    ($after -join "`n") -notmatch 'Saved Kafka records = 43') {
    throw 'Streaming logs do not show the expected replay and restart counts.'
}
$windowPattern = '^\|\d{4}-\d{2}-\d{2} '
$beforeWindows = @($before -match $windowPattern)
$afterWindows = @($after -match $windowPattern)
if ($beforeWindows.Count -eq 0 -or ($beforeWindows -join "`n") -cne ($afterWindows -join "`n")) {
    throw 'Streaming windows changed after pod replacement.'
}
Write-Output "PASS: checkpoint recovery retained 43 records and all $($afterWindows.Count) window rows."
