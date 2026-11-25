$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Set-Location -LiteralPath $repoRoot
$composeFile = 'Development/Docker/compose.realtime.yaml'

docker compose -f $composeFile up -d --build kafka stream
if ($LASTEXITCODE -ne 0) {
    throw 'Could not start the real-time Kafka and Spark services.'
}

Write-Output 'Kafka and the continuous Spark consumer are starting.'
Write-Output "Check status with: docker compose -f $composeFile ps"
Write-Output "Follow events with: docker compose -f $composeFile logs -f stream"
Write-Output 'Publish an event with .\Development\Scripts\publish-meridian-event.ps1 -EventJson <json>'
