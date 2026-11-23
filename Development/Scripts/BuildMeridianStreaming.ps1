param(
    [string]$SparkHome = $env:SPARK_HOME,
    [string]$OutputPath = (Join-Path $PSScriptRoot '..\..\meridian-streaming.jar')
)

$ErrorActionPreference = 'Stop'
if (-not $SparkHome) {
    $sparkSubmit = Get-Command spark-submit -ErrorAction Stop
    $SparkHome = Split-Path (Split-Path $sparkSubmit.Source)
}
$jars = Join-Path $SparkHome 'jars'
if (-not (Test-Path -LiteralPath $jars -PathType Container)) {
    throw "Spark JAR directory does not exist: $jars"
}

$classes = Join-Path ([IO.Path]::GetTempPath()) ('meridian-streaming-' + [guid]::NewGuid())
New-Item -ItemType Directory -Path $classes | Out-Null
try {
    $source = Join-Path $PSScriptRoot '..\..\Data Ingestion\Streaming\MeridianStreaming.scala'
    & java -cp (Join-Path $jars '*') scala.tools.nsc.Main -usejavacp -d $classes $source
    if ($LASTEXITCODE -ne 0) { throw 'Streaming source compilation failed' }
    & jar cf $OutputPath -C $classes .
    if ($LASTEXITCODE -ne 0) { throw 'Streaming JAR packaging failed' }
} finally {
    $resolved = (Resolve-Path -LiteralPath $classes).Path
    $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    if ($resolved.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase) -and
        (Split-Path $resolved -Leaf) -match '^meridian-streaming-[a-f0-9-]{36}$') {
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}
