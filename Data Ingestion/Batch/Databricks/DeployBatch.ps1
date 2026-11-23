param(
    [Parameter(Mandatory = $true)][ValidatePattern('^[A-Za-z_][A-Za-z0-9_]*$')][string]$Catalog,
    [string]$Profile = 'meridian',
    [string]$ClusterId = '1117-113723-6nsppt3v',
    [ValidatePattern('^[A-Za-z_][A-Za-z0-9_]*$')][string]$Schema = 'meridian'
)

$ErrorActionPreference = 'Stop'
function Invoke-Databricks {
    & databricks @args --profile $Profile
    if ($LASTEXITCODE -ne 0) { throw "Databricks command failed: $($args[0..1] -join ' ')" }
}

$cluster = Invoke-Databricks clusters get $ClusterId --output json | ConvertFrom-Json
if ($cluster.state -ne 'RUNNING') {
    throw "Cluster $ClusterId is $($cluster.state). Choose running compatible compute before submitting."
}

$workspaceFolder = '/Shared/meridian-batch'
$workspaceNotebook = "$workspaceFolder/MeridianBatch"
$notebook = Join-Path $PSScriptRoot 'MeridianBatch.scala'
Invoke-Databricks workspace mkdirs $workspaceFolder
Invoke-Databricks workspace import $workspaceNotebook --file $notebook --format SOURCE --language SCALA --overwrite

$requestFile = Join-Path ([IO.Path]::GetTempPath()) ("meridian-job-" + [guid]::NewGuid() + ".json")
try {
    @{
        run_name = 'Meridian batch'
        tasks = @(@{
            task_key = 'batch'
            existing_cluster_id = $ClusterId
            notebook_task = @{
                notebook_path = $workspaceNotebook
                base_parameters = @{ catalog = $Catalog; schema = $Schema; stage_sample = 'true' }
            }
        })
    } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $requestFile -Encoding ascii
    Invoke-Databricks jobs submit --json "@$requestFile"
} finally {
    Remove-Item -LiteralPath $requestFile -ErrorAction SilentlyContinue
}
