## Params and Basic Setup
param(
  [switch]$WarnOnlyDQ,
  [switch]$WriteSilver
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# --- FIXED PATHS ---
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path           # ...\realtime-data-pipeline\spark_jobs
$RepoRoot  = Split-Path -Parent $ScriptDir                              # ...\realtime-data-pipeline
$SparkJobs = $ScriptDir                                                 # stay in the same folder
$LogsDir   = Join-Path $RepoRoot 'logs'
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

# Resolve tools
$Python      = (Get-Command python -ErrorAction Stop).Source
$SparkHome   = $env:SPARK_HOME
$SparkSubmit = if ($SparkHome) { Join-Path $SparkHome 'bin\spark-submit.cmd' } else { '' }

# Windows Spark hygiene
$env:SPARK_LOCAL_DIRS = $env:SPARK_LOCAL_DIRS -as [string]
if ([string]::IsNullOrWhiteSpace($env:SPARK_LOCAL_DIRS)) {
  $env:SPARK_LOCAL_DIRS = 'C:\tmp\spark'
}
New-Item -ItemType Directory -Force -Path $env:SPARK_LOCAL_DIRS | Out-Null

if (-not (Test-Path $env:TEMP)) {
  $env:TEMP = 'C:\tmp'
  New-Item -ItemType Directory -Force -Path $env:TEMP | Out-Null
}
if (-not (Test-Path $env:TMP)) {
  $env:TMP = $env:TEMP
}

Write-Host "=== Starting Batch Pipeline ==="
Write-Host "=== Environment check ==="
Write-Host ("JAVA_HOME  = {0}" -f $env:JAVA_HOME)
Write-Host ("java       = {0}" -f (Join-Path $env:JAVA_HOME 'bin\java.exe'))
Write-Host ("SPARK_HOME = {0}" -f $SparkHome)
Write-Host ("spark-submit = {0}" -f $SparkSubmit)
Write-Host ("winutils   = {0}" -f (Join-Path 'C:\hadoop' 'bin\winutils.exe'))
Write-Host ("python     = {0}" -f $Python)
Write-Host ("SPARK_LOCAL_DIRS = {0}" -f $env:SPARK_LOCAL_DIRS)
Write-Host ("ScriptDir  = {0}" -f $ScriptDir)
Write-Host ("RepoRoot   = {0}" -f $RepoRoot)
Write-Host ("LogsDir    = {0}" -f $LogsDir)

## helpers (timestamps and log paths)
function New-StepLogBase {
  param([string]$title)

  $stamp   = Get-Date -Format 'yyyyMMdd_HHmmss'
  $safe    = ($title -replace '\W+', '_').Trim('_')
  $basename = "step_${safe}_$stamp"

  return [PSCustomObject]@{
    BaseLog   = Join-Path $LogsDir ("{0}.log" -f $basename)
    StdOutLog = Join-Path $LogsDir ("{0}.out" -f $basename)
    StdErrLog = Join-Path $LogsDir ("{0}.err" -f $basename)
  }
}



## Run-Step
function Run-Step {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)][string]$Title,
    [Parameter(Mandatory)][string]$ScriptPath,
    [string[]]$Arguments = @(),
    [string]$Executable = $null
  )

  $logs = New-StepLogBase -title $Title
  $ext  = [System.IO.Path]::GetExtension($ScriptPath).ToLowerInvariant()

  if (-not (Test-Path $ScriptPath)) { throw "Script not found: $ScriptPath" }

  if ([string]::IsNullOrWhiteSpace($Executable)) {
    if ($ext -eq '.py') {
      $exe  = $Python
      $args = @($ScriptPath) + $Arguments
    } else {
      $exe  = $ScriptPath
      $args = $Arguments
    }
  } else {
    $exe  = $Executable
    $args = @($ScriptPath) + $Arguments
  }

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName               = $exe
  $psi.Arguments              = [string]::Join(' ', ($args | ForEach-Object {
                                if ($_ -match '\s') { '"' + ($_ -replace '"','\"') + '"' } else { $_ }
                              }))
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $psi.UseShellExecute        = $false
  $psi.CreateNoWindow         = $true
  $psi.WorkingDirectory       = $RepoRoot

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi

  Write-Host "=== $Title ==="
  $null = $proc.Start()

  $stdOut = $proc.StandardOutput.ReadToEnd()
  $stdErr = $proc.StandardError.ReadToEnd()
  $proc.WaitForExit()

  if ($stdErr) { $stdErr | Out-File -FilePath $logs.StdErrLog -Encoding utf8 }
  if ($stdOut) { $stdOut | Out-File -FilePath $logs.StdOutLog -Encoding utf8 }

  [int]$code = $proc.ExitCode

  if (Test-Path $logs.StdErrLog) { Get-Content $logs.StdErrLog | Out-File -FilePath $logs.BaseLog -Encoding utf8 }
  if (Test-Path $logs.StdOutLog) { Get-Content $logs.StdOutLog | Out-File -FilePath $logs.BaseLog -Encoding utf8 -Append }

  Remove-Item -ErrorAction SilentlyContinue $logs.StdOutLog, $logs.StdErrLog

  if ($code -ne 0) {
    Write-Error "$Title failed (exit $code). See log: $($logs.BaseLog)"
  } else {
    Write-Host "Completed: $Title  → log: $($logs.BaseLog) (exit 0)"
  }

  return $code
}



## Run Pipeline
# Spark job scripts live in $SparkJobs
$etlScript    = Join-Path $SparkJobs 'batch_etl.py'
$dqScript     = Join-Path $SparkJobs 'dq_checks.py'
$silverScript = Join-Path $SparkJobs 'write_silver.py'

# Step 1: Batch ETL
$code = Run-Step -Title 'Batch ETL' -ScriptPath $etlScript
if ($code -ne 0) { exit $code }

# Step 2: Data Quality
if (Test-Path $dqScript) {
  $dqArgs = @()
  if ($WarnOnlyDQ) { $dqArgs += '--warn-only' }
  $code = Run-Step -Title 'Data Quality' -ScriptPath $dqScript -Arguments $dqArgs
  if ($code -ne 0 -and -not $WarnOnlyDQ) { exit $code }
} else {
  Write-Host "Skipping Data Quality (script not found): $dqScript"
}

# Step 3: Write Silver
if ($WriteSilver) {
  if (Test-Path $silverScript) {
    $code = Run-Step -Title 'Write Silver' -ScriptPath $silverScript
    if ($code -ne 0) { exit $code }
  } else {
    Write-Host "Skipping Write Silver (script not found): $silverScript"
  }
}

Write-Host "=== Pipeline completed successfully ==="
exit 0
