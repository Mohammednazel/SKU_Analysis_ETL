param(
  [string]$ProjectDir = "C:\Users\Nazel\OneDrive\Desktop\nadec-analysis\SKU_Frequency_Analysis",
  [string]$LogDir = "$ProjectDir\logs",
  [string]$LockFile = "$ProjectDir\runlocks\etl.lock"
)

# Ensure directories exist
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
New-Item -ItemType Directory -Force -Path "$ProjectDir\runlocks" | Out-Null

# Simple host-level lock (prevents overlap at OS level)
$lockHandle = $null
try {
  $lockHandle = [System.IO.File]::Open($LockFile, 'OpenOrCreate', 'ReadWrite', 'None')
} catch {
  Write-Host "Another ETL run appears to be active (lock busy). Exiting."
  exit 0
}

# Timestamped log file (rotate by date)
$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$logFile = Join-Path $LogDir "etl_cron_$timestamp.log"

# Move to project directory
Set-Location $ProjectDir

# Optional: ensure .env is present for containers
$envPath = Join-Path $ProjectDir ".env"
if (-not (Test-Path $envPath)) {
  Write-Host ".env not found at $envPath — ETL may fail if it relies on it."
}

# Run the ETL once in a short-lived container
$cmd = 'docker compose run --rm etl python src/etl/etl_ingest_resilient.py'
Write-Host "Running: $cmd"

# Run and log all output
Invoke-Expression $cmd | Tee-Object -FilePath $logFile -Append

# Release the OS-level lock
if ($lockHandle) { $lockHandle.Close() }
