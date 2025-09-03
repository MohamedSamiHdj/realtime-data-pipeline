<#
.SYNOPSIS
    Cleanup script to remove runtime / unwanted files before committing to GitHub.
#>

Write-Host "=== Cleaning project ==="

# Paths to nuke (excluding data\raw)
$paths = @(
    ".venv",
    "logs",
    "artifacts",
    "data\bronze",
    "data\silver",
    "data\processed",
    "data\stream_inbox",
    "warehouse",          # comment this out if warehouse is source not runtime
    "__pycache__"
)

foreach ($p in $paths) {
    if (Test-Path $p) {
        Write-Host "Removing $p ..."
        Remove-Item $p -Recurse -Force -ErrorAction SilentlyContinue
    }
}

# Python bytecode
Get-ChildItem -Recurse -Include *.pyc,*.pyo,*.pyd | ForEach-Object {
    Write-Host "Deleting $($_.FullName)"
    Remove-Item $_.FullName -Force
}

# OS/editor junk
Get-ChildItem -Recurse -Include .DS_Store,Thumbs.db | ForEach-Object {
    Write-Host "Deleting $($_.FullName)"
    Remove-Item $_.FullName -Force
}

Write-Host "=== Cleanup complete ==="
