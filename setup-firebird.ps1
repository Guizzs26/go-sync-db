$Containers = @("fb-unit-1", "fb-unit-2")
$SQLSource = "./init-firebird.sql"
$DBPath = "/firebird/data/pax.fdb"
$SQLDest = "/tmp/init-firebird.sql"

Write-Host "--- Starting Firebird provisioning for units: $($Containers -join ', ') ---" -ForegroundColor Cyan

if (-not (Test-Path $SQLSource)) {
    Write-Host "Error: $SQLSource not found!" -ForegroundColor Red
    exit
}

$Success = 0
$Failed = 0

foreach ($Container in $Containers) {
    Write-Host ""
    Write-Host ">>> Unit: $Container <<<" -ForegroundColor Yellow

    # Check if container is running
    $Status = docker inspect -f '{{.State.Running}}' $Container 2>$null
    if ($Status -ne "true") {
        Write-Host "Container $Container is not running. Skipping..." -ForegroundColor Magenta
        $Failed++
        continue
    }

    # Clean old database
    Write-Host "Cleaning old database..."S
    docker exec -i $Container rm -f $DBPath

    # Copy SQL script
    Write-Host "Copying SQL script..."
    docker cp $SQLSource "${Container}:${SQLDest}"

    # Execute ISQL
    Write-Host "Executing metadata update..."
    docker exec -i $Container /usr/local/firebird/bin/isql -q -i $SQLDest

    # 5. Final validation
    $Check = docker exec -i $Container ls -lh $DBPath
    if ($Check -like "*$DBPath*") {
        Write-Host "Successfully provisioned $Container" -ForegroundColor Green
        $Success++
    } else {
        Write-Host "Failed to provision $Container" -ForegroundColor Red
        $Failed++
    }
}

Write-Host ""
Write-Host "=== PROVISIONING SUMMARY ===" -ForegroundColor Cyan
Write-Host "Success: $Success | Failed: $Failed" -ForegroundColor Cyan

if ($Failed -eq 0 -and $Success -gt 0) {
    Write-Host "Done. All units are ready." -ForegroundColor Green
} elseif ($Failed -gt 0) {
    Write-Host "⚠ WARNING: $Failed unit(s) failed provisioning!" -ForegroundColor Red
    exit 1
} else {
    Write-Host "✗ ERROR: No units were provisioned!" -ForegroundColor Red
    exit 1
}