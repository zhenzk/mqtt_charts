$el = Join-Path $PSScriptRoot "node_modules\electron\dist\electron.exe"
if (-not (Test-Path $el)) {
    Write-Host "Electron not found. Run 'npm install' first." -ForegroundColor Red
    exit 1
}
$pr = Start-Process -FilePath $el -ArgumentList "." -WindowStyle Normal -PassThru
Write-Host "MQTT Charts started (PID: $($pr.Id))" -ForegroundColor Green
