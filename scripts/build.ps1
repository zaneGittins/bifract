docker compose -f docker-compose.yml -f docker-compose.dev.yml build bifract
if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed."
    exit 1
}

docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to start containers."
    exit 1
}

Write-Host "Bifract is up and running at http://localhost:8080"