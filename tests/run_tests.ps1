$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$outputDir = Join-Path (Get-Location) 'tests/output'
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
$bootstrapLog = Join-Path $outputDir 'bootstrap.log'
if (-not (Test-Path '.venv')) {
    py -3 -m venv .venv
}
& .\.venv\Scripts\python.exe -m pip install --upgrade pip *> $bootstrapLog
& .\.venv\Scripts\python.exe -m pip install -r .\tests\requirements-test.txt *>> $bootstrapLog
& .\.venv\Scripts\python.exe .\tests\run_comprehensive_tests.py @args
$exitCode = $LASTEXITCODE
Write-Host ""
Write-Host "Bootstrap log : $outputDir/bootstrap.log"
Write-Host "Console log   : $outputDir/console.log"
Write-Host "JSON report   : $outputDir/test_report.json"
Write-Host "MD report     : $outputDir/test_report.md"
Write-Host "Exit code     : $exitCode"
exit $exitCode
