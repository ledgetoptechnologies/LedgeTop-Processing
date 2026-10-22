param([string]$Python = 'python')
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $projectRoot
try {
    if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) {
        & $Python -m venv .venv
        if ($LASTEXITCODE -ne 0) { throw 'Creating the virtual environment failed.' }
    }
    $venvPython = Join-Path $projectRoot '.venv/Scripts/python.exe'
    & $venvPython -m pip install -r requirements-test.lock
    if ($LASTEXITCODE -ne 0) { throw 'Installing the locked dependencies failed.' }
    & $venvPython -m pip install --no-deps --no-build-isolation -e .
    if ($LASTEXITCODE -ne 0) { throw 'Installing TreeDar failed.' }
    & $venvPython -m pytest
    if ($LASTEXITCODE -ne 0) { throw 'TreeDar tests failed.' }
} finally {
    Pop-Location
}
