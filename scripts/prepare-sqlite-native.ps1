param(
    [Parameter(Mandatory = $true)]
    [string]$VersionNumeric,
    [Parameter(Mandatory = $true)]
    [string]$DownloadYear,
    [Parameter(Mandatory = $true)]
    [string]$DestinationDir
)

$ErrorActionPreference = "Stop"

function Get-FirstFile([string]$Path, [string]$Filter) {
    return Get-ChildItem -Path $Path -Recurse -File -Filter $Filter | Select-Object -First 1
}

function Resolve-LibExe {
    $cmd = Get-Command lib.exe -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }

    $linkCmd = Get-Command link.exe -ErrorAction SilentlyContinue
    if ($linkCmd) {
        return $linkCmd.Source
    }

    $vswhere = Join-Path ${env:ProgramFiles(x86)} "Microsoft Visual Studio\Installer\vswhere.exe"
    if (Test-Path $vswhere) {
        $installPath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
        if ($installPath) {
            $candidate = Get-ChildItem -Path (Join-Path $installPath "VC\Tools\MSVC") -Recurse -File -Filter lib.exe |
                Where-Object { $_.FullName -like "*Hostx64\\x64\\lib.exe" } |
                Select-Object -First 1
            if ($candidate) {
                return $candidate.FullName
            }
        }
    }

    throw "Neither lib.exe nor link.exe was found. Install Visual Studio C++ build tools to generate sqlite3.lib from sqlite3.c."
}

function Resolve-ClExe {
    $cmd = Get-Command cl.exe -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }

    $vswhere = Join-Path ${env:ProgramFiles(x86)} "Microsoft Visual Studio\Installer\vswhere.exe"
    if (Test-Path $vswhere) {
        $installPath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
        if ($installPath) {
            $candidate = Get-ChildItem -Path (Join-Path $installPath "VC\Tools\MSVC") -Recurse -File -Filter cl.exe |
                Where-Object { $_.FullName -like "*Hostx64\\x64\\cl.exe" } |
                Select-Object -First 1
            if ($candidate) {
                return $candidate.FullName
            }
        }
    }

    throw "cl.exe was not found. Install Visual Studio C++ build tools to compile sqlite3.c."
}

$dest = [System.IO.Path]::GetFullPath($DestinationDir)
New-Item -ItemType Directory -Path $dest -Force | Out-Null

$sqliteLib = Join-Path $dest "sqlite3.lib"
$sqliteHeader = Join-Path $dest "sqlite3.h"
$sqliteSource = Join-Path $dest "sqlite3.c"

if ((Test-Path $sqliteLib) -and (Test-Path $sqliteHeader) -and (Test-Path $sqliteSource)) {
    Write-Host "SQLite native artifacts already prepared in $dest"
    exit 0
}

$tmpRoot = Join-Path $dest "_tmp"
Remove-Item -Path $tmpRoot -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $tmpRoot -Force | Out-Null

$amalgZip = Join-Path $tmpRoot ("sqlite-amalgamation-{0}.zip" -f $VersionNumeric)

$baseUrl = "https://www.sqlite.org/{0}" -f $DownloadYear
$amalgUrl = "{0}/sqlite-amalgamation-{1}.zip" -f $baseUrl, $VersionNumeric

Write-Host "Downloading $amalgUrl"
Invoke-WebRequest -Uri $amalgUrl -OutFile $amalgZip

$amalgExtract = Join-Path $tmpRoot "amalg"
Expand-Archive -Path $amalgZip -DestinationPath $amalgExtract -Force

$srcC = Get-FirstFile -Path $amalgExtract -Filter "sqlite3.c"
$srcHeader = Get-FirstFile -Path $amalgExtract -Filter "sqlite3.h"

if (-not $srcC) {
    throw "sqlite3.c was not found in downloaded archive $amalgUrl"
}
if (-not $srcHeader) {
    throw "sqlite3.h was not found in downloaded archive $amalgUrl"
}

Copy-Item -Path $srcC.FullName -Destination $sqliteSource -Force
Copy-Item -Path $srcHeader.FullName -Destination $sqliteHeader -Force

$sqliteObj = Join-Path $tmpRoot "sqlite3.obj"
$clExe = Resolve-ClExe
Write-Host "Compiling sqlite3.c using $clExe"
& $clExe /nologo /O2 /MD /DSQLITE_THREADSAFE=1 /c $sqliteSource /Fo:$sqliteObj
if ($LASTEXITCODE -ne 0) {
    throw "Failed to compile sqlite3.c"
}

$libExe = Resolve-LibExe
Write-Host "Generating sqlite3.lib using $libExe"
& $libExe /nologo /lib /machine:x64 /out:$sqliteLib $sqliteObj
if ($LASTEXITCODE -ne 0) {
    throw "Failed to generate sqlite3.lib from sqlite3.c"
}

Remove-Item -Path $tmpRoot -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "Prepared SQLite native artifacts in $dest"
