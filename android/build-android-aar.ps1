param(
    [string]$GradleCommand = "gradle",
    [string]$Version = "1.1.0"
)

$ErrorActionPreference = "Stop"
$androidProject = $PSScriptRoot

if (-not (Get-Command $GradleCommand -ErrorAction SilentlyContinue)) {
    throw "Gradle was not found. Install Gradle 8.7+ or pass -GradleCommand with its executable path."
}
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "Rust cargo was not found. Install Rust 1.86+ and cargo-ndk before building the Android AAR."
}
if (-not (Get-Command cargo-ndk -ErrorAction SilentlyContinue)) {
    throw "cargo-ndk was not found. Install it with: cargo install cargo-ndk"
}

Push-Location $androidProject
try {
    & $GradleCommand "-PsyncliteVersion=$Version" assembleRelease
} finally {
    Pop-Location
}
