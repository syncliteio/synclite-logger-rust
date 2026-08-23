#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

// Post-install script to ensure duckdb.dll is available on Windows
// This works around a Windows tar limitation where .dll files in gzipped tarballs
// are not extracted by tar.exe

const platform = process.platform;
const arch = process.arch;

if (platform !== 'win32') {
  // Not Windows, no DLL needed
  process.exit(0);
}

const pkgDir = path.resolve(__dirname, '..');
const dllPath = path.join(pkgDir, 'duckdb.dll');

// Check if DLL already exists
if (fs.existsSync(dllPath)) {
  console.log('✓ duckdb.dll found');
  process.exit(0);
}

// Try to extract DLL from the original tarball if available
// Look for the tarball in common installation locations
const possibleTarballLocations = [
  // When installed from file path (npm install /path/to/tarball)
  path.join(pkgDir, '..', '..', '..', 'lib', 'nodejs', `synclite-1.1.0-${process.platform}-${process.arch === 'x64' ? 'x64' : arch}-msvc.tgz`),
  // When installed from file in current directory
  path.join(process.cwd(), `synclite-1.1.0-win32-${arch}-msvc.tgz`),
];

for (const tarballPath of possibleTarballLocations) {
  if (fs.existsSync(tarballPath)) {
    try {
      console.log(`Extracting duckdb.dll from ${tarballPath}...`);
      // Use tar to extract just the DLL file
      execSync(`tar -xzOf "${tarballPath}" package/duckdb.dll > "${dllPath}"`, {
        stdio: 'pipe',
        shell: true,
      });
      
      if (fs.existsSync(dllPath)) {
        console.log('✓ duckdb.dll extracted successfully');
        process.exit(0);
      }
    } catch (e) {
      // Extraction failed, continue to next location
    }
  }
}

// DLL not found and extraction failed
console.warn('⚠ Warning: duckdb.dll not found. The addon may fail to load.');
console.warn('This typically means the tar extraction did not preserve .dll files.');
console.warn('If you encounter "The specified module could not be found" errors,');
console.warn('please ensure duckdb.dll is in the same directory as the .node addon.');
process.exit(0); // Don't fail npm install, just warn
