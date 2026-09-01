#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

/**
 * Post-install script for SyncLite Node.js N-API bindings.
 *
 * On Windows, npm/tar cannot extract .dll files from gzipped tarballs due to
 * Windows file locking. This script verifies DLL presence after extraction.
 *
 * Works with both:
 * 1. Main package (synclite@1.1.0) — multiple .node files included
 * 2. Platform-specific packages (@synclite/synclite-{platform}@1.1.0) — single .node file
 *
 * For platform-specific packages, duckdb.dll is bundled and extracted by npm pack.
 * For the main package, this script looks for DLLs alongside the installed addon.
 */

const platform = process.platform;
const arch = process.arch;

// Not Windows, no DLL needed
if (platform !== 'win32') {
  process.exit(0);
}

const pkgDir = path.resolve(__dirname, '..');
const dllPath = path.join(pkgDir, 'duckdb.dll');

// Check if DLL already exists
if (fs.existsSync(dllPath)) {
  console.log('✓ duckdb.dll found');
  process.exit(0);
}

// For platform-specific packages or when DLL was supposed to be bundled
// Just warn — the addon may still work if DLL is in system PATH or installed elsewhere
console.warn('⚠ Warning: duckdb.dll not found in package directory.');
console.warn('  This may be expected for platform-specific packages on non-Windows systems.');
console.warn('  On Windows, ensure duckdb.dll is available via:');
console.warn('    1. System PATH');
console.warn('    2. Alongside the .node addon file');
console.warn('    3. In the Windows/System32 directory');
console.warn('');
console.warn('  If you encounter "The specified module could not be found" errors,');
console.warn('  please report this issue at: https://github.com/syncliteio/SyncLite/issues');
process.exit(0);
