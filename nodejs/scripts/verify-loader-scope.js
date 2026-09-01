#!/usr/bin/env node

/**
 * Ensures the N-API loader uses the scoped platform packages declared in
 * package.json. napi.package.name causes napi-rs to generate these names on
 * subsequent builds; the replacement also makes existing generated loaders
 * correct without rebuilding the Rust native binaries.
 */

const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const loaderPath = path.join(root, 'index.js');
const loader = fs.readFileSync(loaderPath, 'utf8');

if (/require\('@synclite\/native-[^']+'\)/.test(loader)) {
  console.log('✓ index.js uses scoped SyncLite platform package fallbacks');
  process.exit(0);
}

const scopedLoader = loader.replace(
  /require\('(?:@synclite\/synclite-|synclite-)([^']+)'\)/g,
  "require('@synclite/native-$1')",
);

if (scopedLoader === loader) {
  console.error('index.js has no SyncLite platform fallback imports to update.');
  process.exit(1);
}

fs.writeFileSync(loaderPath, scopedLoader);
console.log('✓ Updated index.js to use scoped SyncLite platform package fallbacks');
