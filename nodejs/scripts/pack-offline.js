#!/usr/bin/env node
/**
 * Dual-mode npm package packer for SyncLite Node.js N-API bindings.
 *
 * USAGE:
 *   node pack-offline.js              → Pack main "synclite" package with optional deps
 *   node pack-offline.js win32-x64-msvc → Pack "@synclite/synclite-win32-x64-msvc" platform pkg
 *
 * Main package mode:
 *   Creates dist/synclite-1.1.0.tgz with optional dependencies on all supported platforms.
 *   When installed, npm automatically installs the correct platform binary (if available).
 *   The index.js loader tries local .node file first, then falls back to optional dep packages.
 *
 * Platform-specific mode:
 *   Creates dist/@synclite/synclite-{platform}-1.1.0.tgz with the native binary.
 *   Published as optional peer package; npm install skips if platform doesn't match.
 *   No postinstall overhead for platform packages.
 */

const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
const dist = path.join(root, 'dist');
fs.mkdirSync(dist, { recursive: true });

const MAIN_PACKAGE_NAME = 'synclite';
const VERSION = '1.1.0';

// Purge stale tarballs from the old flat `synclite-<version>-<platform>.tgz`
// naming scheme so the assembly step can never pick up leftovers that no
// longer match this script's current output. Scoped to that specific old
// pattern only — must NOT touch this script's own outputs
// (`synclite-<version>.tgz` and `synclite-synclite-<platform>-<version>.tgz`),
// since main + each platform are packed via separate invocations that all
// share this same dist/ directory.
const staleTarballPattern = new RegExp(`^${MAIN_PACKAGE_NAME}-${VERSION.replace(/\./g, '\\.')}-.+\\.tgz$`);
for (const file of fs.readdirSync(dist)) {
  if (staleTarballPattern.test(file)) {
    fs.rmSync(path.join(dist, file), { force: true });
  }
}

// Supported platforms for optional dependencies in the main package
const PLATFORM_PACKAGES = [
  'win32-x64-msvc',
  'win32-ia32-msvc',
  'win32-arm64-msvc',
  'linux-x64-gnu',
  'linux-x64-musl',
  'linux-arm64-gnu',
  'linux-arm64-musl',
  'darwin-universal',
  'android-arm64',
  'android-arm-eabi',
];

function getPlatformPackageJson(platformTag) {
  return {
    // Unscoped, flat `synclite-<platform>` naming — this MUST match the
    // package names the napi-rs generated index.js falls back to via
    // require('synclite-<platform>') when no local .node file is present.
    // Do NOT scope this (e.g. `@synclite/...`); index.js has no knowledge
    // of any npm scope and the optional-dependency fallback would silently
    // never resolve.
    name: `${MAIN_PACKAGE_NAME}-${platformTag}`,
    version: VERSION,
    description: `SyncLite Node.js native binary for ${platformTag}`,
    main: 'index.js',
    types: 'index.d.ts',
    files: [
      'index.js',
      'index.d.ts',
      `index.${platformTag}.node`,
      '*.dll',
      'scripts/ensure-dlls.js',
      'README.md',
    ],
    scripts: {
      postinstall: 'node scripts/ensure-dlls.js',
    },
    repository: {
      type: 'git',
      url: 'https://github.com/syncliteio/SyncLite.git',
      directory: 'synclite-logger-rust/nodejs',
    },
    keywords: ['synclite', 'database', 'sqlite', 'node-api', 'napi'],
    author: 'SyncLite Contributors',
    license: 'Apache-2.0',
    engines: {
      node: '>=14.0.0',
    },
  };
}

function getMainPackageJson() {
  const optionalDependencies = {};
  for (const platform of PLATFORM_PACKAGES) {
    optionalDependencies[`${MAIN_PACKAGE_NAME}-${platform}`] = VERSION;
  }

  return {
    name: MAIN_PACKAGE_NAME,
    version: VERSION,
    description: 'Node.js bindings for the SyncLite Rust runtime',
    main: 'synclite.js',
    types: 'synclite.d.ts',
    files: [
      'synclite.js',
      'synclite.d.ts',
      'index.js',
      'index.d.ts',
      'scripts/ensure-dlls.js',
      '*.node',
      '*.dll',
      'README.md',
    ],
    scripts: {
      build: 'napi build --platform --release --cargo-cwd ../crates/logger/bindings-node --cargo-name synclite_node_1_1_0 .',
      'build:linux:x64':
        'napi build --platform --release --zig --target x86_64-unknown-linux-gnu --cargo-cwd ../crates/logger/bindings-node --cargo-name synclite_node_1_1_0 .',
      'build:linux:arm64':
        'napi build --platform --release --zig --zig-link-only --target aarch64-unknown-linux-gnu --cargo-cwd ../crates/logger/bindings-node --cargo-name synclite_node_1_1_0 .',
      'pack:offline': 'node scripts/pack-offline.js',
      'pack:offline:all': 'npm run pack:offline && npm run pack:offline -- win32-x64-msvc && npm run pack:offline -- linux-x64-gnu && npm run pack:offline -- linux-arm64-gnu',
      postinstall: 'node scripts/ensure-dlls.js',
    },
    repository: {
      type: 'git',
      url: 'https://github.com/syncliteio/SyncLite.git',
      directory: 'synclite-logger-rust/nodejs',
    },
    keywords: ['synclite', 'database', 'sync', 'replication', 'node-api', 'napi'],
    author: 'SyncLite Contributors',
    license: 'Apache-2.0',
    engines: {
      node: '>=14.0.0',
    },
    devDependencies: {
      '@napi-rs/cli': '^2.18.4',
    },
    optionalDependencies,
  };
}

function packPlatformPackage(platformTag) {
  console.log(`\n📦 Packing platform package: ${MAIN_PACKAGE_NAME}-${platformTag}@${VERSION}`);

  // Find the .node file for this platform
  const addon = `index.${platformTag}.node`;
  if (!fs.existsSync(path.join(root, addon))) {
    console.error(`❌ Node addon not found: ${addon}`);
    console.error('   Run the build script for this platform first.');
    process.exit(1);
  }

  const staging = path.join(dist, `.pack-${platformTag}`);
  const packageJson = getPlatformPackageJson(platformTag);
  const packageJsonPath = path.join(staging, 'package.json');

  // Clean up staging
  fs.rmSync(staging, { recursive: true, force: true });
  fs.mkdirSync(staging, { recursive: true });

  // Write platform-specific package.json
  fs.writeFileSync(packageJsonPath, JSON.stringify(packageJson, null, 2));

  // Copy essential files
  const filesToCopy = ['index.js', 'index.d.ts', 'README.md', addon];
  for (const file of filesToCopy) {
    const src = path.join(root, file);
    if (fs.existsSync(src)) {
      fs.copyFileSync(src, path.join(staging, file));
    }
  }

  // Include postinstall helper (for DLL extraction on Windows)
  fs.mkdirSync(path.join(staging, 'scripts'), { recursive: true });
  if (fs.existsSync(path.join(root, 'scripts', 'ensure-dlls.js'))) {
    fs.copyFileSync(
      path.join(root, 'scripts', 'ensure-dlls.js'),
      path.join(staging, 'scripts', 'ensure-dlls.js'),
    );
  }

  // Copy platform-specific DLL dependencies for Windows packages
  if (platformTag.includes('win32')) {
    const possibleDuckdbPaths = [
      path.join(root, '..', 'target', 'release', 'duckdb.dll'),
      path.join(root, '..', 'target', 'release', 'deps', 'duckdb.dll'),
      path.join(root, '..', 'target', 'debug', 'deps', 'duckdb.dll'),
    ];
    for (const duckdbDll of possibleDuckdbPaths) {
      if (fs.existsSync(duckdbDll)) {
        fs.copyFileSync(duckdbDll, path.join(staging, 'duckdb.dll'));
        console.log(`  ✓ Bundled DuckDB dependency: duckdb.dll`);
        break;
      }
    }
  }

  // Pack with npm
  const result = spawnSync('npm', ['pack', '--pack-destination', dist], {
    cwd: staging,
    stdio: 'inherit',
    shell: process.platform === 'win32',
  });

  if (result.status !== 0) {
    console.error(`❌ npm pack failed for platform: ${platformTag}`);
    fs.rmSync(staging, { recursive: true, force: true });
    process.exit(result.status ?? 1);
  }

  // Clean up staging directory
  fs.rmSync(staging, { recursive: true, force: true });

  const packageName = `${MAIN_PACKAGE_NAME}-${platformTag}`;
  const tarballPath = path.join(dist, `${packageName}-${VERSION}.tgz`);

  if (fs.existsSync(tarballPath)) {
    console.log(`✅ Packed: ${path.relative(root, tarballPath)}`);
    return tarballPath;
  } else {
    console.error(`❌ Expected tarball not found: ${tarballPath}`);
    process.exit(1);
  }
}

function packMainPackage() {
  console.log(`\n📦 Packing main package: ${MAIN_PACKAGE_NAME}@${VERSION}`);

  const staging = path.join(dist, '.pack-main');
  const packageJson = getMainPackageJson();
  const packageJsonPath = path.join(staging, 'package.json');

  // Clean up staging
  fs.rmSync(staging, { recursive: true, force: true });
  fs.mkdirSync(staging, { recursive: true });

  // Write main package.json with optional dependencies
  fs.writeFileSync(packageJsonPath, JSON.stringify(packageJson, null, 2));

  // Copy essential files (including all .node files for cross-platform distribution)
  const filesToCopy = ['synclite.js', 'synclite.d.ts', 'index.js', 'index.d.ts', 'README.md'];
  for (const file of filesToCopy) {
    const src = path.join(root, file);
    if (fs.existsSync(src)) {
      fs.copyFileSync(src, path.join(staging, file));
    }
  }

  // Copy all .node files (for manual cross-platform consumption)
  for (const file of fs.readdirSync(root)) {
    if (file.endsWith('.node')) {
      fs.copyFileSync(path.join(root, file), path.join(staging, file));
    }
  }

  // Include postinstall helper
  fs.mkdirSync(path.join(staging, 'scripts'), { recursive: true });
  if (fs.existsSync(path.join(root, 'scripts', 'ensure-dlls.js'))) {
    fs.copyFileSync(
      path.join(root, 'scripts', 'ensure-dlls.js'),
      path.join(staging, 'scripts', 'ensure-dlls.js'),
    );
  }

  // Copy Windows DLLs if present (for manual cross-platform consumption)
  for (const file of fs.readdirSync(root)) {
    if (file.endsWith('.dll')) {
      fs.copyFileSync(path.join(root, file), path.join(staging, file));
    }
  }

  // Pack with npm
  const result = spawnSync('npm', ['pack', '--pack-destination', dist], {
    cwd: staging,
    stdio: 'inherit',
    shell: process.platform === 'win32',
  });

  if (result.status !== 0) {
    console.error(`❌ npm pack failed for main package`);
    fs.rmSync(staging, { recursive: true, force: true });
    process.exit(result.status ?? 1);
  }

  // Clean up staging directory
  fs.rmSync(staging, { recursive: true, force: true });

  const tarballPath = path.join(dist, `${MAIN_PACKAGE_NAME}-${VERSION}.tgz`);

  if (fs.existsSync(tarballPath)) {
    console.log(`✅ Packed: ${path.relative(root, tarballPath)}`);
    return tarballPath;
  } else {
    console.error(`❌ Expected tarball not found: ${tarballPath}`);
    process.exit(1);
  }
}

// Main execution
const requestedPlatform = process.argv[2];

if (requestedPlatform) {
  // Platform-specific packing mode
  if (!PLATFORM_PACKAGES.includes(requestedPlatform)) {
    console.error(`❌ Unknown platform: ${requestedPlatform}`);
    console.error(`   Supported: ${PLATFORM_PACKAGES.join(', ')}`);
    process.exit(1);
  }
  packPlatformPackage(requestedPlatform);
} else {
  // Main package mode
  packMainPackage();
}

console.log('\n✅ Packaging complete!');
