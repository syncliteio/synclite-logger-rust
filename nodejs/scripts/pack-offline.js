#!/usr/bin/env node
/**
 * Dual-mode npm package packer for SyncLite Node.js N-API bindings.
 *
 * USAGE:
 *   node pack-offline.js              → Pack main "synclite" package with optional deps
 *   node pack-offline.js win32-x64-msvc → Pack "@synclite/native-win32-x64-msvc" platform pkg
 *
 * Main package mode:
 *   Creates dist/synclite-1.2.0.tgz with optional dependencies on all supported platforms.
 *   When installed, npm automatically installs the correct platform binary (if available).
 *   The index.js loader tries local .node file first, then falls back to optional dep packages.
 *
 * Platform-specific mode:
 *   Creates a tarball whose package is @synclite/native-{platform}, containing
 *   only that platform's native binary. npm skips non-matching packages via os/cpu.
 */

const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
const dist = path.join(root, 'dist');
fs.mkdirSync(dist, { recursive: true });

const MAIN_PACKAGE_NAME = 'synclite';
const VERSION = '1.2.0';
const PLATFORM_PACKAGE_SCOPE = '@synclite';
const PLATFORM_PACKAGE_NAME = 'native';

function runNpmPack(cwd) {
  if (process.platform === 'win32') {
    return spawnSync(
      path.join(process.env.SystemRoot || 'C:\\Windows', 'System32', 'cmd.exe'),
      ['/d', '/c', `npm pack --pack-destination ${dist}`],
      { cwd, stdio: 'inherit', shell: false },
    );
  }
  return spawnSync('npm', ['pack', '--pack-destination', dist], {
    cwd,
    stdio: 'inherit',
    shell: false,
  });
}

// Purge artifacts from both previous naming schemes. Do not touch this
// script's own outputs (`synclite-<version>.tgz` and the scoped-package
// tarballs `synclite-native-<platform>-<version>.tgz`), because the main
// package and each platform are packed by separate invocations sharing dist/.
const escapedVersion = VERSION.replace(/\./g, '\\.');
const staleTarballPatterns = [
  new RegExp(`^${MAIN_PACKAGE_NAME}-${escapedVersion}-.+\\.tgz$`),
  new RegExp(`^${MAIN_PACKAGE_NAME}-(?:win32|linux)-.+-${escapedVersion}\\.tgz$`),
  new RegExp(`^${MAIN_PACKAGE_NAME}-${MAIN_PACKAGE_NAME}-.+-${escapedVersion}\\.tgz$`),
];
for (const file of fs.readdirSync(dist)) {
  if (staleTarballPatterns.some((pattern) => pattern.test(file))) {
    fs.rmSync(path.join(dist, file), { force: true });
  }
}

// Supported platforms for optional dependencies in the main package
const PLATFORM_PACKAGES = [
  'win32-x64-msvc',
  'linux-x64-gnu',
  'linux-arm64-gnu',
];

function getPlatformPackageJson(platformTag) {
  const [os, cpu] = platformTag.split('-');
  return {
    name: `${PLATFORM_PACKAGE_SCOPE}/${PLATFORM_PACKAGE_NAME}-${platformTag}`,
    version: VERSION,
    description: `SyncLite Node.js native binary for ${platformTag}`,
    main: 'index.js',
    types: 'index.d.ts',
    os: [os],
    cpu: [cpu],
    files: [
      'index.js',
      'index.d.ts',
      `index.${platformTag}.node`,
      '*.dll',
      '*.so*',
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
    optionalDependencies[`${PLATFORM_PACKAGE_SCOPE}/${PLATFORM_PACKAGE_NAME}-${platform}`] = VERSION;
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
      'README.md',
    ],
    scripts: {
      build: 'napi build --platform --release --cargo-cwd ../crates/logger/bindings-node --cargo-name synclite_node_1_2_0 .',
      'build:linux:x64':
        'napi build --platform --release --zig --target x86_64-unknown-linux-gnu --cargo-cwd ../crates/logger/bindings-node --cargo-name synclite_node_1_2_0 .',
      'build:linux:arm64':
        'napi build --platform --release --zig --zig-link-only --target aarch64-unknown-linux-gnu --cargo-cwd ../crates/logger/bindings-node --cargo-name synclite_node_1_2_0 .',
      'pack:offline': 'node scripts/pack-offline.js',
      'pack:offline:all': 'npm run pack:offline && npm run pack:offline -- win32-x64-msvc && npm run pack:offline -- linux-x64-gnu && npm run pack:offline -- linux-arm64-gnu',
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

function linuxElfMachine(file) {
  const header = Buffer.alloc(20);
  const descriptor = fs.openSync(file, 'r');
  try {
    fs.readSync(descriptor, header, 0, header.length, 0);
  } finally {
    fs.closeSync(descriptor);
  }
  if (header.toString('hex', 0, 4) !== '7f454c46' || header[4] !== 2 || header[5] !== 1) {
    return null;
  }
  return header.readUInt16LE(18);
}

function bundleLinuxDuckdb(staging, platformTag) {
  const triple = platformTag === 'linux-x64-gnu'
    ? 'x86_64-unknown-linux-gnu'
    : 'aarch64-unknown-linux-gnu';
  const machine = platformTag === 'linux-x64-gnu' ? 62 : 183;
  const addon = path.join(staging, `index.${platformTag}.node`);
  if (linuxElfMachine(addon) !== machine) {
    throw new Error(`Wrong ELF architecture for ${platformTag}: ${addon}`);
  }

  const targetRoots = new Set([
    path.resolve(process.env.CARGO_TARGET_DIR || path.join(root, '..', 'target')),
    path.resolve(root, '..', 'target'),
  ]);
  const libraryDirectories = [];
  for (const targetRoot of targetRoots) {
    for (const release of [path.join(targetRoot, triple, 'release'), path.join(targetRoot, 'release')]) {
      const build = path.join(release, 'build');
      if (fs.existsSync(build)) {
        const outputs = fs.readdirSync(build)
          .filter(name => name.startsWith('libduckdb-sys-'))
          .map(name => path.join(build, name, 'output'))
          .filter(output => fs.existsSync(output))
          .sort((left, right) => fs.statSync(right).mtimeMs - fs.statSync(left).mtimeMs);
        for (const output of outputs) {
          for (const line of fs.readFileSync(output, 'utf8').split(/\r?\n/)) {
            const match = line.match(/^cargo:{1,2}rustc-link-search=native=(.+)$/);
            if (match) libraryDirectories.push(match[1]);
          }
          libraryDirectories.push(path.join(path.dirname(output), 'out'));
        }
      }
      libraryDirectories.push(path.join(release, 'deps'), release);
    }
  }

  let bundledLibraries = [];
  for (const directory of new Set(libraryDirectories)) {
    if (!fs.existsSync(directory) || !fs.statSync(directory).isDirectory()) continue;
    const libraries = fs.readdirSync(directory)
      .filter(name => /^libduckdb\.so(?:\..+)?$/.test(name))
      .map(name => path.join(directory, name))
      .filter(file => fs.existsSync(file) && linuxElfMachine(file) === machine);
    if (libraries.length === 0) continue;
    for (const library of libraries) {
      const destination = path.join(staging, path.basename(library));
      fs.copyFileSync(library, destination);
      bundledLibraries.push(destination);
    }
    const canonicalLibrary = path.join(staging, 'libduckdb.so');
    if (!fs.existsSync(canonicalLibrary)) {
      fs.copyFileSync(libraries[0], canonicalLibrary);
      bundledLibraries.push(canonicalLibrary);
    }
    break;
  }
  if (bundledLibraries.length === 0) {
    throw new Error(`No matching libduckdb.so found for ${platformTag}; set CARGO_TARGET_DIR to the target directory used to build this addon.`);
  }
  for (const file of [addon, ...bundledLibraries]) {
    const result = spawnSync('patchelf', ['--set-rpath', '$ORIGIN', file], { encoding: 'utf8' });
    if (result.status !== 0) {
      throw new Error(`patchelf failed for ${file}: ${result.error?.message || result.stderr || result.status}`);
    }
  }
  console.log(`  Bundled Linux DuckDB dependency for ${platformTag} with $ORIGIN lookup`);
}

function packPlatformPackage(platformTag) {
  console.log(`\n📦 Packing platform package: ${PLATFORM_PACKAGE_SCOPE}/${PLATFORM_PACKAGE_NAME}-${platformTag}@${VERSION}`);

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

  if (platformTag.startsWith('win32') || platformTag.startsWith('linux')) {
    const linux = platformTag.startsWith('linux');
    const libraryName = linux ? 'libduckdb.so' : 'duckdb.dll';
    const targetDir = path.resolve(process.env.CARGO_TARGET_DIR || path.join(root, '..', 'target'));
    const targetTriple = {
      'linux-x64-gnu': 'x86_64-unknown-linux-gnu',
      'linux-arm64-gnu': 'aarch64-unknown-linux-gnu',
    }[platformTag];
    const releaseDirs = targetTriple
      ? [path.join(targetDir, targetTriple, 'release'), path.join(targetDir, 'release')]
      : [path.join(targetDir, 'release')];
    const cargoOutputs = releaseDirs.flatMap((releaseDir) => {
      const buildDir = path.join(releaseDir, 'build');
      return fs.existsSync(buildDir)
        ? fs.readdirSync(buildDir)
          .filter((name) => name.startsWith('libduckdb-sys-'))
          .map((name) => path.join(buildDir, name, 'output'))
          .filter((output) => fs.existsSync(output))
          .sort((left, right) => fs.statSync(right).mtimeMs - fs.statSync(left).mtimeMs)
        : [];
    });
    const cargoDuckdbPaths = cargoOutputs.flatMap((output) =>
      fs.readFileSync(output, 'utf8').split(/\r?\n/)
        .filter((line) => /^cargo::?rustc-link-search=native=/.test(line))
        .map((line) => path.join(line.slice(line.indexOf('=native=') + 8), libraryName)),
    );
    const possibleDuckdbPaths = [
      ...cargoDuckdbPaths,
      ...cargoOutputs.map((output) => path.join(path.dirname(output), 'out', libraryName)),
      ...releaseDirs.flatMap((releaseDir) => [
        path.join(releaseDir, libraryName),
        path.join(releaseDir, 'deps', libraryName),
      ]),
    ];
    const duckdbLibrary = possibleDuckdbPaths.find((candidate) => fs.existsSync(candidate));
    if (!duckdbLibrary) {
      console.error(`Cannot package ${platformTag}: ${libraryName} was not found in Cargo release link paths.`);
      fs.rmSync(staging, { recursive: true, force: true });
      process.exit(1);
    }
    const stagedLibrary = path.join(staging, libraryName);
    fs.copyFileSync(duckdbLibrary, stagedLibrary);
    if (linux) {
      for (const file of [path.join(staging, addon), stagedLibrary]) {
        const header = Buffer.alloc(20);
        const descriptor = fs.openSync(file, 'r');
        try {
          fs.readSync(descriptor, header, 0, header.length, 0);
        } finally {
          fs.closeSync(descriptor);
        }
        const machine = platformTag === 'linux-x64-gnu' ? 62 : 183;
        if (header.toString('hex', 0, 4) !== '7f454c46'
            || header[4] !== 2 || header[5] !== 1 || header.readUInt16LE(18) !== machine) {
          console.error(`Native library architecture does not match ${platformTag}: ${file}`);
          fs.rmSync(staging, { recursive: true, force: true });
          process.exit(1);
        }
        const patched = spawnSync('patchelf', ['--set-rpath', '$ORIGIN', file], { stdio: 'inherit' });
        if (patched.status !== 0) {
          console.error(`Cannot set package-relative runpath for ${file}; Linux packaging requires patchelf.`);
          fs.rmSync(staging, { recursive: true, force: true });
          process.exit(patched.status || 1);
        }
      }
    }
    console.log(`  Bundled DuckDB dependency: ${duckdbLibrary}`);
  }

  if (platformTag.startsWith('linux-')) {
    bundleLinuxDuckdb(staging, platformTag);
  }

  // Pack with npm
  const result = runNpmPack(staging);

  if (result.status !== 0) {
    console.error(`❌ npm pack failed for platform: ${platformTag}`);
    fs.rmSync(staging, { recursive: true, force: true });
    process.exit(result.status ?? 1);
  }

  // Clean up staging directory
  fs.rmSync(staging, { recursive: true, force: true });

  // npm removes @ and / from a scoped package when deriving the tarball name.
  // This filename is conventional; the embedded package name is the canonical
  // identity used by npm and by the generated N-API loader.
  const packageName = `${PLATFORM_PACKAGE_SCOPE.slice(1)}-${PLATFORM_PACKAGE_NAME}-${platformTag}`;
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

  // The published main package contains only the JavaScript loader. The native
  // addon is installed from the matching optional platform package.
  const filesToCopy = ['synclite.js', 'synclite.d.ts', 'index.js', 'index.d.ts', 'README.md'];
  for (const file of filesToCopy) {
    const src = path.join(root, file);
    if (fs.existsSync(src)) {
      fs.copyFileSync(src, path.join(staging, file));
    }
  }

  // Pack with npm
  const result = runNpmPack(staging);

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
const packHost = process.argv[2] === '--host';
const requestedPlatform = packHost
  ? `${process.platform}-${process.arch}-${process.platform === 'win32' ? 'msvc' : 'gnu'}`
  : process.argv[2];

if (requestedPlatform === '--host') {
  const hostPlatform = PLATFORM_PACKAGES.find(platform =>
    platform.startsWith(`${process.platform}-${process.arch}-`),
  );
  if (!hostPlatform) {
    console.error(`Unsupported host: ${process.platform}-${process.arch}`);
    console.error(`   Supported: ${PLATFORM_PACKAGES.join(', ')}`);
    process.exit(1);
  }
  packPlatformPackage(hostPlatform);
  packMainPackage();
} else if (requestedPlatform) {
  // Platform-specific packing mode
  if (!PLATFORM_PACKAGES.includes(requestedPlatform)) {
    console.error(`❌ Unknown platform: ${requestedPlatform}`);
    console.error(`   Supported: ${PLATFORM_PACKAGES.join(', ')}`);
    process.exit(1);
  }
}
if (!requestedPlatform || packHost) {
  packMainPackage();
}
if (requestedPlatform) {
  packPlatformPackage(requestedPlatform);
}

console.log('\n✅ Packaging complete!');
