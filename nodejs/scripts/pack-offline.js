const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
const dist = path.join(root, 'dist');
fs.mkdirSync(dist, { recursive: true });

const packageJson = require(path.join(root, 'package.json'));
const requestedPlatform = process.argv[2];
const addon = requestedPlatform
  ? `index.${requestedPlatform}.node`
  : fs.readdirSync(root).find((file) => /^index\..+\.node$/.test(file));
if (!addon) {
  console.error('No platform Node addon found. Run `npm run build` first.');
  process.exit(1);
}

if (!fs.existsSync(path.join(root, addon))) {
  console.error(`Requested Node addon was not built: ${addon}`);
  process.exit(1);
}

const platformTag = addon.slice('index.'.length, -'.node'.length);
const defaultTarball = `${packageJson.name}-${packageJson.version}.tgz`;
const platformTarball = `${packageJson.name}-${packageJson.version}-${platformTag}.tgz`;
const staging = path.join(dist, `.pack-${platformTag}`);

for (const file of [defaultTarball, platformTarball]) {
  fs.rmSync(path.join(dist, file), { force: true });
}
fs.rmSync(staging, { recursive: true, force: true });
fs.mkdirSync(staging, { recursive: true });
for (const file of ['package.json', 'synclite.js', 'synclite.d.ts', 'index.js', 'index.d.ts', 'README.md', addon]) {
  fs.copyFileSync(path.join(root, file), path.join(staging, file));
}

// Include the postinstall helper referenced by package.json scripts.postinstall.
fs.mkdirSync(path.join(staging, 'scripts'), { recursive: true });
fs.copyFileSync(
  path.join(root, 'scripts', 'ensure-dlls.js'),
  path.join(staging, 'scripts', 'ensure-dlls.js'),
);

// Copy platform-specific DLL dependencies for Windows packages
if (platformTag.includes('win32')) {
  console.log('Looking for duckdb.dll for Windows package...');
  const possibleDuckdbPaths = [
    path.join(root, '..', 'target', 'release', 'duckdb.dll'),
    path.join(root, '..', 'target', 'release', 'deps', 'duckdb.dll'),
    path.join(root, '..', 'target', 'debug', 'deps', 'duckdb.dll'),
  ];
  for (const duckdbDll of possibleDuckdbPaths) {
    console.log(`  Checking: ${duckdbDll}`);
    if (fs.existsSync(duckdbDll)) {
      fs.copyFileSync(duckdbDll, path.join(staging, 'duckdb.dll'));
      console.log(`  ✓ Bundled DuckDB dependency: ${duckdbDll}`);
      break;
    }
  }
} else {
  console.log(`Not a Windows package (${platformTag}), skipping DLL bundling`);
}

const result = spawnSync('npm', ['pack', '--pack-destination', dist], {
  cwd: staging,
  stdio: 'inherit',
  shell: process.platform === 'win32',
});
fs.rmSync(staging, { recursive: true, force: true });
if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

const src = path.join(dist, defaultTarball);
const dst = path.join(dist, platformTarball);
fs.renameSync(src, dst);
console.log(`Packed ${path.relative(root, dst)}`);
