# SyncLite Node.js N-API Package Distribution

This document describes the dual-mode npm packaging system for SyncLite Node.js N-API bindings.

Applications normally consume `synclite@1.2.0` from npm. The main and matching
native packages must be published before that version can be installed.
The [sample guide](../../synclite-code-samples/nodejs/README.md) also retains
bundled-release testing. Android applications consume the Maven Central AAR
`io.synclite:synclite-android:1.2.0`, not an Android Node addon.

## Architecture Overview

The packaging follows the standard **npm optional-dependencies pattern** used by projects like `sqlite3`, `canvas`, and other native modules:

```
Main Package (synclite@1.2.0)
│
├─ Includes: synclite.js, synclite.d.ts, index.js, index.d.ts, README.md
├─ Optional Dependencies:
│  ├─ @synclite/native-win32-x64-msvc@1.2.0
│  ├─ @synclite/native-linux-x64-gnu@1.2.0
│  ├─ @synclite/native-linux-arm64-gnu@1.2.0
│  └─ Only these three platforms are advertised by the current manifest
│
└─ Loading Strategy (index.js):
   1. Try local .node file (synclite.js loads index.js → resolves platform-specific addon)
   2. Fall back to optional dependency package @synclite/native-{platform}
   3. Bundled binaries load without rebuild on target platform
```

Platform-specific packages are **scoped** (`@synclite/*`) and only contain:
- A single `.node` file for their platform
- `index.js` (the N-API binding interface)
- Matching DuckDB runtime (`duckdb.dll` on Windows, `libduckdb.so` on Linux)
- Minimal metadata

## Maintainer Packing & Publishing

### 1. Build binaries for all platforms (or your subset)

```bash
cd synclite-logger-rust/nodejs

# Build for current platform
npm run build

# Or build cross-platform (requires Zig/cross-compile toolchain)
npm run build:linux:x64
npm run build:linux:arm64
# ... repeat for each target platform
```

This creates `.node` files like:
- `index.win32-x64-msvc.node`
- `index.linux-x64-gnu.node`
- `index.linux-arm64-gnu.node`

### 2. Pack into distribution tarballs

Linux packages must be packed on Linux or WSL with `patchelf` available. The
packer bundles the matching DuckDB shared library and sets `$ORIGIN` runpaths
on the staged addon and library. Missing libraries or mismatched architectures
fail packaging rather than producing an incomplete tarball. Pass the same
`CARGO_TARGET_DIR` used for each build to its packaging command.

**Single-platform pack:**
```bash
npm run pack:offline -- win32-x64-msvc
# Creates: dist/synclite-native-win32-x64-msvc-1.2.0.tgz
```

**All supported platforms + main package (Linux, with all build outputs in one Cargo target tree):**
```bash
npm run pack:offline:all
# Creates:
#   dist/synclite-1.2.0.tgz                    (main)
#   dist/synclite-native-win32-x64-msvc-1.2.0.tgz
#   dist/synclite-native-linux-x64-gnu-1.2.0.tgz
#   dist/synclite-native-linux-arm64-gnu-1.2.0.tgz
```

### 3. Publish to npm

Publish platform-specific packages first, then the main package, so npm can
resolve all advertised native dependencies immediately:
```bash
npm publish dist/synclite-native-win32-x64-msvc-1.2.0.tgz --access public
npm publish dist/synclite-native-linux-x64-gnu-1.2.0.tgz --access public
npm publish dist/synclite-native-linux-arm64-gnu-1.2.0.tgz --access public
npm publish dist/synclite-1.2.0.tgz
# ... etc
```

All platform packages are published with the **@synclite** scope, allowing:
- Main package namespace: `synclite@1.2.0` (no scope)
- Platform packages: `@synclite/native-win32-x64-msvc@1.2.0` (scoped)

## Installation & Usage

### Standard npm install (recommended for end users)

```bash
npm install synclite@1.2.0
```

**What happens:**
1. npm installs `synclite@1.2.0` (main package)
2. npm checks `optionalDependencies` against the current platform
3. If a match exists, npm automatically installs `@synclite/native-{your-platform}@1.2.0`
4. At runtime, `index.js` tries to load the local platform-specific `.node` file first
5. Falls back to loading the optional dependency package if no local file found

**Result:** A single command, automatic platform detection, no rebuild, no user configuration.

```javascript
const synclite = require('synclite');
```

### Private registries and bundled sample testing

For private or air-gapped deployments, mirror the main package and matching
native packages into a private npm registry. Applications still use
`npm install synclite@1.2.0` with the configured registry.

To test a packaged platform release without publishing it, use
`node install-synclite.js` from its `sample-apps/nodejs/` directory. The
installer selects the bundled main and native tarballs. The main tarball does
not contain all platform binaries. Never install these artifacts into the SDK
source package directory, which would create a circular build dependency.

## File Structure

### Main package (`synclite-1.2.0.tgz`)
```
synclite-1.2.0/
├── package.json                    (lists optionalDependencies)
├── synclite.js                     (ergonomic API wrapper)
├── synclite.d.ts                   (TypeScript definitions)
├── index.js                        (NAPI generated, multi-platform loader)
├── index.d.ts                      (Low-level NAPI types)
└── README.md
```

### Platform-specific package (`synclite-native-win32-x64-msvc-1.2.0.tgz`)
```
synclite-native-win32-x64-msvc-1.2.0/
├── package.json                    (@synclite/native-win32-x64-msvc)
├── index.js                        (Same loader as main package)
├── index.d.ts
├── index.win32-x64-msvc.node       (Single platform)
├── duckdb.dll                      (Windows dependency)
├── README.md
└── scripts/
    └── ensure-dlls.js
```

## How Loading Works (index.js)

The `index.js` file is auto-generated by `@napi-rs/cli` and includes platform-aware fallback logic:

```javascript
// Simplified pseudocode
const { platform, arch } = process;

let nativeBinding = null;

// 1. Try local .node file first
const localPath = join(__dirname, `index.${platform}-${arch}-${abi}.node`);
if (existsSync(localPath)) {
  nativeBinding = require(localPath);
}

// 2. Fall back to optional dependency package
if (!nativeBinding) {
  nativeBinding = require(`@synclite/native-${platform}-${arch}-${abi}`);
}

// Export the resolved binding
module.exports = nativeBinding;
```

**Result:** Seamless cross-platform support without user intervention.

## Supported Platforms

The `pack-offline.js` script supports:

| Platform Tag | Platform | Architecture | ABI |
|---|---|---|---|
| `win32-x64-msvc` | Windows | x64 | MSVC |
| `linux-x64-gnu` | Linux | x64 | glibc |
| `linux-arm64-gnu` | Linux | ARM64 | glibc |

Add or remove platforms by editing the `PLATFORM_PACKAGES` array in `pack-offline.js`.

## Customization

### Building only specific platforms

Edit `package.json` scripts or run custom builds:

```bash
# Current platform
npm run build

# Linux x64 (requires cross-compile environment)
npm run build:linux:x64

# Linux ARM64
npm run build:linux:arm64
```

### Changing the package scope

Edit `pack-offline.js`:

```javascript
const PLATFORM_PACKAGE_SCOPE = '@synclite';
const PLATFORM_PACKAGE_NAME = 'native';
```

Then republish under the new scope:
```bash
npm publish --access=public  # For public scopes
```

### Adding platform-specific metadata

Platform packages inherit from `getPlatformPackageJson()` in `pack-offline.js`. Add any npm fields there:

```javascript
function getPlatformPackageJson(platformTag) {
  return {
    name: `${PLATFORM_PACKAGE_SCOPE}/${PLATFORM_PACKAGE_NAME}-${platformTag}`,
    // ... add os, cpu fields if needed:
    os: [platformTag.split('-')[0]],
    cpu: [platformTag.split('-')[1]],
  };
}
```

## Troubleshooting

### "Module not found" or "The specified module could not be found"

1. Check if the `.node` file exists:
   ```bash
   ls node_modules/synclite/*.node
   ls node_modules/@synclite/native-{platform}/*.node
   ```

2. On Windows, verify `duckdb.dll` is present:
   ```bash
  ls node_modules/@synclite/native-win32-x64-msvc/duckdb.dll
   ```

3. Check `npm list` for optional dependencies:
   ```bash
   npm list @synclite/native-${platform}
   ```

### Optional dependency not installed

npm skips optional dependencies if they:
- Don't match your platform (expected behavior)
- Fail to install (check npm logs)

Manually install a specific platform package:
```bash
npm install @synclite/native-linux-x64-gnu@1.2.0
```

Install dependencies on the deployment target platform; do not copy another
operating system's native addon or omit optional dependencies.

## Development & Testing

### Local testing

Build and pack for current platform:
```bash
npm run build
npm run pack:offline
```

Test in a separate sample directory using the release installer or npm.
For registry testing after publishing to a staging registry:
```bash
cd /tmp/test-synclite
npm init -y
npm install synclite@1.2.0
node -e "const {initialize} = require('synclite'); console.log('✓ Loaded successfully');"
```

### CI/CD Integration

Example GitHub Actions workflow:

```yaml
build-matrixed:
  runs-on: ${{ matrix.os }}
  strategy:
    matrix:
      include:
        - os: windows-latest
          platform: win32-x64-msvc
        - os: ubuntu-latest
          platform: linux-x64-gnu
        - os: ubuntu-24.04-arm
          platform: linux-arm64-gnu
  steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-node@v3
      with:
        node-version: 18
    - run: |
        cd synclite-logger-rust/nodejs
        npm install
        npm run build
        npm run pack:offline
    - uses: actions/upload-artifact@v3
      with:
        name: packages-${{ matrix.os }}
        path: synclite-logger-rust/nodejs/dist/*.tgz

publish:
  needs: build-matrixed
  runs-on: ubuntu-latest
  steps:
    - uses: actions/download-artifact@v3
    - run: |
        for package in dist/synclite-native-*.tgz; do
          npm publish "$package" --access public
        done
        npm publish dist/synclite-1.2.0.tgz --access public
```

## References

- [npm Optional Dependencies](https://docs.npmjs.com/cli/v8/configuring-npm/package-json#optionaldependencies)
- [@napi-rs/cli Documentation](https://napi.rs/)
- [Node.js N-API Documentation](https://nodejs.org/api/n_api.html)
- Example projects: `sqlite3`, `canvas`, `better-sqlite3`
