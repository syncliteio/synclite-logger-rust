# SyncLite Node.js N-API Package Distribution

This document describes the dual-mode npm packaging system for SyncLite Node.js N-API bindings.

## Architecture Overview

The packaging follows the standard **npm optional-dependencies pattern** used by projects like `sqlite3`, `canvas`, and other native modules:

```
Main Package (synclite@1.1.0)
│
├─ Includes: synclite.js, index.js, all .node files (for manual cross-platform use)
├─ Optional Dependencies:
│  ├─ @synclite/synclite-win32-x64-msvc@1.1.0
│  ├─ @synclite/synclite-linux-x64-gnu@1.1.0
│  ├─ @synclite/synclite-linux-arm64-gnu@1.1.0
│  └─ ... (10 platforms total)
│
└─ Loading Strategy (index.js):
   1. Try local .node file (synclite.js loads index.js → resolves platform-specific addon)
   2. Fall back to optional dependency package @synclite/synclite-{platform}
   3. Bundled binaries load without rebuild on target platform
```

Platform-specific packages are **scoped** (`@synclite/*`) and only contain:
- A single `.node` file for their platform
- `index.js` (the N-API binding interface)
- Platform-specific DLLs (Windows only)
- Minimal metadata

## Packing & Publishing

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

**Single-platform pack:**
```bash
npm run pack:offline -- win32-x64-msvc
# Creates: dist/synclite-synclite-win32-x64-msvc-1.1.0.tgz
```

**All supported platforms + main package:**
```bash
npm run pack:offline:all
# Creates:
#   dist/synclite-1.1.0.tgz                    (main)
#   dist/synclite-synclite-win32-x64-msvc-1.1.0.tgz
#   dist/synclite-synclite-linux-x64-gnu-1.1.0.tgz
#   dist/synclite-synclite-linux-arm64-gnu-1.1.0.tgz
#   dist/synclite-synclite-darwin-universal-1.1.0.tgz
#   ... (all platforms defined in pack-offline.js)
```

### 3. Publish to npm

**Publish main package:**
```bash
npm publish dist/synclite-1.1.0.tgz
```

**Publish platform-specific packages:**
```bash
npm publish dist/synclite-synclite-win32-x64-msvc-1.1.0.tgz
npm publish dist/synclite-synclite-linux-x64-gnu-1.1.0.tgz
npm publish dist/synclite-synclite-linux-arm64-gnu-1.1.0.tgz
# ... etc
```

All platform packages are published with the **@synclite** scope, allowing:
- Main package namespace: `synclite@1.1.0` (no scope)
- Platform packages: `@synclite/synclite-win32-x64-msvc@1.1.0` (scoped)

## Installation & Usage

### Standard npm install (recommended for end users)

```bash
npm install synclite
```

**What happens:**
1. npm installs `synclite@1.1.0` (main package)
2. npm checks `optionalDependencies` against the current platform
3. If a match exists, npm automatically installs `@synclite/synclite-{your-platform}@1.1.0`
4. At runtime, `index.js` tries to load the local platform-specific `.node` file first
5. Falls back to loading the optional dependency package if no local file found

**Result:** A single command, automatic platform detection, no rebuild, no user configuration.

```javascript
const { initialize, open } = require('synclite');

// Identical API on all platforms
const db = open(':memory:');
```

### Manual cross-platform distribution

For environments where optional dependencies don't work (e.g., monorepos, custom CI):

**Ship the main tarball with all .node files:**
```bash
tar -tzf dist/synclite-1.1.0.tgz | grep "\.node"
# Extracts all platform binaries:
# index.win32-x64-msvc.node
# index.linux-x64-gnu.node
# index.linux-arm64-gnu.node
# ... (all platforms)
```

**Consumer can:**
1. Use one tarball across all platforms
2. `npm install file:../dist/synclite-1.1.0.tgz`
3. `index.js` auto-selects the correct `.node` file at runtime based on platform/arch

**Or distribute platform-specific tarballs separately:**
- Dev team publishes `dist/synclite-synclite-{platform}-1.1.0.tgz` to internal artifact store
- CI/Deployment automatically pulls the matching tarball
- Example: Linux x64 Docker container pulls `synclite-synclite-linux-x64-gnu-1.1.0.tgz`

## File Structure

### Main package (`synclite-1.1.0.tgz`)
```
synclite-1.1.0/
├── package.json                    (lists optionalDependencies)
├── synclite.js                     (ergonomic API wrapper)
├── synclite.d.ts                   (TypeScript definitions)
├── index.js                        (NAPI generated, multi-platform loader)
├── index.d.ts                      (Low-level NAPI types)
├── index.win32-x64-msvc.node       (All platforms included)
├── index.linux-x64-gnu.node
├── index.linux-arm64-gnu.node
├── ... (other platforms)
├── README.md
└── scripts/
    └── ensure-dlls.js              (Windows DLL workaround)
```

### Platform-specific package (`synclite-synclite-win32-x64-msvc-1.1.0.tgz`)
```
synclite-synclite-win32-x64-msvc-1.1.0/
├── package.json                    (@synclite/synclite-win32-x64-msvc)
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
  nativeBinding = require(`@synclite/synclite-${platform}-${arch}-${abi}`);
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
| `win32-ia32-msvc` | Windows | IA32 | MSVC |
| `win32-arm64-msvc` | Windows | ARM64 | MSVC |
| `linux-x64-gnu` | Linux | x64 | glibc |
| `linux-x64-musl` | Linux | x64 | musl |
| `linux-arm64-gnu` | Linux | ARM64 | glibc |
| `linux-arm64-musl` | Linux | ARM64 | musl |
| `darwin-universal` | macOS | Universal (x64+ARM64) | - |
| `android-arm64` | Android | ARM64 | - |
| `android-arm-eabi` | Android | ARM | EABI |

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
const SCOPE = 'synclite';  // Change to your organization/scope
const MAIN_PACKAGE_NAME = 'synclite';  // Package name
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
    name: `@${SCOPE}/${MAIN_PACKAGE_NAME}-${platformTag}`,
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
   ls node_modules/@synclite/synclite-{platform}/*.node
   ```

2. On Windows, verify `duckdb.dll` is present:
   ```bash
   ls node_modules/synclite/duckdb.dll
   ```

3. Check `npm list` for optional dependencies:
   ```bash
   npm list @synclite/synclite-${platform}
   ```

### Optional dependency not installed

npm skips optional dependencies if they:
- Don't match your platform (expected behavior)
- Fail to install (check npm logs)

Manually install a specific platform package:
```bash
npm install @synclite/synclite-linux-x64-gnu@1.1.0
```

### Cross-platform tarball usage

If using the main tarball with all binaries included, ensure your extraction preserves file permissions (especially on Linux/macOS):

```bash
tar -xzf synclite-1.1.0.tgz --preserve
```

## Development & Testing

### Local testing

Build and pack for current platform:
```bash
npm run build
npm run pack:offline
npm install file:../dist/synclite-1.1.0.tgz
```

Test in a separate directory:
```bash
cd /tmp/test-synclite
npm install /path/to/synclite-1.1.0.tgz
node -e "const {initialize} = require('synclite'); console.log('✓ Loaded successfully');"
```

### CI/CD Integration

Example GitHub Actions workflow:

```yaml
build-matrixed:
  runs-on: ${{ matrix.os }}
  strategy:
    matrix:
      os: [ubuntu-latest, windows-latest, macos-latest]
  steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-node@v3
      with:
        node-version: 18
    - run: |
        cd synclite-logger-rust/nodejs
        npm ci
        npm run build
        npm run pack:offline -- ${{ matrix.platform }}
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
        npm publish dist/synclite-1.1.0.tgz --access public
        npm publish dist/synclite-synclite-*.tgz --access public
```

## References

- [npm Optional Dependencies](https://docs.npmjs.com/cli/v8/configuring-npm/package-json#optionaldependencies)
- [@napi-rs/cli Documentation](https://napi.rs/)
- [Node.js N-API Documentation](https://nodejs.org/api/n_api.html)
- Example projects: `sqlite3`, `canvas`, `better-sqlite3`
