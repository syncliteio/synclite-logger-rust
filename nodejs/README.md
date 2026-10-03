# SyncLite Node.js SDK

`synclite` embeds the SyncLite Rust runtime through Node-API. The published
package is a small JavaScript loader; npm resolves one matching native optional
dependency automatically:

- `@synclite/native-win32-x64-msvc`
- `@synclite/native-linux-x64-gnu`
- `@synclite/native-linux-arm64-gnu`

## Consumers

Install from npm:

```text
npm install synclite@1.1.0
```

No Rust toolchain or native rebuild is required on a supported platform.

## Initialization

Use `initialize(options)` for both single and multiple destinations. Supply
`destination` for one target or a non-empty ordered `destinations` array for
fan-out; the two properties are mutually exclusive:

```js
const synclite = require('synclite');

synclite.initialize({
	device_type: 'SQLITE',
	device_name: 'ordersdevice',
	db_path: 'orders.db',
	destinations: [
		// Destination 1
		{
			dst_type: 'POSTGRES',
			dst_connection_string: 'postgresql://user:pw@localhost:5432/syncdb',
			dst_database: 'syncdb',
			dst_schema: 'public',
		},
		// Destination 2
		{
			dst_type: 'SQLITE',
			dst_connection_string: 'orders-destination-2.db',
		},
	],
});
```

Array order defines stable one-based destination indexes. SyncLite validates
the whole array first and initializes destinations sequentially. If a later
destination cannot start, workers already started by that call are stopped
before the initialization error is returned.

For an extracted SyncLite platform/runtime release, run the platform-detecting
installer from `sample-apps/nodejs/` instead:

```text
node install-synclite.js
```

It installs `lib/nodejs/synclite-1.1.0.tgz` and the matching
`lib/nodejs/synclite-native-<platform>-1.1.0.tgz` without accessing an npm
registry.

## Maintainers

Build the current host's native addon from this directory:

```text
npm install
npm run build
```

This creates an `index.<platform>.node` file, such as
`index.win32-x64-msvc.node`. Package the main loader and the three already-built
native artifacts with:

```text
npm run pack:offline:all
```

Publish the three `@synclite/native-*` packages before publishing
`synclite@1.1.0`, so its optional dependencies can resolve immediately.
