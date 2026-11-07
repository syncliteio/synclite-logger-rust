# SyncLite Android AAR

This Android library packages the existing Rust `synclite_jni` bridge as an AAR for Kotlin/JVM Android applications. It reuses the same JNI symbols as the desktop Java runtime; Kotlin is a thin layer over those calls.

## Current scope

The first Android target is **SQLite-focused** and mirrors the Python and Node core runtime surface:

- initialize a device with one SQLite or PostgreSQL destination;
- open a logged SQLite connection;
- execute, query, prepare, batch, commit, roll back, flush, and close;
- await background synchronization;
- optionally use the lower-level in-process consolidator lifecycle API.

It deliberately does **not** package the desktop JDBC logger and does not expose DuckDB device or destination APIs. The Rust build sets `DUCKDB_DOWNLOAD_LIB=false`, preventing the desktop prebuilt DuckDB sidecar from being used when producing Android libraries.

## Build requirements

### Native CDC build reference

The Android CDC sidecar is built from the sibling `SyncLiteNative` checkout's
`sqlite3.c`, `replayer.c`, and `rust_bindings.c`, not rusqlite's stock SQLite.
Desktop Profile A enables `SQLITE_THREADSAFE=1` and
`SQLITE_ENABLE_PREUPDATE_HOOK`; Android must use the same definitions and export
the `synclite_rs_*` ABI used by the consolidator. The NDK supplies JNI headers.
Both supported ABIs must package `libsynclitecdc.so` alongside `libsynclite_jni.so`.
The runtime loads this sidecar by SONAME in Android's application linker namespace,
including applications loading unextracted libraries directly from an APK.
It must not extract a desktop prebuilt into an Android temporary directory.

Scope/decomposition: this is one atomic packaging/loading change; no scenario
skill root, assessment, or workflow task path was supplied. The installed SDK has
NDK 26.3.11579264, CMake 3.22.1, and both Rust Android targets. Validation should
build both C ABIs, inspect exported symbols/dependencies, and inspect the AAR.
Device execution remains a separate validation if no connected device is available.

Research confirmed that desktop Profile A compiles all three C translation units
with `SQLITE_THREADSAFE=1` and `SQLITE_ENABLE_PREUPDATE_HOOK`. The Rust ABI header
marks `synclite_rs_*` with default visibility; `replayer.c` needs the NDK JNI
headers. Keep all SQLite symbols private to avoid binding to the JNI bridge's
independently bundled rusqlite. CMake owns source/header dependency tracking and
Gradle owns ABI-specific compilation, stripping, and AAR packaging. An AAR
verification task must check both native libraries for both ABIs, rather than
treating a successful Rust build as proof of CDC packaging.

Install the following before building:

| Tool | Required version / setup |
| --- | --- |
| JDK | 17 |
| Android SDK | API 35 platform and build tools |
| Android NDK | r26b or later; set `ANDROID_NDK_HOME` if Android SDK discovery does not find it |
| Gradle | Wrapper included; first build downloads Gradle 8.7 |
| Rust | Rust 1.86+ via `rustup` |
| Cargo NDK | `cargo install cargo-ndk` |
| Rust Android targets | `rustup target add aarch64-linux-android x86_64-linux-android` |
| CMake and Ninja | CMake 3.22.1 through Android SDK Manager; builds the custom CDC sidecar (not DuckDB) |

The AAR ships these ABIs:

- `arm64-v8a` for physical Android devices;
- `x86_64` for Android emulators.

Important: the native CDC sidecar is only supported on Android API 26 or newer. The Android app must set `minSdk` to 26 or higher, and the Gradle/CMake build explicitly rejects lower API levels. This is a build-time requirement for the custom native library, not a PostgreSQL connectivity issue.

## Build

From this directory, run:

```text
gradlew.bat assembleRelease
```

On Linux/macOS use `./gradlew assembleRelease`. Set `ANDROID_JAVA_HOME` to a
JDK 17 installation when the platform's primary `JAVA_HOME` points to JDK 25.

The Gradle `preBuild` task invokes:

```text
cargo ndk -t arm64-v8a -t x86_64 --platform 26 -o build/generated/jniLibs build --release --package synclite-bindings-java
```

For a packaging/API-only check, skip the native build explicitly:

```text
gradle -PskipAndroidRust=true assembleRelease
```

This skips only the Rust JNI compilation. CMake still builds and packages the
custom CDC library for both ABIs. A clean build is not runnable until matching
ABI-specific `libsynclite_jni.so` files are supplied; an incremental build may
reuse existing JNI outputs, so this flag is not release validation.

The resulting AAR is written under `build/outputs/aar/`. To publish to the local Maven repository, run:

```text
gradle publishReleasePublicationToMavenLocal
```

The local Maven coordinates are:

```text
io.synclite:synclite-android:1.2.0
```

An application can consume the locally published artifact with:

```kotlin
repositories {
        mavenLocal()
        google()
        mavenCentral()
}

dependencies {
        implementation("io.synclite:synclite-android:1.2.0")
}
```

## Publish to a Maven repository

The Android module uses Gradle `maven-publish`. To publish to an internal
Maven-compatible repository, provide the repository URL and credentials through
Gradle properties or environment variables. Do not commit credentials.

```text
MAVEN_USERNAME=... MAVEN_PASSWORD=... gradle \
    -PmavenRepoUrl=https://repo.example.com/releases \
    -PsyncliteVersion=1.2.0 \
    publishReleasePublicationToMavenRepository
```

On Windows PowerShell:

```powershell
$env:MAVEN_USERNAME = "..."
$env:MAVEN_PASSWORD = "..."
gradle -PmavenRepoUrl=https://repo.example.com/releases `
             -PsyncliteVersion=1.2.0 `
             publishReleasePublicationToMavenRepository
```

For Maven Central, the same AAR should be published through the repository's
required staging/signing workflow. The release should include the generated
POM, module metadata, sources/Javadoc artifacts if required by the repository,
and the Android AAR with both `arm64-v8a` and `x86_64` JNI libraries. The AAR
must be built without `skipAndroidRust` for developer consumption.

## Consume from Kotlin

```kotlin
implementation("io.synclite:synclite-android:1.2.0")
```

Android loads the ABI-specific `libsynclite_jni.so` directly from the AAR. Do not use the desktop `synclite` JAR in an Android application.

```kotlin
val dbPath = context.filesDir.resolve("orders.db").path
SyncLiteAndroid.initialize(
    AndroidDeviceOptions(
        deviceName = "orders-device",
        databasePath = dbPath,
        destination = AndroidDestination(
            type = "POSTGRES",
            connectionString = "postgresql://user:password@host:5432/syncdb",
            database = "syncdb",
            schema = "syncschema",
            syncMode = "REPLICATION"
        )
    )
)

val connection = SyncLiteConnection.open(dbPath)
try {
    connection.execute("CREATE TABLE IF NOT EXISTS orders(id INTEGER, item TEXT)")
    connection.prepare("INSERT INTO orders VALUES(?, ?)").apply {
        addBatch(listOf(1L, "widget"))
        addBatch(listOf(2L, "gadget"))
        executeBatch()
    }
    connection.commit()
    connection.flush()
    SyncLiteAndroid.awaitSync(dbPath, 30_000)
} finally {
    connection.close()
}
```

`initialize()` and subsequent connection opens reuse one background worker per device/destination when the configuration matches. Closing a connection leaves that worker running so offline catch-up can continue. A different configuration for the same live worker is rejected rather than silently using the old destination.

Reinitialization stops the device's workers before removing local state. If a worker is still stopping, reinitialization returns an error and must be retried after it exits.

`awaitSync()` returns when every configured destination reaches the source checkpoint, or throws when its timeout expires. It does not stop the worker, finish an Activity, or terminate the application.

### Initialization traces

The consolidator device trace records snapshot start, table schema initialization, copied row counts, and snapshot completion at INFO. Table completion is logged only after the snapshot transaction commits. Empty snapshots still produce start/completion events; checkpoint-based recovery is identified explicitly and does not copy data again.

PostgreSQL metadata CREATE/ALTER statements are DEBUG-only. Fresh checkpoint tables already use BIGINT; only legacy integer columns are upgraded. Required migration failures stop initialization and roll back the migration instead of continuing with an incompatible checkpoint schema.

Host-side PostgreSQL regressions can run against an isolated, plaintext local test server using `SYNCLITE_TEST_POSTGRES_URL`:

```powershell
$env:SYNCLITE_TEST_POSTGRES_URL = 'host=127.0.0.1 port=55439 user=postgres dbname=postgres sslmode=disable'
cargo test -p consolidator-runtime --lib postgres_metadata_and_snapshot_trace_regression -- --ignored --nocapture
cargo test -p synclite-rs --test sync_status postgres_initialize_reopen_await_uses_one_worker_and_one_snapshot -- --ignored --nocapture
```

These tests verify metadata migration/rollback, checkpoint preservation, snapshot recovery, and initialize/open/reopen/await behavior with trace assertions. They do not replace Android instrumentation or an on-device AWS RDS TLS check. Plaintext is only for the isolated loopback test server, not a production destination.

## Run the sample app

The `sample` module is a runnable Android application. It provides editable PostgreSQL connection fields, writes direct and batched rows to local SQLite, displays the local query results, flushes the log segment, and waits for replication.

```text
gradle :sample:installDebug
```

Start **SyncLite Android Sample** and configure your PostgreSQL hostname or IP address.

An instrumentation smoke test uses a second SQLite file as the destination and therefore requires no external server:

```text
gradle :sample:connectedDebugAndroidTest
```

Network destinations require the `INTERNET` permission, which is declared by the library manifest and merged into the consuming application.
