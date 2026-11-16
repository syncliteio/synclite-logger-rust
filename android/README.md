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

The Android CDC sidecar is consumed from the Rust workspace's checked-in native
directory, separately from the desktop libraries:

```text
native/android/arm64-v8a/libsynclitecdc.so
native/android/x86_64/libsynclitecdc.so
```

These are Android NDK builds from `SyncLiteNative`, not desktop Linux binaries or
rusqlite's stock SQLite. The AAR build no longer needs a `SyncLiteNative` checkout
or recompiles the CDC C sources. `SQLITE_THREADSAFE=1` and
`SQLITE_ENABLE_PREUPDATE_HOOK` must be enabled, and `synclite_rs_*` must be exported.
Both supported ABIs must package `libsynclitecdc.so` alongside `libsynclite_jni.so`.
The runtime loads this sidecar by SONAME in Android's application linker namespace,
including applications loading unextracted libraries directly from an APK.
It must not extract a desktop prebuilt into an Android temporary directory.

Keep SQLite symbols private to avoid binding to the JNI bridge's independently
bundled rusqlite. The SyncLiteNative Android build uses a linker export map and
16 KB ELF page alignment. Gradle validates the prebuilt ELF architectures before
building Rust, packages both ABI directories, and verifies that the release AAR
contains both native libraries and byte-for-byte copies of the CDC prebuilts.

To refresh the CDC binaries, build both ABIs in `SyncLiteNative`, then copy them
from its `build/android/jniLibs` directory. From the Rust workspace on Windows:

```powershell
$source = 'C:\work\synclite\repos\SyncLiteNative\build\android\jniLibs'
foreach ($abi in @('arm64-v8a', 'x86_64')) {
    New-Item -ItemType Directory -Force "native\android\$abi" | Out-Null
    Copy-Item "$source\$abi\libsynclitecdc.so" "native\android\$abi\libsynclitecdc.so"
}
```

Do not copy the architecture-suffixed desktop `.so` files. Commit both updated
Android binaries together. The old `android/native/CMakeLists.txt` remains
available for manual source builds but is not used by Gradle.

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

The AAR ships these ABIs:

- `arm64-v8a` for physical Android devices;
- `x86_64` for Android emulators.

The Android AAR requires API 26 or newer. The CDC prebuilts are built for API 21
and can run at this higher minimum; the Rust JNI bridge and Gradle module retain
their API 26 minimum. This is not a PostgreSQL connectivity issue.

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

This skips only the Rust JNI compilation. Gradle still validates and packages
the CDC prebuilts for both ABIs. A clean build is not runnable until matching
ABI-specific `libsynclite_jni.so` files are supplied; an incremental build may
reuse existing JNI outputs, so this flag is not release validation.

The resulting AAR is written under `build/outputs/aar/`. Application developers
should consume `io.synclite:synclite-android:1.2.0` from Maven Central, without
building the AAR. The local publication workflow below is for maintainer testing.
To publish to the local Maven repository, run:

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

## Consume from Maven Central (Kotlin)

Configure the application's `settings.gradle.kts` dependency repositories:

```kotlin
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
    }
}
```

Add the following dependency to `app/build.gradle.kts` once release `1.2.0`
has been published to Maven Central:

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
