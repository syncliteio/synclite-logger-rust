package io.synclite

/** Configuration for one SQLite-focused embedded consolidator destination. */
data class AndroidDestination(
    val type: String,
    val connectionString: String,
    val syncMode: String = "REPLICATION",
    val database: String? = null,
    val schema: String? = null,
    val metadataStore: String = "LOCAL"
)

/** Device registration options for the SQLite Android runtime. */
data class AndroidDeviceOptions(
    val deviceType: String = "SQLITE",
    val deviceName: String,
    val databasePath: String,
    val destination: AndroidDestination
) {
    init {
        require(java.io.File(databasePath).isAbsolute) {
            "databasePath must be an absolute path in app-private storage, such as context.filesDir"
        }
    }
}

/** SQLite connection backed by the Rust SyncLite runtime. */
class SyncLiteConnection private constructor(private var handle: Long) : AutoCloseable {
    private fun requireHandle(): Long {
        check(handle != 0L) { "The SyncLite connection is closed" }
        return handle
    }

    fun execute(sql: String, params: List<Any?> = emptyList()): Long =
        NativeSqlite.nativeExecute(requireHandle(), sql, params.toTypedArray())

    fun query(sql: String, params: List<Any?> = emptyList()): List<List<Any?>> =
        NativeSqlite.nativeQuery(requireHandle(), sql, params.toTypedArray())
            .map { it.toList() }

    fun prepare(sql: String): SyncLiteStatement = SyncLiteStatement(this, sql)

    fun setAutoCommit(enabled: Boolean) {
        NativeSqlite.nativeSetAutoCommit(requireHandle(), enabled)
    }

    fun getAutoCommit(): Boolean = NativeSqlite.nativeGetAutoCommit(requireHandle())

    fun commit() {
        NativeSqlite.nativeCommit(requireHandle())
    }

    fun rollback() {
        NativeSqlite.nativeRollback(requireHandle())
    }

    /** Finalizes the active log segment so [SyncLiteAndroid.awaitSync] can observe it. */
    fun flush() {
        NativeSqlite.nativeFlush(requireHandle())
    }

    internal fun executePrepared(sql: String, params: List<Any?>): Long = execute(sql, params)

    internal fun queryPrepared(sql: String, params: List<Any?>): List<List<Any?>> = query(sql, params)

    override fun close() {
        val activeHandle = handle
        if (activeHandle != 0L) {
            handle = 0L
            NativeSqlite.nativeClose(activeHandle)
        }
    }

    companion object {
        /** Open an initialized SQLite SyncLite device. */
        @JvmStatic
        fun open(databasePath: String): SyncLiteConnection =
            SyncLiteConnection(NativeSqlite.nativeOpen(databasePath).requireNativeHandle())

        /** Initialize and open a SQLite device using path-derived defaults. */
        @JvmStatic
        fun initialize(databasePath: String): SyncLiteConnection =
            SyncLiteConnection(NativeSqlite.nativeConnectionInitialize(databasePath).requireNativeHandle())
    }
}

/** Managed prepared statement that mirrors the Python/Node binding API. */
class SyncLiteStatement internal constructor(
    private val connection: SyncLiteConnection,
    private val sql: String
) {
    private val batch = mutableListOf<List<Any?>>()

    fun execute(params: List<Any?> = emptyList()): Long = connection.executePrepared(sql, params)

    fun query(params: List<Any?> = emptyList()): List<List<Any?>> = connection.queryPrepared(sql, params)

    fun addBatch(params: List<Any?>) {
        batch += params
    }

    fun clearBatch() {
        batch.clear()
    }

    fun executeBatch(): List<Long> {
        val pending = batch.toList()
        batch.clear()
        return pending.map { execute(it) }
    }
}

private fun Long.requireNativeHandle(): Long {
    check(this != 0L) { "SyncLite did not return a native connection handle" }
    return this
}

/** Configuration for an Android instance of the existing Rust consolidator bridge. */
data class AndroidConsolidatorConfig(
    val databasePath: String,
    val workDirectory: String,
    val deviceDataRoot: String,
    val deviceId: String,
    val deviceName: String,
    val databaseName: String,
    val stageDirectory: String,
    val destination: AndroidDestination,
    val destinationIndex: Int = 1,
    val destinationCount: Int = 1,
    val pollingIntervalMs: Long = 1_000
) {
    init {
        require(destinationIndex in 1..destinationCount) {
            "destinationIndex must be between 1 and destinationCount"
        }
        require(destinationCount > 0) { "destinationCount must be positive" }
    }
}

/** Opaque native consolidator handle. Close it when the owner is destroyed. */
class AndroidConsolidatorSession internal constructor(private var handle: Long) : AutoCloseable {
    private fun requireHandle(): Long {
        check(handle != 0L) { "The SyncLite consolidator session is closed" }
        return handle
    }

    fun notifyStagePath(stagePath: String) {
        NativeConsolidator.nativeNotifyStagePath(requireHandle(), stagePath)
    }

    fun notifyBootstrapReady(backupPath: String, metadataPath: String) {
        NativeConsolidator.nativeNotifyBootstrapReady(requireHandle(), backupPath, metadataPath)
    }

    fun catchUpStageDirectory(stageDirectory: String) {
        NativeConsolidator.nativeCatchUpStageDir(requireHandle(), stageDirectory)
    }

    override fun close() {
        val activeHandle = handle
        if (activeHandle != 0L) {
            handle = 0L
            NativeConsolidator.nativeStopConsolidator(activeHandle)
        }
    }
}

/**
 * Kotlin access to the Rust JNI consolidator shipped in the Android AAR.
 *
 * The initial Android target is SQLite source devices. Application code owns
 * its Android SQLite database and supplies SyncLite stage/bootstrap artifacts
 * to the native consolidator through [AndroidConsolidatorSession].
 */
object SyncLiteAndroid {
    /**
     * Register a SQLite SyncLite device before opening a connection.
     *
    * Use an absolute path under context.filesDir. Stage and work state default
    * to synclite/job1/stageDir and workDir beneath the database's parent.
     * The initial Android AAR deliberately rejects DuckDB device and destination types.
     */
    @JvmStatic
    fun initialize(options: AndroidDeviceOptions) {
        val destination = options.destination
        NativeSqlite.nativeInitialize(
            options.deviceType,
            options.deviceName,
            options.databasePath,
            destination.type,
            destination.connectionString,
            destination.database,
            destination.schema,
            destination.syncMode
        )
    }

    /**
     * Waits for committed writes to reach every destination, returning as soon
     * as they are applied or throwing a native exception when timeoutMs elapses.
     * Call off the main thread. This does not stop background replication or
     * finish the owning Activity.
     */
    @JvmStatic
    fun awaitSync(databasePath: String, timeoutMs: Long) =
        NativeSqlite.nativeAwaitSync(databasePath, timeoutMs)

    @JvmStatic
    fun startConsolidator(config: AndroidConsolidatorConfig): AndroidConsolidatorSession {
        val destination = config.destination
        val handle = NativeConsolidator.nativeSpawnConsolidator(
            config.databasePath,
            config.workDirectory,
            config.deviceDataRoot,
            config.deviceId,
            config.deviceName,
            "SQLITE",
            config.databaseName,
            destination.type,
            destination.connectionString,
            destination.syncMode,
            destination.database,
            destination.schema,
            destination.metadataStore,
            config.stageDirectory,
            config.destinationIndex,
            config.destinationCount,
            config.pollingIntervalMs
        )
        check(handle != 0L) { "SyncLite did not return a consolidator handle" }
        return AndroidConsolidatorSession(handle)
    }

    @JvmStatic
    fun pause(databasePath: String) = NativeConsolidator.nativePauseSync(databasePath)

    @JvmStatic
    fun resume(databasePath: String) = NativeConsolidator.nativeResumeSync(databasePath)

    @JvmStatic
    fun isPaused(databasePath: String): Boolean = NativeConsolidator.nativeIsSyncPaused(databasePath)

    @JvmStatic
    fun awaitAppliedCommit(databasePath: String, commitId: Long, timeoutMs: Long) =
        NativeConsolidator.nativeAwaitAppliedCommit(databasePath, commitId, timeoutMs)

    @JvmStatic
    fun statistics(databasePath: String): LongArray = NativeConsolidator.nativeSyncStatistics(databasePath)

    @JvmStatic
    fun latency(databasePath: String): LongArray = NativeConsolidator.nativeSyncLatency(databasePath)
}
