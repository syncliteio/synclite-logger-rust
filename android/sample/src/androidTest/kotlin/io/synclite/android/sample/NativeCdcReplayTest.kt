package io.synclite.android.sample

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.util.Log
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import io.synclite.AndroidDestination
import io.synclite.AndroidDeviceOptions
import io.synclite.SyncLiteAndroid
import io.synclite.SyncLiteConnection
import java.io.File
import java.util.UUID
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Device/emulator regression: platform SQLite seeds a nonempty database BEFORE
 * registration; only SyncLite's JNI logger/worker may subsequently write the
 * destination. No mocks, host service, environment overrides, or second worker.
 *
 * Run :sample:connectedDebugAndroidTest with instrumentation argument
 * android.testInstrumentationRunnerArguments.class=io.synclite.android.sample.NativeCdcReplayTest.
 * Native replay is mandatory: a matching destination alone cannot pass without
 * worker-written writer=native trace records and a replayed working replica.
 */
@RunWith(AndroidJUnit4::class)
class NativeCdcReplayTest {
    @Test
    fun nonemptyBootstrapThenPreparedBlobCdcInReplicationMode() = exercise("REPLICATION")

    @Test
    fun nonemptyBootstrapThenPreparedBlobCdcInConsolidationMode() = exercise("CONSOLIDATION")

    private fun exercise(mode: String) {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val root = File(context.filesDir, "native-cdc-${UUID.randomUUID()}")
        assertTrue("Create private test root $root", root.mkdirs())
        val source = File(root, "source.db")
        val destination = File(root, "destination.db")
        val deviceName = "nativecdc${UUID.randomUUID().toString().replace("-", "") }"
        val seed = byteArrayOf(0, 1, 127, -128, -1)
        val inserted = ByteArray(8192) { (it % 256).toByte() }
        val updated = byteArrayOf(-1, 0, 42, -128, 0)
        var connection: SyncLiteConnection? = null
        var failure: Throwable? = null
        try {
            // These rows have no command log. They can reach the destination
            // only through the immutable initial snapshot/bootstrap path.
            SQLiteDatabase.openOrCreateDatabase(source, null).use { db ->
                db.execSQL("CREATE TABLE payloads(id INTEGER PRIMARY KEY, label TEXT, payload BLOB)")
                db.execSQL("INSERT INTO payloads VALUES(?, ?, ?)", arrayOf(1L, "bootstrap", seed))
                db.execSQL("INSERT INTO payloads VALUES(?, ?, ?)", arrayOf(2L, "delete me", null))
            }
            assertFalse("No destination exists before initialization", destination.exists())
            SyncLiteAndroid.initialize(
                AndroidDeviceOptions(
                    deviceName = deviceName,
                    databasePath = source.absolutePath,
                    destination = AndroidDestination(
                        type = "SQLITE",
                        connectionString = destination.absolutePath,
                        syncMode = mode
                    )
                )
            )
            val active = SyncLiteConnection.open(source.absolutePath)
            connection = active
            SyncLiteAndroid.awaitSync(source.absolutePath, SYNC_TIMEOUT_MS)
            val bootstrap = listOf(Row(1, "bootstrap", seed), Row(2, "delete me", null))
            assertRows(destination, bootstrap, mode, deviceName)

            val work = File(root, "synclite/job1/workDir")
            assertTrue("JNI worker work directory exists", work.isDirectory)
            val nativeBeforeInsert = nativeReplayCount(work)
            active.setAutoCommit(false)
            val insert = active.prepare("INSERT INTO payloads(id, label, payload) VALUES(?, ?, ?)")
            insert.execute(listOf(10L, "binary insert", inserted))
            insert.execute(listOf(11L, "empty insert", byteArrayOf()))
            insert.execute(listOf(12L, "null insert", null))
            active.commit()
            active.flush()
            SyncLiteAndroid.awaitSync(source.absolutePath, SYNC_TIMEOUT_MS)
            val afterInsert = bootstrap + listOf(
                Row(10, "binary insert", inserted), Row(11, "empty insert", byteArrayOf()),
                Row(12, "null insert", null)
            )
            assertRows(destination, afterInsert, mode, deviceName)
            assertWorkingReplica(work, afterInsert)
            val nativeAfterInsert = nativeReplayCount(work)
            assertTrue("Prepared INSERT must use the custom native CDC writer", nativeAfterInsert > nativeBeforeInsert)

            val update = active.prepare("UPDATE payloads SET label = ?, payload = ? WHERE id = ?")
            // Updating a pre-registration row also checks that replay was seeded
            // from the nonempty bootstrap rather than an empty schema replica.
            update.execute(listOf("bootstrap updated", updated, 1L))
            update.execute(listOf("now null", null, 11L))
            update.execute(listOf("now empty", byteArrayOf(), 12L))
            val delete = active.prepare("DELETE FROM payloads WHERE id = ?")
            delete.execute(listOf(2L))
            delete.execute(listOf(10L))
            active.commit()
            active.flush()
            SyncLiteAndroid.awaitSync(source.absolutePath, SYNC_TIMEOUT_MS)
            val finalRows = listOf(
                Row(1, "bootstrap updated", updated), Row(11, "now null", null),
                Row(12, "now empty", byteArrayOf())
            )
            assertRows(destination, finalRows, mode, deviceName)
            assertWorkingReplica(work, finalRows)
            assertTrue("UPDATE/DELETE must also use native CDC replay", nativeReplayCount(work) > nativeAfterInsert)

            // close() joins the connection-owned shipper/consolidator workers.
            // Inspect optional retained logs only after that, avoiding cleanup
            // racing a reader. A cleaned .cdclog is not a test failure.
            active.close()
            connection = null
            inspectRetainedCdcLogs(work)
            Log.i(TAG, "$mode: bootstrap, prepared BLOB/empty/NULL INSERT, UPDATE, DELETE and native replay verified")
        } catch (error: Throwable) {
            failure = error
            Log.e(TAG, "Failed $mode in $root; worker traces:\n${workerTraces(root)}", error)
            throw error
        } finally {
            try {
                connection?.close()
            } catch (closeError: Throwable) {
                if (failure != null) failure.addSuppressed(closeError) else throw closeError
            } finally {
                // Never touch shared app/sample databases or other tests' paths.
                val cleaned = root.deleteRecursively()
                if (!cleaned) {
                    val error = AssertionError("Unable to remove owned test directory $root")
                    if (failure != null) failure.addSuppressed(error) else throw error
                }
            }
        }
    }

    private data class Row(val id: Long, val label: String, val payload: ByteArray?)

    private fun assertRows(file: File, expected: List<Row>, mode: String? = null, deviceName: String? = null) {
        assertTrue("Expected database $file", file.isFile)
        SQLiteDatabase.openDatabase(file.absolutePath, null, SQLiteDatabase.OPEN_READONLY).use { db ->
            db.rawQuery("SELECT id, label, payload, typeof(payload) FROM payloads ORDER BY id", null).use { rows ->
                assertEquals("Row count in $file", expected.size, rows.count)
                for (row in expected) {
                    assertTrue(rows.moveToNext())
                    assertEquals(row.id, rows.getLong(0))
                    assertEquals(row.label, rows.getString(1))
                    assertEquals("SQLite storage class for id=${row.id}", if (row.payload == null) "null" else "blob", rows.getString(3))
                    if (row.payload == null) {
                        assertTrue("SQL NULL must not become an empty BLOB", rows.isNull(2))
                    } else {
                        assertFalse("BLOB must not become SQL NULL", rows.isNull(2))
                        assertArrayEquals("BLOB bytes for id=${row.id}", row.payload, rows.getBlob(2))
                    }
                }
            }
            if (mode != null) {
                val columns = db.rawQuery("PRAGMA table_info(payloads)", null).use { cursor ->
                    buildSet { while (cursor.moveToNext()) add(cursor.getString(cursor.getColumnIndexOrThrow("name"))) }
                }
                if (mode == "REPLICATION") {
                    assertEquals(setOf("id", "label", "payload"), columns)
                } else {
                    assertTrue(columns.containsAll(listOf("synclite_device_id", "synclite_device_name", "synclite_update_timestamp")))
                    db.rawQuery("SELECT synclite_device_id, synclite_device_name FROM payloads", null).use { cursor ->
                        while (cursor.moveToNext()) {
                            assertFalse(cursor.isNull(0))
                            assertTrue(cursor.getString(0).isNotEmpty())
                            assertEquals(deviceName, cursor.getString(1))
                        }
                    }
                }
            }
        }
    }

    private fun assertWorkingReplica(work: File, expected: List<Row>) {
        val replicas = work.walkTopDown().filter {
            it.isFile && (it.name == "source.db.synclite.backup" || it.name == "synclite_replicator_replica.db")
        }.toList()
        assertTrue("Native replay must retain a working replica under $work", replicas.isNotEmpty())
        for (replica in replicas) {
            assertRows(replica, expected)
            SQLiteDatabase.openDatabase(replica.absolutePath, null, SQLiteDatabase.OPEN_READONLY).use { db ->
                db.rawQuery("SELECT commit_id FROM replay_checkpoint", null).use { cursor ->
                    assertTrue("Native replay checkpoint must exist in $replica", cursor.moveToFirst())
                    assertTrue("Replay must advance beyond bootstrap", cursor.getLong(0) > 0)
                }
            }
        }
    }

    private fun nativeReplayCount(work: File): Int = workerTraces(work).lineSequence().count {
        it.contains("Replicated ") && it.contains("writer=native")
    }

    private fun workerTraces(root: File): String = root.walkTopDown()
        .filter { it.isFile && it.name.startsWith("synclite_device.trace") }
        .joinToString("\n") { "${it.name}:\n${it.readText()}" }

    private fun inspectRetainedCdcLogs(work: File) {
        val logs = work.walkTopDown().filter { it.isFile && it.extension == "cdclog" }.toList()
        Log.i(TAG, "Retained CDC logs: ${logs.size}; worker cleanup may remove all applied segments")
        for (file in logs) {
            SQLiteDatabase.openDatabase(file.absolutePath, null, SQLiteDatabase.OPEN_READONLY).use { db ->
                db.rawQuery("SELECT COUNT(*) FROM cdclog", null).use { cursor ->
                    assertTrue(cursor.moveToFirst())
                    Log.i(TAG, "${file.name}: ${cursor.getLong(0)} CDC rows")
                }
            }
        }
    }

    private companion object {
        const val TAG = "NativeCdcReplayTest"
        const val SYNC_TIMEOUT_MS = 60_000L
    }
}