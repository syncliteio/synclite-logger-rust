package io.synclite.android.sample

import android.database.sqlite.SQLiteDatabase
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import io.synclite.AndroidDestination
import io.synclite.AndroidDeviceOptions
import io.synclite.SyncLiteAndroid
import io.synclite.SyncLiteConnection
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class SyncLiteSmokeTest {
    @Test
    fun writesAndReplicatesToSqliteDestination() {
        val context = ApplicationProvider.getApplicationContext<android.content.Context>()
        val root = context.filesDir.resolve("smoke-${java.util.UUID.randomUUID()}")
        check(root.mkdirs())
        val source = root.resolve("instrumented-source.db")
        val destination = root.resolve("instrumented-destination.db")

        SyncLiteAndroid.initialize(
            AndroidDeviceOptions(
                deviceName = "androidtest",
                databasePath = source.path,
                destination = AndroidDestination(
                    type = "SQLITE",
                    connectionString = destination.path,
                    syncMode = "REPLICATION"
                )
            )
        )

        assertTrue(root.resolve("synclite/job1/stageDir").isDirectory)
        assertTrue(root.resolve("synclite/job1/workDir").isDirectory)

        val connection = SyncLiteConnection.open(source.path)
        try {
            connection.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)")
            connection.execute("INSERT INTO items VALUES(?, ?)", listOf(1L, "widget"))
            connection.commit()
            connection.flush()
            SyncLiteAndroid.awaitSync(source.path, 30_000)
        } finally {
            connection.close()
        }

        SQLiteDatabase.openDatabase(destination.path, null, SQLiteDatabase.OPEN_READONLY).use { db ->
            db.rawQuery("SELECT name FROM items WHERE id = 1", null).use { cursor ->
                check(cursor.moveToFirst())
                assertEquals("widget", cursor.getString(0))
            }
        }
    }
}
