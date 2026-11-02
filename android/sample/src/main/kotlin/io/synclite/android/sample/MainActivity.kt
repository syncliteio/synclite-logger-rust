package io.synclite.android.sample

import android.graphics.Typeface
import android.os.Bundle
import android.text.InputType
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import androidx.activity.ComponentActivity
import androidx.core.view.setPadding
import io.synclite.AndroidDestination
import io.synclite.AndroidDeviceOptions
import io.synclite.SyncLiteAndroid
import io.synclite.SyncLiteConnection
import java.util.concurrent.Executors

/** Runnable SQLite-to-PostgreSQL demonstration of the SyncLite Android AAR. */
class MainActivity : ComponentActivity() {
    private val worker = Executors.newSingleThreadExecutor()

    private lateinit var connectionUrl: EditText
    private lateinit var databaseName: EditText
    private lateinit var schemaName: EditText
    private lateinit var runButton: Button
    private lateinit var output: TextView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(buildContent())
        runButton.setOnClickListener { runSample() }
    }

    override fun onDestroy() {
        worker.shutdownNow()
        super.onDestroy()
    }

    private fun buildContent(): ScrollView {
        val density = resources.displayMetrics.density
        fun dp(value: Int): Int = (value * density).toInt()

        val content = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(20))
        }

        content.addView(TextView(this).apply {
            text = "SyncLite Android"
            textSize = 28f
            setTypeface(typeface, Typeface.BOLD)
        })
        content.addView(TextView(this).apply {
            text = "Local SQLite → PostgreSQL replication"
            textSize = 16f
            setPadding(0, dp(4), 0, dp(20))
        })

        connectionUrl = field(
            label = "PostgreSQL connection URL",
            initialValue = "postgresql://postgres:postgres@10.0.2.2:5432/syncdb"
        ).also(content::addView)
        databaseName = field("Destination database", "syncdb").also(content::addView)
        schemaName = field("Destination schema", "syncschema").also(content::addView)

        runButton = Button(this).apply {
            text = "Run SQLite → PostgreSQL sample"
        }
        content.addView(runButton)

        output = TextView(this).apply {
            text = "Ready. The emulator reaches the host machine at 10.0.2.2."
            textSize = 14f
            typeface = Typeface.MONOSPACE
            setPadding(0, dp(16), 0, dp(16))
            setTextIsSelectable(true)
        }
        content.addView(
            output,
            LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.WRAP_CONTENT
            )
        )

        return ScrollView(this).apply { addView(content) }
    }

    private fun field(label: String, initialValue: String): EditText = EditText(this).apply {
        hint = label
        setText(initialValue)
        inputType = InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_URI
        setSingleLine(true)
    }

    private fun runSample() {
        val url = connectionUrl.text.toString().trim()
        val database = databaseName.text.toString().trim()
        val schema = schemaName.text.toString().trim()
        if (url.isEmpty() || database.isEmpty() || schema.isEmpty()) {
            appendLine("All destination fields are required.")
            return
        }

        runButton.isEnabled = false
        output.text = "Starting…"
        worker.execute {
            try {
                executeSample(url, database, schema)
                appendLine("\nSUCCESS: local writes were applied to PostgreSQL.")
            } catch (error: Throwable) {
                appendLine("\nFAILED: ${error.message ?: error.javaClass.simpleName}")
            } finally {
                runOnUiThread { runButton.isEnabled = true }
            }
        }
    }

    private fun executeSample(url: String, database: String, schema: String) {
        val dbPath = filesDir.resolve("sampledevice.db").path
        appendLine("Database: $dbPath")
        appendLine("Initializing SyncLite…")

        SyncLiteAndroid.initialize(
            AndroidDeviceOptions(
                deviceName = "androidsample",
                databasePath = dbPath,
                destination = AndroidDestination(
                    type = "POSTGRES",
                    connectionString = url,
                    database = database,
                    schema = schema,
                    syncMode = "REPLICATION"
                )
            )
        )

        val connection = SyncLiteConnection.open(dbPath)
        try {
            appendLine("Creating users table…")
            connection.execute("DROP TABLE IF EXISTS users")
            connection.execute(
                "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, score INTEGER)"
            )

            appendLine("Writing direct and batched rows…")
            connection.prepare("INSERT INTO users(id, name, score) VALUES(?, ?, ?)").apply {
                execute(listOf(1L, "Alice", 100L))
                execute(listOf(2L, "Bob", 200L))
                addBatch(listOf(3L, "Carol", 300L))
                addBatch(listOf(4L, "Dave", 400L))
                executeBatch()
            }
            connection.execute(
                "UPDATE users SET score = ? WHERE name = ?",
                listOf(250L, "Bob")
            )
            connection.commit()

            val rows = connection.query("SELECT id, name, score FROM users ORDER BY id")
            appendLine("Local rows:")
            rows.forEach { appendLine("  $it") }
            check(rows.size == 4) { "Expected four local rows, found ${rows.size}" }

            appendLine("Rolling the active segment…")
            connection.flush()
            appendLine("Waiting for PostgreSQL apply…")
            SyncLiteAndroid.awaitSync(dbPath, 30_000)
        } finally {
            connection.close()
        }
    }

    private fun appendLine(message: String) {
        runOnUiThread {
            output.append(if (output.text.isEmpty()) message else "\n$message")
        }
    }
}
