package dev.kyle.storeboard

import android.Manifest
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.OpenableColumns
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.core.content.IntentCompat
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.File

/**
 * One screen: whether the engine is up, what it thinks, where files come
 * from, the config it reads, and the tail of its log. Small on purpose — the
 * document it renders is the daemon's own, so there is nothing to invent.
 */
class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val config = Engine.configFile(this)
        if (!config.exists()) config.writeText(Engine.defaultConfig(this))

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }

        setContent {
            MaterialTheme {
                Surface(modifier = Modifier.fillMaxSize()) { App() }
            }
        }

        if (!Engine.running()) SyncService.start(this)
        handleSend(intent)
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        handleSend(intent)
    }

    /** A file shared into storeboard is a send into the camera folder. */
    private fun handleSend(intent: Intent?) {
        if (intent?.action != Intent.ACTION_SEND) return
        val uri = IntentCompat.getParcelableExtra(intent, Intent.EXTRA_STREAM, Uri::class.java)
        if (uri != null) sendUri(uri)
    }

    fun sendUri(uri: Uri) {
        lifecycleScope.launch {
            val result = withContext(Dispatchers.IO) { send(uri) }
            Toast.makeText(this@MainActivity, result, Toast.LENGTH_LONG).show()
        }
    }

    // The engine writes into its own directories, so a shared file is copied
    // in first: the share sheet hands over a URI, the mesh deals in paths.
    private fun send(uri: Uri): String {
        return try {
            val name = displayName(uri)
            val inbox = File(cacheDir, "shared").apply { mkdirs() }
            val copy = File(inbox, name)
            contentResolver.openInputStream(uri)?.use { input ->
                copy.outputStream().use { output -> input.copyTo(output) }
            } ?: return "could not read $name"

            val raw = Engine.send("camera", copy, "shared/$name")
            val sent = JSONObject(raw)
            copy.delete()
            "sent ${sent.optString("path")} (${sent.optLong("size")} bytes)"
        } catch (e: Exception) {
            "send failed: ${e.message}"
        }
    }

    private fun displayName(uri: Uri): String {
        contentResolver.query(uri, null, null, null, null)?.use { cursor ->
            if (cursor.moveToFirst()) {
                val at = cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME)
                if (at >= 0) cursor.getString(at)?.let { return File(it).name }
            }
        }
        return uri.lastPathSegment?.let { File(it).name } ?: "shared-file"
    }
}

@Composable
private fun App() {
    var running by remember { mutableStateOf(Engine.running()) }
    var status by remember { mutableStateOf("") }
    var error by remember { mutableStateOf("") }
    var log by remember { mutableStateOf("") }
    var config by remember { mutableStateOf("") }
    val context = androidx.compose.ui.platform.LocalContext.current
    val activity = context as MainActivity

    LaunchedEffect(Unit) {
        config = Engine.configFile(activity).takeIf { it.exists() }?.readText()
            ?: Engine.defaultConfig(activity)
        while (true) {
            running = Engine.running()
            status = Engine.status()
            error = Engine.lastError ?: ""
            log = tail(Engine.logFile(activity))
            delay(2000)
        }
    }

    val picker = rememberLauncherForActivityResult(
        ActivityResultContracts.OpenDocument(),
    ) { uri -> if (uri != null) activity.sendUri(uri) }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .verticalScroll(rememberScrollState())
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Text("storeboard", style = MaterialTheme.typography.headlineSmall)
        Text(if (running) "engine: running" else "engine: stopped")

        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Button(onClick = { SyncService.start(activity) }) { Text("start") }
            OutlinedButton(onClick = { SyncService.stop(activity) }) { Text("stop") }
            OutlinedButton(onClick = { picker.launch(arrayOf("*/*")) }) { Text("send a file") }
        }

        if (error.isNotEmpty()) Text("error: $error", color = MaterialTheme.colorScheme.error)

        Summary(status)

        HorizontalDivider()
        Text("config", style = MaterialTheme.typography.titleMedium)
        OutlinedTextField(
            value = config,
            onValueChange = { config = it },
            modifier = Modifier.fillMaxWidth(),
            textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
            minLines = 8,
        )
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Button(onClick = {
                Engine.configFile(activity).writeText(config)
                Toast.makeText(activity, "saved; restart to apply", Toast.LENGTH_SHORT).show()
            }) { Text("save") }
        }

        HorizontalDivider()
        Text("log", style = MaterialTheme.typography.titleMedium)
        Text(
            log.ifEmpty { "(nothing yet)" },
            style = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
        )
    }
}

/** The status document, rendered: folders, connections, and the peer name. */
@Composable
private fun Summary(status: String) {
    if (status.isEmpty()) {
        Text("no status yet")
        return
    }
    val doc = runCatching { JSONObject(status) }.getOrNull() ?: run {
        Text(status, style = MaterialTheme.typography.bodySmall)
        return
    }

    Text("peer ${doc.optString("peer")} (${doc.optString("version")})")

    val folders = doc.optJSONArray("sync")
    if (folders != null) {
        for (i in 0 until folders.length()) {
            val f = folders.optJSONObject(i) ?: continue
            val pending = if (f.has("pending")) ", ${f.optInt("pending")} pending" else ""
            Text(
                "${f.optString("folder")}: ${f.optString("state")}, " +
                    "${f.optInt("entries")} entries$pending",
                style = MaterialTheme.typography.bodyMedium,
            )
        }
    }

    val connections = doc.optJSONArray("connections")
    if (connections == null || connections.length() == 0) {
        Text("no one connected", style = MaterialTheme.typography.bodyMedium)
    } else {
        for (i in 0 until connections.length()) {
            val c = connections.optJSONObject(i) ?: continue
            val endpoint = c.optString("endpoint")
            val suffix = if (endpoint.isEmpty()) "" else " at $endpoint"
            Text(
                "peer ${c.optString("peer")}: ${c.optString("state")}$suffix",
                style = MaterialTheme.typography.bodyMedium,
            )
        }
    }
}

/** The tail of the log, without reading a file that grows without bound. */
private fun tail(file: File, bytes: Long = 32 * 1024, lines: Int = 60): String {
    if (!file.exists()) return ""
    return try {
        val length = file.length()
        val from = if (length > bytes) length - bytes else 0
        val text = StringBuilder()
        file.inputStream().use { input ->
            input.skip(from)
            input.bufferedReader().forEachLine { text.appendLine(it) }
        }
        text.lines().takeLast(lines).joinToString("\n")
    } catch (e: Exception) {
        "log unreadable: ${e.message}"
    }
}
