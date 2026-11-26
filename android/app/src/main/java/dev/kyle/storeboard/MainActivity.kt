package dev.kyle.storeboard

import android.Manifest
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.OpenableColumns
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.Checkbox
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.RadioButton
import androidx.compose.material3.SegmentedButton
import androidx.compose.material3.SegmentedButtonDefaults
import androidx.compose.material3.SingleChoiceSegmentedButtonRow
import androidx.compose.material3.Surface
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.dynamicDarkColorScheme
import androidx.compose.material3.dynamicLightColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import androidx.core.content.IntentCompat
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.io.File

/**
 * Setup the first time (this phone's name, and a machine to learn stores
 * from), then one screen: what the engine is doing, the stores peers offer
 * and how the phone carries each, camera backup, sharing, the peers, and the
 * raw config and log for when something is wrong.
 */
class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        // Android 15 draws apps under the system bars; the screen keeps
        // clear of them itself (safeDrawingPadding).
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)

        setContent {
            val dark = isSystemInDarkTheme()
            val scheme = when {
                Build.VERSION.SDK_INT >= 31 && dark -> dynamicDarkColorScheme(this)
                Build.VERSION.SDK_INT >= 31 -> dynamicLightColorScheme(this)
                dark -> darkColorScheme()
                else -> lightColorScheme()
            }
            MaterialTheme(colorScheme = scheme) {
                Surface(modifier = Modifier.fillMaxSize()) {
                    var configured by remember { mutableStateOf(Engine.configured(this)) }
                    if (configured) {
                        App()
                    } else {
                        Setup { phone, peer, endpoint ->
                            Engine.configFile(this).writeText(Engine.setupConfig(this, phone, peer, endpoint))
                            askNotifications()
                            SyncService.start(this)
                            configured = true
                            Background.ask(this)
                        }
                    }
                }
            }
        }

        if (Engine.configured(this)) {
            askNotifications()
            if (!Engine.running()) SyncService.start(this)
        }
        // A recreated activity (rotation, process restore) carries the same
        // launching intent; only a fresh launch is a new share, or every
        // rotation would send the file again.
        if (savedInstanceState == null) handleSend(intent)
    }

    private fun askNotifications() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        handleSend(intent)
    }

    /** A file shared into storeboard is a send, to where the person chose. */
    private fun handleSend(intent: Intent?) {
        if (intent?.action != Intent.ACTION_SEND) return
        // Reopened from recents, the activity is handed the share that
        // launched it the first time; that file was already sent.
        if ((intent.flags and Intent.FLAG_ACTIVITY_LAUNCHED_FROM_HISTORY) != 0) return
        val uri = IntentCompat.getParcelableExtra(intent, Intent.EXTRA_STREAM, Uri::class.java)
        // Consumed: nothing that reads this intent again may send it twice.
        intent.action = null
        if (uri != null) sendUri(uri)
    }

    fun sendUri(uri: Uri) {
        lifecycleScope.launch {
            val result = withContext(Dispatchers.IO) {
                // On a cold start the share arrives before the service has
                // brought the engine up; wait for it rather than failing.
                if (Engine.awaitRunning()) {
                    send(uri)
                } else {
                    "Couldn't send it: ${Engine.lastError ?: "storeboard isn't running"}"
                }
            }
            Toast.makeText(this@MainActivity, result, Toast.LENGTH_LONG).show()
        }
    }

    // The engine writes into its own directories, so a shared file is copied
    // in first: the share sheet hands over a URI, the mesh deals in paths.
    // The engine copies, never moves, so the cache copy is this side's to
    // delete — whether or not the send succeeded.
    private fun send(uri: Uri): String {
        val target = SharedFiles.target(this)
            ?: return "Choose where shared files go first, in storeboard"
        var copy: File? = null
        return try {
            val name = displayName(uri)
            val inbox = File(cacheDir, "shared").apply { mkdirs() }
            val file = File(inbox, name)
            copy = file
            contentResolver.openInputStream(uri)?.use { input ->
                file.outputStream().use { output -> input.copyTo(output) }
            } ?: return "Couldn't read $name"
            val dest = if (target.second.isEmpty()) name else "${target.second}/$name"
            val sent = JSONObject(Engine.send(target.first, file, dest))
            "Sent to ${target.first}/${sent.optString("path")}"
        } catch (e: Exception) {
            "Couldn't send it: ${e.message}"
        } finally {
            copy?.delete()
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

/** Where files shared into storeboard go: a store and a folder in it. */
object SharedFiles {
    private fun prefs(ctx: Context) = ctx.getSharedPreferences("shared-files", Context.MODE_PRIVATE)

    fun target(ctx: Context): Pair<String, String>? {
        val store = prefs(ctx).getString("store", "") ?: ""
        return if (store.isEmpty()) null else store to (prefs(ctx).getString("folder", "") ?: "")
    }

    fun set(ctx: Context, store: String, folder: String) {
        prefs(ctx).edit().putString("store", store).putString("folder", folder.trim('/')).apply()
    }
}

/** A name storeboard accepts for a peer: letters, digits, '-', '_', '.'. */
private fun validName(s: String) = s.isNotEmpty() && !s.startsWith(".") && s.all { it.isLetterOrDigit() || it in "-_." }

@Composable
private fun Setup(onDone: (String, String, String) -> Unit) {
    var phone by remember { mutableStateOf("") }
    var peer by remember { mutableStateOf("") }
    var endpoint by remember { mutableStateOf("") }
    Column(
        Modifier.fillMaxSize().safeDrawingPadding().verticalScroll(rememberScrollState()).padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Text("storeboard", style = MaterialTheme.typography.headlineMedium)
        Text(
            "This phone syncs with machines that list it in their config. Name it as they do, " +
                "and give one of them to start from: the stores it offers show up here, and you " +
                "choose which the phone carries.",
            style = MaterialTheme.typography.bodyMedium,
        )
        OutlinedTextField(
            phone, { phone = it.trim() }, label = { Text("This phone's name") }, placeholder = { Text("phone") },
            singleLine = true, isError = phone.isNotEmpty() && !validName(phone), modifier = Modifier.fillMaxWidth(),
        )
        OutlinedTextField(
            peer, { peer = it.trim() }, label = { Text("A machine to sync with") }, placeholder = { Text("server") },
            singleLine = true, isError = peer.isNotEmpty() && !validName(peer), modifier = Modifier.fillMaxWidth(),
        )
        OutlinedTextField(
            endpoint, { endpoint = it.trim() }, label = { Text("Its address") }, placeholder = { Text("server.example.org:4242") },
            supportingText = { Text("host:port where its storeboard listens") },
            singleLine = true, keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri), modifier = Modifier.fillMaxWidth(),
        )
        Button(
            enabled = validName(phone) && validName(peer) && phone != peer && endpoint.contains(':'),
            onClick = { onDone(phone, peer, endpoint) },
        ) { Text("Start") }
    }
}

/** What the screen shows, read off the main thread. */
private data class Snapshot(
    val running: Boolean = false,
    val status: JSONObject? = null,
    val offers: JSONObject? = null,
    val error: String = "",
    val log: String = "",
)

private fun snapshot(ctx: Context) = Snapshot(
    running = Engine.running(),
    status = Engine.status().takeIf { it.isNotEmpty() }?.let { runCatching { JSONObject(it) }.getOrNull() },
    offers = Engine.offers().takeIf { it.isNotEmpty() }?.let { runCatching { JSONObject(it) }.getOrNull() },
    error = Engine.lastError ?: "",
    log = tail(Engine.logFile(ctx)),
)

@Composable
private fun App() {
    val ctx = LocalContext.current
    val activity = ctx as MainActivity
    val scope = rememberCoroutineScope()
    var snap by remember { mutableStateOf(Snapshot()) }
    var busy by remember { mutableStateOf("") }
    var message by remember { mutableStateOf("") }
    var tick by remember { mutableStateOf(0) }

    LaunchedEffect(tick) {
        while (true) {
            snap = withContext(Dispatchers.IO) { snapshot(ctx) }
            if (busy.isNotEmpty() && snap.running && !Engine.restartWanted()) busy = ""
            delay(2000)
        }
    }

    /** A subscription change: record it, and start again when it changes what is carried. */
    fun choose(store: String, mode: String) {
        message = ""
        scope.launch {
            val r = withContext(Dispatchers.IO) { runCatching { Engine.subscribe(store, mode) } }
            r.onSuccess { restart ->
                if (restart) {
                    busy = "Applying…"
                    Engine.restartAsync(ctx)
                }
            }.onFailure { message = it.message ?: it.toString() }
            tick++
        }
    }

    val picker = rememberLauncherForActivityResult(ActivityResultContracts.OpenDocument()) { uri ->
        if (uri != null) activity.sendUri(uri)
    }

    Column(
        Modifier.fillMaxSize().safeDrawingPadding().verticalScroll(rememberScrollState()).padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        val connections = snap.status?.optJSONArray("connections") ?: JSONArray()
        val up = (0 until connections.length()).map { connections.getJSONObject(it) }.filter { it.optString("state") == "connected" }
        Row(verticalAlignment = Alignment.CenterVertically) {
            Column(Modifier.weight(1f)) {
                Text("storeboard", style = MaterialTheme.typography.headlineMedium)
                Text(
                    when {
                        !snap.running -> "Off"
                        up.isNotEmpty() -> "Syncing with " + up.joinToString(", ") { it.optString("peer") }
                        else -> "On, waiting for a peer"
                    },
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
            Switch(
                checked = snap.running,
                onCheckedChange = {
                    if (it) SyncService.start(ctx) else SyncService.stop(ctx)
                    tick++
                },
            )
        }
        if (busy.isNotEmpty()) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                CircularProgressIndicator(Modifier.size(18.dp), strokeWidth = 2.dp)
                Spacer(Modifier.width(10.dp))
                Text(busy)
            }
        }
        val problem = message.ifEmpty { snap.error }
        if (problem.isNotEmpty()) {
            Card(Modifier.fillMaxWidth(), colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.errorContainer)) {
                Text(problem, Modifier.padding(16.dp), color = MaterialTheme.colorScheme.onErrorContainer)
            }
        }

        BackgroundCard()
        StoresCard(snap, ::choose)
        val carried = carriedStores(snap)
        CameraCard(carried)
        SharedCard(carried) { picker.launch(arrayOf("*/*")) }
        PeersCard(snap) { tick++ }
        AdvancedCard(snap) { tick++ }
    }
}

/** The stores the phone carries now, by name. */
private fun carriedStores(snap: Snapshot): List<String> {
    val sync = snap.status?.optJSONArray("sync") ?: return emptyList()
    return (0 until sync.length()).map { sync.getJSONObject(it).optString("store") }.filter { it.isNotEmpty() }.sorted()
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun StoresCard(snap: Snapshot, choose: (String, String) -> Unit) {
    val offers = snap.offers?.optJSONArray("offers") ?: JSONArray()
    val sync = snap.status?.optJSONArray("sync") ?: JSONArray()
    val state = (0 until sync.length()).associate { sync.getJSONObject(it).let { s -> s.optString("store") to s } }
    val me = snap.status?.optString("peer").orEmpty()
    val connections = snap.status?.optJSONArray("connections") ?: JSONArray()
    val connected = (0 until connections.length()).map { connections.getJSONObject(it) }
        .filter { it.optString("state") == "connected" }.map { it.optString("peer") }.toSet()
    Card(Modifier.fillMaxWidth()) {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
            Text("Stores", style = MaterialTheme.typography.titleMedium)
            Text(
                "Stream: in the Files app, fetched as you open things. Keep: a full copy on the phone, for offline.",
                style = MaterialTheme.typography.bodySmall,
            )
            if (offers.length() == 0) {
                Text(
                    if (snap.running) {
                        "No stores offered yet. They come when a peer connects; its config must list this phone in a group with it."
                    } else {
                        "Turn storeboard on to see the stores your peers offer."
                    },
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
            val modes = listOf("off" to "Off", "stream" to "Stream", "local" to "Keep")
            for (i in 0 until offers.length()) {
                val o = offers.getJSONObject(i)
                val name = o.getString("name")
                val from = o.optJSONArray("from")?.let { a -> (0 until a.length()).joinToString(", ") { a.getString(it) } } ?: ""
                val s = state[name]
                val holders = o.optJSONArray("holders")?.let { a -> (0 until a.length()).map { a.getString(it) } } ?: emptyList()
                val holder = me in holders
                val detail = buildString {
                    append(if (holders.isNotEmpty()) "held by ${holders.joinToString(", ")}" else "from $from")
                    if (holders.isNotEmpty() && !holder && o.optString("mode") == "stream" && holders.none { it in connected }) {
                        append(" · no holder connected")
                    }
                    if (s != null) {
                        append(" · ${s.optInt("entries")} files")
                        // Pending is what the store has that is not on the
                        // phone: all of a streamed store, by design. Only a
                        // kept store is still waiting for it.
                        if (o.optString("mode") == "local" && s.optInt("pending") > 0) {
                            append(", ${s.optInt("pending")} still arriving")
                        }
                    }
                }
                Column {
                    Text(name, style = MaterialTheme.typography.bodyLarge)
                    Text(detail, style = MaterialTheme.typography.bodySmall)
                    if (o.optString("carried") == "declared") {
                        Text("Set in this phone's config", style = MaterialTheme.typography.bodySmall)
                    } else if (holder) {
                        Text("This phone is a holder: it keeps it in full", style = MaterialTheme.typography.bodySmall)
                    } else {
                        SingleChoiceSegmentedButtonRow(Modifier.fillMaxWidth().padding(top = 4.dp)) {
                            modes.forEachIndexed { at, (mode, label) ->
                                SegmentedButton(
                                    selected = o.optString("mode") == mode,
                                    onClick = { if (o.optString("mode") != mode) choose(name, mode) },
                                    shape = SegmentedButtonDefaults.itemShape(at, modes.size),
                                ) { Text(label) }
                            }
                        }
                    }
                }
            }
        }
    }
}

/** A store and a folder in it, chosen from the stores the phone carries. */
@Composable
private fun StoreAndFolder(
    carried: List<String>,
    store: String,
    folder: String,
    onStore: (String) -> Unit,
    onFolder: (String) -> Unit,
    folderHint: String,
) {
    if (carried.isEmpty()) {
        Text("Subscribe to a store first.", style = MaterialTheme.typography.bodySmall)
        return
    }
    Text("Into the store", style = MaterialTheme.typography.labelLarge)
    for (s in carried) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            RadioButton(selected = store == s, onClick = { onStore(s) })
            Text(s)
        }
    }
    OutlinedTextField(
        folder, onFolder, label = { Text("Folder in it") }, placeholder = { Text(folderHint) },
        supportingText = { Text("Empty puts them at the top of the store") },
        singleLine = true, modifier = Modifier.fillMaxWidth(),
    )
}

@Composable
private fun CameraCard(carried: List<String>) {
    val ctx = LocalContext.current
    var settings by remember { mutableStateOf(CameraBackup.settings(ctx)) }
    var editing by remember { mutableStateOf(false) }
    var store by remember { mutableStateOf(settings.store) }
    var folder by remember { mutableStateOf(settings.folder) }
    var existing by remember { mutableStateOf(false) }
    var result by remember { mutableStateOf(CameraBackup.lastResult) }
    LaunchedEffect(Unit) {
        while (true) {
            result = CameraBackup.lastResult
            delay(2000)
        }
    }
    val ask = rememberLauncherForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { granted ->
        if (granted.values.all { it }) {
            CameraBackup.enable(ctx, store, folder, existing)
            settings = CameraBackup.settings(ctx)
            editing = false
        } else {
            result = "Camera backup needs permission to read photos and videos"
        }
    }
    Card(Modifier.fillMaxWidth()) {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Column(Modifier.weight(1f)) {
                    Text("Camera backup", style = MaterialTheme.typography.titleMedium)
                    Text(
                        if (settings.enabled) {
                            "What the camera takes goes to ${settings.store}" + (if (settings.folder.isNotEmpty()) "/${settings.folder}" else "")
                        } else {
                            "Off"
                        },
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
                Switch(checked = settings.enabled || editing, onCheckedChange = {
                    if (it) {
                        editing = true
                    } else {
                        editing = false
                        CameraBackup.disable(ctx)
                        settings = CameraBackup.settings(ctx)
                    }
                })
            }
            if (editing) {
                StoreAndFolder(carried, store, folder, { store = it }, { folder = it }, "Phone/Camera")
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Checkbox(existing, { existing = it })
                    Text("Also the photos already on the phone", style = MaterialTheme.typography.bodyMedium)
                }
                Button(enabled = store.isNotEmpty(), onClick = { ask.launch(CameraBackup.permissions()) }) { Text("Turn on") }
            }
            if (settings.enabled && result.isNotEmpty()) Text(result, style = MaterialTheme.typography.bodySmall)
        }
    }
}

@Composable
private fun SharedCard(carried: List<String>, onSendFile: () -> Unit) {
    val ctx = LocalContext.current
    var target by remember { mutableStateOf(SharedFiles.target(ctx)) }
    var editing by remember { mutableStateOf(false) }
    var store by remember { mutableStateOf(target?.first ?: "") }
    var folder by remember { mutableStateOf(target?.second ?: "") }
    Card(Modifier.fillMaxWidth()) {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("Sharing into storeboard", style = MaterialTheme.typography.titleMedium)
            Text(
                target?.let { "A file shared to storeboard goes to ${it.first}" + (if (it.second.isNotEmpty()) "/${it.second}" else "") }
                    ?: "Choose where files shared to storeboard go.",
                style = MaterialTheme.typography.bodySmall,
            )
            if (editing) {
                StoreAndFolder(carried, store, folder, { store = it }, { folder = it }, "Shared")
                Button(enabled = store.isNotEmpty(), onClick = {
                    SharedFiles.set(ctx, store, folder)
                    target = SharedFiles.target(ctx)
                    editing = false
                }) { Text("Save") }
            } else {
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    OutlinedButton(onClick = { editing = true }) { Text(if (target == null) "Choose" else "Change") }
                    if (target != null) OutlinedButton(onClick = onSendFile) { Text("Send a file") }
                }
            }
        }
    }
}

@Composable
private fun PeersCard(snap: Snapshot, changed: () -> Unit) {
    val ctx = LocalContext.current
    val connections = snap.status?.optJSONArray("connections") ?: JSONArray()
    var adding by remember { mutableStateOf(false) }
    var name by remember { mutableStateOf("") }
    var endpoint by remember { mutableStateOf("") }
    Card(Modifier.fillMaxWidth()) {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
            Text("Peers", style = MaterialTheme.typography.titleMedium)
            for (i in 0 until connections.length()) {
                val c = connections.getJSONObject(i)
                val stateText = when (c.optString("state")) {
                    "connected" -> "connected" + c.optString("endpoint").let { if (it.isEmpty()) "" else " at $it" }
                    "dialing" -> "trying to reach it"
                    "backoff" -> "not reachable; trying again soon"
                    "unaddressed" -> "no address: it connects to this phone"
                    else -> c.optString("state")
                }
                Text("${c.optString("peer")}: $stateText", style = MaterialTheme.typography.bodyMedium)
            }
            if (adding) {
                OutlinedTextField(name, { name = it.trim() }, label = { Text("Its name") }, singleLine = true, modifier = Modifier.fillMaxWidth())
                OutlinedTextField(
                    endpoint, { endpoint = it.trim() }, label = { Text("Its address") }, placeholder = { Text("host:4242") },
                    singleLine = true, keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Uri), modifier = Modifier.fillMaxWidth(),
                )
                Button(enabled = validName(name) && endpoint.contains(':'), onClick = {
                    Engine.addPeer(ctx, name, endpoint)
                    Engine.restartAsync(ctx)
                    adding = false
                    name = ""
                    endpoint = ""
                    changed()
                }) { Text("Add") }
            } else {
                TextButton(onClick = { adding = true }) { Text("Add a peer") }
            }
        }
    }
}

@Composable
private fun AdvancedCard(snap: Snapshot, changed: () -> Unit) {
    val ctx = LocalContext.current
    var open by remember { mutableStateOf(false) }
    var config by remember { mutableStateOf("") }
    TextButton(onClick = {
        open = !open
        if (open) config = Engine.configFile(ctx).takeIf { it.exists() }?.readText() ?: ""
    }) { Text(if (open) "Hide the config and log" else "Config and log") }
    if (!open) return
    Card(Modifier.fillMaxWidth()) {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("Config", style = MaterialTheme.typography.titleMedium)
            OutlinedTextField(
                config, { config = it }, modifier = Modifier.fillMaxWidth(),
                textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace), minLines = 8,
            )
            Button(onClick = {
                Engine.configFile(ctx).writeText(config)
                Engine.restartAsync(ctx)
                changed()
                Toast.makeText(ctx, "Saved; starting again with it", Toast.LENGTH_SHORT).show()
            }) { Text("Save and restart") }
            Text("Log", style = MaterialTheme.typography.titleMedium)
            SelectionContainer {
                Text(
                    snap.log.ifEmpty { "Nothing logged yet." },
                    style = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                )
            }
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

/** Running in the background: Android's battery optimization and notifications. */
object Background {
    fun unrestricted(ctx: Context): Boolean =
        ctx.getSystemService(android.os.PowerManager::class.java).isIgnoringBatteryOptimizations(ctx.packageName)

    fun notificationsAllowed(ctx: Context): Boolean =
        Build.VERSION.SDK_INT < 33 ||
            ctx.checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) == android.content.pm.PackageManager.PERMISSION_GRANTED

    /** Android's own dialog: let storeboard run in the background. */
    fun ask(ctx: Context) {
        if (unrestricted(ctx)) return
        runCatching {
            ctx.startActivity(
                Intent(android.provider.Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS, Uri.parse("package:${ctx.packageName}")),
            )
        }
    }
}

/** Shown until storeboard may run in the background and show its notification. */
@Composable
private fun BackgroundCard() {
    val ctx = LocalContext.current
    var unrestricted by remember { mutableStateOf(Background.unrestricted(ctx)) }
    var notifications by remember { mutableStateOf(Background.notificationsAllowed(ctx)) }
    LaunchedEffect(Unit) {
        while (true) {
            unrestricted = Background.unrestricted(ctx)
            notifications = Background.notificationsAllowed(ctx)
            delay(2000)
        }
    }
    val askNotifications = rememberLauncherForActivityResult(ActivityResultContracts.RequestPermission()) { notifications = it }
    if (unrestricted && notifications) return
    Card(Modifier.fillMaxWidth(), colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.secondaryContainer)) {
        Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
            Text("Keep syncing in the background", style = MaterialTheme.typography.titleMedium)
            if (!unrestricted) {
                Text(
                    "Android may stop storeboard to save battery, and then this phone drops off your mesh and camera backup waits. Let it run unrestricted.",
                    style = MaterialTheme.typography.bodySmall,
                )
                Button(onClick = { Background.ask(ctx) }) { Text("Allow background use") }
            }
            if (!notifications && Build.VERSION.SDK_INT >= 33) {
                Text(
                    "Its notification is how Android knows it is working; without it, it may be stopped sooner.",
                    style = MaterialTheme.typography.bodySmall,
                )
                OutlinedButton(onClick = { askNotifications.launch(Manifest.permission.POST_NOTIFICATIONS) }) { Text("Allow notifications") }
            }
        }
    }
}
