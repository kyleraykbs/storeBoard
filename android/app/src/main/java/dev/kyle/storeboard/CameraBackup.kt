package dev.kyle.storeboard

import android.Manifest
import android.content.ContentUris
import android.content.Context
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.provider.MediaStore
import java.io.File
import java.time.LocalTime
import java.time.format.DateTimeFormatter

/**
 * Camera backup: the photos and videos the camera takes go into a store, at
 * a folder the person chooses. Off until they choose: nothing on the phone
 * is sent anywhere by default.
 *
 * It is a send (the engine copies, never moves), so the camera roll stays
 * as it is; the store keeps its own copy and a name already there is parked,
 * never overwritten. What has been sent is a watermark of MediaStore's
 * DATE_ADDED, with the ids sent in that last second, so a pass cut short
 * picks up where it stopped and a photo is sent once.
 */
object CameraBackup {
    data class Settings(
        val enabled: Boolean,
        val store: String,
        val folder: String,
        /** DATE_ADDED (seconds) of the newest item sent. */
        val since: Long,
        /** Ids sent with DATE_ADDED == since. */
        val doneAtSince: Set<Long>,
    )

    /** What the last pass did, for the screen. */
    @Volatile var lastResult: String = ""

    /**
     * A streamed store's sends wait in the engine's queue on the phone until
     * a holder takes them. Past this much, a pass waits for the queue to
     * drain rather than copying the whole camera roll onto the phone again.
     */
    private const val QUEUE_LIMIT = 1L shl 30
    private const val DRAIN_WAIT_MS = 10 * 60 * 1000L

    private fun queueBytes(ctx: Context, store: String): Long =
        File(File(Engine.dataDir(ctx), "queue"), store).walkTopDown().filter { it.isFile }.sumOf { it.length() }

    /** Starts a pass off the main thread; one runs at a time. */
    fun runSoon(ctx: Context) {
        val app = ctx.applicationContext
        Thread({ run(app) }, "camera-backup").start()
    }

    private val clock = DateTimeFormatter.ofPattern("HH:mm")

    private fun prefs(ctx: Context) = ctx.getSharedPreferences("camera-backup", Context.MODE_PRIVATE)

    fun settings(ctx: Context): Settings {
        val p = prefs(ctx)
        return Settings(
            enabled = p.getBoolean("enabled", false),
            store = p.getString("store", "") ?: "",
            folder = p.getString("folder", "") ?: "",
            since = p.getLong("since", 0),
            doneAtSince = (p.getString("done", "") ?: "").split(",").mapNotNull { it.toLongOrNull() }.toSet(),
        )
    }

    /**
     * Turns it on. [includeExisting] sends what the camera already took as
     * well; otherwise only what it takes from now on.
     */
    fun enable(ctx: Context, store: String, folder: String, includeExisting: Boolean) {
        prefs(ctx).edit()
            .putBoolean("enabled", true)
            .putString("store", store)
            .putString("folder", folder.trim('/'))
            .putLong("since", if (includeExisting) 0 else System.currentTimeMillis() / 1000)
            .putString("done", "")
            .apply()
        lastResult = ""
    }

    fun disable(ctx: Context) {
        prefs(ctx).edit().putBoolean("enabled", false).apply()
        lastResult = ""
    }

    /** The permissions reading the camera roll takes on this Android. */
    fun permissions(): Array<String> =
        if (Build.VERSION.SDK_INT >= 33) {
            arrayOf(Manifest.permission.READ_MEDIA_IMAGES, Manifest.permission.READ_MEDIA_VIDEO)
        } else {
            arrayOf(Manifest.permission.READ_EXTERNAL_STORAGE)
        }

    fun allowed(ctx: Context): Boolean =
        permissions().all { ctx.checkSelfPermission(it) == PackageManager.PERMISSION_GRANTED }

    private data class Item(val uri: Uri, val id: Long, val name: String, val added: Long)

    /** The camera's items added at or after [since], oldest first. */
    private fun newItems(ctx: Context, since: Long): List<Item> {
        val out = mutableListOf<Item>()
        val sources = listOf(
            MediaStore.Images.Media.EXTERNAL_CONTENT_URI,
            MediaStore.Video.Media.EXTERNAL_CONTENT_URI,
        )
        for (base in sources) {
            val projection = arrayOf(
                MediaStore.MediaColumns._ID,
                MediaStore.MediaColumns.DISPLAY_NAME,
                MediaStore.MediaColumns.DATE_ADDED,
            )
            // The camera's own folder: not screenshots, downloads or what
            // other apps save.
            val (where, args) = if (Build.VERSION.SDK_INT >= 29) {
                "${MediaStore.MediaColumns.DATE_ADDED} >= ? AND ${MediaStore.MediaColumns.RELATIVE_PATH} LIKE ? AND ${MediaStore.MediaColumns.IS_PENDING} = 0" to
                    arrayOf(since.toString(), "DCIM/Camera%")
            } else {
                @Suppress("DEPRECATION")
                "${MediaStore.MediaColumns.DATE_ADDED} >= ? AND ${MediaStore.MediaColumns.DATA} LIKE ?" to
                    arrayOf(since.toString(), "%/DCIM/Camera/%")
            }
            ctx.contentResolver.query(base, projection, where, args, "${MediaStore.MediaColumns.DATE_ADDED} ASC")?.use { c ->
                val idCol = c.getColumnIndexOrThrow(MediaStore.MediaColumns._ID)
                val nameCol = c.getColumnIndexOrThrow(MediaStore.MediaColumns.DISPLAY_NAME)
                val addedCol = c.getColumnIndexOrThrow(MediaStore.MediaColumns.DATE_ADDED)
                while (c.moveToNext()) {
                    val id = c.getLong(idCol)
                    val name = c.getString(nameCol) ?: "IMG_$id"
                    out += Item(ContentUris.withAppendedId(base, id), id, File(name).name, c.getLong(addedCol))
                }
            }
        }
        return out.sortedWith(compareBy({ it.added }, { it.id }))
    }

    /**
     * One pass: sends everything new. Returns how many were sent. Runs off
     * the main thread; the service calls it.
     */
    @Synchronized fun run(ctx: Context): Int {
        var s = settings(ctx)
        if (!s.enabled || s.store.isEmpty() || !Engine.running()) return 0
        if (!allowed(ctx)) {
            lastResult = "Needs permission to read photos and videos"
            return 0
        }
        val items = try {
            newItems(ctx, s.since).filter { !(it.added == s.since && it.id in s.doneAtSince) }
        } catch (e: Exception) {
            lastResult = "Couldn't read the camera roll: ${e.message}"
            return 0
        }
        if (items.isEmpty()) {
            if (lastResult.isEmpty()) lastResult = "Up to date"
            return 0
        }
        val inbox = File(ctx.cacheDir, "camera").apply { mkdirs() }
        var sent = 0
        for (it in items) {
            lastResult = "Backing up: ${sent + 1} of ${items.size}"
            // Let the holder catch up before queueing more on the phone.
            val waitUntil = System.currentTimeMillis() + DRAIN_WAIT_MS
            while (queueBytes(ctx, s.store) > QUEUE_LIMIT) {
                if (System.currentTimeMillis() > waitUntil || !Engine.running() || !settings(ctx).enabled) {
                    lastResult = "Paused after $sent of ${items.size}: waiting for a holder to take what's queued"
                    return sent
                }
                lastResult = "Backing up: ${sent + 1} of ${items.size} (waiting for the upload to catch up)"
                Thread.sleep(5_000)
            }
            if (!settings(ctx).enabled) {
                lastResult = "Stopped after $sent"
                return sent
            }
            val copy = File(inbox, it.name)
            try {
                ctx.contentResolver.openInputStream(it.uri)?.use { input ->
                    copy.outputStream().use { out -> input.copyTo(out) }
                } ?: continue
                val dest = if (s.folder.isEmpty()) it.name else "${s.folder}/${it.name}"
                Engine.send(s.store, copy, dest)
                sent++
            } catch (e: Exception) {
                lastResult = "Stopped at ${it.name}: ${e.message}"
                return sent
            } finally {
                copy.delete()
            }
            // Record each as it goes, so a pass cut short resumes after it.
            val done = if (it.added == s.since) s.doneAtSince + it.id else setOf(it.id)
            s = s.copy(since = it.added, doneAtSince = done)
            prefs(ctx).edit().putLong("since", s.since).putString("done", done.joinToString(",")).apply()
        }
        lastResult = "Backed up $sent at ${LocalTime.now().format(clock)}"
        return sent
    }
}
