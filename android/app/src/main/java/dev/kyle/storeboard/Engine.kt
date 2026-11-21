package dev.kyle.storeboard

import android.content.Context
import mobile.Daemon
import mobile.Mobile
import java.io.File
import java.util.concurrent.Executors

/**
 * The one embedded engine. gomobile turned the Go package into [Mobile], and
 * everything in and out is JSON with the shapes the daemon's /v1 document,
 * the config file and the CLI already use — so the phone is a peer with the
 * same contract as the desktop, just without a local socket to serve.
 */
object Engine {
    private val lock = Any()

    /**
     * Start and stop open and close journals, mount state and sockets: slow
     * work that must not run on the main thread (an ANR on a large store), and
     * must not reorder either — a stop queued after a start has to run after
     * it, or a service destroyed while starting leaves an engine nobody owns.
     * One worker thread gives both.
     */
    private val worker = Executors.newSingleThreadExecutor { r ->
        Thread(r, "storeboard-engine").apply { isDaemon = true }
    }

    /** Requests queued on [worker] that have not finished yet. */
    @Volatile
    private var pending = 0

    @Volatile
    var daemon: Daemon? = null
        private set

    @Volatile
    var lastError: String? = null
        private set

    /** Kept from [start] so [stop], which has no context, can notify too. */
    @Volatile
    private var appContext: Context? = null

    fun start(context: Context, config: String): Boolean {
        val ok = synchronized(lock) {
            if (daemon != null) return true
            appContext = context.applicationContext
            lastError = null
            try {
                daemon = Mobile.start(config, logFile(context).absolutePath)
                true
            } catch (e: Exception) {
                lastError = e.message ?: e.toString()
                false
            }
        }
        // The Files app shows no stores while the engine is down; this is
        // what makes them appear once it is up, without a pull to refresh.
        if (ok) StoreboardDocumentsProvider.notifyChanged(context)
        return ok
    }

    fun stop() {
        val was = synchronized(lock) {
            val d = daemon
            try {
                d?.stop()
            } catch (_: Exception) {
            }
            daemon = null
            d
        }
        if (was != null) appContext?.let { StoreboardDocumentsProvider.notifyChanged(it) }
    }

    /** [start] on the engine thread; [done] gets the result on that thread. */
    fun startAsync(context: Context, config: String, done: (Boolean) -> Unit = {}) {
        val app = context.applicationContext
        synchronized(lock) { pending++ }
        worker.execute {
            val ok = try {
                start(app, config)
            } finally {
                synchronized(lock) { pending-- }
            }
            done(ok)
        }
    }

    /** [stop] on the engine thread, after any start queued before it. */
    fun stopAsync() {
        synchronized(lock) { pending++ }
        worker.execute {
            try {
                stop()
            } finally {
                synchronized(lock) { pending-- }
            }
        }
    }

    /**
     * Blocks (call it off the main thread) until the engine runs, a queued
     * start fails, or [timeoutMs] passes. A share on a cold start arrives
     * before the service has brought the engine up; failing it then would
     * drop the user's file for no reason.
     */
    fun awaitRunning(timeoutMs: Long = 30_000): Boolean {
        val started = System.currentTimeMillis()
        val deadline = started + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (daemon != null) return true
            // Nothing queued and still not running: a start already failed
            // (lastError says why) — but only once the service has had a
            // moment to queue its start at all, or an error left over from an
            // earlier attempt would fail this one before it began.
            val settled = System.currentTimeMillis() - started > 2_000
            if (settled && pending == 0 && lastError != null) return false
            Thread.sleep(100)
        }
        return daemon != null
    }

    fun running(): Boolean = daemon != null

    /** The daemon's own status document, or "" while stopped. */
    fun status(): String = daemon?.let { runCatching { it.status() }.getOrElse { "" } } ?: ""

    fun send(folder: String, source: File, dest: String): String =
        engine().send(folder, source.absolutePath, dest)

    fun versions(folder: String, path: String): String = engine().versions(folder, path)

    fun restore(folder: String, path: String, blob: String, at: String, to: File): String =
        engine().restore(folder, path, blob, at, to.absolutePath)

    /** The children of [dir] in [store] as a JSON array of BrowseEntry. */
    fun browse(store: String, dir: String): String = engine().browse(store, dir)

    /** One path in [store] as a JSON BrowseEntry. */
    fun stat(store: String, path: String): String = engine().stat(store, path)

    /**
     * The file's own path on the phone, or null when its bytes are not here
     * and current (always, for a streamed store) or the engine is down.
     */
    fun localPath(store: String, path: String): String? =
        daemon?.let { d -> runCatching { d.localPath(store, path) }.getOrNull()?.takeIf { it.isNotEmpty() } }

    /** Blocks while a holder answers: never on the main or a binder thread. */
    fun readAt(store: String, path: String, offset: Long, length: Long): ByteArray =
        engine().readAt(store, path, offset, length) ?: ByteArray(0)

    private fun engine(): Daemon =
        daemon ?: throw IllegalStateException(lastError ?: "the engine is not running")

    fun dataDir(context: Context): File = File(context.filesDir, "storeboard").apply { mkdirs() }

    fun logFile(context: Context): File = File(dataDir(context), "storeboard.log")

    fun configFile(context: Context): File = File(dataDir(context), "config.json")

    /**
     * What the config editor opens with: this phone and the two stores a
     * phone usually wants — the camera roll, which lives here and syncs out,
     * and the photo library, which comes down from the pool and stays
     * readable offline.
     *
     * The root is the one place the phone's bytes live: every store is a
     * directory directly under it. The peer list must name this phone exactly
     * as the pool's config declares it, and must carry the pool's own name
     * and a dialable endpoint — the phone dials out, so the endpoint is what
     * makes the pool reachable. The pool needs the matching line on its side:
     * this phone listed in its group, no endpoint needed, because a declared
     * but unaddressed peer simply waits to be dialled.
     *
     * This document is pinned by TestAppDefaultConfigIsValid in mobile/ —
     * the config is strict (an unknown key is an error), so the two have to
     * move together.
     */
    fun defaultConfig(context: Context): String {
        val data = dataDir(context)
        // The root lives beside the data directory, never inside it: that
        // tree is the daemon's own (blobs, journals, history) and a store
        // rooted in there would index the daemon's state.
        val root = File(context.filesDir, "Storeboard").absolutePath
        return """
        {
          "configVersion": 2,
          "root": "$root",
          "peer": "phone",
          "dataDir": "${data.absolutePath}",
          "socket": "${File(data, "storeboard.sock").absolutePath}",
          "backup": { "enable": true, "keep": "1y" },
          "peers": {
            "phone": {},
            "pool": { "endpoints": ["192.168.1.10:4242"] }
          },
          "groups": {
            "kyle": { "peers": ["phone", "pool"] }
          },
          "stores": {
            "camera": { "group": "kyle" },
            "photos": { "group": "kyle" }
          }
        }
        """.trimIndent()
    }
}
