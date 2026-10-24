package dev.kyle.storeboard

import android.content.Context
import mobile.Daemon
import mobile.Mobile
import java.io.File

/**
 * The one embedded engine. gomobile turned the Go package into [Mobile], and
 * everything in and out is JSON with the shapes the daemon's /v1 document,
 * the config file and the CLI already use — so the phone is a peer with the
 * same contract as the desktop, just without a local socket to serve.
 */
object Engine {
    private val lock = Any()

    @Volatile
    var daemon: Daemon? = null
        private set

    @Volatile
    var lastError: String? = null
        private set

    fun start(context: Context, config: String): Boolean = synchronized(lock) {
        if (daemon != null) return true
        lastError = null
        try {
            daemon = Mobile.start(config, logFile(context).absolutePath)
            true
        } catch (e: Exception) {
            lastError = e.message ?: e.toString()
            false
        }
    }

    fun stop() = synchronized(lock) {
        try {
            daemon?.stop()
        } catch (_: Exception) {
        }
        daemon = null
    }

    fun running(): Boolean = daemon != null

    /** The daemon's own status document, or "" while stopped. */
    fun status(): String = daemon?.let { runCatching { it.status() }.getOrElse { "" } } ?: ""

    fun send(folder: String, source: File, dest: String): String =
        engine().send(folder, source.absolutePath, dest)

    fun versions(folder: String, path: String): String = engine().versions(folder, path)

    fun restore(folder: String, path: String, blob: String, at: String, to: File): String =
        engine().restore(folder, path, blob, at, to.absolutePath)

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
