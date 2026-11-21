package dev.kyle.storeboard

import android.content.Context
import android.database.Cursor
import android.database.MatrixCursor
import android.net.Uri
import android.os.Bundle
import android.os.CancellationSignal
import android.os.Handler
import android.os.HandlerThread
import android.os.ParcelFileDescriptor
import android.os.ProxyFileDescriptorCallback
import android.os.storage.StorageManager
import android.provider.DocumentsContract
import android.provider.DocumentsContract.Document
import android.provider.DocumentsContract.Root
import android.provider.DocumentsProvider
import android.system.ErrnoException
import android.system.OsConstants
import android.webkit.MimeTypeMap
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.time.Instant

/**
 * Every store, in the system file picker and the Files app. This is the
 * phone's version of the desktop's mount: the journal seen as folders, and a
 * streamed store's files read from a holder a range at a time as an app asks
 * for them — the bytes are never on the phone, so a 20 GiB video opens
 * without filling it.
 *
 * Document ids are `store` for a store's top and `store/relative/path` below
 * it; store names are identifiers (never paths), so the first slash always
 * splits them.
 *
 * The provider lives in the app process but does not own the engine; the
 * service does. While the engine is down there are no roots and a folder
 * says why, and [Engine] notifies when it comes up so the stores appear on
 * their own. Starting the service from here was the other option, but it
 * would override the user's own stop button, and Android 12+ refuses a
 * foreground service started from the background anyway.
 */
class StoreboardDocumentsProvider : DocumentsProvider() {
    override fun onCreate(): Boolean = true

    override fun queryRoots(projection: Array<out String>?): Cursor {
        val cursor = MatrixCursor(projection ?: DEFAULT_ROOT_PROJECTION)
        cursor.setNotificationUri(context!!.contentResolver, DocumentsContract.buildRootsUri(AUTHORITY))
        val status = Engine.status()
        if (status.isEmpty()) return cursor
        val doc = runCatching { JSONObject(status) }.getOrNull() ?: return cursor
        val stores = doc.optJSONArray("stores") ?: return cursor
        for (i in 0 until stores.length()) {
            val store = stores.optJSONObject(i) ?: continue
            val name = store.optString("name").takeIf { it.isNotEmpty() } ?: continue
            // Said in the drawer because it decides what opening a file costs:
            // a streamed store's bytes come over the network as they are read.
            val summary = if (store.optString("sync") == "stream") "storeboard · streamed" else "storeboard"
            cursor.newRow()
                .add(Root.COLUMN_ROOT_ID, name)
                .add(Root.COLUMN_DOCUMENT_ID, name)
                .add(Root.COLUMN_TITLE, name)
                .add(Root.COLUMN_SUMMARY, summary)
                .add(Root.COLUMN_FLAGS, Root.FLAG_SUPPORTS_IS_CHILD)
                .add(Root.COLUMN_ICON, R.drawable.ic_store)
                .add(Root.COLUMN_MIME_TYPES, "*/*")
        }
        return cursor
    }

    override fun queryDocument(documentId: String, projection: Array<out String>?): Cursor {
        val cursor = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)
        val (store, path) = split(documentId)
        val entry = stat(store, path)
        addRow(cursor, documentId, entry, if (path.isEmpty()) store else null)
        return cursor
    }

    override fun queryChildDocuments(
        parentDocumentId: String,
        projection: Array<out String>?,
        sortOrder: String?,
    ): Cursor {
        val cursor = MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION)
        // Registered on the folder, so the notification [Engine] sends on a
        // start or stop (on the authority, which covers every folder) makes
        // an open listing load again.
        cursor.setNotificationUri(
            context!!.contentResolver,
            DocumentsContract.buildChildDocumentsUri(AUTHORITY, parentDocumentId),
        )
        val (store, dir) = split(parentDocumentId)
        // Browse is the journal's in-memory tree, so this does not hold the
        // binder thread on anything slow; it never touches the network.
        val raw = try {
            Engine.browse(store, dir)
        } catch (e: IllegalStateException) {
            return cursor.withError("storeboard is not running")
        } catch (e: Exception) {
            return cursor.withError(e.message ?: "cannot list this folder")
        }
        val kids = runCatching { JSONArray(raw) }.getOrNull() ?: return cursor
        for (i in 0 until kids.length()) {
            val kid = kids.optJSONObject(i) ?: continue
            val path = kid.optString("path").takeIf { it.isNotEmpty() } ?: continue
            addRow(cursor, "$store/$path", kid, null)
        }
        return cursor
    }

    override fun isChildDocument(parentDocumentId: String, documentId: String): Boolean =
        documentId.startsWith("$parentDocumentId/")

    override fun getDocumentType(documentId: String): String {
        val (store, path) = split(documentId)
        return mimeType(stat(store, path))
    }

    override fun openDocument(
        documentId: String,
        mode: String,
        signal: CancellationSignal?,
    ): ParcelFileDescriptor {
        val (store, path) = split(documentId)
        if (path.isEmpty()) throw FileNotFoundException("$documentId is a folder")
        val flags = ParcelFileDescriptor.parseMode(mode)
        // MODE_READ_WRITE carries the write-only bit, so this catches "rw" too.
        val writing = flags and ParcelFileDescriptor.MODE_WRITE_ONLY != 0

        // Bytes that are here and current open as the file itself: no proxy,
        // full seek speed, and a write the store's scanner picks up as an
        // ordinary local change.
        Engine.localPath(store, path)?.let { return ParcelFileDescriptor.open(File(it), flags) }
        if (writing) throw FileNotFoundException("read-only: streamed store")

        val entry = stat(store, path)
        if (entry.optBoolean("dir")) throw FileNotFoundException("$documentId is a folder")
        val size = entry.optLong("size")

        // ReadAt blocks while a holder answers, so each open file gets its own
        // thread: a stalled fetch for one video cannot hold another's reads,
        // and none of it runs on a binder thread.
        val storage = context!!.getSystemService(StorageManager::class.java)
            ?: throw FileNotFoundException("$documentId: no storage service")
        val thread = HandlerThread("storeboard-read").apply { start() }
        return try {
            storage.openProxyFileDescriptor(
                ParcelFileDescriptor.MODE_READ_ONLY,
                RemoteFile(store, path, size, thread),
                Handler(thread.looper),
            )
        } catch (e: IOException) {
            thread.quitSafely()
            throw FileNotFoundException("$documentId: ${e.message}")
        }
    }

    /**
     * A file read through the engine. The kernel asks in pages of at most
     * 128 KiB; each of those as its own round trip to a holder made playback
     * crawl, so one engine read fetches a whole window ahead and the
     * following sequential reads come from it.
     */
    private class RemoteFile(
        private val store: String,
        private val path: String,
        private val size: Long,
        private val thread: HandlerThread,
    ) : ProxyFileDescriptorCallback() {
        private var windowAt = 0L
        private var window = ByteArray(0)

        override fun onGetSize(): Long = size

        override fun onRead(offset: Long, size: Int, data: ByteArray): Int {
            if (offset >= this.size || size <= 0) return 0
            val want = minOf(size.toLong(), this.size - offset).toInt()
            if (offset < windowAt || offset + want > windowAt + window.size) {
                window = try {
                    Engine.readAt(store, path, offset, maxOf(want.toLong(), READ_AHEAD))
                } catch (e: Exception) {
                    throw ErrnoException("onRead", OsConstants.EIO)
                }
                windowAt = offset
            }
            val from = (offset - windowAt).toInt()
            val n = minOf(want, window.size - from)
            if (n <= 0) return 0
            System.arraycopy(window, from, data, 0, n)
            return n
        }

        override fun onRelease() {
            window = ByteArray(0)
            thread.quitSafely()
        }
    }

    private fun stat(store: String, path: String): JSONObject {
        val raw = try {
            Engine.stat(store, path)
        } catch (e: Exception) {
            throw FileNotFoundException("$store/$path: ${e.message}")
        }
        return runCatching { JSONObject(raw) }.getOrElse {
            throw FileNotFoundException("$store/$path: unreadable entry")
        }
    }

    private fun addRow(cursor: MatrixCursor, documentId: String, entry: JSONObject, name: String?) {
        val dir = entry.optBoolean("dir")
        var flags = 0
        // Only a replica's own bytes are writable in place; a streamed file
        // or one still on its way down has nothing here to write to.
        if (!dir && entry.optBoolean("local")) flags = flags or Document.FLAG_SUPPORTS_WRITE
        cursor.newRow()
            .add(Document.COLUMN_DOCUMENT_ID, documentId)
            .add(Document.COLUMN_DISPLAY_NAME, name ?: entry.optString("name"))
            .add(Document.COLUMN_MIME_TYPE, mimeType(entry))
            .add(Document.COLUMN_SIZE, if (dir) null else entry.optLong("size"))
            .add(Document.COLUMN_LAST_MODIFIED, mtime(entry))
            .add(Document.COLUMN_FLAGS, flags)
    }

    private fun MatrixCursor.withError(message: String): Cursor {
        extras = Bundle().apply { putString(DocumentsContract.EXTRA_ERROR, message) }
        return this
    }

    companion object {
        const val AUTHORITY = "dev.kyle.storeboard.documents"

        /** The engine's own read limit: one window is one holder request. */
        private const val READ_AHEAD = 1L shl 20

        private val DEFAULT_ROOT_PROJECTION = arrayOf(
            Root.COLUMN_ROOT_ID,
            Root.COLUMN_DOCUMENT_ID,
            Root.COLUMN_TITLE,
            Root.COLUMN_SUMMARY,
            Root.COLUMN_FLAGS,
            Root.COLUMN_ICON,
            Root.COLUMN_MIME_TYPES,
        )

        private val DEFAULT_DOCUMENT_PROJECTION = arrayOf(
            Document.COLUMN_DOCUMENT_ID,
            Document.COLUMN_DISPLAY_NAME,
            Document.COLUMN_MIME_TYPE,
            Document.COLUMN_SIZE,
            Document.COLUMN_LAST_MODIFIED,
            Document.COLUMN_FLAGS,
        )

        /**
         * Tells the Files app the stores changed: the engine came up or went
         * down. Notifying the authority itself reaches the roots and every
         * folder an app has open, since their cursors watch URIs below it.
         */
        fun notifyChanged(context: Context) {
            val base = Uri.Builder().scheme("content").authority(AUTHORITY).build()
            runCatching { context.contentResolver.notifyChange(base, null) }
        }

        private fun split(documentId: String): Pair<String, String> {
            val slash = documentId.indexOf('/')
            return if (slash < 0) documentId to "" else documentId.substring(0, slash) to documentId.substring(slash + 1)
        }

        private fun mimeType(entry: JSONObject): String {
            if (entry.optBoolean("dir")) return Document.MIME_TYPE_DIR
            val name = entry.optString("name")
            val ext = name.substringAfterLast('.', "").lowercase()
            if (ext.isEmpty()) return "application/octet-stream"
            return MimeTypeMap.getSingleton().getMimeTypeFromExtension(ext) ?: "application/octet-stream"
        }

        /** RFC 3339 from the engine; Go's zero time means "unknown". */
        private fun mtime(entry: JSONObject): Long? {
            val raw = entry.optString("mtime").takeIf { it.isNotEmpty() } ?: return null
            val ms = runCatching { Instant.parse(raw).toEpochMilli() }.getOrNull() ?: return null
            return ms.takeIf { it > 0 }
        }
    }
}
