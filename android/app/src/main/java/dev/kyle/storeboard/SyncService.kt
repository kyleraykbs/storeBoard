package dev.kyle.storeboard

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.database.ContentObserver
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.provider.MediaStore
import java.util.concurrent.Executors
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import androidx.core.content.ContextCompat

/**
 * Keeps the engine alive off-screen. On Android a socket in a background
 * process is killed with the process, and a peer that disappears is not a
 * peer, so the engine runs under a data-sync foreground service — the same
 * arrangement as the network drives' clients, and the one the platform
 * expects for exactly this.
 *
 * The service is the engine's only owner: the UI reads and calls the same
 * in-process object, and nothing else starts or stops it.
 */
class SyncService : Service() {
    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        ServiceCompat.startForeground(
            this,
            NOTIFICATION_ID,
            notification(),
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
            } else {
                0
            },
        )

        val config = Engine.configFile(this)
        if (!config.exists()) {
            stopSelf()
            return
        }
        // Opening journals and binding sockets is slow work; onCreate runs on
        // the main thread, where it froze the UI on every start. The engine
        // thread does it, in order with any stop queued after it.
        Engine.startAsync(this, config.readText()) { ok ->
            // The UI shows Engine.lastError; a service that cannot start must
            // not sit there pretending the mesh is up.
            if (!ok) stopSelf()
        }
        // New photos wake camera backup at once, not at the next tick.
        observer = object : ContentObserver(handler) {
            override fun onChange(selfChange: Boolean) {
                handler.removeCallbacks(backupSoon)
                handler.postDelayed(backupSoon, 5_000)
            }
        }.also {
            contentResolver.registerContentObserver(MediaStore.Images.Media.EXTERNAL_CONTENT_URI, true, it)
            contentResolver.registerContentObserver(MediaStore.Video.Media.EXTERNAL_CONTENT_URI, true, it)
        }
        handler.postDelayed(tick, TICK_MS)
    }

    private val handler = Handler(Looper.getMainLooper())
    private var observer: ContentObserver? = null
    private var ticks = 0
    private val background = Executors.newSingleThreadExecutor()

    private val backupSoon = Runnable { background.execute { CameraBackup.run(applicationContext) } }

    /**
     * Every few seconds: the engine asks to start again when the stores to
     * carry changed (a subscription, or a peer's offer under the policy);
     * every few minutes, camera backup looks for anything it missed.
     */
    private val tick = object : Runnable {
        override fun run() {
            if (Engine.restartWanted()) Engine.restartAsync(applicationContext)
            // Shortly after starting, then every few minutes.
            if (++ticks == 3 || ticks % (BACKUP_EVERY_MS / TICK_MS).toInt() == 0) backupSoon.run()
            handler.postDelayed(this, TICK_MS)
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int = START_STICKY

    override fun onDestroy() {
        handler.removeCallbacksAndMessages(null)
        observer?.let { contentResolver.unregisterContentObserver(it) }
        background.shutdown()
        Engine.stopAsync()
        super.onDestroy()
    }

    private fun notification(): Notification {
        val manager = getSystemService(NotificationManager::class.java)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O &&
            manager.getNotificationChannel(CHANNEL_ID) == null
        ) {
            manager.createNotificationChannel(
                NotificationChannel(
                    CHANNEL_ID,
                    getString(R.string.channel_name),
                    NotificationManager.IMPORTANCE_LOW,
                ),
            )
        }
        val open = PendingIntent.getActivity(
            this,
            0,
            Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE,
        )
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle(getString(R.string.notification_title))
            .setSmallIcon(android.R.drawable.stat_sys_upload_done)
            .setContentIntent(open)
            .setOngoing(true)
            .build()
    }

    companion object {
        private const val CHANNEL_ID = "storeboard.sync"
        private const val NOTIFICATION_ID = 1
        private const val TICK_MS = 5_000L
        private const val BACKUP_EVERY_MS = 300_000L

        fun start(context: Context) {
            ContextCompat.startForegroundService(context, Intent(context, SyncService::class.java))
        }

        fun stop(context: Context) {
            context.stopService(Intent(context, SyncService::class.java))
        }
    }
}
