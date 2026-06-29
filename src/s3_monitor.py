#!/usr/bin/env python3
"""
S3/R2 Ingestion Watcher for Automated Audio Processing.

Companion to ``src/file_monitor.py``. Where ``FileMonitor`` watches a local
directory, ``S3FileMonitor`` watches an object-storage prefix (the ``inbox/``
prefix of the same S3/R2 bucket Speakr already uses for storage) and feeds new
objects into the *same* transcription pipeline via ``FileMonitor._process_file``.

Intended deployment: an external job (rclone) moves files from a Google
Workspace Shared Drive into ``s3://<bucket>/inbox/``. This watcher then:

  1. Lists new objects under the inbox prefix.
  2. Claims each via a server-side copy to a ``processing/`` prefix then deletes
     the inbox original (object stores have no atomic rename; the copy
     succeeding is the claim, so two workers can't both take the same key).
  3. Downloads the claimed object to the staging dir for ffprobe/hash/convert.
  4. Hands the local path to ``FileMonitor._process_file`` with the claimed key
     as ``source_s3_key`` — so when no conversion is needed the bytes are
     *adopted in place* (server-side copy to ``recordings/``), never re-uploaded.
  5. Deletes the ``processing/`` object on success; on failure moves it to a
     ``failed/`` prefix for inspection rather than losing it.

Modes mirror the local monitor (``admin_only`` | ``user_directories`` |
``single_user``); "user directories" and auto-process tag folders map to inbox
key sub-prefixes (``inbox/user<id>/...``, ``inbox/<tag-folder>/...``).
"""

import os
import time
import threading
import logging
import posixpath
from pathlib import Path


class S3FileMonitor:
    def __init__(self, *, inbox_prefix='inbox/', processing_prefix='processing/',
                 failed_prefix='failed/', check_interval=60, mode='admin_only',
                 default_username=None):
        self.inbox_prefix = self._norm_prefix(inbox_prefix)
        self.processing_prefix = self._norm_prefix(processing_prefix)
        self.failed_prefix = self._norm_prefix(failed_prefix)
        self.check_interval = check_interval
        self.mode = mode
        self.default_username = default_username
        self.running = False
        self.thread = None

        self.logger = logging.getLogger('s3_monitor')
        self.logger.setLevel(logging.INFO)

        # Keys we've already claimed this process lifetime (claim is also
        # enforced atomically by the copy-to-processing/ step; this set just
        # avoids re-listing churn within a process).
        self._seen_keys = set()

        # Reuse the local monitor purely for its user-cache + _process_file.
        # It never starts its own thread; we only borrow its methods.
        from src.file_monitor import FileMonitor
        # base_watch_directory is required but unused here; point it at the
        # staging dir so the mkdir is harmless.
        from src.services.storage import get_storage_service
        staging = get_storage_service().get_staging_dir()
        self._fm = FileMonitor(base_watch_directory=staging,
                               check_interval=check_interval, mode=mode)

    @staticmethod
    def _norm_prefix(p):
        p = (p or '').strip().lstrip('/')
        if p and not p.endswith('/'):
            p += '/'
        return p

    def start(self):
        if self.running:
            self.logger.warning("S3 file monitor is already running")
            return
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True,
                                       name="S3FileMonitor")
        self.thread.start()
        self.logger.info(
            f"S3 ingestion watcher started in '{self.mode}' mode, "
            f"watching prefix '{self.inbox_prefix}'")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        self.logger.info("S3 ingestion watcher stopped")

    # --- storage helpers -------------------------------------------------

    def _storage(self):
        from src.services.storage import get_storage_service
        return get_storage_service()

    def _client_and_bucket(self):
        storage = self._storage()
        if storage.settings.backend != 's3' or not storage.s3:
            raise RuntimeError(
                "S3 ingestion requires FILE_STORAGE_BACKEND=s3 with an S3 backend")
        return storage.s3._get_client(), storage.s3.bucket

    # --- main loop -------------------------------------------------------

    def _monitor_loop(self):
        while self.running:
            try:
                self._fm._update_user_cache()
                self._scan_once()
            except Exception as e:
                self.logger.error(f"Error during S3 ingestion scan: {e}", exc_info=True)
            time.sleep(self.check_interval)

    def _scan_once(self):
        client, bucket = self._client_and_bucket()
        paginator = client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=self.inbox_prefix):
            for obj in page.get('Contents', []) or []:
                key = obj['Key']
                # Skip "directory" placeholder keys and anything already seen.
                if key.endswith('/') or obj.get('Size', 0) == 0:
                    continue
                if key in self._seen_keys:
                    continue
                self._seen_keys.add(key)
                try:
                    self._ingest_key(client, bucket, key)
                except Exception as e:
                    self.logger.error(f"Failed to ingest {key}: {e}", exc_info=True)

    # --- per-object ingestion -------------------------------------------

    def _resolve_user_and_tag(self, rel_key):
        """Map an inbox-relative key to (user_id, tag_id) per the active mode.

        rel_key is the object key with the inbox prefix stripped, e.g.
        'user12/foo.mp3' or 'MyTagFolder/foo.mp3' or 'foo.mp3'.
        """
        fm = self._fm
        parts = rel_key.split('/')
        first = parts[0] if len(parts) > 1 else None

        if self.mode == 'single_user':
            username = self.default_username or os.environ.get('S3_INGEST_DEFAULT_USERNAME')
            user_id = fm._username_to_id.get(username) if username else None
            if not user_id:
                return None, None
            return user_id, self._tag_for(user_id, first)

        if self.mode == 'user_directories':
            user_id = fm._extract_user_id_from_dirname(first) if first else None
            if user_id and user_id in fm._valid_users:
                # tag folder is the *next* segment, if any
                tag_seg = parts[1] if len(parts) > 2 else None
                return user_id, self._tag_for(user_id, tag_seg)
            return None, None

        # admin_only (default)
        user_id = fm._admin_user_id
        if not user_id:
            return None, None
        return user_id, self._tag_for(user_id, first)

    def _tag_for(self, user_id, folder_name):
        if not user_id or not folder_name:
            return None
        from src.app import app
        from src.models import Tag
        with app.app_context():
            tag = Tag.query.filter_by(
                user_id=user_id,
                is_auto_process=True,
                auto_process_folder_name=folder_name,
            ).first()
            return tag.id if tag else None

    def _ingest_key(self, client, bucket, key):
        from botocore.exceptions import ClientError

        rel_key = key[len(self.inbox_prefix):] if key.startswith(self.inbox_prefix) else key
        filename = posixpath.basename(rel_key)
        if not filename:
            return

        # Resolve target user/tag before claiming so we don't orphan a claim
        # for an object that maps to no valid user.
        user_id, tag_id = self._resolve_user_and_tag(rel_key)
        if not user_id:
            self.logger.warning(
                f"No target user for inbox object '{key}' in mode '{self.mode}'; skipping")
            self._seen_keys.discard(key)  # allow retry once config/users change
            return

        # --- Claim: server-side copy inbox -> processing/, then delete inbox ---
        processing_key = self.processing_prefix + rel_key
        try:
            client.copy_object(Bucket=bucket, Key=processing_key,
                               CopySource={'Bucket': bucket, 'Key': key})
        except ClientError as e:
            self.logger.error(f"Could not claim {key} (copy to processing failed): {e}")
            return
        try:
            client.delete_object(Bucket=bucket, Key=key)
        except ClientError as e:
            # Another worker may have deleted/claimed it; if our processing copy
            # exists we still own it, so continue.
            self.logger.warning(f"Inbox delete for {key} failed (continuing): {e}")

        self.logger.info(f"Claimed {key} -> {processing_key}")

        # --- Download claimed object to staging for probe/hash/convert ---
        storage = self._storage()
        staging_dir = Path(storage.get_staging_dir())
        staging_dir.mkdir(parents=True, exist_ok=True)
        local_path = staging_dir / f"s3ingest_{int(time.time())}_{filename}"
        try:
            client.download_file(bucket, processing_key, str(local_path))
        except ClientError as e:
            self.logger.error(f"Download of {processing_key} failed: {e}")
            self._move_to_failed(client, bucket, processing_key, rel_key)
            return

        # --- Content dedup: skip if this exact file is already a recording ---
        # Belt-and-braces alongside the single-instance lock: if the upstream
        # rclone job re-copies the same file into inbox/ (e.g. a read-only Drive
        # service account can't `move`-delete the source, so it re-copies every
        # run), we must NOT create a duplicate recording. The local monitor only
        # *warns* on a hash match (by design); for object-store ingestion we hard
        # skip — drop the processing/ object and move on.
        if self._is_duplicate(local_path, user_id):
            self.logger.info(f"Skipping {key}: identical content already ingested (file_hash match)")
            try:
                client.delete_object(Bucket=bucket, Key=processing_key)
            except ClientError:
                pass
            self._safe_unlink(local_path)
            return

        # --- Run the existing pipeline, adopting in place when possible ---
        try:
            self._fm._process_file(
                local_path,
                user_id,
                tag_id=tag_id,
                source_s3_key=processing_key,
                original_filename_override=filename,
            )
        except Exception as e:
            self.logger.error(f"Pipeline failed for {processing_key}: {e}", exc_info=True)
            # Leave a copy for inspection; the local staging file is cleaned up
            # by _process_file's own error path or below.
            self._move_to_failed(client, bucket, processing_key, rel_key)
            self._safe_unlink(local_path)
            return

        # --- Success cleanup: drop the processing/ object ---
        # (_process_file already adopted-in-place to recordings/ or uploaded the
        # converted output; the processing/ copy is now redundant.)
        try:
            client.delete_object(Bucket=bucket, Key=processing_key)
        except ClientError as e:
            self.logger.warning(f"Cleanup delete of {processing_key} failed: {e}")
        self._safe_unlink(local_path)
        self.logger.info(f"Ingested {key} successfully")

    def _is_duplicate(self, local_path, user_id):
        """True if a Recording with this file's content hash already exists.

        Uses the same SHA-256 the pipeline computes (on the original bytes,
        pre-conversion), so a re-copied identical file is recognised.
        """
        try:
            from src.utils.file_hash import compute_file_sha256
            file_hash = compute_file_sha256(str(local_path))
        except Exception as e:
            self.logger.warning(f"Could not hash {local_path} for dedup: {e}")
            return False
        if not file_hash:
            return False
        from src.app import app, Recording
        with app.app_context():
            return Recording.query.filter_by(user_id=user_id, file_hash=file_hash).first() is not None

    def _move_to_failed(self, client, bucket, processing_key, rel_key):
        from botocore.exceptions import ClientError
        failed_key = self.failed_prefix + rel_key
        try:
            client.copy_object(Bucket=bucket, Key=failed_key,
                               CopySource={'Bucket': bucket, 'Key': processing_key})
            client.delete_object(Bucket=bucket, Key=processing_key)
            self.logger.info(f"Moved failed object to {failed_key}")
        except ClientError as e:
            self.logger.error(
                f"Could not move {processing_key} to failed/ (left in place): {e}")

    @staticmethod
    def _safe_unlink(path):
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass


# Global instance + lifecycle (mirrors file_monitor.py) ---------------------

s3_monitor = None

# Single-instance guard. Under gunicorn there are N worker processes, each of
# which runs run_startup_tasks() and would otherwise start its own watcher —
# N watchers racing on the same inbox/ objects produce duplicate ingests (the
# copy-to-processing claim is NOT atomic across processes, unlike the local
# monitor's os.rename). We hold an exclusive flock for the lifetime of the
# winning process; the other workers fail the non-blocking lock and skip.
_s3_ingest_lock_fh = None


def _acquire_single_instance_lock(app):
    """Return True iff this process won the cross-worker ingestion lock."""
    global _s3_ingest_lock_fh
    if _s3_ingest_lock_fh is not None:
        return True  # already held by this process
    import fcntl
    lock_path = os.environ.get('S3_INGEST_LOCK_PATH', '/tmp/speakr-s3-ingest.lock')
    try:
        fh = open(lock_path, 'w')
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, BlockingIOError):
        # Another worker holds it — this worker must not start a second watcher.
        app.logger.info("S3 ingestion watcher: another worker holds the lock; not starting here")
        return False
    fh.write(str(os.getpid()))
    fh.flush()
    _s3_ingest_lock_fh = fh  # keep the fd open for the process lifetime to hold the lock
    return True


def start_s3_monitor():
    """Start the S3 ingestion watcher from environment configuration."""
    global s3_monitor

    if s3_monitor and s3_monitor.running:
        return

    from src.app import app

    if os.environ.get('ENABLE_S3_INGEST', 'false').lower() != 'true':
        app.logger.info("S3 ingestion watcher is disabled (ENABLE_S3_INGEST=false)")
        return

    # Ingestion only makes sense when the storage backend is S3/R2.
    from src.services.storage import get_storage_service
    storage = get_storage_service()
    if storage.settings.backend != 's3' or not storage.s3:
        app.logger.warning(
            "ENABLE_S3_INGEST=true but FILE_STORAGE_BACKEND is not 's3'; "
            "S3 ingestion watcher not started")
        return

    mode = os.environ.get('S3_INGEST_MODE', 'admin_only')
    valid_modes = ['admin_only', 'user_directories', 'single_user']
    if mode not in valid_modes:
        app.logger.error(f"Invalid S3_INGEST_MODE: {mode}. Must be one of: {valid_modes}")
        return

    # Only ONE worker process may run the watcher (see _s3_ingest_lock_fh).
    if not _acquire_single_instance_lock(app):
        return

    check_interval = int(os.environ.get('S3_INGEST_CHECK_INTERVAL', '60'))
    inbox_prefix = os.environ.get('S3_INGEST_PREFIX', 'inbox/')
    processing_prefix = os.environ.get('S3_INGEST_PROCESSING_PREFIX', 'processing/')
    failed_prefix = os.environ.get('S3_INGEST_FAILED_PREFIX', 'failed/')
    default_username = os.environ.get('S3_INGEST_DEFAULT_USERNAME')

    s3_monitor = S3FileMonitor(
        inbox_prefix=inbox_prefix,
        processing_prefix=processing_prefix,
        failed_prefix=failed_prefix,
        check_interval=check_interval,
        mode=mode,
        default_username=default_username,
    )
    s3_monitor.start()
    app.logger.info(f"S3 ingestion watcher started in '{mode}' mode")


def stop_s3_monitor():
    global s3_monitor
    if s3_monitor:
        s3_monitor.stop()
        s3_monitor = None


def get_s3_monitor_status():
    global s3_monitor
    if s3_monitor and s3_monitor.running:
        return {
            'running': True,
            'mode': s3_monitor.mode,
            'inbox_prefix': s3_monitor.inbox_prefix,
            'check_interval': s3_monitor.check_interval,
        }
    return {'running': False}
