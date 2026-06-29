# Google Drive → R2 ingestion via rclone (recommended) + Drive-native alternative

**Status:** Design / implementation plan (no app code changes yet)
**Goal:** Files dropped into a Google Workspace **Shared Drive** are transcribed by Speakr, with the raw video/audio retained in object storage and **S3 presigned URLs** kept for UI playback.

---

## TL;DR — recommended architecture

```
Google Shared Drive  ──(rclone, outside Speakr)──►  Cloudflare R2 bucket
                                                        │
                                                        ├─ inbox/    ← Speakr watches this prefix
                                                        └─ recordings/ ← Speakr stores final media here
                                                        │
Speakr (FILE_STORAGE_BACKEND=s3 → R2 endpoint)  ◄───────┘
   • existing S3 backend = storage (presigned URLs work on R2)
   • NEW: small S3/R2 ingestion watcher → triggers transcription
```

**Why this beats a direct Google Drive integration:**

| | Direct Drive backend (previous design) | **rclone → R2 (this design)** |
| --- | --- | --- |
| New storage backend | `GDriveStorageBackend` (~250 lines) | **None** — R2 is S3-compatible, already supported |
| Presigned URLs / UI playback | Lost → had to proxy/stream bytes through the app | **Kept** — R2 supports S3 presigned URLs |
| Google API client + auth | Required (`google-api-python-client`, service account, `supportsAllDrives`) | **None in Speakr** — lives in rclone config |
| Locator scheme | New `gdrive://<file_id>` | Existing `s3://bucket/key` |
| Net-new Speakr code | Backend + streaming proxy + Drive monitor | **One S3/R2 ingestion watcher** |

> **rclone, not rsync.** rsync only speaks local filesystem / SSH — it cannot talk to the Google Drive or S3/R2 APIs. rclone has native backends for *both* and copies directly between them. This is the right (and only) tool for the Drive→R2 hop.

R2 is already usable as Speakr's storage today: `FILE_STORAGE_BACKEND=s3` with `S3_ENDPOINT_URL=https://<accountid>.r2.cloudflarestorage.com` (and typically `S3_USE_PATH_STYLE=true`). The S3 backend's `generate_presigned_url` (s3v4) works against R2, so `get_audio_delivery` keeps returning `redirect_url` and the UI is unchanged.

---

## Part A — rclone: Drive → R2 (outside Speakr)

Runs as a scheduled job (cron / systemd timer / a small sidecar container) — **not** part of Speakr.

### 1. Configure two rclone remotes
```
rclone config       # create:
#   drive:  → type=drive,  scope=drive.readonly (service account JSON or OAuth)
#   r2:     → type=s3, provider=Cloudflare, endpoint=https://<accountid>.r2.cloudflarestorage.com,
#             access_key_id=…, secret_access_key=…
```
For headless servers use a **service account** on the `drive:` remote (`service_account_file=…`) so no interactive OAuth refresh is needed — and a **Shared Drive** so the service account isn't blocked by My-Drive quota (`team_drive=<shared drive id>`).

### 2. Move new files Drive → R2 inbox
```
rclone move drive:Inbox r2:speakr/inbox \
  --transfers 4 --checkers 8 \
  --drive-skip-gdocs --min-age 1m \
  --log-level INFO
```
- **`move`** (not `sync`) drains the Drive inbox as it copies, giving natural "already processed" semantics on the Drive side and never deleting anything on R2.
- **`--min-age 1m`** skips files still uploading to Drive (the rclone analog of Speakr's local stability check).
- Schedule every 1–5 min via cron: `*/2 * * * * flock -n /tmp/rclone-speakr.lock rclone move …` (the `flock` prevents overlapping runs).

> Alternative `rclone mount r2:speakr/inbox /data/auto-process` (FUSE) + the **existing** local `file_monitor` = zero Speakr code. Caveats: needs `--cap-add SYS_ADMIN`/`/dev/fuse` in containers, and the monitor's `os.rename` lock becomes a server-side copy+delete on object storage (works, not atomic). Fine for a single low-volume instance; the native watcher below is more robust.

---

## Part B — the one Speakr change: an S3/R2 ingestion watcher

Today `src/file_monitor.py` watches a local directory. The transcription pipeline itself is cleanly factored into **`FileMonitor._process_file(local_path, user_id, tag_id=None)`** (`file_monitor.py:312`) which does: staging copy → hash dedupe → `ffprobe` → `convert_if_needed()` → create `Recording(processing_source='auto_process', is_inbox=True)` → `storage.build_recording_key()` + upload → `job_queue.enqueue(job_type='transcribe')`.

A new watcher only has to **list new R2 objects, download each, and hand its local path to that same `_process_file`.** Everything downstream is reused unchanged.

### New module `src/s3_monitor.py` — `S3FileMonitor`
Background daemon thread, same lifecycle (`start()`/`stop()`/loop on `check_interval`) as `FileMonitor`. Reuses the already-configured boto3 client from the S3 storage backend (`get_storage_service().s3._get_client()`), so no new dependency. Per poll:

1. **List** new objects under the inbox prefix:
   `client.list_objects_v2(Bucket=…, Prefix='inbox/')` (paginate). Skip "directory" keys.
2. **Skip already-seen** keys: a key is "new" if not in the processed set. Track processed keys in a small DB table (or reuse the existing `file_hash` duplicate check, which already guards re-ingestion of identical content).
3. **Claim / lock** (object storage has no atomic rename): server-side **copy** the object to a `processing/<key>` prefix then delete the inbox original (`copy_object` + `delete_object`). The copy succeeding is the claim; if it 404s, another worker already took it. (For a single Speakr instance, an in-process lock + DB seen-set is enough; the copy-to-`processing/` approach makes it safe for multiple workers.)
4. **Download** to the staging dir (`storage.get_staging_dir()`), preserving the original filename and extension.
5. **Run the existing pipeline** for hash/probe/convert and the DB record: call `monitor._process_file(local_staging_path, user_id, tag_id)`, but with the storage step doing **adopt-in-place** rather than a re-upload (see below). It creates the `Recording(processing_source='auto_process', is_inbox=True)` and enqueues transcription; `audio_path` ends up an `s3://…` locator → presigned playback works.
6. **Cleanup:** delete the `processing/<key>` object once the recording's media lives under `recordings/`. On failure, leave it in `processing/` (or move to `failed/`) for inspection rather than losing it.

**Adopt in place — no re-upload (the chosen behaviour).** The bytes are *already* in R2, so Speakr must not upload them a second time. After analysis:
- **No conversion needed** (`convert_if_needed()` is a no-op): server-side **`copy_object`** from `processing/<key>` to the computed `recordings/<key>` and set `audio_path = s3://<bucket>/recordings/<key>` directly. Bytes never leave R2.
- **Conversion did happen** (codec/size constraints from the active connector produced a new file): upload only the converted output to `recordings/`, then delete the original.

This means `_process_file` needs the storage step parameterised so the S3 watcher can pass an "already-in-R2 source key" and get a server-side copy instead of `storage.upload_local_file()`. The local `file_monitor` keeps its current upload path; only the S3 watcher takes the copy branch. The download in step 4 is still required for `ffprobe`/hashing/conversion analysis, but it is a transient temp file, not a re-upload.

### Modes & tags
Reuse the existing `AUTO_PROCESS_MODE` semantics. In R2, "user directories" = key sub-prefixes like `inbox/user<id>/…`; auto-process **tag** folders = `inbox/<tag-folder>/…`, mapped exactly as the local monitor maps sub-directories today.

### Startup wiring
Mirror `start_file_monitor()` (`file_monitor.py:553`) and `initialize_file_monitor()` (`config/startup.py:15`). Add `initialize_s3_monitor(app)` to `run_startup_tasks()`, gated on its own switch so it's independent of the local monitor:
```
ENABLE_S3_INGEST=true
S3_INGEST_PREFIX=inbox/
S3_INGEST_CHECK_INTERVAL=60        # R2 list calls are cheap; 30–120s is reasonable
S3_INGEST_MODE=admin_only          # admin_only | user_directories | single_user
S3_INGEST_DEFAULT_USERNAME=…       # for single_user
```

> **Even cheaper than polling (later):** R2 supports **event notifications** to a Cloudflare Queue. A consumer could hit a Speakr endpoint when an object lands, eliminating the poll loop. Keep the watcher's "list new keys" behind one function so this is a drop-in swap.

---

## Part C — configuration summary

**Storage (already supported — point Speakr at R2):**
```
FILE_STORAGE_BACKEND=s3
S3_BUCKET_NAME=speakr
S3_ENDPOINT_URL=https://<accountid>.r2.cloudflarestorage.com
S3_ACCESS_KEY_ID=…
S3_SECRET_ACCESS_KEY=…
S3_USE_PATH_STYLE=true
FILE_STORAGE_KEY_PREFIX=recordings        # final media under recordings/
S3_PRESIGN_TTL_SECONDS=900                 # UI playback signed-URL TTL
```

**Ingestion (new):** the `ENABLE_S3_INGEST` block above.

**rclone (outside Speakr):** the `drive:` + `r2:` remotes and the scheduled `rclone move`.

---

## Files touched (this design)

**New:**
- `src/s3_monitor.py` — `S3FileMonitor` + `start_s3_monitor()`
- `config/env.r2-ingest.example` — sample storage + ingest env
- `docs/admin-guide/google-drive-r2-ingestion.md` — operator how-to (rclone setup + Speakr config)

**Edited:**
- `src/config/app_config.py` — parse `S3_INGEST_*` / `ENABLE_S3_INGEST`
- `src/config/startup.py` — `initialize_s3_monitor`
- `src/file_monitor.py` — parameterise the storage step in `_process_file` so the S3 watcher can supply an "already-in-R2 source key" and get a server-side `copy_object` (adopt-in-place) instead of `storage.upload_local_file()`. The local monitor keeps its existing upload path.
- *(possibly)* `src/services/storage/service.py` — a thin `copy_within_backend(src_key, dest_key)` helper wrapping S3 `copy_object`, so the watcher doesn't reach into the boto3 client directly.

**No changes needed** to the storage backend, locators, playback/delivery, or the UI.

---

## Testing
- **Unit (mock boto3):** `list_objects_v2` returns a new key → assert download → `_process_file` invoked → `Recording(processing_source='auto_process')` created and `transcribe` job enqueued; assert `audio_path` is an `s3://` locator. Test the copy-to-`processing/` claim and the seen-set dedupe.
- **Presigned playback:** confirm `get_audio_delivery` returns `mode='redirect_url'` against the R2 endpoint and the URL is fetchable.
- **rclone (integration):** drop a file in the Drive inbox → it appears in `r2:speakr/inbox` → Speakr ingests → transcription appears → plays back via signed URL.

## Risks / notes
- **Two hops of latency:** Drive→R2 (rclone schedule) + R2 poll interval. Tune both; or use rclone more frequently + R2 event notifications for near-real-time.
- **Duplicate suppression:** rely on the existing `file_hash` check plus the processed-key set so a re-listed object isn't ingested twice.
- **rclone runs outside Speakr:** it needs its own deployment (cron/systemd/sidecar) and the Drive service-account credentials live there, not in Speakr — a cleaner security boundary.
- **R2 specifics:** use path-style addressing; R2 ignores AWS regions (any region string is fine for signing). Presigned URLs and `copy_object` (server-side copy) are both supported.

---

## Appendix — Drive-native backend (previous design, not recommended)

The earlier revision of this document specified a direct `GDriveStorageBackend` + `gdrive://` locator + a byte-streaming playback proxy + a `GDriveFileMonitor`, because Google Drive has no S3-style presigned URLs and addresses files by opaque ID. That approach works but pushes all playback bandwidth through the app and adds a Google API client and ~3 new components. The rclone→R2 design above supersedes it: it keeps presigned-URL playback, reuses the existing S3 backend wholesale, and reduces the Speakr change to a single ingestion watcher. The Drive-native notes are retained in git history (previous commit on this branch) if a no-R2, Drive-only deployment is ever required.
