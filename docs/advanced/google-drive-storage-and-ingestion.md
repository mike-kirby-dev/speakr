# Google Drive (Workspace Shared Drive) — Storage Backend + Ingestion Design

**Status:** Proposed design / implementation plan (no code changes yet)
**Scope:** Add Google Drive as (1) a first-class storage backend alongside `local` and `s3`, and (2) an ingestion source that auto-triggers transcriptions, with the raw video/audio files living in a Workspace **Shared Drive**.
**Decisions baked in:**
- **Auth:** Service account + **Shared Drive** (the Shared Drive — not the service account — owns the storage quota, so a headless service account can write freely).
- **Playback:** **Proxy/stream through the app** (Drive has no S3-style presigned URLs).
- **Topology:** Drive is both the storage backend *and* the ingestion inbox, so an ingested file is **moved** into the recordings area rather than re-uploaded — one copy that never leaves Drive.

---

## 1. Why this is feasible (and where it isn't a drop-in)

The storage layer (`src/services/storage/`) is already a clean, scheme-routed abstraction:

- `interfaces.py` — `StorageLocator`, `StoredObject`, `ObjectStat`, `MaterializedFile`, `AudioDeliveryResult`
- `local.py` / `s3.py` — backends implementing the same method surface
- `locator.py` — parse/serialize locator strings (`local://…`, `s3://bucket/key`)
- `factory.py` — build backends from `StorageSettings`
- `service.py` — `StorageService` facade; routes by `locator.scheme`

A recording's `audio_path` column stores a locator string, and **all** business logic (transcription, playback, delete, share) goes through `StorageService` — never a backend directly. Adding a `gdrive` backend is therefore "add one backend class + extend the locator parser + wire ~5 call sites in `service.py`/`factory.py`."

**Three genuine mismatches vs. S3** (each addressed below):

| Concern | S3 today | Google Drive |
| --- | --- | --- |
| Addressing | Computable hierarchical key path (`recordings/2024/06/123/…`) | Opaque **file IDs** + folder tree; cannot PUT to a path |
| Public delivery | Short-lived **presigned URL**; bytes stream straight from S3 | No equivalent → **proxy bytes through Flask** with range support |
| Ingestion lock | `os.rename` to `*.processing` (atomic on a filesystem) | Emulate via **move to a `processing/` folder** + dedupe on file ID |

---

## 2. Dependencies

Add to `requirements.txt`:

```
google-api-python-client
google-auth
```

(Both pure-Python; no native build. `google-auth` brings `google.oauth2.service_account`.) The import is lazy inside the backend (mirroring how `s3.py` imports `boto3` only in `_get_client()`), so installs that don't use Drive pay nothing and the app still boots without the libs present.

---

## 3. The `gdrive://` locator

Drive files are addressed by **file ID**, not path. Encode the file ID in the locator and (optionally) carry a human-readable name for logs/debugging only.

**Format:** `gdrive://<file_id>` (e.g. `gdrive://1A2b3C4d5E6f7G8h9I0j`)

Extend `src/services/storage/interfaces.py::StorageLocator`:
- add `file_id: Optional[str] = None`
- add `is_gdrive` property (`scheme == 'gdrive'`)

Extend `src/services/storage/locator.py`:
- `GDRIVE_SCHEME = 'gdrive://'`
- `build_gdrive_locator(file_id)` → `f"gdrive://{file_id}"`
- In `parse_locator`, before the absolute-path fallback:
  ```python
  if raw.startswith(GDRIVE_SCHEME):
      file_id = raw[len(GDRIVE_SCHEME):].strip().strip('/')
      if not file_id:
          raise ValueError(f"Invalid gdrive locator (missing file id): {raw}")
      return StorageLocator(scheme='gdrive', raw=raw, file_id=file_id)
  ```

> **Why file ID, not path:** Drive lets two files share a name in the same folder, paths aren't unique, and `files.get` is by ID. The existing `build_recording_key()` (year/month/recording-id naming) is still used — but as the **Drive filename + folder layout**, not as an addressable key.

---

## 4. `GDriveStorageBackend` (new: `src/services/storage/gdrive.py`)

Mirror the `S3StorageBackend` shape exactly so `StorageService` can treat it like any other backend. Lazy client init like `s3.py::_get_client()`.

```python
class GDriveStorageBackend:
    def __init__(self, *, shared_drive_id, root_folder_id, credentials_json_path=None,
                 credentials_json_inline=None, subject=None, key_prefix='recordings'):
        ...
        self._service = None  # googleapiclient discovery 'drive' v3

    def _get_service(self):
        # google.oauth2.service_account.Credentials.from_service_account_file/info
        # scopes=['https://www.googleapis.com/auth/drive']
        # optional .with_subject(subject) for domain-wide delegation
        # build('drive', 'v3', credentials=creds, cache_discovery=False)
        ...
```

### Method-by-method mapping to the storage interface

| Interface method | Drive implementation |
| --- | --- |
| `build_locator(key)` | Can't build a locator from a key pre-upload (no file ID yet). Used only as a fallback; real locator comes from `upload_local_file`'s returned `StoredObject`. See note below. |
| `upload_local_file(local_path, key, content_type, metadata, delete_source)` | Ensure folder path (`recordings/YYYY/MM/<rec_id>/`) exists via `_ensure_folder_path(key)` → returns parent folder ID. **Resumable** `files().create(media_body=MediaFileUpload(..., resumable=True), body={'name': basename, 'parents': [folder_id]}, supportsAllDrives=True, fields='id,size,mimeType,md5Checksum')`. Return `StoredObject(locator='gdrive://'+id, key=key, size=…, content_type=…, etag=md5Checksum)`. |
| `save_fileobj(fileobj, key, …)` | Same as above with `MediaIoBaseUpload`. |
| `exists(locator)` | `files().get(fileId=locator.file_id, fields='id, trashed', supportsAllDrives=True)` → True unless 404 or `trashed`. |
| `stat(locator)` | `files().get(fields='size,modifiedTime,md5Checksum,mimeType', supportsAllDrives=True)` → `ObjectStat`. |
| `delete(locator, missing_ok)` | `files().delete(fileId=…, supportsAllDrives=True)` (or move-to-trash). Swallow 404 when `missing_ok`. |
| `materialize(locator)` | `MediaIoBaseDownload` chunked download to a `tempfile.mkstemp(...)` → `MaterializedFile(local_path, cleanup_required=True)`. Identical contract to S3 so the transcription pipeline is unchanged. |
| **`open_stream(locator, range_header)`** *(new)* | Returns a file-like / generator of bytes for proxy playback (see §5). Supports HTTP `Range`. |

> **Folder management (`_ensure_folder_path`)**: split the key on `/`, walk/create each folder under `root_folder_id` using `files().list(q="name='…' and '<parent>' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false", supportsAllDrives=True, includeItemsFromAllDrives=True, corpora='drive', driveId=<shared_drive_id>)`; create missing with `mimeType='application/vnd.google-apps.folder'`. Cache folder IDs in-process to avoid repeated lookups (a simple `dict` keyed by relative folder path). All calls pass `supportsAllDrives=True`; all `list` calls also pass `includeItemsFromAllDrives=True`, `corpora='drive'`, `driveId=<shared_drive_id>`.

---

## 5. Playback — proxy/stream through the app

S3 returns `AudioDeliveryResult(mode='redirect_url', url=<presigned>)`; the API 302-redirects (`recordings.py:3666`, `api_v1.py:2531`, `shares.py:161`). Drive has no presigned URL, so add a **third delivery mode** rather than forcing a download-to-temp on every play.

### 5.1 Extend `AudioDeliveryResult`
In `interfaces.py`, document a new `mode` value `stream` and add fields:
```python
mode: str  # local_file | redirect_url | stream
stream_locator: Optional[str] = None   # gdrive:// locator to stream
size: Optional[int] = None
```

### 5.2 `StorageService.get_audio_delivery`
Add a branch before the S3 presign path:
```python
if resolved.kind == 'gdrive':
    st = resolved.backend.stat(resolved.locator)
    return AudioDeliveryResult(mode='stream', stream_locator=locator_value,
                               mimetype=mime_type, size=st.size)
```

### 5.3 API handlers (3 call sites)
At each consumer (`recordings.py`, `api_v1.py`, `shares.py`), add a `mode == 'stream'` branch that proxies bytes with **range support** (audio scrubbing needs `206 Partial Content`). Centralize in one helper, e.g. `src/api/_audio_proxy.py::stream_gdrive_audio(locator, mimetype, size, download_name)`:
- Parse `Range` header → start/end.
- Call `backend.open_stream(locator, start, end)`.
- Return a Flask `Response(generator, status=206|200, headers={Content-Range, Accept-Ranges: bytes, Content-Length, Content-Type, Content-Disposition?})`.

Drive supports byte ranges on media downloads (the `Range` header on `alt=media`, or `MediaIoBaseDownload` chunking), so seeking works without pulling the whole file.

**Trade-off to accept:** all playback/download bandwidth flows through the app process (unlike the S3 offload). For a self-hosted Workspace deployment this is usually fine; document it. A future optimization is a short-lived signed proxy URL or Drive `webContentLink`, deliberately out of scope here.

---

## 6. Factory + settings wiring

`src/config/app_config.py` — add env parsing (next to the `S3_*` block, lines ~90–104):
```
FILE_STORAGE_BACKEND = 'gdrive'                      # now accepts local | s3 | gdrive
GDRIVE_SHARED_DRIVE_ID
GDRIVE_ROOT_FOLDER_ID                                # folder (inside the Shared Drive) that holds recordings/
GDRIVE_CREDENTIALS_JSON                              # path to service-account JSON
GDRIVE_CREDENTIALS_JSON_INLINE                       # OR inline JSON (for secret-managers); one of the two
GDRIVE_DELEGATED_SUBJECT                             # optional: user email for domain-wide delegation
```

`src/services/storage/factory.py`:
- Add the `gdrive_*` fields to `StorageSettings`.
- `build_gdrive_backend(settings) -> Optional[GDriveStorageBackend]` (returns `None` if `shared_drive_id`/`root_folder_id` unset, mirroring `build_s3_backend`).

`src/services/storage/service.py`:
- `__init__`: `self.gdrive = build_gdrive_backend(self.settings)`.
- `_resolve_backend_for_locator`: route `scheme == 'gdrive'` → `self.gdrive` (raise if unconfigured, like the S3 branch).
- `upload_local_file` and `build_default_locator`: add a `backend == 'gdrive'` branch.
- `get_audio_delivery`: add the `stream` branch from §5.2.

No other business logic changes — `materialize()` (used by the transcription worker at `services/job_queue.py`) is backend-agnostic already.

---

## 7. Ingestion — `GDriveFileMonitor` (new)

`src/file_monitor.py` is **not** abstracted like storage — it's hardwired to a local `Path` and uses `os.rename` locking. So ingestion is **net-new code**, but everything downstream of "a file is sitting in the staging dir" is fully reused: hashing (`utils/file_hash`), `ffprobe`, `convert_if_needed()`, `Recording` creation, and `job_queue.enqueue(job_type='transcribe')`.

### 7.1 New class `GDriveFileMonitor` (own module, e.g. `src/gdrive_monitor.py`)
Background daemon thread, same lifecycle as `FileMonitor` (`start()`/`stop()`/loop on `check_interval`). Per poll:

1. **List new files** in the configured inbox folder:
   `files().list(q="'<inbox_folder_id>' in parents and trashed=false", fields='files(id,name,size,mimeType,modifiedTime,md5Checksum)', supportsAllDrives=True, includeItemsFromAllDrives=True, corpora='drive', driveId=<shared_drive_id>)`.
   Skip sub-folders (`mimeType == application/vnd.google-apps.folder`).
2. **Stability check** — compare `size`/`modifiedTime` to the previous poll (Drive analog of the local size-stability check) so partially-uploaded files are skipped.
3. **Claim/lock** — `files().update(fileId=…, addParents=<processing_folder_id>, removeParents=<inbox_folder_id>, supportsAllDrives=True)`. The move is the atomic claim (replaces `rename → *.processing`). If another worker already moved it, the update 404s → skip.
4. **Download** to the existing staging dir (`storage.get_staging_dir()`), then hand to the **existing pipeline** — reuse the body of `FileMonitor`'s per-file processing (hash, probe, `convert_if_needed`, create `Recording` with `processing_source='auto_process'`, `is_inbox=True`, apply tag if the file came from a tag sub-folder, `job_queue.enqueue`).
5. **Store (move, don't re-upload):** since Drive is also the storage backend, after the `Recording` row exists, **move the original Drive file** into the recordings folder (`_ensure_folder_path(build_recording_key(...))`) and set `recording.audio_path = 'gdrive://' + file_id`. The only time we re-upload is when conversion/compression produced a *new* file (codec/size constraints from the active connector); then upload the converted output and trash the original.
6. **Dedupe** — record processed Drive file IDs (and the existing `file_hash` duplicate detection) so a file is never ingested twice. A small `processed/` folder move + the existing hash check both guard this.

### 7.2 Modes
Reuse the existing `AUTO_PROCESS_MODE` semantics (`admin_only` / `user_directories` / `single_user`). In Drive, "user directories" = sub-folders of the inbox folder named `user<id>` (resolved via `files().list`). Tag sub-folders work the same way (the existing `is_auto_process` tag → folder mapping, just resolved by Drive folder instead of `Path`).

### 7.3 Startup wiring
Mirror `start_file_monitor()` (`file_monitor.py:553`) and `initialize_file_monitor()` (`config/startup.py:15`). Add a sibling `initialize_gdrive_monitor(app)` called from `run_startup_tasks()`, gated on a new master switch so the two ingestors are independent:
```
ENABLE_GDRIVE_INGEST=true
GDRIVE_INGEST_FOLDER_ID=<inbox folder id>
GDRIVE_INGEST_CHECK_INTERVAL=60
GDRIVE_INGEST_MODE=admin_only
```

> **Polling vs. push:** v1 uses simple polling (`GDRIVE_INGEST_CHECK_INTERVAL`, default 60s — Drive API quota is ~queries/100s so don't go too low). A later upgrade is the Drive **Changes API** (`changes.list` + a saved `startPageToken`) or **push channels** (`files.watch` webhook) for near-real-time and far fewer API calls. Design the poll loop around a pluggable "list new file IDs since last check" function so swapping in Changes API later is localized.

---

## 8. Google Workspace setup (operator runbook)

1. **Create a service account** in Google Cloud Console; download the JSON key. Enable the **Google Drive API** on the project.
2. **Create a Shared Drive** in Workspace (storage is owned by the org, not the service account — this is the crux that makes a headless service account viable; service accounts have *no* usable My Drive quota).
3. **Add the service account** (its `...@...iam.gserviceaccount.com` email) as a **member of the Shared Drive** with **Content manager** (or Manager) access.
4. Inside the Shared Drive, create:
   - a **recordings root folder** → `GDRIVE_ROOT_FOLDER_ID`
   - an **inbox folder** → `GDRIVE_INGEST_FOLDER_ID` (+ a `processing/` and `processed/` subfolder, auto-created on first run)
5. Set env: `FILE_STORAGE_BACKEND=gdrive`, `GDRIVE_SHARED_DRIVE_ID`, `GDRIVE_ROOT_FOLDER_ID`, `GDRIVE_CREDENTIALS_JSON=/path/to/key.json`, and (for ingestion) `ENABLE_GDRIVE_INGEST=true`, `GDRIVE_INGEST_FOLDER_ID`.
6. *(Optional)* **Domain-wide delegation** — only if you need files to appear *owned by a specific human user* rather than the service account. Authorize the client ID for scope `https://www.googleapis.com/auth/drive` in the Admin console, set `GDRIVE_DELEGATED_SUBJECT=user@yourdomain.com`. Not required for the Shared-Drive model.

**Scopes:** `https://www.googleapis.com/auth/drive` (read/write across the Shared Drive). `drive.file` is insufficient because it only sees files the app itself created — it would break ingesting files dropped by users.

---

## 9. Files touched / added (summary)

**New:**
- `src/services/storage/gdrive.py` — `GDriveStorageBackend`
- `src/gdrive_monitor.py` — `GDriveFileMonitor` + `start_gdrive_monitor()`
- `src/api/_audio_proxy.py` — shared range-streaming helper
- `docs/admin-guide/google-drive.md` — operator docs + env reference (this file is the design; that would be the user-facing how-to)
- `config/env.gdrive.example` — sample env

**Edited:**
- `src/services/storage/interfaces.py` — `StorageLocator.file_id`/`is_gdrive`; `AudioDeliveryResult` `stream` mode + fields
- `src/services/storage/locator.py` — `gdrive://` parse/build
- `src/services/storage/factory.py` — settings fields + `build_gdrive_backend`
- `src/services/storage/service.py` — resolve/upload/delivery branches
- `src/config/app_config.py` — `GDRIVE_*` env parsing
- `src/config/startup.py` — `initialize_gdrive_monitor`
- `src/api/recordings.py`, `src/api/api_v1.py`, `src/api/shares.py` — `mode == 'stream'` branch
- `requirements.txt` — Google libs

---

## 10. Testing

- **Unit (no network):** mock the Drive `service` object; test locator parse/build, `_ensure_folder_path` caching, `upload_local_file` → `StoredObject`, `materialize` temp-file contract, range math in the proxy helper.
- **Backend conformance:** reuse the existing storage tests against a faked Drive client so `gdrive` satisfies the same contract as `local`/`s3`.
- **Ingestion:** mock `files().list` returning a new file → assert `Recording` created with `processing_source='auto_process'` and a `transcribe` job enqueued; assert the "move not re-upload" path sets `audio_path='gdrive://…'`.
- **Manual/integration:** a real service account + a throwaway Shared Drive folder; drop an audio file → transcription appears; play back (verify `206` range responses in the network tab); download.

---

## 11. Risks / open questions

- **Bandwidth through the app** for playback (accepted trade-off vs. S3 offload). Revisit if a deployment is playback-heavy.
- **API quotas** — default Drive quota is generous but polling intervals and large fan-out ingestion should respect it; move to Changes API/push if needed.
- **Large files** — must use resumable upload + chunked download (designed in). Workspace per-file limits are far above typical recordings.
- **Eventual consistency** — a freshly created folder may take a beat to appear in `list`; the folder-ID cache + create-on-demand avoids racing on this.
- **Shared Drive member access** — if an admin removes the service account from the Shared Drive, all storage ops fail; surface a clear health/error message (the existing `RuntimeError('… backend is not configured')` pattern).
