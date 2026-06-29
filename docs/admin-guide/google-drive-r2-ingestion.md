# Google Drive → R2 ingestion (operator guide)

Transcribe files dropped into a Google Workspace **Shared Drive** automatically,
keeping the raw media in object storage and **S3 presigned URLs** for UI
playback. Two moving parts:

1. **rclone** (outside Speakr) moves files from the Shared Drive into the R2
   bucket's `inbox/` prefix.
2. **Speakr's S3 ingestion watcher** (`ENABLE_S3_INGEST=true`) picks new objects
   out of `inbox/`, transcribes them through the normal pipeline, and stores the
   final media under `recordings/`.

```
Google Shared Drive ──(rclone move)──► r2:<bucket>/inbox/ ──► Speakr watcher ──► recordings/
                                                                     │
                                              presigned-URL playback ◄┘ (unchanged)
```

The design rationale (why rclone→R2 instead of a Drive-native backend) lives in
`docs/advanced/google-drive-storage-and-ingestion.md`.

---

## Prerequisites

- Speakr already configured with **`FILE_STORAGE_BACKEND=s3`** pointed at your
  R2 bucket (see `config/env.r2-ingest.example`). The watcher only runs when the
  backend is S3.
- An R2 bucket with three logical prefixes the watcher uses: `inbox/`,
  `processing/`, `recordings/` (and `failed/` for objects that error). You don't
  need to pre-create them — they're just key prefixes.
- A Google **Shared Drive** (not "My Drive") and a Google Cloud **service
  account** with read access to it.

---

## Part 1 — rclone: Drive → R2

rclone runs **outside** Speakr (host cron, systemd timer, or a small sidecar).
rsync can't do this — it only speaks local FS/SSH; rclone has native Drive and
S3/R2 backends and copies directly between them.

### 1. Configure two remotes

```bash
rclone config
#   drive:  type=drive
#           scope=drive.readonly
#           service_account_file=/path/to/sa.json
#           team_drive=<shared drive id>     # so the SA isn't blocked by My-Drive quota
#
#   r2:     type=s3
#           provider=Cloudflare
#           endpoint=https://<accountid>.r2.cloudflarestorage.com
#           access_key_id=…
#           secret_access_key=…
```

A **service account** on `drive:` means no interactive OAuth refresh on a
headless box. The Shared Drive must be shared *with the service account's email*.

### 2. Schedule the move

```bash
# every 2 min, no overlapping runs
*/2 * * * * flock -n /tmp/rclone-speakr.lock \
  rclone move drive:Inbox r2:<bucket>/inbox \
    --transfers 4 --checkers 8 \
    --drive-skip-gdocs --min-age 1m \
    --log-level INFO >> /var/log/rclone-speakr.log 2>&1
```

- **`move`** (not `sync`) drains the Drive inbox as it copies — natural
  "already processed" semantics on the Drive side, and it never deletes on R2.
- **`--min-age 1m`** skips files still uploading to Drive (the rclone analog of
  Speakr's local stability check).
- **`flock`** prevents overlapping runs from racing.

---

## Part 2 — Speakr: turn on the watcher

Set these in Speakr's `.env` (full sample: `config/env.r2-ingest.example`):

```bash
ENABLE_S3_INGEST=true
S3_INGEST_PREFIX=inbox/
S3_INGEST_CHECK_INTERVAL=60        # seconds; R2 list calls are cheap, 30–120 is fine
S3_INGEST_MODE=admin_only          # admin_only | user_directories | single_user
# S3_INGEST_DEFAULT_USERNAME=…      # only for single_user mode
```

Restart Speakr. On boot you'll see:

```
S3 ingestion watcher started in 'admin_only' mode
```

### Modes

| Mode | Who owns ingested files | inbox layout |
| --- | --- | --- |
| `admin_only` (default) | the admin user | `inbox/<file>` |
| `user_directories` | per-user from the path | `inbox/user<id>/<file>` |
| `single_user` | the user in `S3_INGEST_DEFAULT_USERNAME` | `inbox/<file>` |

**Auto-process tag folders** work the same as the local monitor: an object at
`inbox/<TagFolderName>/<file>` (or `inbox/user<id>/<TagFolderName>/<file>` in
`user_directories` mode) is tagged with the matching auto-process tag.

---

## How a file flows through the watcher

1. **List** — poll `inbox/`, skip directory placeholders, zero-byte keys, and
   keys already seen this process.
2. **Claim** — server-side **copy** `inbox/<key>` → `processing/<key>`, then
   delete the inbox original. The copy succeeding is the claim, so two workers
   can't both take the same key (object stores have no atomic rename).
3. **Download** the claimed object to staging for ffprobe / hashing / optional
   conversion.
4. **Process** through the existing pipeline (`_process_file`). The recording is
   created with `processing_source='auto_process'` and a `transcribe` job is
   enqueued.
5. **Adopt in place** — if no conversion was needed, the bytes are promoted from
   `processing/<key>` to `recordings/<key>` with a **server-side copy** — never
   re-uploaded. If conversion *did* happen, the converted output is uploaded and
   the original dropped.
6. **Cleanup** — the `processing/<key>` object is deleted on success. On failure
   it's moved to `failed/<key>` for inspection rather than lost.

`audio_path` ends up an `s3://…` locator, so presigned-URL playback in the UI is
unchanged.

---

## Verifying

```bash
# Drop a test file in the Drive inbox, wait for the rclone schedule, then:
rclone ls r2:<bucket>/inbox          # should briefly show it, then empty as Speakr claims it
rclone ls r2:<bucket>/recordings     # the final media lands here
```

In Speakr it appears as an auto-processed recording in the inbox; play it back to
confirm the presigned URL works.

---

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `ENABLE_S3_INGEST=true but FILE_STORAGE_BACKEND is not 's3'` in logs | Storage backend isn't R2/S3; the watcher won't start. |
| Objects pile up in `inbox/`, never claimed | `No target user for inbox object …` — the key doesn't map to a user for the active mode (check `S3_INGEST_MODE` and the inbox layout). |
| Objects land in `failed/` | Pipeline error (bad/corrupt media, ffprobe failure). Inspect the object and Speakr logs. |
| Two-stage latency | Drive→R2 (rclone schedule) + R2 poll interval. Tune both; R2 event notifications can replace the poll later (see the design doc). |

---

## Notes

- The watcher is independent of the local file monitor — you can run either,
  both, or neither.
- Duplicate suppression: the existing `file_hash` check plus the per-process
  seen-set stop a re-listed object being ingested twice.
- The Drive service-account credentials live only in rclone, never in Speakr — a
  cleaner security boundary.
