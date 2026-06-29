#!/usr/bin/env python3
"""
Unit tests for the S3/R2 ingestion watcher (src/s3_monitor.py) and the
server-side copy helper on the S3 storage backend.

These tests mock boto3 entirely — no network, no real bucket, no DB. They
verify the parts of the design that are pure object-store mechanics:

- S3StorageBackend.copy_object issues a server-side copy and sets ContentType
  via MetadataDirective=REPLACE.
- S3FileMonitor._scan_once lists the inbox prefix, skips directory/zero-byte
  keys and already-seen keys, and ingests genuinely new ones exactly once.
- _ingest_key claims via copy-to-processing/ + delete-inbox, downloads, calls
  _process_file with the right source_s3_key/original_filename_override, and
  cleans up the processing/ object on success.
- On pipeline failure the object is moved to failed/ rather than lost.
- _resolve_user_and_tag maps inbox-relative keys to (user_id, tag_id) per mode.

Run standalone:  python tests/test_s3_ingestion.py
Or with pytest:  pytest tests/test_s3_ingestion.py
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))


PASSED = 0
FAILED = 0


def run(name, func):
    global PASSED, FAILED
    try:
        func()
        print(f"  ✓ {name}")
        PASSED += 1
    except AssertionError as e:
        print(f"  ✗ {name}: {e}")
        FAILED += 1
        if "pytest" in sys.modules:
            raise
    except Exception as e:
        print(f"  ✗ {name}: EXCEPTION - {e}")
        FAILED += 1
        if "pytest" in sys.modules:
            raise


# --------------------------------------------------------------------------
# Backend: server-side copy
# --------------------------------------------------------------------------

def test_copy_object_is_server_side_and_sets_content_type():
    from src.services.storage.s3 import S3StorageBackend

    backend = S3StorageBackend(bucket='speakr-recordings', use_path_style=True)
    fake_client = MagicMock()
    # stat() after the copy
    fake_client.head_object.return_value = {
        'ContentLength': 123, 'ETag': '"abc"', 'ContentType': 'audio/mpeg',
    }
    backend._client = fake_client

    stored = backend.copy_object('processing/inbox/foo.mp3',
                                 'recordings/2026/06/9/ts_foo.mp3',
                                 content_type='audio/mpeg')

    assert fake_client.copy_object.called, "copy_object not called"
    kwargs = fake_client.copy_object.call_args.kwargs
    assert kwargs['Bucket'] == 'speakr-recordings'
    assert kwargs['Key'] == 'recordings/2026/06/9/ts_foo.mp3'
    assert kwargs['CopySource'] == {'Bucket': 'speakr-recordings', 'Key': 'processing/inbox/foo.mp3'}
    assert kwargs['ContentType'] == 'audio/mpeg'
    assert kwargs['MetadataDirective'] == 'REPLACE'
    # No upload_file / put — bytes never leave the store.
    assert not fake_client.upload_file.called
    assert not fake_client.put_object.called
    assert stored.locator == 's3://speakr-recordings/recordings/2026/06/9/ts_foo.mp3'


# --------------------------------------------------------------------------
# Watcher helpers (no app/db needed)
# --------------------------------------------------------------------------

def _make_monitor(mode='admin_only', bucket='speakr-recordings'):
    """Build an S3FileMonitor without running its __init__ side effects."""
    from src.s3_monitor import S3FileMonitor
    mon = S3FileMonitor.__new__(S3FileMonitor)
    mon.inbox_prefix = 'inbox/'
    mon.processing_prefix = 'processing/'
    mon.failed_prefix = 'failed/'
    mon.check_interval = 60
    mon.mode = mode
    mon.default_username = None
    mon.running = False
    mon.thread = None
    import logging
    mon.logger = logging.getLogger('s3_monitor_test')
    mon._seen_keys = set()
    mon._fm = MagicMock()
    mon._bucket = bucket
    return mon


def test_norm_prefix():
    from src.s3_monitor import S3FileMonitor
    assert S3FileMonitor._norm_prefix('inbox') == 'inbox/'
    assert S3FileMonitor._norm_prefix('/inbox/') == 'inbox/'
    assert S3FileMonitor._norm_prefix('') == ''


def test_scan_skips_dirs_zerobyte_and_seen():
    mon = _make_monitor()
    fake_client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{
        'Contents': [
            {'Key': 'inbox/', 'Size': 0},              # directory placeholder
            {'Key': 'inbox/empty.mp3', 'Size': 0},     # zero-byte
            {'Key': 'inbox/already.mp3', 'Size': 10},  # already seen
            {'Key': 'inbox/new.mp3', 'Size': 20},      # the only real one
        ]
    }]
    fake_client.get_paginator.return_value = paginator
    mon._seen_keys.add('inbox/already.mp3')

    ingested = []
    with patch.object(mon, '_client_and_bucket', return_value=(fake_client, 'speakr-recordings')), \
         patch.object(mon, '_ingest_key', side_effect=lambda c, b, k: ingested.append(k)):
        mon._scan_once()

    assert ingested == ['inbox/new.mp3'], f"unexpected ingest set: {ingested}"
    # new.mp3 now tracked as seen
    assert 'inbox/new.mp3' in mon._seen_keys


def test_resolve_admin_only():
    mon = _make_monitor(mode='admin_only')
    mon._fm._admin_user_id = 7
    with patch.object(mon, '_tag_for', return_value=None):
        uid, tag = mon._resolve_user_and_tag('foo.mp3')
    assert uid == 7 and tag is None


def test_resolve_user_directories():
    mon = _make_monitor(mode='user_directories')
    mon._fm._extract_user_id_from_dirname.side_effect = lambda n: 12 if n == 'user12' else None
    mon._fm._valid_users = {12: 'alice'}
    with patch.object(mon, '_tag_for', return_value=None):
        uid, tag = mon._resolve_user_and_tag('user12/sub/foo.mp3')
    assert uid == 12, f"expected user 12, got {uid}"


def test_resolve_user_directories_invalid_user():
    mon = _make_monitor(mode='user_directories')
    mon._fm._extract_user_id_from_dirname.side_effect = lambda n: None
    mon._fm._valid_users = {}
    uid, tag = mon._resolve_user_and_tag('garbage/foo.mp3')
    assert uid is None and tag is None


def test_ingest_claims_downloads_processes_and_cleans_up():
    mon = _make_monitor(mode='admin_only')
    fake_client = MagicMock()
    # storage service mock for staging dir + download
    storage = MagicMock()
    storage.get_staging_dir.return_value = '/tmp'

    with patch.object(mon, '_resolve_user_and_tag', return_value=(7, None)), \
         patch.object(mon, '_storage', return_value=storage), \
         patch('time.time', return_value=1000):
        mon._ingest_key(fake_client, 'speakr-recordings', 'inbox/foo.mp3')

    # Claim: copy inbox -> processing, then delete inbox original
    copy_calls = fake_client.copy_object.call_args_list
    assert any(c.kwargs.get('Key') == 'processing/foo.mp3' for c in copy_calls), \
        "did not claim via copy to processing/"
    del_keys = [c.kwargs.get('Key') for c in fake_client.delete_object.call_args_list]
    assert 'inbox/foo.mp3' in del_keys, "did not delete inbox original"
    assert 'processing/foo.mp3' in del_keys, "did not clean up processing/ on success"

    # Downloaded the claimed object
    assert fake_client.download_file.called
    dl_args = fake_client.download_file.call_args.args
    assert dl_args[0] == 'speakr-recordings' and dl_args[1] == 'processing/foo.mp3'

    # Handed to the pipeline with adopt-in-place hints
    assert mon._fm._process_file.called
    pf_kwargs = mon._fm._process_file.call_args.kwargs
    assert pf_kwargs['source_s3_key'] == 'processing/foo.mp3'
    assert pf_kwargs['original_filename_override'] == 'foo.mp3'
    assert mon._fm._process_file.call_args.args[1] == 7  # user_id


def test_ingest_moves_to_failed_on_pipeline_error():
    mon = _make_monitor(mode='admin_only')
    fake_client = MagicMock()
    storage = MagicMock()
    storage.get_staging_dir.return_value = '/tmp'
    mon._fm._process_file.side_effect = RuntimeError("boom")

    with patch.object(mon, '_resolve_user_and_tag', return_value=(7, None)), \
         patch.object(mon, '_storage', return_value=storage), \
         patch('time.time', return_value=1000):
        mon._ingest_key(fake_client, 'speakr-recordings', 'inbox/foo.mp3')

    # On failure, the processing object is copied to failed/ then deleted.
    failed_copy = [c for c in fake_client.copy_object.call_args_list
                   if c.kwargs.get('Key') == 'failed/foo.mp3']
    assert failed_copy, "did not move failed object to failed/"


def test_ingest_skips_object_with_no_user():
    mon = _make_monitor(mode='admin_only')
    fake_client = MagicMock()
    mon._seen_keys.add('inbox/foo.mp3')

    with patch.object(mon, '_resolve_user_and_tag', return_value=(None, None)):
        mon._ingest_key(fake_client, 'speakr-recordings', 'inbox/foo.mp3')

    assert not fake_client.copy_object.called, "claimed an object with no target user"
    # Allowed to retry later once config/users change
    assert 'inbox/foo.mp3' not in mon._seen_keys


def main():
    print("=== S3/R2 ingestion watcher ===\n")
    run("copy_object is server-side + sets ContentType", test_copy_object_is_server_side_and_sets_content_type)
    run("_norm_prefix normalises", test_norm_prefix)
    run("scan skips dirs/zero-byte/seen, ingests new once", test_scan_skips_dirs_zerobyte_and_seen)
    run("resolve admin_only -> admin user", test_resolve_admin_only)
    run("resolve user_directories -> user from prefix", test_resolve_user_directories)
    run("resolve user_directories invalid user -> None", test_resolve_user_directories_invalid_user)
    run("ingest claims, downloads, processes, cleans up", test_ingest_claims_downloads_processes_and_cleans_up)
    run("ingest moves to failed/ on pipeline error", test_ingest_moves_to_failed_on_pipeline_error)
    run("ingest skips object with no target user", test_ingest_skips_object_with_no_user)

    print(f"\nResults: {PASSED} passed, {FAILED} failed")
    return 0 if FAILED == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
