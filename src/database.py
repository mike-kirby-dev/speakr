"""
Database initialization module.

This module creates and exports the SQLAlchemy database instance
that is used across all models.
"""

from flask_sqlalchemy import SQLAlchemy

# Create the SQLAlchemy database instance
# This will be initialized with the Flask app using db.init_app(app)
db = SQLAlchemy()


# --- SQLite concurrency hardening (Clawd 2026-06-26) ---
# WAL + NORMAL sync + long busy_timeout on EVERY pooled connection, so the
# bulk transcript_chunk rewrite during speaker-save does not collide with the
# transcribe/summary workers ("database is locked"). Set per-connection because
# busy_timeout/synchronous are connection-scoped, not persisted in the DB file.
from sqlalchemy import event as _sa_event
from sqlalchemy.engine import Engine as _SA_Engine

@_sa_event.listens_for(_SA_Engine, "connect")
def _set_sqlite_pragmas(dbapi_connection, connection_record):
    try:
        cur = dbapi_connection.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=60000")
        cur.close()
    except Exception:
        pass


def _release_clean_read_txn():
    """End a read-only autobegin transaction on the shared session so it can't
    be held across a following slow network call (LLM/ASR) — the SQLite
    lock-window bug (Clawd 2026-07-07). Only commits when the session has NO
    pending writes, so a caller's uncommitted changes are never discarded.
    Safe no-op if the session is dirty or the commit fails.
    """
    try:
        if not (db.session.dirty or db.session.new or db.session.deleted):
            db.session.commit()
    except Exception:
        pass



