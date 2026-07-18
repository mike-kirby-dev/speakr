"""
SystemSetting model for application configuration.

This module defines the SystemSetting model for storing
dynamic system configuration in the database.
"""

from datetime import datetime
from src.database import db


class SystemSetting(db.Model):
    """Stores system-wide configuration settings."""

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True, nullable=False)
    value = db.Column(db.Text, nullable=True)
    description = db.Column(db.Text, nullable=True)
    setting_type = db.Column(db.String(50), nullable=False, default='string')  # string, integer, boolean, float
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        """Convert model to dictionary representation."""
        return {
            'id': self.id,
            'key': self.key,
            'value': self.value,
            'description': self.description,
            'setting_type': self.setting_type,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    @staticmethod
    def get_setting(key, default_value=None):
        """Get a system setting value by key, with optional default."""
        setting = SystemSetting.query.filter_by(key=key).first()
        # --- SQLite lock-window fix (Clawd 2026-07-07) ---
        # This read triggers SQLAlchemy autobegin on the shared worker session.
        # get_setting() is called by the summary/title/transcribe tasks right
        # BEFORE their multi-minute LLM/ASR network calls, and nothing commits
        # until AFTER the call — so this otherwise-trivial read left a
        # transaction open for 40-54s (measured via txn trace 2026-07-07),
        # holding a lock on the SQLite file and blocking unrelated writes
        # (e.g. a user saving speaker labels -> "database is locked"). This was
        # THE dominant holder. Materialise the two fields we need into plain
        # locals FIRST, then rollback to end the read txn (so it can't straddle
        # the network call). Using locals avoids a lazy-load re-opening the txn
        # after rollback expires the ORM object.
        if setting is None:
            _stype = _sval = None
            _found = False
        else:
            _stype = setting.setting_type
            _sval = setting.value
            _found = True
        # Only release if there are NO pending writes — never discard a caller's
        # uncommitted changes. A clean read-only autobegin txn is ended with
        # commit() (write-wise a no-op) which returns the connection to idle so
        # it can't hold a lock across the following network call.
        try:
            if not (db.session.dirty or db.session.new or db.session.deleted):
                db.session.commit()
        except Exception:
            pass
        if _found:
            # Convert value based on type
            if _stype == 'integer':
                try:
                    return int(_sval) if _sval is not None else default_value
                except (ValueError, TypeError):
                    return default_value
            elif _stype == 'boolean':
                return _sval.lower() in ('true', '1', 'yes') if _sval else default_value
            elif _stype == 'float':
                try:
                    return float(_sval) if _sval is not None else default_value
                except (ValueError, TypeError):
                    return default_value
            else:  # string
                return _sval if _sval is not None else default_value
        return default_value

    @staticmethod
    def set_setting(key, value, description=None, setting_type='string'):
        """Set a system setting value."""
        setting = SystemSetting.query.filter_by(key=key).first()
        if setting:
            setting.value = str(value) if value is not None else None
            setting.updated_at = datetime.utcnow()
            if description:
                setting.description = description
            if setting_type:
                setting.setting_type = setting_type
        else:
            setting = SystemSetting(
                key=key,
                value=str(value) if value is not None else None,
                description=description,
                setting_type=setting_type
            )
            db.session.add(setting)
        db.session.commit()
        return setting
