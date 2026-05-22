"""Phase 26 - Bilingual Voice Continuity Store.

Optional local SQLite continuity store. Dry_run=True by default. Never
auto-saves. Requires non-empty consent_marker AND explicit dry_run=False
to write. Stores only preferences + summaries - never full transcripts.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Optional


DEFAULT_STORE_PATH = Path("bilingual_stack/voice_memory/store/voice_continuity.sqlite")


_FORBIDDEN_PERSONAL_BUCKETS = (
    "medical", "political", "religious", "identity", "legal",
    "intimate", "biometric", "financial_identity", "location_history",
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS voice_sessions (
    session_id TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    memory_scope TEXT NOT NULL DEFAULT 'session_only',
    preferred_language_mode TEXT NOT NULL DEFAULT 'auto',
    preferred_spoken_mode TEXT NOT NULL DEFAULT 'auto',
    preferred_code_switch_density REAL,
    preferred_formality TEXT NOT NULL DEFAULT 'unknown',
    preferred_turn_style TEXT NOT NULL DEFAULT 'unknown',
    user_is_practicing_language TEXT NOT NULL DEFAULT 'none',
    summary_json TEXT NOT NULL DEFAULT '{}',
    consent_marker TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS ix_vs_updated ON voice_sessions(updated_at);

CREATE TABLE IF NOT EXISTS voice_session_events (
    event_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    created_at REAL NOT NULL,
    event_type TEXT NOT NULL DEFAULT '',
    language_mode TEXT NOT NULL DEFAULT '',
    preference_update_json TEXT NOT NULL DEFAULT '{}',
    correction_json TEXT NOT NULL DEFAULT '{}',
    safety_flags_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_vse_session ON voice_session_events(session_id);
CREATE INDEX IF NOT EXISTS ix_vse_created ON voice_session_events(created_at);
"""


def _connect(db_path: Optional[str | Path]) -> sqlite3.Connection:
    p = Path(db_path) if db_path is not None else DEFAULT_STORE_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_voice_continuity_store(db_path: Optional[str | Path] = None) -> str:
    p = Path(db_path) if db_path is not None else DEFAULT_STORE_PATH
    conn = _connect(p)
    try:
        for stmt in [s.strip() for s in SCHEMA.split(";") if s.strip()]:
            conn.execute(stmt)
    finally:
        conn.close()
    return str(p)


def _strip_forbidden(state: dict[str, Any]) -> dict[str, Any]:
    """Defense-in-depth: drop any key that looks like a forbidden bucket."""
    out: dict[str, Any] = {}
    for k, v in state.items():
        kl = str(k).lower()
        if any(b in kl for b in _FORBIDDEN_PERSONAL_BUCKETS):
            continue
        out[k] = v
    return out


def _summary_only(state: dict[str, Any]) -> dict[str, Any]:
    """Return a state shape acceptable for persistence: preferences +
    summary only, no full corrections list."""
    keep = ("session_id", "created_at", "updated_at", "memory_scope",
            "preferred_language_mode", "preferred_spoken_mode",
            "preferred_code_switch_density", "preferred_formality",
            "preferred_turn_style", "user_is_practicing_language",
            "emotional_tone_trend", "personality_continuity_score")
    out: dict[str, Any] = {k: state.get(k) for k in keep
                            if k in state}
    out["recent_language_modes_summary"] = (
        state.get("recent_language_modes") or [])[-5:]
    out["n_safety_flags_seen"] = sum(
        int(v) for v in (state.get("safety_flags_seen") or {}).values()
        if isinstance(v, (int, float)))
    out["n_corrections"] = len(state.get("recent_corrections") or [])
    return _strip_forbidden(out)


def save_voice_session_state(state: dict[str, Any],
                             consent_marker: str = "",
                             dry_run: bool = True,
                             db_path: Optional[str | Path] = None
                             ) -> dict[str, Any]:
    if not isinstance(state, dict):
        return {"ok": False, "reason": "state_not_dict"}
    sid = state.get("session_id")
    if not sid:
        return {"ok": False, "reason": "session_id_required"}
    cleaned = _summary_only(state)
    if dry_run:
        return {"ok": True, "dry_run": True,
                "intended": "save_voice_session",
                "session_id": sid,
                "summary_to_persist": cleaned,
                "note": "no_write_performed"}
    if not (consent_marker or "").strip():
        return {"ok": False,
                "reason": "consent_marker_required_for_write"}
    init_voice_continuity_store(db_path)
    now = time.time()
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO voice_sessions "
            "(session_id, created_at, updated_at, memory_scope, "
            " preferred_language_mode, preferred_spoken_mode, "
            " preferred_code_switch_density, preferred_formality, "
            " preferred_turn_style, user_is_practicing_language, "
            " summary_json, consent_marker, notes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (sid, float(cleaned.get("created_at") or now),
             now,
             str(cleaned.get("memory_scope") or "session_only"),
             str(cleaned.get("preferred_language_mode") or "auto"),
             str(cleaned.get("preferred_spoken_mode") or "auto"),
             (float(cleaned["preferred_code_switch_density"])
              if isinstance(cleaned.get("preferred_code_switch_density"),
                            (int, float)) else None),
             str(cleaned.get("preferred_formality") or "unknown"),
             str(cleaned.get("preferred_turn_style") or "unknown"),
             str(cleaned.get("user_is_practicing_language") or "none"),
             json.dumps(cleaned, ensure_ascii=False, default=str),
             str(consent_marker), str(state.get("notes") or "")))
    finally:
        conn.close()
    return {"ok": True, "dry_run": False, "session_id": sid,
            "wrote": "summary_only"}


def load_voice_session_state(session_id: str,
                              dry_run: bool = True,
                              db_path: Optional[str | Path] = None
                              ) -> dict[str, Any]:
    init_voice_continuity_store(db_path)
    if dry_run:
        return {"ok": True, "dry_run": True, "session_id": session_id,
                "note": "no_read_performed_in_dry_run_default"}
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT session_id, created_at, updated_at, memory_scope, "
            "preferred_language_mode, preferred_spoken_mode, "
            "preferred_code_switch_density, preferred_formality, "
            "preferred_turn_style, user_is_practicing_language, "
            "summary_json, consent_marker, notes "
            "FROM voice_sessions WHERE session_id=?", (session_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return {"ok": False, "reason": "session_not_found",
                "session_id": session_id}
    keys = ("session_id", "created_at", "updated_at", "memory_scope",
            "preferred_language_mode", "preferred_spoken_mode",
            "preferred_code_switch_density", "preferred_formality",
            "preferred_turn_style", "user_is_practicing_language",
            "summary_json", "consent_marker", "notes")
    out = dict(zip(keys, row))
    try:
        out["summary_json"] = json.loads(out["summary_json"])
    except Exception:
        out["summary_json"] = {}
    return {"ok": True, "dry_run": False, "row": out}


def append_voice_session_event(session_id: str,
                                event: dict[str, Any],
                                consent_marker: str = "",
                                dry_run: bool = True,
                                db_path: Optional[str | Path] = None
                                ) -> dict[str, Any]:
    if not isinstance(event, dict):
        return {"ok": False, "reason": "event_not_dict"}
    if dry_run:
        return {"ok": True, "dry_run": True, "session_id": session_id,
                "intended_event_type": str(event.get("event_type") or ""),
                "note": "no_write_performed"}
    if not (consent_marker or "").strip():
        return {"ok": False,
                "reason": "consent_marker_required_for_write"}
    init_voice_continuity_store(db_path)
    eid = f"evt_{int(time.time())}_{uuid.uuid4().hex[:10]}"
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT INTO voice_session_events "
            "(event_id, session_id, created_at, event_type, language_mode, "
            " preference_update_json, correction_json, safety_flags_json, "
            " metadata_json) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (eid, session_id, time.time(),
             str(event.get("event_type") or ""),
             str(event.get("language_mode") or ""),
             json.dumps(event.get("preference_update") or {},
                        ensure_ascii=False, default=str),
             json.dumps(event.get("correction") or {},
                        ensure_ascii=False, default=str),
             json.dumps(event.get("safety_flags") or {},
                        ensure_ascii=False, default=str),
             json.dumps(event.get("metadata") or {},
                        ensure_ascii=False, default=str)))
    finally:
        conn.close()
    return {"ok": True, "dry_run": False, "event_id": eid}


def list_voice_sessions(limit: int = 50,
                         db_path: Optional[str | Path] = None
                         ) -> list[dict[str, Any]]:
    init_voice_continuity_store(db_path)
    cap = max(1, min(int(limit), 500))
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            "SELECT session_id, updated_at, memory_scope, "
            "preferred_language_mode, preferred_spoken_mode "
            "FROM voice_sessions ORDER BY updated_at DESC LIMIT ?",
            (cap,))
        rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {"session_id": r[0], "updated_at": r[1], "memory_scope": r[2],
         "preferred_language_mode": r[3], "preferred_spoken_mode": r[4]}
        for r in rows
    ]


def delete_voice_session(session_id: str, dry_run: bool = True,
                          db_path: Optional[str | Path] = None
                          ) -> dict[str, Any]:
    if dry_run:
        return {"ok": True, "dry_run": True,
                "session_id": session_id,
                "note": "no_delete_performed_in_dry_run_default"}
    init_voice_continuity_store(db_path)
    conn = _connect(db_path)
    try:
        n1 = conn.execute(
            "DELETE FROM voice_session_events WHERE session_id=?",
            (session_id,)).rowcount
        n2 = conn.execute(
            "DELETE FROM voice_sessions WHERE session_id=?",
            (session_id,)).rowcount
    finally:
        conn.close()
    return {"ok": True, "dry_run": False, "events_deleted": int(n1 or 0),
            "sessions_deleted": int(n2 or 0)}


def write_voice_continuity_store_report(report: dict[str, Any],
                                         output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "DEFAULT_STORE_PATH",
    "init_voice_continuity_store",
    "save_voice_session_state",
    "load_voice_session_state",
    "append_voice_session_event",
    "list_voice_sessions",
    "delete_voice_session",
    "write_voice_continuity_store_report",
]
