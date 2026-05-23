"""Phase 20 - Dual Vocab Backup / Restore.

Snapshots the English and Russian lexicon SQLite files plus the
``seed_packs`` manifests into ``corpus_sources/backups/<snapshot_id>/`` so a
real million-scale import can be rolled back to a known-good point.

Restore is dry_run=True by default. No daemon. No scheduler. No background
loop. No network.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Optional


BACKUP_BASE = Path("corpus_sources/backups")


def _ensure_flags() -> None:
    os.environ.setdefault("LUNA_VOCABULARY_RUNTIME", "1")
    os.environ.setdefault("LUNA_RUSSIAN_STACK", "1")


def _resolve_db_paths() -> dict[str, Path]:
    import cognitive_lexicon_store as enlex
    import russian_lexicon_store as rulex
    enlex.init_db()
    rulex.init_db()
    return {
        "en": Path(enlex._resolve_db_path(None)),
        "ru": Path(rulex._resolve(None)),
    }


def _stream_sha256(p: Path) -> str:
    if not p.exists() or not p.is_file():
        return ""
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _new_snapshot_id(label: str) -> str:
    safe = "".join(c if c.isalnum() or c in "_-" else "_" for c in (label or "backup"))[:32]
    return f"snap_{int(time.time())}_{safe}_{uuid.uuid4().hex[:8]}"


def _snapshot_dir(snapshot_id: str) -> Path:
    return BACKUP_BASE / snapshot_id


def _count_words(db: Path) -> int:
    try:
        conn = sqlite3.connect(str(db))
        try:
            return int(conn.execute("SELECT COUNT(*) FROM words").fetchone()[0])
        finally:
            conn.close()
    except Exception:
        return 0


def _count_phrases(db: Path) -> int:
    try:
        conn = sqlite3.connect(str(db))
        try:
            return int(conn.execute("SELECT COUNT(*) FROM phrases").fetchone()[0])
        finally:
            conn.close()
    except Exception:
        return 0


def backup_english_db(snapshot_id: str) -> dict[str, Any]:
    _ensure_flags()
    paths = _resolve_db_paths()
    src = paths["en"]
    if not src.exists():
        return {"ok": False, "error": "english_db_missing",
                "source_path": str(src)}
    dst_dir = _snapshot_dir(snapshot_id) / "english"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name
    try:
        conn = sqlite3.connect(str(src))
        try:
            backup_conn = sqlite3.connect(str(dst))
            try:
                conn.backup(backup_conn)
            finally:
                backup_conn.close()
        finally:
            conn.close()
    except Exception as e:
        return {"ok": False, "error": f"sqlite_backup_failed: {e}",
                "source_path": str(src)}
    size = dst.stat().st_size if dst.exists() else 0
    return {"ok": True, "snapshot_id": snapshot_id,
            "source_path": str(src), "backup_path": str(dst),
            "size_bytes": int(size),
            "sha256": _stream_sha256(dst),
            "word_count": _count_words(dst)}


def backup_russian_db(snapshot_id: str) -> dict[str, Any]:
    _ensure_flags()
    paths = _resolve_db_paths()
    src = paths["ru"]
    if not src.exists():
        return {"ok": False, "error": "russian_db_missing",
                "source_path": str(src)}
    dst_dir = _snapshot_dir(snapshot_id) / "russian"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name
    try:
        conn = sqlite3.connect(str(src))
        try:
            backup_conn = sqlite3.connect(str(dst))
            try:
                conn.backup(backup_conn)
            finally:
                backup_conn.close()
        finally:
            conn.close()
    except Exception as e:
        return {"ok": False, "error": f"sqlite_backup_failed: {e}",
                "source_path": str(src)}
    size = dst.stat().st_size if dst.exists() else 0
    return {"ok": True, "snapshot_id": snapshot_id,
            "source_path": str(src), "backup_path": str(dst),
            "size_bytes": int(size),
            "sha256": _stream_sha256(dst),
            "word_count": _count_words(dst),
            "phrase_count": _count_phrases(dst)}


def backup_manifest_dir(snapshot_id: str) -> dict[str, Any]:
    """Snapshot seed_packs/<lang>/*.pack_manifest.json into the backup dir."""
    src = Path("seed_packs")
    if not src.exists():
        return {"ok": True, "snapshot_id": snapshot_id,
                "manifests_copied": 0, "note": "seed_packs_absent"}
    dst = _snapshot_dir(snapshot_id) / "manifests"
    dst.mkdir(parents=True, exist_ok=True)
    n = 0
    for p in src.rglob("*pack_manifest.json"):
        rel = p.relative_to(src)
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(p, target)
            n += 1
        except Exception:
            continue
    return {"ok": True, "snapshot_id": snapshot_id,
            "manifests_copied": int(n),
            "manifest_dir": str(dst)}


def create_backup_snapshot(label: str = "manual",
                           include_manifests: bool = True
                           ) -> dict[str, Any]:
    _ensure_flags()
    snap_id = _new_snapshot_id(label)
    sd = _snapshot_dir(snap_id)
    sd.mkdir(parents=True, exist_ok=True)
    started = time.time()
    en = backup_english_db(snap_id)
    ru = backup_russian_db(snap_id)
    mans = backup_manifest_dir(snap_id) if include_manifests else {
        "ok": True, "manifests_copied": 0, "note": "skipped"}
    meta = {
        "snapshot_id": snap_id,
        "label": str(label),
        "created_at": started,
        "completed_at": time.time(),
        "english": en,
        "russian": ru,
        "manifests": mans,
    }
    (sd / "snapshot_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")
    ok = bool(en.get("ok")) and bool(ru.get("ok")) and bool(mans.get("ok"))
    return {"ok": ok, "snapshot_id": snap_id,
            "snapshot_dir": str(sd), "meta": meta}


def list_backup_snapshots(limit: int = 50) -> list[dict[str, Any]]:
    cap = max(1, min(int(limit), 500))
    if not BACKUP_BASE.exists():
        return []
    snaps = sorted([p for p in BACKUP_BASE.iterdir() if p.is_dir()],
                   key=lambda x: x.stat().st_mtime,
                   reverse=True)[:cap]
    out: list[dict[str, Any]] = []
    for s in snaps:
        meta = s / "snapshot_meta.json"
        if meta.exists():
            try:
                m = json.loads(meta.read_text(encoding="utf-8"))
            except Exception:
                m = {}
        else:
            m = {}
        out.append({"snapshot_id": s.name, "path": str(s),
                    "label": m.get("label"),
                    "created_at": m.get("created_at"),
                    "english_ok": (m.get("english") or {}).get("ok"),
                    "russian_ok": (m.get("russian") or {}).get("ok")})
    return out


def verify_backup_snapshot(snapshot_id: str) -> dict[str, Any]:
    sd = _snapshot_dir(snapshot_id)
    if not sd.exists():
        return {"ok": False, "error": "snapshot_not_found",
                "snapshot_id": snapshot_id}
    en = list((sd / "english").glob("*.sqlite*")) if (sd / "english").exists() else []
    ru = list((sd / "russian").glob("*.sqlite*")) if (sd / "russian").exists() else []
    meta_path = sd / "snapshot_meta.json"
    meta_ok = meta_path.exists()
    en_ok = bool(en) and all(p.stat().st_size > 0 for p in en)
    ru_ok = bool(ru) and all(p.stat().st_size > 0 for p in ru)
    return {"ok": meta_ok and en_ok and ru_ok,
            "snapshot_id": snapshot_id,
            "meta_present": meta_ok,
            "english_files": [str(p) for p in en],
            "russian_files": [str(p) for p in ru],
            "english_sizes_ok": en_ok,
            "russian_sizes_ok": ru_ok}


def compare_db_counts_before_after(snapshot_id: str) -> dict[str, Any]:
    sd = _snapshot_dir(snapshot_id)
    meta_path = sd / "snapshot_meta.json"
    if not meta_path.exists():
        return {"ok": False, "error": "snapshot_meta_missing"}
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {"ok": False, "error": "snapshot_meta_unreadable"}
    _ensure_flags()
    paths = _resolve_db_paths()
    live_en = _count_words(paths["en"])
    live_ru = _count_words(paths["ru"])
    live_ru_ph = _count_phrases(paths["ru"])
    snap_en = int((meta.get("english") or {}).get("word_count") or 0)
    snap_ru = int((meta.get("russian") or {}).get("word_count") or 0)
    snap_ru_ph = int((meta.get("russian") or {}).get("phrase_count") or 0)
    return {"ok": True, "snapshot_id": snapshot_id,
            "snapshot": {"en_words": snap_en, "ru_words": snap_ru,
                         "ru_phrases": snap_ru_ph},
            "live": {"en_words": live_en, "ru_words": live_ru,
                     "ru_phrases": live_ru_ph},
            "delta": {"en_words": live_en - snap_en,
                      "ru_words": live_ru - snap_ru,
                      "ru_phrases": live_ru_ph - snap_ru_ph}}


def restore_backup_snapshot(snapshot_id: str,
                            dry_run: bool = True) -> dict[str, Any]:
    """Restore the snapshot. Dry-run by default - no live DB touched."""
    sd = _snapshot_dir(snapshot_id)
    ver = verify_backup_snapshot(snapshot_id)
    if not ver.get("ok"):
        return {"ok": False, "error": "snapshot_verification_failed",
                "details": ver}
    intended: list[dict[str, Any]] = []
    _ensure_flags()
    paths = _resolve_db_paths()
    if ver["english_files"]:
        intended.append({"action": "overwrite_english_db",
                         "from": ver["english_files"][0],
                         "to": str(paths["en"])})
    if ver["russian_files"]:
        intended.append({"action": "overwrite_russian_db",
                         "from": ver["russian_files"][0],
                         "to": str(paths["ru"])})
    if dry_run:
        return {"ok": True, "dry_run": True,
                "snapshot_id": snapshot_id,
                "intended_actions": intended,
                "note": "no_live_files_touched"}
    # Real restore: copy backup files into place, atomic via temp+rename.
    for op in intended:
        src = Path(op["from"])
        dst = Path(op["to"])
        if not src.exists():
            return {"ok": False, "error": "source_missing", "op": op}
        tmp = dst.with_suffix(dst.suffix + ".restore_tmp")
        try:
            shutil.copy2(src, tmp)
            os.replace(tmp, dst)
            op["status"] = "restored"
        except Exception as e:
            return {"ok": False, "error": f"restore_failed: {e}",
                    "op": op}
    return {"ok": True, "dry_run": False,
            "snapshot_id": snapshot_id,
            "performed_actions": intended}


def write_backup_report(report: dict[str, Any],
                        output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "BACKUP_BASE",
    "create_backup_snapshot",
    "backup_english_db",
    "backup_russian_db",
    "backup_manifest_dir",
    "list_backup_snapshots",
    "verify_backup_snapshot",
    "restore_backup_snapshot",
    "compare_db_counts_before_after",
    "write_backup_report",
]
