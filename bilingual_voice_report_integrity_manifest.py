"""Phase 32 - Report Integrity Manifest.

Streaming SHA-256 hashing of Phase 27-32 reports + evidence files.
Excludes runtime DBs / backups / synthetic-corpus directories.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase32.integrity_manifest.v1"


_CHUNK = 64 * 1024


_EXCLUDED_PATTERNS = (
    "/backups/", "/synthetic_million/", "/quality_samples/",
    "/pilot_imports/", "/checkpoints/", "/phase20/ledger.sqlite",
    "ruvector.db", "luna_vocabulary.sqlite",
    "russian_lexicon.sqlite", "russian_memory.sqlite",
    "bilingual_links.sqlite",
)


def _is_excluded(path: str) -> bool:
    s = str(path).replace("\\", "/").lower()
    return any(p in s for p in (e.lower()
                                 for e in _EXCLUDED_PATTERNS))


def compute_file_sha256(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "reason": "file_not_found",
                "path": str(p)}
    h = hashlib.sha256()
    size = 0
    try:
        with p.open("rb") as fh:
            while True:
                chunk = fh.read(_CHUNK)
                if not chunk:
                    break
                h.update(chunk)
                size += len(chunk)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "reason": f"read_error:{type(e).__name__}",
                "path": str(p)}
    return {"ok": True, "path": str(p), "sha256": h.hexdigest(),
            "size_bytes": size}


def create_report_integrity_manifest(
    paths: list[str],
    manifest_id: str = "phase32_integrity",
) -> dict[str, Any]:
    if not isinstance(paths, list):
        paths = []
    entries: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for raw in paths:
        if _is_excluded(str(raw)):
            skipped.append({"path": str(raw),
                            "reason": "excluded_pattern"})
            continue
        r = compute_file_sha256(str(raw))
        if r["ok"]:
            entries.append({
                "path": r["path"],
                "sha256": r["sha256"],
                "size_bytes": r["size_bytes"],
            })
        else:
            skipped.append({"path": r["path"],
                            "reason": r["reason"]})
    return {
        "manifest_id": (
            f"{manifest_id}_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}"),
        "created_at": time.time(),
        "phase": _PHASE,
        "entries": entries,
        "skipped": skipped,
        "entry_count": len(entries),
        "skipped_count": len(skipped),
        "notes": ("phase32 integrity manifest; SHA-256 streaming; "
                  "excludes runtime DBs and large synthetic data"),
    }


def validate_report_integrity_manifest(
    manifest: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(manifest, dict):
        return {"ok": False, "reasons": ["manifest_not_dict"]}
    for f in ("manifest_id", "created_at", "phase", "entries",
              "skipped", "entry_count", "skipped_count"):
        if f not in manifest:
            reasons.append(f"missing_field:{f}")
    if not isinstance(manifest.get("entries"), list):
        reasons.append("entries_not_list")
    else:
        for i, e in enumerate(manifest["entries"]):
            if not isinstance(e, dict):
                reasons.append(f"entry_{i}_not_dict")
                continue
            for k in ("path", "sha256", "size_bytes"):
                if k not in e:
                    reasons.append(f"entry_{i}_missing:{k}")
            sh = str(e.get("sha256") or "")
            if len(sh) != 64:
                reasons.append(f"entry_{i}_bad_sha256_len")
    return {"ok": not reasons, "reasons": reasons}


def verify_report_integrity_manifest(
    manifest: Any,
) -> dict[str, Any]:
    if not isinstance(manifest, dict):
        return {"ok": False, "reasons": ["manifest_not_dict"]}
    reasons: list[str] = []
    rehashed: list[dict[str, Any]] = []
    missing: list[str] = []
    for e in manifest.get("entries") or []:
        if not isinstance(e, dict):
            continue
        path = e.get("path")
        expected = e.get("sha256")
        r = compute_file_sha256(str(path or ""))
        if not r["ok"]:
            missing.append(str(path))
            reasons.append(f"missing_file:{path}")
            continue
        if r["sha256"] != expected:
            reasons.append(f"hash_mismatch:{path}")
        rehashed.append({"path": path,
                          "expected": expected,
                          "computed": r["sha256"],
                          "matches": r["sha256"] == expected})
    return {
        "ok": not reasons,
        "reasons": reasons,
        "missing": missing,
        "rehashed": rehashed,
        "phase": _PHASE,
    }


def write_report_integrity_manifest(
    manifest: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(manifest)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def read_report_integrity_manifest(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def write_report_integrity_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "compute_file_sha256",
    "create_report_integrity_manifest",
    "validate_report_integrity_manifest",
    "verify_report_integrity_manifest",
    "write_report_integrity_manifest",
    "read_report_integrity_manifest",
    "write_report_integrity_report",
]
