"""Shared pack manifest schema for English (Track A) + Russian (Track B).

Every future knowledge pack import — English or Russian — must emit a manifest
following this schema. Streams SHA256 (no full-file load), validates against
the canonical taxonomy, and writes JSON to disk.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, Optional

import coverage_taxonomy as tax

REQUIRED_FIELDS: tuple[str, ...] = (
    "pack_id",
    "source_name",
    "language",
    "coverage_categories",
    "register_tags",
    "safety_tags",
    "domain_tags",
    "row_count",
    "accepted_count",
    "rejected_count",
    "duplicate_count",
    "sha256",
    "created_at",
)

OPTIONAL_FIELDS: tuple[str, ...] = (
    "source_path",
    "import_report_path",
    "notes",
)

ALLOWED_LANGUAGES: tuple[str, ...] = ("en", "ru")

_SHA_CHUNK = 65_536


def compute_sha256(path: str | Path) -> str:
    """Stream-hash a file. Never loads the full file into memory."""
    p = Path(path)
    if not p.exists() or not p.is_file():
        return ""
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            chunk = f.read(_SHA_CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def create_pack_manifest(
    source_name: str,
    language: str,
    coverage_categories: Iterable[str],
    register_tags: Iterable[str],
    safety_tags: Iterable[str],
    domain_tags: Iterable[str],
    row_count: int = 0,
    accepted_count: int = 0,
    rejected_count: int = 0,
    duplicate_count: int = 0,
    source_path: Optional[str | Path] = None,
    import_report_path: Optional[str | Path] = None,
    notes: str = "",
    pack_id: Optional[str] = None,
) -> dict[str, Any]:
    """Build a manifest dict. Normalizes taxonomy values via coverage_taxonomy."""
    lang = (language or "").strip().lower()
    if lang not in ALLOWED_LANGUAGES:
        raise ValueError(f"language must be one of {ALLOWED_LANGUAGES!r}, got {language!r}")
    cov = tax.validate_coverage_categories(coverage_categories or [])
    reg = tax.validate_register_tags(register_tags or [])
    saf = tax.validate_safety_tags(safety_tags or [])
    dom = sorted({d.strip().lower() for d in (domain_tags or []) if isinstance(d, str) and d.strip()})

    sha = compute_sha256(source_path) if source_path else ""
    manifest: dict[str, Any] = {
        "pack_id": pack_id or uuid.uuid4().hex,
        "source_name": (source_name or "").strip()[:120] or "unknown",
        "source_path": str(source_path) if source_path else "",
        "language": lang,
        "coverage_categories": cov["accepted"],
        "register_tags": reg["accepted"],
        "safety_tags": saf["accepted"],
        "domain_tags": dom,
        "row_count": max(0, int(row_count)),
        "accepted_count": max(0, int(accepted_count)),
        "rejected_count": max(0, int(rejected_count)),
        "duplicate_count": max(0, int(duplicate_count)),
        "sha256": sha,
        "created_at": time.time(),
        "import_report_path": str(import_report_path) if import_report_path else "",
        "notes": (notes or "")[:1000],
        "_taxonomy_rejected": {
            "coverage_categories": cov["rejected"],
            "register_tags": reg["rejected"],
            "safety_tags": saf["rejected"],
        },
    }
    return manifest


def validate_pack_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """Return {ok: bool, missing: [...], invalid: [...]}."""
    if not isinstance(manifest, dict):
        return {"ok": False, "missing": list(REQUIRED_FIELDS),
                "invalid": ["manifest_is_not_a_dict"]}
    missing = [f for f in REQUIRED_FIELDS if f not in manifest]
    invalid: list[str] = []
    if manifest.get("language") not in ALLOWED_LANGUAGES:
        invalid.append("language")
    for f in ("coverage_categories", "register_tags", "safety_tags", "domain_tags"):
        if f in manifest and not isinstance(manifest[f], list):
            invalid.append(f)
    for f in ("row_count", "accepted_count", "rejected_count", "duplicate_count"):
        if f in manifest and not isinstance(manifest[f], int):
            invalid.append(f)
    if "sha256" in manifest:
        sha = manifest["sha256"]
        if sha and (not isinstance(sha, str) or len(sha) not in (0, 64)):
            invalid.append("sha256")
    ok = not missing and not invalid
    return {"ok": ok, "missing": missing, "invalid": invalid}


def write_pack_manifest(manifest: dict[str, Any], output_path: str | Path) -> str:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(manifest)
    payload.setdefault("created_at", time.time())
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    return str(out)


def read_pack_manifest(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


__all__ = [
    "REQUIRED_FIELDS",
    "OPTIONAL_FIELDS",
    "ALLOWED_LANGUAGES",
    "compute_sha256",
    "create_pack_manifest",
    "validate_pack_manifest",
    "write_pack_manifest",
    "read_pack_manifest",
]
