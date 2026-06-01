"""Phase 36 - Secret Boundary.

Validates that secret-bearing artifacts only live in approved local
paths and never leak into reports / witness packages / public
descriptors. Bounded reads only.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any


_PHASE = "phase36.secret_boundary.v1"


_SECRET_FIELDS = (
    "private_key", "secret", "material_hex",
    "signing_key_material", "sealed_payload",
    "raw_key", "hmac_key",
)


_SECRET_TEXT_PATTERNS = (
    re.compile(r"-----BEGIN [A-Z ]+PRIVATE KEY-----"),
    re.compile(r"\"material_hex\"\s*:\s*\"[0-9a-fA-F]{32,}\""),
    re.compile(r"\"private_key\"\s*:"),
    re.compile(r"\"signing_key_material\"\s*:"),
    re.compile(r"\"sealed_payload\"\s*:"),
)


_SAFE_SECRET_PATH_TOKEN = "local_secret_handoff"


_PUBLIC_FORBIDDEN_PATH_TOKENS = (
    "/reports/",
    "/public_descriptors/",
    "/witness_packages/",
    "/exporter_packets/",
    "/witness_inputs/",
    "/witness_outputs/",
    "/handshake_records/",
    "/integrity_manifests/",
)


_MAX_DEFAULT_BYTES = 1_000_000


def get_phase36_secret_boundary_policy() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "secret_fields": list(_SECRET_FIELDS),
        "safe_secret_path_token": _SAFE_SECRET_PATH_TOKEN,
        "public_forbidden_path_tokens":
            list(_PUBLIC_FORBIDDEN_PATH_TOKENS),
        "secret_text_patterns": [p.pattern
                                  for p in _SECRET_TEXT_PATTERNS],
        "max_default_scan_bytes": _MAX_DEFAULT_BYTES,
        "notes": [
            "Secret-bearing files allowed ONLY under "
            "local_secret_handoff folder.",
            "Reports / witness packages / public descriptors / "
            "manifests / exporter packets / witness inputs/outputs "
            "MUST reject secret fields.",
            "Bounded reads only.",
        ],
    }


def _normpath(p: str) -> str:
    return str(p).replace("\\", "/").lower()


def is_secret_safe_path(path: str) -> bool:
    return _SAFE_SECRET_PATH_TOKEN in _normpath(str(path or ""))


def scan_object_for_secret_fields(obj: Any) -> list[str]:
    hits: list[str] = []
    visited: list[int] = []

    def _walk(o: Any) -> None:
        if id(o) in visited:
            return
        visited.append(id(o))
        if isinstance(o, dict):
            for k, v in o.items():
                ks = str(k).lower()
                if ks in _SECRET_FIELDS and ks not in hits:
                    hits.append(ks)
                _walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                _walk(v)
    _walk(obj)
    return hits


def scan_file_for_secret_indicators(
    path: str,
    max_bytes: int = _MAX_DEFAULT_BYTES,
) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "reason": "file_not_found",
                "path": str(p)}
    cap = max(1, min(int(max_bytes or 1), 16 * 1024 * 1024))
    try:
        size = p.stat().st_size
    except Exception:  # noqa: BLE001
        return {"ok": False, "reason": "stat_error",
                "path": str(p)}
    if size > cap:
        return {"ok": False, "reason": "file_too_large",
                "path": str(p), "size_bytes": size, "cap": cap}
    try:
        text = p.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:  # noqa: BLE001
        return {"ok": False,
                "reason": f"read_error:{type(e).__name__}",
                "path": str(p)}
    hits: list[str] = []
    for pat in _SECRET_TEXT_PATTERNS:
        if pat.search(text):
            hits.append(pat.pattern)
    return {
        "ok": not hits,
        "path": str(p),
        "size_bytes": size,
        "pattern_hits": hits,
        "phase": _PHASE,
    }


def validate_secret_artifact_location(
    path: str,
) -> dict[str, Any]:
    np = _normpath(str(path or ""))
    if _SAFE_SECRET_PATH_TOKEN not in np:
        return {
            "ok": False,
            "reason": "secret_artifact_outside_safe_folder",
            "path": str(path), "phase": _PHASE,
        }
    for tok in _PUBLIC_FORBIDDEN_PATH_TOKENS:
        if tok in np:
            return {
                "ok": False,
                "reason": (
                    f"secret_artifact_in_forbidden_path:{tok}"),
                "path": str(path), "phase": _PHASE,
            }
    return {"ok": True, "path": str(path), "phase": _PHASE}


def validate_no_secret_leakage_in_public_artifact(
    obj: Any,
) -> dict[str, Any]:
    hits = scan_object_for_secret_fields(obj)
    return {
        "ok": not hits,
        "secret_fields_present": hits,
        "phase": _PHASE,
    }


def validate_no_secret_leakage_in_directory(
    path: str,
    limit: int = 200,
) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_dir():
        return {"ok": False, "reason": "directory_not_found",
                "path": str(p)}
    cap = max(1, min(int(limit or 1), 1000))
    leaks: list[dict[str, Any]] = []
    scanned = 0
    for entry in p.rglob("*"):
        if scanned >= cap:
            break
        if not entry.is_file():
            continue
        scanned += 1
        # Skip files inside an approved secret-handoff folder; the
        # public-artifact leakage scan must not falsely flag the
        # secret folder.
        if _SAFE_SECRET_PATH_TOKEN in _normpath(str(entry)):
            continue
        # Only inspect text-like artifacts (JSON / MD / TXT)
        ext = entry.suffix.lower()
        if ext not in (".json", ".md", ".txt", ".log"):
            continue
        res = scan_file_for_secret_indicators(str(entry))
        if not res.get("ok"):
            leaks.append({"path": res["path"],
                           "reason": res.get(
                               "reason", "pattern_hits"),
                           "hits": res.get("pattern_hits", [])})
    return {
        "ok": not leaks,
        "scanned_files": scanned,
        "leaks": leaks,
        "phase": _PHASE,
    }


def write_secret_boundary_report(
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
    "get_phase36_secret_boundary_policy",
    "is_secret_safe_path",
    "scan_object_for_secret_fields",
    "scan_file_for_secret_indicators",
    "validate_secret_artifact_location",
    "validate_no_secret_leakage_in_public_artifact",
    "validate_no_secret_leakage_in_directory",
    "write_secret_boundary_report",
]
