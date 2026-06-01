"""Phase 44 - Import Manifest."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase44.import_manifest.v1"


_REQUIRED_MANIFEST_FIELDS = (
    "import_manifest_id", "created_at", "phase",
    "import_id", "source_bundle_id",
    "imported_artifact_count",
    "imported_artifact_hashes",
    "imported_artifact_sizes",
    "source_manifest_root_hash",
    "import_manifest_root_hash",
    "source_phase", "phase21_status",
    "boundary_summary",
)


_BANNED_MANIFEST_FIELDS = (
    "raw_transcript", "full_transcript",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex",
    "sealed_payload", "audio_bytes",
    "audio_path", "audio_file",
    "command", "command_line",
)


_RUNTIME_DB_TOKENS = (".sqlite", ".sqlite3", ".db")
_AUDIO_TOKENS = (".wav", ".mp3", ".ogg", ".flac",
                  ".m4a", ".aac", ".opus")


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _extract_source_root_hash(
    imported_bundle: dict[str, Any],
) -> str:
    """Read the imported portable_bundle artifact's inline
    content (if present) to extract the source manifest's
    root hash basis. We hash the bundle's artifact_hashes
    dict — the same construction Phase 43 used."""
    for e in imported_bundle.get("entries") or []:
        if not isinstance(e, dict):
            continue
        if e.get("artifact_key") != "portable_bundle":
            continue
        p = e.get("imported_path")
        if not isinstance(p, str):
            continue
        path = Path(p)
        if not path.exists() or not path.is_file():
            continue
        try:
            body = path.read_text(encoding="utf-8",
                                    errors="ignore")
            obj = json.loads(body)
            return _stable_hash(
                obj.get("artifact_hashes") or {})
        except Exception:  # noqa: BLE001
            return ""
    return ""


def _extract_source_bundle_id(
    imported_bundle: dict[str, Any],
) -> str:
    for e in imported_bundle.get("entries") or []:
        if not isinstance(e, dict):
            continue
        if e.get("artifact_key") != "portable_bundle":
            continue
        p = e.get("imported_path")
        if not isinstance(p, str):
            continue
        path = Path(p)
        if not path.exists() or not path.is_file():
            continue
        try:
            obj = json.loads(path.read_text(
                encoding="utf-8", errors="ignore"))
            return str(obj.get("bundle_id") or "")
        except Exception:  # noqa: BLE001
            return ""
    return ""


def create_phase44_import_manifest(
    imported_bundle: Any,
) -> dict[str, Any]:
    if not isinstance(imported_bundle, dict):
        return {
            "import_manifest_id": "",
            "phase": _PHASE,
            "status": "refused",
            "reason": "imported_bundle_not_dict",
        }
    entries = imported_bundle.get("entries") or []
    hashes: dict[str, str] = {}
    sizes: dict[str, int] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        k = e.get("artifact_key")
        if not k:
            continue
        hashes[k] = e.get("imported_sha256") or ""
        sizes[k] = int(e.get("size_bytes") or 0)
    source_root = _extract_source_root_hash(
        imported_bundle)
    source_bundle_id = _extract_source_bundle_id(
        imported_bundle)
    return {
        "import_manifest_id":
            f"p44man_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "import_id":
            imported_bundle.get("import_id", ""),
        "source_bundle_id": source_bundle_id,
        "imported_artifact_count":
            int(imported_bundle.get("imported_count")
                 or 0),
        "imported_artifact_hashes": dict(hashes),
        "imported_artifact_sizes": sizes,
        "source_manifest_root_hash": source_root,
        "import_manifest_root_hash":
            _stable_hash(hashes),
        "source_phase": "phase43",
        "phase21_status":
            imported_bundle.get("phase21_status_text",
                                  "BLOCKED"),
        "boundary_summary":
            dict(imported_bundle.get("boundary_summary")
                 or {}),
        "notes": [
            "Import manifest is content-addressed.",
            "Source root hash is rederived from the "
            "imported portable_bundle file.",
            "Phase 21 status carried; never unblocked.",
        ],
    }


def validate_phase44_import_manifest(
    manifest: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(manifest, dict):
        return {"ok": False,
                "reasons": ["manifest_not_dict"]}
    for f in _REQUIRED_MANIFEST_FIELDS:
        if f not in manifest:
            reasons.append(f"missing_field:{f}")
    for k in _BANNED_MANIFEST_FIELDS:
        if k in manifest and manifest.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    hashes = manifest.get(
        "imported_artifact_hashes") or {}
    if not isinstance(hashes, dict):
        reasons.append("hashes_not_dict")
    else:
        for k, v in hashes.items():
            if not isinstance(v, str) or len(v) != 64:
                reasons.append(f"bad_hash:{k}")
            kl = str(k).lower()
            for tok in _RUNTIME_DB_TOKENS:
                if kl.endswith(tok):
                    reasons.append(
                        f"runtime_db_in_manifest:{k}")
            for tok in _AUDIO_TOKENS:
                if kl.endswith(tok):
                    reasons.append(
                        f"audio_in_manifest:{k}")
    p21 = str(manifest.get("phase21_status") or "")
    if p21 not in ("BLOCKED",
                    "STAGED_AWAITING_OPERATOR"):
        reasons.append(f"phase21_status_unexpected:{p21}")
    expected_root = _stable_hash(hashes)
    if manifest.get(
            "import_manifest_root_hash") != expected_root:
        reasons.append("import_manifest_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons}


def verify_phase44_import_manifest(
    imported_bundle: Any,
    manifest: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(imported_bundle, dict) \
            or not isinstance(manifest, dict):
        return {"ok": False,
                "reasons": ["non_dict_input"]}
    if imported_bundle.get("import_id") != manifest.get(
            "import_id"):
        reasons.append("import_id_mismatch")
    if int(imported_bundle.get("imported_count") or 0) \
            != int(manifest.get(
                "imported_artifact_count") or 0):
        reasons.append("artifact_count_mismatch")
    declared = manifest.get(
        "imported_artifact_hashes") or {}
    observed: dict[str, str] = {}
    for e in imported_bundle.get("entries") or []:
        if not isinstance(e, dict):
            continue
        observed[e.get("artifact_key")] = \
            e.get("imported_sha256") or ""
    if dict(observed) != dict(declared):
        reasons.append("imported_hash_drift")
    expected_root = _stable_hash(declared)
    if manifest.get(
            "import_manifest_root_hash") != expected_root:
        reasons.append("import_manifest_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def detect_phase44_import_manifest_tampering(
    imported_bundle: Any,
    manifest: Any,
) -> dict[str, Any]:
    res = verify_phase44_import_manifest(
        imported_bundle, manifest)
    return {
        "tampered": not res.get("ok"),
        "reasons": res.get("reasons", []),
        "phase": _PHASE,
    }


def write_phase44_import_manifest(
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


def read_phase44_import_manifest(
    path: str,
) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "reason": "not_found",
                "phase": _PHASE}
    try:
        return json.loads(
            p.read_text(encoding="utf-8",
                          errors="ignore"))
    except Exception as e:  # noqa: BLE001
        return {"ok": False,
                "reason": f"json_decode_failed:{e}",
                "phase": _PHASE}


def write_phase44_import_manifest_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase44_import_manifest(report,
                                           output_path)


__all__ = [
    "create_phase44_import_manifest",
    "validate_phase44_import_manifest",
    "verify_phase44_import_manifest",
    "detect_phase44_import_manifest_tampering",
    "write_phase44_import_manifest",
    "read_phase44_import_manifest",
    "write_phase44_import_manifest_report",
]
