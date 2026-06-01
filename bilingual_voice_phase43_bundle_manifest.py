"""Phase 43 - Bundle Manifest.

Creates and verifies the bundle manifest. Detects tampering,
missing artifacts, and forbidden references.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase43.bundle_manifest.v1"


_REQUIRED_MANIFEST_FIELDS = (
    "manifest_id", "created_at", "phase",
    "bundle_id", "artifact_count",
    "artifact_hashes", "artifact_sizes",
    "excluded_patterns", "source_phase",
    "phase21_status",
    "production_baseline_summary",
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


_EXCLUDED_PATTERNS = (
    "*.sqlite", "*.sqlite3", "*.db",
    "*.wav", "*.mp3", "*.ogg", "*.flac",
    "*.m4a", "*.aac", "*.opus",
    "backups/", "synthetic_million/",
    "quality_samples/", "pilot_imports/",
    "checkpoints/", "local_secret_handoff/",
    "corpus_sources/english/incoming/",
    "corpus_sources/russian/incoming/",
    ".claude/",
)


_RUNTIME_DB_TOKENS = (".sqlite", ".sqlite3", ".db")


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def create_phase43_bundle_manifest(
    bundle: Any,
) -> dict[str, Any]:
    if not isinstance(bundle, dict):
        return {
            "manifest_id": "",
            "phase": _PHASE,
            "status": "refused",
            "reason": "bundle_not_dict",
        }
    hashes = bundle.get("artifact_hashes") or {}
    sizes: dict[str, int] = {}
    for e in bundle.get("artifacts") or []:
        if isinstance(e, dict):
            k = e.get("artifact_key")
            if k:
                sizes[k] = int(e.get("size_bytes") or 0)
    return {
        "manifest_id":
            f"p43man_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "bundle_id": bundle.get("bundle_id", ""),
        "source_phase": bundle.get("source_phase",
                                     "phase42"),
        "artifact_count":
            int(bundle.get("artifact_count") or 0),
        "artifact_hashes": dict(hashes),
        "artifact_sizes": sizes,
        "excluded_patterns": list(_EXCLUDED_PATTERNS),
        "phase21_status":
            bundle.get("phase21_status_text", "BLOCKED"),
        "production_baseline_summary":
            dict(bundle.get(
                "production_baseline_expected") or {}),
        "boundary_summary":
            dict(bundle.get("boundary_summary") or {}),
        "manifest_root_hash": _stable_hash(hashes),
        "notes": [
            "Manifest is content-addressed by "
            "manifest_root_hash over artifact hashes.",
            "No raw content in manifest.",
            "Phase 21 status carried, not unblocked.",
        ],
    }


def validate_phase43_bundle_manifest(
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
    # Hash structure
    hashes = manifest.get("artifact_hashes") or {}
    if not isinstance(hashes, dict):
        reasons.append("hashes_not_dict")
    else:
        for k, v in hashes.items():
            if not isinstance(v, str) or len(v) != 64:
                reasons.append(f"bad_hash:{k}")
    # Phase 21 status text
    p21 = str(manifest.get("phase21_status") or "")
    if p21 not in ("BLOCKED", "STAGED_AWAITING_OPERATOR"):
        reasons.append(f"phase21_status_unexpected:{p21}")
    # Manifest root hash matches hash chain
    expected_root = _stable_hash(hashes)
    if manifest.get("manifest_root_hash") != expected_root:
        reasons.append("manifest_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons}


def verify_phase43_bundle_manifest(
    bundle: Any,
    manifest: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(bundle, dict) or not isinstance(
            manifest, dict):
        return {"ok": False,
                "reasons": ["non_dict_input"]}
    if bundle.get("bundle_id") != manifest.get("bundle_id"):
        reasons.append("bundle_id_mismatch")
    if bundle.get("artifact_count") != manifest.get(
            "artifact_count"):
        reasons.append("artifact_count_mismatch")
    bhashes = bundle.get("artifact_hashes") or {}
    mhashes = manifest.get("artifact_hashes") or {}
    if dict(bhashes) != dict(mhashes):
        reasons.append("artifact_hashes_mismatch")
    # Re-derive manifest_root_hash from bundle hashes
    expected_root = _stable_hash(bhashes)
    if manifest.get("manifest_root_hash") != expected_root:
        reasons.append("manifest_root_hash_drift")
    # Re-compute bundle entry hashes against listed sha256
    for e in bundle.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        k = e.get("artifact_key")
        sha = e.get("sha256")
        if k in mhashes and mhashes[k] != sha:
            reasons.append(f"per_artifact_hash_drift:{k}")
    # Excluded artifacts must not appear in manifest
    for k in mhashes.keys():
        if any(tok in str(k).lower()
                for tok in _RUNTIME_DB_TOKENS):
            reasons.append(
                f"runtime_db_in_manifest:{k}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def detect_phase43_manifest_tampering(
    bundle: Any,
    manifest: Any,
) -> dict[str, Any]:
    res = verify_phase43_bundle_manifest(bundle, manifest)
    return {
        "tampered": not res.get("ok"),
        "reasons": res.get("reasons", []),
        "phase": _PHASE,
    }


def write_phase43_bundle_manifest(
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


def read_phase43_bundle_manifest(
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


def write_phase43_bundle_manifest_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase43_bundle_manifest(report,
                                           output_path)


__all__ = [
    "create_phase43_bundle_manifest",
    "validate_phase43_bundle_manifest",
    "verify_phase43_bundle_manifest",
    "detect_phase43_manifest_tampering",
    "write_phase43_bundle_manifest",
    "read_phase43_bundle_manifest",
    "write_phase43_bundle_manifest_report",
]
