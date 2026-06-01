"""Phase 48 - Capsule Manifest."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase48.capsule_manifest.v1"


_REQUIRED_MANIFEST_FIELDS = (
    "manifest_id", "created_at", "phase",
    "capsule_id", "artifact_count",
    "artifact_hashes", "artifact_sizes",
    "source_phase", "excluded_patterns",
    "phase21_status",
    "adapter_allowlist_count",
    "production_baseline_summary",
    "boundary_summary",
    "capsule_root_hash",
    "manifest_root_hash",
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
_AUDIO_TOKENS = (".wav", ".mp3", ".ogg", ".flac",
                  ".m4a", ".aac", ".opus")


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def create_phase48_capsule_manifest(
    capsule: Any,
) -> dict[str, Any]:
    if not isinstance(capsule, dict):
        return {"manifest_id": "", "phase": _PHASE,
                "status": "refused",
                "reason": "capsule_not_dict"}
    hashes = dict(capsule.get("artifact_hashes") or {})
    sizes: dict[str, int] = {}
    for e in capsule.get("artifacts") or []:
        if isinstance(e, dict):
            k = e.get("artifact_key")
            if k:
                sizes[k] = int(e.get("size_bytes") or 0)
    return {
        "manifest_id":
            f"p48man_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "capsule_id": capsule.get("capsule_id", ""),
        "artifact_count":
            int(capsule.get("artifact_count") or 0),
        "artifact_hashes": hashes,
        "artifact_sizes": sizes,
        "source_phase":
            capsule.get("source_phase", "phase47"),
        "excluded_patterns":
            list(_EXCLUDED_PATTERNS),
        "phase21_status":
            capsule.get("phase21_status_text",
                          "BLOCKED"),
        "adapter_allowlist_count":
            int(capsule.get(
                "adapter_allowlist_count") or 0),
        "production_baseline_summary":
            dict(capsule.get(
                "production_baseline_expected") or {}),
        "boundary_summary":
            dict(capsule.get("boundary_summary") or {}),
        "capsule_root_hash":
            capsule.get("capsule_root_hash"),
        "manifest_root_hash": _stable_hash({
            "capsule_root_hash":
                capsule.get("capsule_root_hash"),
            "hashes": hashes,
        }),
        "notes": [
            "Manifest is content-addressed over capsule "
            "root + per-artifact hashes.",
            "Phase 21 status carried; never unblocked.",
        ],
    }


def validate_phase48_capsule_manifest(
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
    hashes = manifest.get("artifact_hashes") or {}
    if isinstance(hashes, dict):
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
        reasons.append(f"phase21_unexpected:{p21}")
    if int(manifest.get(
            "adapter_allowlist_count") or 0) != 5:
        reasons.append("adapter_count_not_5")
    cr = manifest.get("capsule_root_hash") or ""
    if not (isinstance(cr, str) and len(cr) == 64):
        reasons.append("bad_capsule_root_hash")
    expected = _stable_hash({
        "capsule_root_hash": cr,
        "hashes": hashes if isinstance(
            hashes, dict) else {},
    })
    if expected != manifest.get(
            "manifest_root_hash"):
        reasons.append("manifest_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons}


def verify_phase48_capsule_manifest(
    capsule: Any,
    manifest: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(capsule, dict) or not isinstance(
            manifest, dict):
        return {"ok": False,
                "reasons": ["non_dict_input"]}
    if capsule.get("capsule_id") != manifest.get(
            "capsule_id"):
        reasons.append("capsule_id_mismatch")
    if int(capsule.get("artifact_count") or 0) \
            != int(manifest.get(
                "artifact_count") or 0):
        reasons.append("artifact_count_mismatch")
    chash = dict(capsule.get(
        "artifact_hashes") or {})
    mhash = dict(manifest.get(
        "artifact_hashes") or {})
    if chash != mhash:
        reasons.append("artifact_hashes_mismatch")
    if capsule.get("capsule_root_hash") != manifest.get(
            "capsule_root_hash"):
        reasons.append("capsule_root_hash_mismatch")
    # Re-derive manifest_root_hash
    expected = _stable_hash({
        "capsule_root_hash":
            capsule.get("capsule_root_hash"),
        "hashes": chash,
    })
    if expected != manifest.get(
            "manifest_root_hash"):
        reasons.append("manifest_root_hash_drift")
    # Per-artifact sha drift
    for e in capsule.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        k = e.get("artifact_key")
        sha = e.get("sha256")
        if k in mhash and mhash[k] != sha:
            reasons.append(f"per_artifact_drift:{k}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def detect_phase48_manifest_tampering(
    capsule: Any,
    manifest: Any,
) -> dict[str, Any]:
    res = verify_phase48_capsule_manifest(
        capsule, manifest)
    return {
        "tampered": not res.get("ok"),
        "reasons": res.get("reasons", []),
        "phase": _PHASE,
    }


def write_phase48_capsule_manifest(
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


def read_phase48_capsule_manifest(
    path: str,
) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return {"ok": False, "reason": "not_found",
                "phase": _PHASE}
    try:
        return json.loads(p.read_text(
            encoding="utf-8", errors="ignore"))
    except Exception as e:  # noqa: BLE001
        return {"ok": False,
                "reason": f"json_decode_failed:{e}",
                "phase": _PHASE}


def write_phase48_capsule_manifest_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase48_capsule_manifest(report,
                                             output_path)


__all__ = [
    "create_phase48_capsule_manifest",
    "validate_phase48_capsule_manifest",
    "verify_phase48_capsule_manifest",
    "detect_phase48_manifest_tampering",
    "write_phase48_capsule_manifest",
    "read_phase48_capsule_manifest",
    "write_phase48_capsule_manifest_report",
]
