"""Phase 45 - Archive Manifest."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase45.archive_manifest.v1"


_REQUIRED_MANIFEST_FIELDS = (
    "manifest_id", "created_at", "phase",
    "archive_id", "artifact_count",
    "phase_counts", "artifact_hashes",
    "artifact_sizes", "source_phases",
    "chain_link_ids", "excluded_patterns",
    "phase21_status",
    "production_baseline_summary",
    "boundary_summary",
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


_REQUIRED_CHAIN_LINKS = (
    "phase42_to_phase43_bundle",
    "phase43_to_phase44_import",
    "phase44_import_to_roundtrip_receipt",
    "phase44_tamper_suite_to_operator_packet",
    "phase44_operator_packet_to_dashboard",
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


def create_phase45_archive_manifest(
    archive: Any,
) -> dict[str, Any]:
    if not isinstance(archive, dict):
        return {"manifest_id": "",
                "phase": _PHASE,
                "status": "refused",
                "reason": "archive_not_dict"}
    hashes = dict(archive.get("artifact_hashes") or {})
    sizes: dict[str, int] = {}
    for e in archive.get("artifacts") or []:
        if isinstance(e, dict):
            k = e.get("artifact_key")
            if k:
                sizes[k] = int(e.get("size_bytes") or 0)
    return {
        "manifest_id":
            f"p45man_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "archive_id": archive.get("archive_id", ""),
        "artifact_count":
            int(archive.get("artifact_count") or 0),
        "phase_counts":
            dict(archive.get("phase_counts") or {}),
        "artifact_hashes": hashes,
        "artifact_sizes": sizes,
        "source_phases":
            list(archive.get("source_phases") or []),
        "chain_link_ids":
            list(_REQUIRED_CHAIN_LINKS),
        "excluded_patterns": list(_EXCLUDED_PATTERNS),
        "phase21_status":
            archive.get("phase21_status_text", "BLOCKED"),
        "production_baseline_summary":
            dict(archive.get(
                "production_baseline_expected") or {}),
        "boundary_summary":
            dict(archive.get("boundary_summary") or {}),
        "manifest_root_hash": _stable_hash(hashes),
        "notes": [
            "Manifest is content-addressed by "
            "manifest_root_hash over artifact hashes.",
            "Phase 21 status carried; never unblocked.",
        ],
    }


def validate_phase45_archive_manifest(
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
        reasons.append(
            f"phase21_status_unexpected:{p21}")
    expected = _stable_hash(hashes)
    if manifest.get("manifest_root_hash") != expected:
        reasons.append("manifest_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons}


def verify_phase45_archive_manifest(
    archive: Any,
    manifest: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(archive, dict) or not isinstance(
            manifest, dict):
        return {"ok": False,
                "reasons": ["non_dict_input"]}
    if archive.get("archive_id") != manifest.get(
            "archive_id"):
        reasons.append("archive_id_mismatch")
    if archive.get("artifact_count") != manifest.get(
            "artifact_count"):
        reasons.append("artifact_count_mismatch")
    bhashes = archive.get("artifact_hashes") or {}
    mhashes = manifest.get("artifact_hashes") or {}
    if dict(bhashes) != dict(mhashes):
        reasons.append("artifact_hashes_mismatch")
    expected = _stable_hash(bhashes)
    if manifest.get("manifest_root_hash") != expected:
        reasons.append("manifest_root_hash_drift")
    for e in archive.get("artifacts") or []:
        if not isinstance(e, dict):
            continue
        k = e.get("artifact_key")
        sha = e.get("sha256")
        if k in mhashes and mhashes[k] != sha:
            reasons.append(f"per_artifact_hash_drift:{k}")
    for k in mhashes.keys():
        kl = str(k).lower()
        for tok in _RUNTIME_DB_TOKENS:
            if kl.endswith(tok):
                reasons.append(
                    f"runtime_db_in_manifest:{k}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def detect_phase45_manifest_tampering(
    archive: Any,
    manifest: Any,
) -> dict[str, Any]:
    res = verify_phase45_archive_manifest(archive,
                                            manifest)
    return {
        "tampered": not res.get("ok"),
        "reasons": res.get("reasons", []),
        "phase": _PHASE,
    }


def write_phase45_archive_manifest(
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


def read_phase45_archive_manifest(
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


def write_phase45_archive_manifest_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase45_archive_manifest(report,
                                            output_path)


__all__ = [
    "create_phase45_archive_manifest",
    "validate_phase45_archive_manifest",
    "verify_phase45_archive_manifest",
    "detect_phase45_manifest_tampering",
    "write_phase45_archive_manifest",
    "read_phase45_archive_manifest",
    "write_phase45_archive_manifest_report",
]
