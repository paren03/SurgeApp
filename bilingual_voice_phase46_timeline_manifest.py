"""Phase 46 - Timeline Manifest."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase46.timeline_manifest.v1"


_REQUIRED_MANIFEST_FIELDS = (
    "manifest_id", "created_at", "phase",
    "timeline_id", "archive_count",
    "ordered_captured_ids",
    "captured_sha_dict",
    "archive_id_dict",
    "archive_created_at_dict",
    "chain_link_count",
    "chain_link_hashes",
    "phase21_status",
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


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def create_phase46_timeline_manifest(
    timeline: Any,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"manifest_id": "", "phase": _PHASE,
                "status": "refused",
                "reason": "timeline_not_dict"}
    oa = timeline.get("ordered_archives") or []
    captured_ids = [s.get("captured_id")
                     for s in oa
                     if isinstance(s, dict)]
    sha_dict = {s.get("captured_id"):
                s.get("captured_sha256") or ""
                for s in oa
                if isinstance(s, dict)}
    aid_dict = {s.get("captured_id"):
                s.get("archive_id")
                for s in oa
                if isinstance(s, dict)}
    at_dict = {s.get("captured_id"):
                float(s.get("archive_created_at") or 0)
                for s in oa
                if isinstance(s, dict)}
    links = timeline.get("chain_links") or []
    link_hashes = [c.get("link_hash")
                    for c in links
                    if isinstance(c, dict)]
    return {
        "manifest_id":
            f"p46man_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "timeline_id":
            timeline.get("timeline_id", ""),
        "archive_count":
            int(timeline.get("archive_count") or 0),
        "ordered_captured_ids": captured_ids,
        "captured_sha_dict": sha_dict,
        "archive_id_dict": aid_dict,
        "archive_created_at_dict": at_dict,
        "chain_link_count": len(link_hashes),
        "chain_link_hashes": link_hashes,
        "phase21_status":
            timeline.get("phase21_status_text",
                           "BLOCKED"),
        "boundary_summary":
            dict(timeline.get("boundary_summary") or {}),
        "manifest_root_hash":
            _stable_hash({
                "captured": sha_dict,
                "links": link_hashes,
            }),
        "notes": [
            "Manifest is content-addressed over the "
            "captured-archive SHA dict + chain link "
            "hashes.",
            "Phase 21 status carried; never unblocked.",
        ],
    }


def validate_phase46_timeline_manifest(
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
    sha_dict = manifest.get("captured_sha_dict") or {}
    if isinstance(sha_dict, dict):
        for k, v in sha_dict.items():
            if not isinstance(v, str) or len(v) != 64:
                reasons.append(f"bad_sha:{k}")
    p21 = str(manifest.get("phase21_status") or "")
    if p21 not in ("BLOCKED",
                    "STAGED_AWAITING_OPERATOR"):
        reasons.append(
            f"phase21_unexpected:{p21}")
    rec = _stable_hash({
        "captured": sha_dict,
        "links": manifest.get(
            "chain_link_hashes") or [],
    })
    if rec != manifest.get("manifest_root_hash"):
        reasons.append("manifest_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons}


def verify_phase46_timeline_manifest(
    timeline: Any,
    manifest: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(timeline, dict) \
            or not isinstance(manifest, dict):
        return {"ok": False,
                "reasons": ["non_dict_input"]}
    if timeline.get("timeline_id") != manifest.get(
            "timeline_id"):
        reasons.append("timeline_id_mismatch")
    if int(timeline.get("archive_count") or 0) \
            != int(manifest.get(
                "archive_count") or 0):
        reasons.append("archive_count_mismatch")
    oa = timeline.get("ordered_archives") or []
    observed_sha = {s.get("captured_id"):
                    s.get("captured_sha256") or ""
                    for s in oa
                    if isinstance(s, dict)}
    if dict(observed_sha) != dict(
            manifest.get("captured_sha_dict") or {}):
        reasons.append("captured_sha_dict_mismatch")
    links = timeline.get("chain_links") or []
    observed_links = [c.get("link_hash")
                       for c in links
                       if isinstance(c, dict)]
    if observed_links != list(
            manifest.get("chain_link_hashes") or []):
        reasons.append("chain_link_hashes_mismatch")
    rec = _stable_hash({
        "captured": observed_sha,
        "links": observed_links,
    })
    if rec != manifest.get("manifest_root_hash"):
        reasons.append("manifest_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def detect_phase46_manifest_tampering(
    timeline: Any,
    manifest: Any,
) -> dict[str, Any]:
    res = verify_phase46_timeline_manifest(
        timeline, manifest)
    return {
        "tampered": not res.get("ok"),
        "reasons": res.get("reasons", []),
        "phase": _PHASE,
    }


def write_phase46_timeline_manifest(
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


def read_phase46_timeline_manifest(
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


def write_phase46_timeline_manifest_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase46_timeline_manifest(report,
                                              output_path)


__all__ = [
    "create_phase46_timeline_manifest",
    "validate_phase46_timeline_manifest",
    "verify_phase46_timeline_manifest",
    "detect_phase46_manifest_tampering",
    "write_phase46_timeline_manifest",
    "read_phase46_timeline_manifest",
    "write_phase46_timeline_manifest_report",
]
