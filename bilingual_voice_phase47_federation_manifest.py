"""Phase 47 - Federation Manifest."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any


_PHASE = "phase47.federation_manifest.v1"


_REQUIRED_MANIFEST_FIELDS = (
    "manifest_id", "created_at", "phase",
    "federation_id", "checkout_count",
    "checkout_ids", "timeline_root_hashes",
    "imported_package_hashes",
    "federation_root_hash",
    "phase21_status_history",
    "adapter_allowlist_history",
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


def _packages_from(imported: list[Any]
                     ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in imported or []:
        if isinstance(i, dict):
            p = i.get("package")
            if isinstance(p, dict):
                out.append(p)
    return out


def create_phase47_federation_manifest(
    graph: Any,
    imported_timelines: Any,
) -> dict[str, Any]:
    if not isinstance(graph, dict) \
            or not isinstance(imported_timelines, list):
        return {"manifest_id": "", "phase": _PHASE,
                "status": "refused",
                "reason": "non_dict_or_non_list"}
    pkgs = _packages_from(imported_timelines)
    cids = [str(p.get("checkout_id") or "")
            for p in pkgs]
    troots = {p.get("checkout_id"):
              str(p.get("timeline_root_hash") or "")
              for p in pkgs}
    phashes = {p.get("checkout_id"):
                 str(p.get("package_hash") or "")
                 for p in pkgs}
    p21_hist = {p.get("checkout_id"):
                  str(p.get("phase21_status_text") or "")
                  for p in pkgs}
    allow_hist = {p.get("checkout_id"):
                    int(p.get(
                        "adapter_allowlist_count") or 0)
                    for p in pkgs}
    bs = dict(graph.get("boundary_summary") or {})
    # Strip per-checkout dict from the summary copy
    bs_clean = {k: v for k, v in bs.items()
                if k != "per_checkout_boundary_intact"}
    return {
        "manifest_id":
            f"p47man_{int(time.time())}_"
            f"{uuid.uuid4().hex[:10]}",
        "created_at": time.time(),
        "phase": _PHASE,
        "federation_id": graph.get("graph_id", ""),
        "checkout_count":
            int(graph.get("checkout_count") or 0),
        "checkout_ids": cids,
        "timeline_root_hashes": troots,
        "imported_package_hashes": phashes,
        "federation_root_hash":
            graph.get("federation_root_hash"),
        "phase21_status_history": p21_hist,
        "adapter_allowlist_history": allow_hist,
        "boundary_summary": bs_clean,
        "manifest_root_hash": _stable_hash({
            "cids": cids,
            "timeline_roots": troots,
            "package_hashes": phashes,
            "federation_root_hash":
                graph.get("federation_root_hash"),
        }),
        "notes": [
            "Manifest is content-addressed over checkout "
            "ids + timeline roots + package hashes + "
            "federation root.",
            "Phase 21 status carried; never unblocked.",
        ],
    }


def validate_phase47_federation_manifest(
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
    cc = manifest.get("checkout_count")
    if not (isinstance(cc, int) and cc >= 2):
        reasons.append("checkout_count_lt_2")
    cids = manifest.get("checkout_ids") or []
    if not isinstance(cids, list):
        reasons.append("checkout_ids_not_list")
    else:
        if len(set(cids)) != len(cids):
            reasons.append("duplicate_checkout_id")
        if len(cids) != cc:
            reasons.append("checkout_id_count_mismatch")
    troots = manifest.get(
        "timeline_root_hashes") or {}
    if isinstance(troots, dict):
        for k, v in troots.items():
            if not (isinstance(v, str)
                    and len(v) == 64):
                reasons.append(
                    f"bad_timeline_root:{k}")
    phashes = manifest.get(
        "imported_package_hashes") or {}
    if isinstance(phashes, dict):
        for k, v in phashes.items():
            if not (isinstance(v, str)
                    and len(v) == 64):
                reasons.append(f"bad_package_hash:{k}")
    p21_hist = manifest.get(
        "phase21_status_history") or {}
    if isinstance(p21_hist, dict):
        for k, v in p21_hist.items():
            if str(v) not in ("BLOCKED",
                               "STAGED_AWAITING_OPERATOR"):
                reasons.append(
                    f"phase21_unexpected:{k}:{v}")
    allow_hist = manifest.get(
        "adapter_allowlist_history") or {}
    if isinstance(allow_hist, dict):
        for k, v in allow_hist.items():
            if int(v) != 5:
                reasons.append(
                    f"adapter_count_not_5:{k}:{v}")
    fr = manifest.get("federation_root_hash") or ""
    if not (isinstance(fr, str) and len(fr) == 64):
        reasons.append("bad_federation_root_hash")
    expected = _stable_hash({
        "cids": list(cids) if isinstance(cids, list)
                else [],
        "timeline_roots": dict(troots) if isinstance(
            troots, dict) else {},
        "package_hashes": dict(phashes) if isinstance(
            phashes, dict) else {},
        "federation_root_hash": fr,
    })
    if expected != manifest.get(
            "manifest_root_hash"):
        reasons.append("manifest_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons}


def verify_phase47_federation_manifest(
    graph: Any,
    imported_timelines: Any,
    manifest: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(graph, dict) or not isinstance(
            manifest, dict) or not isinstance(
                imported_timelines, list):
        return {"ok": False,
                "reasons": ["non_compatible_input"]}
    if graph.get("graph_id") != manifest.get(
            "federation_id"):
        reasons.append("federation_id_mismatch")
    if int(graph.get("checkout_count") or 0) \
            != int(manifest.get(
                "checkout_count") or 0):
        reasons.append("checkout_count_mismatch")
    if dict(graph.get(
            "timeline_root_hashes") or {}) != dict(
                manifest.get(
                    "timeline_root_hashes") or {}):
        reasons.append("timeline_roots_mismatch")
    pkgs = _packages_from(imported_timelines)
    observed_phashes = {p.get("checkout_id"):
                          str(p.get("package_hash")
                               or "")
                          for p in pkgs}
    if dict(observed_phashes) != dict(
            manifest.get(
                "imported_package_hashes") or {}):
        reasons.append("package_hashes_mismatch")
    if graph.get("federation_root_hash") != manifest.get(
            "federation_root_hash"):
        reasons.append("federation_root_hash_mismatch")
    expected = _stable_hash({
        "cids": list(manifest.get(
            "checkout_ids") or []),
        "timeline_roots": dict(manifest.get(
            "timeline_root_hashes") or {}),
        "package_hashes": dict(manifest.get(
            "imported_package_hashes") or {}),
        "federation_root_hash": str(
            manifest.get("federation_root_hash")
            or ""),
    })
    if expected != manifest.get(
            "manifest_root_hash"):
        reasons.append("manifest_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def detect_phase47_manifest_tampering(
    graph: Any,
    imported_timelines: Any,
    manifest: Any,
) -> dict[str, Any]:
    res = verify_phase47_federation_manifest(
        graph, imported_timelines, manifest)
    return {
        "tampered": not res.get("ok"),
        "reasons": res.get("reasons", []),
        "phase": _PHASE,
    }


def write_phase47_federation_manifest(
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


def read_phase47_federation_manifest(
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


def write_phase47_federation_manifest_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase47_federation_manifest(report,
                                                output_path)


__all__ = [
    "create_phase47_federation_manifest",
    "validate_phase47_federation_manifest",
    "verify_phase47_federation_manifest",
    "detect_phase47_manifest_tampering",
    "write_phase47_federation_manifest",
    "read_phase47_federation_manifest",
    "write_phase47_federation_manifest_report",
]
