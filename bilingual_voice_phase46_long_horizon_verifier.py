"""Phase 46 - Long-Horizon Verifier."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase46_timeline_manifest as tm


_PHASE = "phase46.long_horizon_verifier.v1"


_RUNTIME_DB_TOKENS = (".sqlite", ".sqlite3", ".db")


_BANNED_INLINE_KEYS = (
    "raw_transcript", "full_transcript",
    "raw_user_utterance", "raw_assistant_utterance",
    "sensitive_facts", "personal_facts",
    "operator_id", "signing_key_material",
    "private_key", "material_hex", "sealed_payload",
    "audio_bytes", "audio_path", "audio_file",
    "command", "command_line",
)


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def verify_phase46_monotonic_ordering(
    timeline: Any,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"ok": False,
                "reasons": ["timeline_not_dict"]}
    oa = timeline.get("ordered_archives") or []
    prev = -1.0
    reasons: list[str] = []
    for i, s in enumerate(oa):
        if not isinstance(s, dict):
            reasons.append(f"summary_not_dict:{i}")
            continue
        ts = float(s.get("archive_created_at") or 0)
        if ts < prev:
            reasons.append(
                f"non_monotonic_at_index:{i}")
        prev = ts
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase46_unique_archive_ids(
    timeline: Any,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"ok": False,
                "reasons": ["timeline_not_dict"]}
    oa = timeline.get("ordered_archives") or []
    seen: set[str] = set()
    reasons: list[str] = []
    for s in oa:
        if not isinstance(s, dict):
            continue
        aid = str(s.get("archive_id") or "")
        if aid and aid in seen:
            reasons.append(f"duplicate_archive_id:{aid}")
        if aid:
            seen.add(aid)
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase46_chain_integrity(
    timeline: Any,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"ok": False,
                "reasons": ["timeline_not_dict"]}
    reasons: list[str] = []
    links = timeline.get("chain_links") or []
    for c in links:
        if not isinstance(c, dict):
            reasons.append("link_not_dict")
            continue
        if c.get("ok") is not True:
            reasons.append(
                f"link_not_ok:"
                f"{c.get('from_captured_id')}->"
                f"{c.get('to_captured_id')}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase46_boundary_claims(
    timeline: Any,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"ok": False,
                "reasons": ["timeline_not_dict"]}
    reasons: list[str] = []
    bs = timeline.get("boundary_summary") or {}
    for k in ("no_audio", "no_tts", "no_subprocess",
              "no_network", "no_multiprocessing",
              "no_main_runtime_integration",
              "no_adapter_invocation_in_timeline",
              "no_production_db_read_in_timeline"):
        if bs.get(k) is not True:
            reasons.append(f"boundary_false:{k}")
    # Per-archive boundary intact
    for s in timeline.get("ordered_archives") or []:
        if not isinstance(s, dict):
            continue
        abs_ = s.get("boundary_summary") or {}
        if abs_.get(
                "no_adapter_invocation_in_archive") \
                is not True:
            reasons.append(
                f"archive_boundary_adapter_invocation:"
                f"{s.get('captured_id')}")
        if abs_.get(
                "no_production_db_read_in_archive") \
                is not True:
            reasons.append(
                f"archive_boundary_db_read:"
                f"{s.get('captured_id')}")
    # No banned inline fields at top level
    for k in _BANNED_INLINE_KEYS:
        if k in timeline and timeline.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase46_phase21_claim(
    timeline: Any,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"ok": False,
                "reasons": ["timeline_not_dict"]}
    reasons: list[str] = []
    txt = str(timeline.get(
        "phase21_status_text") or "")
    if txt not in ("BLOCKED",
                    "STAGED_AWAITING_OPERATOR"):
        reasons.append(f"timeline_phase21:{txt}")
    for s in timeline.get("ordered_archives") or []:
        if not isinstance(s, dict):
            continue
        ap = str(s.get("phase21_status_text") or "")
        if ap not in ("BLOCKED",
                       "STAGED_AWAITING_OPERATOR"):
            reasons.append(
                f"archive_phase21:"
                f"{s.get('captured_id')}:{ap}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase46_no_runtime_state(
    timeline: Any,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"ok": False,
                "reasons": ["timeline_not_dict"]}
    reasons: list[str] = []
    bs = timeline.get("boundary_summary") or {}
    if bs.get(
            "no_production_db_read_in_timeline") \
            is not True:
        reasons.append(
            "no_production_db_read_must_be_true")
    for s in timeline.get("ordered_archives") or []:
        if not isinstance(s, dict):
            continue
        for path_key in ("captured_path",):
            v = str(s.get(path_key) or "").lower()
            for tok in _RUNTIME_DB_TOKENS:
                if v.endswith(tok):
                    reasons.append(
                        f"runtime_db_path:"
                        f"{s.get('captured_id')}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def verify_phase46_timeline_root_hash(
    timeline: Any,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"ok": False,
                "reasons": ["timeline_not_dict"]}
    oa = timeline.get("ordered_archives") or []
    links = timeline.get("chain_links") or []
    rec = _stable_hash({
        "ordered_captured_ids":
            [s.get("captured_id")
             for s in oa if isinstance(s, dict)],
        "chain_link_hashes":
            [c.get("link_hash")
             for c in links
             if isinstance(c, dict)],
    })
    if rec != timeline.get("timeline_root_hash"):
        return {"ok": False,
                "reasons": ["timeline_root_hash_drift"],
                "phase": _PHASE}
    return {"ok": True, "phase": _PHASE}


def create_phase46_long_horizon_verification_result(
    checks: dict[str, Any],
) -> dict[str, Any]:
    mono = checks.get("monotonic") or {}
    uniq = checks.get("unique_ids") or {}
    chain = checks.get("chain") or {}
    boundary = checks.get("boundary") or {}
    p21 = checks.get("phase21") or {}
    nrs = checks.get("no_runtime_state") or {}
    root = checks.get("root_hash") or {}
    manifest = checks.get("manifest") or {}
    ok = all([
        bool(mono.get("ok")),
        bool(uniq.get("ok")),
        bool(chain.get("ok")),
        bool(boundary.get("ok")),
        bool(p21.get("ok")),
        bool(nrs.get("ok")),
        bool(root.get("ok")),
        bool(manifest.get("ok")) if manifest else True,
    ])
    return {
        "verification_id":
            f"p46ver_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "monotonic_check": mono,
        "unique_ids_check": uniq,
        "chain_check": chain,
        "boundary_check": boundary,
        "phase21_check": p21,
        "no_runtime_state_check": nrs,
        "root_hash_check": root,
        "manifest_check": manifest,
        "ok": ok,
        "summary": (
            f"phase46 verify: mono={mono.get('ok')} "
            f"uniq={uniq.get('ok')} chain={chain.get('ok')} "
            f"boundary={boundary.get('ok')} "
            f"phase21={p21.get('ok')} "
            f"no_runtime_state={nrs.get('ok')} "
            f"root={root.get('ok')}"),
    }


def verify_phase46_long_horizon_timeline(
    timeline: Any,
    manifest: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    checks = {
        "monotonic":
            verify_phase46_monotonic_ordering(timeline),
        "unique_ids":
            verify_phase46_unique_archive_ids(timeline),
        "chain":
            verify_phase46_chain_integrity(timeline),
        "boundary":
            verify_phase46_boundary_claims(timeline),
        "phase21":
            verify_phase46_phase21_claim(timeline),
        "no_runtime_state":
            verify_phase46_no_runtime_state(timeline),
        "root_hash":
            verify_phase46_timeline_root_hash(timeline),
    }
    if isinstance(manifest, dict):
        checks["manifest"] = \
            tm.verify_phase46_timeline_manifest(
                timeline, manifest)
    return \
        create_phase46_long_horizon_verification_result(
            checks)


def write_phase46_long_horizon_verification_report(
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
    "verify_phase46_long_horizon_timeline",
    "verify_phase46_monotonic_ordering",
    "verify_phase46_unique_archive_ids",
    "verify_phase46_chain_integrity",
    "verify_phase46_boundary_claims",
    "verify_phase46_phase21_claim",
    "verify_phase46_no_runtime_state",
    "verify_phase46_timeline_root_hash",
    "create_phase46_long_horizon_verification_result",
    "write_phase46_long_horizon_verification_report",
]
