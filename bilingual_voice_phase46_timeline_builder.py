"""Phase 46 - Timeline Builder.

Builds the timeline-ordered chain across N Phase 45 archives.
Strict monotonic-by-created_at ordering. Re-derives chain hashes
without invoking adapters or reading production DBs.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any, Optional


_PHASE = "phase46.timeline_builder.v1"


_REQUIRED_TIMELINE_FIELDS = (
    "timeline_id", "created_at", "phase",
    "source_phase",
    "archive_count", "ordered_archives",
    "chain_links",
    "timeline_root_hash",
    "phase21_status_text",
    "boundary_summary",
    "rehearsal_dry_run_only",
)


_BANNED_FIELDS = (
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


def _summarize_archive(entry: dict[str, Any]
                         ) -> dict[str, Any]:
    a = entry.get("archive") or {}
    return {
        "captured_id": entry.get("captured_id"),
        "captured_sha256":
            entry.get("captured_sha256"),
        "archive_id": a.get("archive_id"),
        "archive_created_at":
            float(a.get("created_at") or 0),
        "phase": a.get("phase"),
        "source_phases":
            list(a.get("source_phases") or []),
        "phase_counts": dict(
            a.get("phase_counts") or {}),
        "artifact_count":
            int(a.get("artifact_count") or 0),
        "artifact_hashes_root":
            _stable_hash(a.get("artifact_hashes") or {}),
        "phase21_status_text":
            a.get("phase21_status_text"),
        "boundary_summary":
            dict(a.get("boundary_summary") or {}),
    }


def build_phase46_timeline(
    collection: Any,
    timeline_id: Optional[str] = None,
) -> dict[str, Any]:
    if not isinstance(collection, dict):
        return {"timeline_id": "",
                "phase": _PHASE,
                "status": "refused",
                "reason": "collection_not_dict"}
    entries = collection.get("entries") or []
    if not isinstance(entries, list) or len(entries) < 2:
        return {"timeline_id": "",
                "phase": _PHASE,
                "status": "refused",
                "reason": "need_min_2_archives",
                "count": len(entries) if isinstance(
                    entries, list) else 0}
    summaries = [_summarize_archive(e)
                  for e in entries
                  if isinstance(e, dict)]
    # Strict monotonic ordering by archive_created_at
    summaries.sort(key=lambda s: s.get(
        "archive_created_at") or 0)
    # Build chain links between successive summaries
    chain_links: list[dict[str, Any]] = []
    for i in range(1, len(summaries)):
        prev = summaries[i - 1]
        cur = summaries[i]
        prev_at = prev.get("archive_created_at") or 0
        cur_at = cur.get("archive_created_at") or 0
        ok_monotonic = cur_at >= prev_at
        ok_distinct = (prev.get("archive_id")
                        != cur.get("archive_id"))
        ok_phase21 = (
            str(cur.get("phase21_status_text") or "")
            in ("BLOCKED", "STAGED_AWAITING_OPERATOR")
            and str(prev.get("phase21_status_text")
                     or "")
            in ("BLOCKED", "STAGED_AWAITING_OPERATOR"))
        ok_boundary = (
            (prev.get("boundary_summary") or {}).get(
                "no_adapter_invocation_in_archive")
            is True
            and (cur.get("boundary_summary") or {}).get(
                "no_adapter_invocation_in_archive")
            is True)
        link_hash = _stable_hash({
            "prev_captured_sha":
                prev.get("captured_sha256"),
            "cur_captured_sha":
                cur.get("captured_sha256"),
            "prev_archive_id": prev.get("archive_id"),
            "cur_archive_id": cur.get("archive_id"),
        })
        chain_links.append({
            "index": i,
            "from_captured_id": prev.get("captured_id"),
            "to_captured_id": cur.get("captured_id"),
            "from_archive_id": prev.get("archive_id"),
            "to_archive_id": cur.get("archive_id"),
            "delta_seconds": float(cur_at) - float(prev_at),
            "monotonic_ok": ok_monotonic,
            "archive_ids_distinct": ok_distinct,
            "phase21_intact": ok_phase21,
            "boundary_intact": ok_boundary,
            "link_hash": link_hash,
            "ok": (ok_monotonic and ok_distinct
                   and ok_phase21 and ok_boundary),
        })
    timeline_root = _stable_hash({
        "ordered_captured_ids":
            [s.get("captured_id") for s in summaries],
        "chain_link_hashes":
            [c.get("link_hash") for c in chain_links],
    })
    return {
        "timeline_id":
            str(timeline_id
                 or f"p46tl_{int(time.time())}_"
                    f"{uuid.uuid4().hex[:10]}"),
        "created_at": time.time(),
        "phase": _PHASE,
        "source_phase": "phase45",
        "archive_count": len(summaries),
        "ordered_archives": summaries,
        "chain_links": chain_links,
        "timeline_root_hash": timeline_root,
        "phase21_status_text": "BLOCKED",
        "boundary_summary": {
            "no_audio": True,
            "no_tts": True,
            "no_subprocess": True,
            "no_network": True,
            "no_multiprocessing": True,
            "no_main_runtime_integration": True,
            "no_adapter_invocation_in_timeline": True,
            "no_production_db_read_in_timeline": True,
        },
        "rehearsal_dry_run_only": True,
        "notes": [
            "Timeline orders Phase 45 archives by "
            "monotonic created_at.",
            "Each archive's boundary_summary must keep "
            "no_adapter_invocation_in_archive=True.",
            "Each archive's Phase 21 status must remain "
            "BLOCKED or STAGED_AWAITING_OPERATOR.",
        ],
    }


def validate_phase46_timeline(
    timeline: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(timeline, dict):
        return {"ok": False,
                "reasons": ["timeline_not_dict"]}
    for f in _REQUIRED_TIMELINE_FIELDS:
        if f not in timeline:
            reasons.append(f"missing_field:{f}")
    if timeline.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    for k in _BANNED_FIELDS:
        if k in timeline and timeline.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    ac = timeline.get("archive_count")
    if not (isinstance(ac, int) and ac >= 2):
        reasons.append("archive_count_lt_2")
    oa = timeline.get("ordered_archives") or []
    if not isinstance(oa, list) or len(oa) != ac:
        reasons.append("ordered_archives_count_mismatch")
    # Monotonic check
    prev = -1.0
    seen_ids: set[str] = set()
    for s in oa:
        if not isinstance(s, dict):
            reasons.append("archive_summary_not_dict")
            continue
        ts = float(s.get("archive_created_at") or 0)
        if ts < prev:
            reasons.append("non_monotonic_created_at")
        prev = ts
        aid = str(s.get("archive_id") or "")
        if aid and aid in seen_ids:
            reasons.append(f"duplicate_archive_id:{aid}")
        if aid:
            seen_ids.add(aid)
        p21 = str(s.get("phase21_status_text") or "")
        if p21 not in ("BLOCKED",
                        "STAGED_AWAITING_OPERATOR"):
            reasons.append(
                f"phase21_unexpected:{p21}")
        bs = s.get("boundary_summary") or {}
        if bs.get("no_adapter_invocation_in_archive") \
                is not True:
            reasons.append(
                "boundary_adapter_invocation_must_be_false")
    links = timeline.get("chain_links") or []
    expected_links = max(0, len(oa) - 1)
    if len(links) != expected_links:
        reasons.append(
            f"chain_link_count_mismatch:"
            f"{len(links)}!={expected_links}")
    # Timeline root hash deterministic re-derive
    rec = _stable_hash({
        "ordered_captured_ids":
            [s.get("captured_id")
             for s in oa
             if isinstance(s, dict)],
        "chain_link_hashes":
            [c.get("link_hash")
             for c in links
             if isinstance(c, dict)],
    })
    if rec != timeline.get("timeline_root_hash"):
        reasons.append("timeline_root_hash_drift")
    return {"ok": not reasons, "reasons": reasons}


def summarize_phase46_timeline(
    timeline: Any,
) -> dict[str, Any]:
    if not isinstance(timeline, dict):
        return {"ok": False, "summary": "no_timeline"}
    links = timeline.get("chain_links") or []
    ok_links = sum(1 for c in links
                    if isinstance(c, dict)
                    and c.get("ok") is True)
    return {
        "ok": True,
        "summary": (
            f"phase46 timeline: archives="
            f"{timeline.get('archive_count')} "
            f"ok_links={ok_links}/{len(links)} "
            f"phase21="
            f"{timeline.get('phase21_status_text')}"),
        "timeline_id": timeline.get("timeline_id"),
        "phase": _PHASE,
    }


def write_phase46_timeline(
    timeline: dict[str, Any],
    output_path: str,
) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(timeline)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


def write_phase46_timeline_builder_report(
    report: dict[str, Any],
    output_path: str,
) -> str:
    return write_phase46_timeline(report, output_path)


__all__ = [
    "build_phase46_timeline",
    "validate_phase46_timeline",
    "summarize_phase46_timeline",
    "write_phase46_timeline",
    "write_phase46_timeline_builder_report",
]
