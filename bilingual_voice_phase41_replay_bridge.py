"""Phase 41 - Replay Bridge.

Project Phase 41 invocation output into a Phase 40-style audit
replay projection WITHOUT modifying Phase 40 or re-invoking any
adapter.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any


_PHASE = "phase41.replay_bridge.v1"


_REQUIRED_PROJECTION_FIELDS = (
    "projection_id", "created_at", "phase",
    "source_phase", "selected_adapter_name",
    "trace_hash_summary", "adapter_selection_summary",
    "governance_result", "baseline_summary",
    "phase21_status",
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
    "produced_audio_true", "invoked_tts_true",
)


_PRODUCTION_INVARIANTS = {
    "english_words": 2814,
    "russian_words": 2518,
    "russian_phrases": 35,
    "bilingual_concepts": 26,
    "bilingual_entry_links": 52,
    "live_pack_manifests": 90,
}


def _stable_hash(obj: Any) -> str:
    try:
        body = json.dumps(obj, sort_keys=True,
                          ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        body = str(obj)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def create_phase41_replay_projection(
    invocation_output: Any,
) -> dict[str, Any]:
    out = invocation_output if isinstance(
        invocation_output, dict) else {}
    sel = out.get("selection_choice") or {}
    chosen = sel.get("chosen") if isinstance(
        sel, dict) else {}
    adapter_name = (chosen.get("adapter_name")
                    if isinstance(chosen, dict) else None)
    pipe = out.get("signed_witness_pipeline") or {}
    rv = out.get("result_verification") or {}
    gov = out.get("governance_recheck") or {}
    # Trace-hash summary (lightweight: hash the
    # signed_evidence_summary if present, else hash the
    # invocation receipt id)
    se = pipe.get("signed_evidence_summary") or {}
    receipt_id = (out.get("invocation_receipt") or {}
                   ).get("receipt_id", "")
    trace_hash_basis = {
        "phase41_id": out.get("phase41_id"),
        "selected_adapter_name": adapter_name,
        "signed_evidence_root":
            se.get("evidence_root_hash")
            or se.get("evidence_chain_root")
            or se.get("evidence_hash") or "",
        "invocation_receipt_id": receipt_id,
    }
    trace_hash = _stable_hash(trace_hash_basis)
    return {
        "projection_id": f"p41proj_{int(time.time())}",
        "created_at": time.time(),
        "phase": _PHASE,
        "source_phase": "phase41",
        "selected_adapter_name": adapter_name,
        "trace_hash_summary": {
            "trace_hash": trace_hash,
            "basis": trace_hash_basis,
        },
        "adapter_selection_summary": {
            "candidate_adapters":
                sel.get("candidate_adapters", []),
            "score_summary":
                sel.get("score_summary", {}),
            "reason": sel.get("reason", ""),
        },
        "governance_result": {
            "result_verification_ok": bool(rv.get("ok")),
            "governance_recheck_ok": bool(gov.get("ok")),
            "signed_evidence_validates": bool(
                se.get("evidence_validates")),
            "witness_export_status":
                (pipe.get("witness_export_summary") or {}
                 ).get("status"),
            "exchange_status":
                (pipe.get("exchange_summary") or {}
                 ).get("status"),
        },
        "baseline_summary": {
            "expected": dict(_PRODUCTION_INVARIANTS),
            "note": ("Phase 41 does NOT read production DBs "
                     "during projection."),
        },
        "phase21_status": {
            "status_text": "BLOCKED",
            "note": ("Projection makes no Phase 21 claim "
                     "beyond restating it remains BLOCKED "
                     "unless operator stages and runs "
                     "Phase 21 explicitly."),
        },
        "rehearsal_dry_run_only": True,
        "notes": [
            "Projection is read-only.",
            "No adapter re-invocation.",
            "No audio / TTS / subprocess / network / "
            "multiprocessing.",
        ],
    }


def validate_phase41_replay_projection(
    projection: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(projection, dict):
        return {"ok": False,
                "reasons": ["projection_not_dict"]}
    for f in _REQUIRED_PROJECTION_FIELDS:
        if f not in projection:
            reasons.append(f"missing_field:{f}")
    if projection.get("rehearsal_dry_run_only") is not True:
        reasons.append("dry_run_only_must_be_true")
    for k in _BANNED_FIELDS:
        if k in projection and projection.get(k) not in (
                None, "", False, [], {}):
            reasons.append(f"banned_field:{k}")
    return {"ok": not reasons, "reasons": reasons}


def compare_phase41_projection_to_phase40_contract(
    projection: Any,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(projection, dict):
        return {"ok": False,
                "reasons": ["projection_not_dict"]}
    # Phase 40 contract expects: trace hash, selection
    # summary, governance result, baseline summary, Phase 21
    # status.
    for f in ("trace_hash_summary",
              "adapter_selection_summary",
              "governance_result",
              "baseline_summary",
              "phase21_status"):
        if f not in projection:
            reasons.append(f"missing_required_for_phase40:{f}")
    return {"ok": not reasons, "reasons": reasons,
            "phase": _PHASE}


def summarize_phase41_replay_compatibility(
    projection: Any,
) -> dict[str, Any]:
    if not isinstance(projection, dict):
        return {"ok": False, "summary": "no_projection"}
    return {
        "ok": True,
        "summary": (
            f"phase41 projection: adapter="
            f"{projection.get('selected_adapter_name')} "
            f"trace_hash="
            f"{(projection.get('trace_hash_summary') or {}).get('trace_hash', '')[:16]} "
            f"gov_ok="
            f"{(projection.get('governance_result') or {}).get('governance_recheck_ok')}"),
        "projection_id": projection.get("projection_id"),
        "phase": _PHASE,
    }


def write_phase41_replay_bridge_report(
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
    "create_phase41_replay_projection",
    "validate_phase41_replay_projection",
    "compare_phase41_projection_to_phase40_contract",
    "summarize_phase41_replay_compatibility",
    "write_phase41_replay_bridge_report",
]
