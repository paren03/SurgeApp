"""Phase 31 - Multi-Adapter Selection Policy.

Selects between dummy_metadata_adapter and
bilingual_segment_metadata_adapter. Rejects anything else.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase31_adapter_interface as p31i
import bilingual_voice_dummy_metadata_adapter as dma
import bilingual_segment_metadata_adapter as bsma


_PHASE = "phase31.selection_policy.v1"


_MIXED_MODES = {
    "mixed", "mixed_en_ru",
    "english_with_russian_terms", "russian_with_english_terms",
}


def get_phase31_selection_policy() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "allowed_adapter_types": list(p31i.ALLOWED_ADAPTER_TYPES),
        "rules": [
            "If preferred_adapter is valid, use it.",
            "If payload language_mode is mixed or has code-switch "
            "boundaries, prefer bilingual_segment_metadata_adapter.",
            "If safety summary has warnings, prefer "
            "bilingual_segment_metadata_adapter.",
            "If payload is single-language and no segmentation "
            "analysis is needed, dummy_metadata_adapter is acceptable.",
            "Reject any adapter outside the two allowed.",
            "Reject any adapter with execution-shape flags.",
        ],
        "notes": [
            "Selection never enables audio/TTS/subprocess.",
            "Both adapters return metadata only.",
        ],
    }


def reject_disallowed_phase31_adapter(
    adapter_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(adapter_descriptor, dict):
        return {"rejected": True,
                "reason": "descriptor_not_dict",
                "phase": _PHASE}
    at = str(adapter_descriptor.get("adapter_type") or "")
    if at not in p31i.ALLOWED_ADAPTER_TYPES:
        return {"rejected": True,
                "reason": f"disallowed_adapter_type:{at}",
                "phase": _PHASE}
    for k in ("produces_audio", "invokes_tts", "uses_subprocess",
              "uses_network", "writes_files"):
        if adapter_descriptor.get(k) is not False:
            return {"rejected": True,
                    "reason": f"{k}_must_be_false",
                    "phase": _PHASE}
    return {"rejected": False, "reason": "", "phase": _PHASE}


def score_phase31_adapter_for_request(
    request: Any,
    adapter_descriptor: Any,
) -> dict[str, Any]:
    rej = reject_disallowed_phase31_adapter(adapter_descriptor)
    if rej["rejected"]:
        return {"score": 0.0, "ok": False, "reason": rej["reason"],
                "phase": _PHASE}
    req = request if isinstance(request, dict) else {}
    spoken = req.get("spoken_render_payload") or {}
    mode = str(req.get("language_mode") or
                spoken.get("language_mode") or "").lower()
    segs = spoken.get("segments") or []
    csb = spoken.get("code_switch_boundaries") or []
    safety = req.get("safety_summary") or {}
    at = adapter_descriptor.get("adapter_type") or ""
    score = 0.5
    reasons: list[str] = []
    safety_warn = bool(safety.get("unsafe") or
                        safety.get("high_risk") or
                        safety.get("blocked"))
    if at == "bilingual_segment_metadata_adapter":
        if mode in _MIXED_MODES:
            score += 0.4
            reasons.append("mixed_language_mode")
        if csb:
            score += 0.1
            reasons.append("code_switch_boundaries_present")
        if (isinstance(segs, list) and len(segs) > 1):
            score += 0.05
            reasons.append("multi_segment_payload")
        if safety_warn:
            score += 0.4
            reasons.append("safety_warning_prefers_richer_metadata")
    elif at == "dummy_metadata_adapter":
        if mode not in _MIXED_MODES and not csb and not safety_warn:
            score += 0.2
            reasons.append("single_language_payload")
        if (isinstance(segs, list) and len(segs) <= 1
                and not safety_warn):
            score += 0.05
            reasons.append("single_segment_payload")
        if safety_warn:
            # Drop default base so richer adapter wins when safety
            # is risky
            score -= 0.1
            reasons.append("safety_warn_prefers_richer_adapter")
    return {
        "score": min(1.0, score),
        "ok": True,
        "reasons": reasons,
        "phase": _PHASE,
    }


def _all_descriptors() -> list[dict[str, Any]]:
    return [
        dma.get_dummy_metadata_adapter_descriptor(),
        bsma.get_bilingual_segment_metadata_adapter_descriptor(),
    ]


def choose_phase31_adapter(
    request: Any,
    available_descriptors: Optional[list[dict[str, Any]]] = None,
    preferred_adapter: Optional[str] = None,
) -> dict[str, Any]:
    pool = list(available_descriptors or _all_descriptors())
    # Filter pool to only allowed + safe
    safe_pool = []
    rejected: list[dict[str, Any]] = []
    for d in pool:
        rej = reject_disallowed_phase31_adapter(d)
        if rej["rejected"]:
            rejected.append({"descriptor": d, "reason": rej["reason"]})
        else:
            safe_pool.append(d)
    if not safe_pool:
        return {
            "ok": False,
            "chosen": None,
            "reason": "no_safe_adapter_in_pool",
            "rejected": rejected,
            "candidate_adapters": [],
            "score_summary": {},
            "phase": _PHASE,
        }
    # Preferred adapter wins if valid
    if preferred_adapter:
        for d in safe_pool:
            if d.get("adapter_type") == preferred_adapter or \
                    d.get("adapter_name") == preferred_adapter:
                return {
                    "ok": True,
                    "chosen": d,
                    "reason": "preferred_adapter_valid",
                    "rejected": rejected,
                    "candidate_adapters":
                        [d2.get("adapter_name") for d2 in safe_pool],
                    "score_summary": {
                        d.get("adapter_name"):
                            score_phase31_adapter_for_request(
                                request, d).get("score", 0.0)},
                    "phase": _PHASE,
                }
        # Preferred but not in allowed pool → rejected; fall through
        rejected.append({"descriptor": {"adapter_type":
                                          preferred_adapter},
                         "reason": "preferred_not_in_safe_pool"})
    # Score every safe candidate, pick highest
    scored = []
    for d in safe_pool:
        s = score_phase31_adapter_for_request(request, d)
        scored.append((d, s.get("score", 0.0), s))
    scored.sort(key=lambda t: t[1], reverse=True)
    top = scored[0]
    return {
        "ok": True,
        "chosen": top[0],
        "reason": ("highest_score:" +
                    ",".join(top[2].get("reasons", []) or
                              ["default"])),
        "rejected": rejected,
        "candidate_adapters": [d.get("adapter_name")
                               for d, _, _ in scored],
        "score_summary": {d.get("adapter_name"): score
                          for d, score, _ in scored},
        "phase": _PHASE,
    }


def explain_phase31_selection(choice: Any) -> dict[str, Any]:
    if not isinstance(choice, dict):
        return {"ok": False, "summary": "no_choice_dict"}
    chosen = choice.get("chosen") or {}
    return {
        "ok": bool(choice.get("ok")),
        "summary": (
            f"phase31 selection: adapter="
            f"{chosen.get('adapter_name') or 'none'} "
            f"reason={choice.get('reason') or 'unknown'}"),
        "phase": _PHASE,
        "candidate_adapters": choice.get("candidate_adapters", []),
        "score_summary": choice.get("score_summary", {}),
    }


def write_phase31_selection_policy_report(
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
    "get_phase31_selection_policy",
    "score_phase31_adapter_for_request",
    "choose_phase31_adapter",
    "explain_phase31_selection",
    "reject_disallowed_phase31_adapter",
    "write_phase31_selection_policy_report",
]
