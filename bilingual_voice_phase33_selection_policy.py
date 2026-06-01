"""Phase 33 - Three-Adapter Selection Policy.

Chooses between dummy_metadata_adapter, bilingual_segment_metadata_adapter,
and prosody_density_metadata_adapter. Rejects anything else.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase33_adapter_interface as p33i
import bilingual_voice_dummy_metadata_adapter as dma
import bilingual_segment_metadata_adapter as bsma
import bilingual_prosody_density_metadata_adapter as pdma


_PHASE = "phase33.selection_policy.v1"


_MIXED_MODES = {
    "mixed", "mixed_en_ru",
    "english_with_russian_terms", "russian_with_english_terms",
}


def get_phase33_selection_policy() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "allowed_adapter_types":
            list(p33i.ALLOWED_ADAPTER_TYPES),
        "rules": [
            "If preferred_adapter is valid, use it.",
            "Mixed EN/RU or code-switch boundaries -> "
            "bilingual_segment_metadata_adapter.",
            "High prosody density / many pause/emphasis/tone markers "
            "-> prosody_density_metadata_adapter.",
            "Simple single-language payload -> "
            "dummy_metadata_adapter is acceptable.",
            "Safety warning -> bilingual_segment_metadata_adapter "
            "unless prosody risk is higher.",
            "Reject any adapter outside allowed three.",
        ],
        "notes": [
            "Selection never enables audio/TTS/subprocess.",
            "All three adapters return metadata only.",
        ],
    }


def reject_disallowed_phase33_adapter(
    adapter_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(adapter_descriptor, dict):
        return {"rejected": True, "reason": "descriptor_not_dict",
                "phase": _PHASE}
    at = str(adapter_descriptor.get("adapter_type") or "")
    if at not in p33i.ALLOWED_ADAPTER_TYPES:
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


def _prosody_signal(payload: dict[str, Any]) -> dict[str, Any]:
    pros = payload.get("prosody") or {}
    pause = sum(1 for k in (pros if isinstance(pros, dict) else {})
                 if "pause" in str(k).lower() or
                 "break" in str(k).lower())
    emp = sum(1 for k in (pros if isinstance(pros, dict) else {})
                if "emphasis" in str(k).lower() or
                "stress" in str(k).lower() or
                "accent" in str(k).lower())
    tone = sum(1 for k in (pros if isinstance(pros, dict) else {})
                 if "tone" in str(k).lower() or
                 "pitch" in str(k).lower() or
                 "intonation" in str(k).lower())
    return {"pause": pause, "emphasis": emp, "tone": tone,
             "total": pause + emp + tone}


def score_phase33_adapter_for_request(
    request: Any,
    adapter_descriptor: Any,
) -> dict[str, Any]:
    rej = reject_disallowed_phase33_adapter(adapter_descriptor)
    if rej["rejected"]:
        return {"score": 0.0, "ok": False,
                "reason": rej["reason"], "phase": _PHASE}
    req = request if isinstance(request, dict) else {}
    spoken = req.get("spoken_render_payload") or {}
    mode = str(req.get("language_mode") or
                spoken.get("language_mode") or "").lower()
    segs = spoken.get("segments") or []
    csb = spoken.get("code_switch_boundaries") or []
    safety = req.get("safety_summary") or {}
    safety_warn = bool(safety.get("unsafe") or
                        safety.get("high_risk") or
                        safety.get("blocked"))
    pros = _prosody_signal(spoken)
    at = adapter_descriptor.get("adapter_type") or ""
    score = 0.5
    reasons: list[str] = []
    if at == "bilingual_segment_metadata_adapter":
        if mode in _MIXED_MODES:
            score += 0.4
            reasons.append("mixed_language_mode")
        if csb:
            score += 0.1
            reasons.append("code_switch_boundaries_present")
        if isinstance(segs, list) and len(segs) > 1:
            score += 0.05
            reasons.append("multi_segment_payload")
        if safety_warn and pros["total"] < 3:
            score += 0.3
            reasons.append("safety_warning_prefers_richer_metadata")
    elif at == "prosody_density_metadata_adapter":
        if pros["total"] >= 3:
            score += 0.4
            reasons.append("high_prosody_density")
        if pros["pause"] >= 1 and pros["emphasis"] >= 1:
            score += 0.1
            reasons.append("multiple_marker_kinds")
        if csb and pros["total"] >= 2:
            score += 0.1
            reasons.append("code_switch_plus_prosody")
        if safety_warn and pros["total"] >= 3:
            score += 0.2
            reasons.append("safety_plus_high_prosody")
    elif at == "dummy_metadata_adapter":
        if mode not in _MIXED_MODES and not csb \
                and pros["total"] == 0 and not safety_warn:
            score += 0.3
            reasons.append("simple_single_language_payload")
        if isinstance(segs, list) and len(segs) <= 1 \
                and pros["total"] == 0:
            score += 0.05
            reasons.append("single_segment_payload")
        if safety_warn or pros["total"] >= 3:
            score -= 0.1
            reasons.append("not_preferred_when_complex")
    return {
        "score": max(0.0, min(1.0, score)),
        "ok": True,
        "reasons": reasons,
        "phase": _PHASE,
    }


def _all_descriptors() -> list[dict[str, Any]]:
    return [
        dma.get_dummy_metadata_adapter_descriptor(),
        bsma.get_bilingual_segment_metadata_adapter_descriptor(),
        pdma.get_prosody_density_metadata_adapter_descriptor(),
    ]


def choose_phase33_adapter(
    request: Any,
    available_descriptors: Optional[list[dict[str, Any]]] = None,
    preferred_adapter: Optional[str] = None,
) -> dict[str, Any]:
    pool = list(available_descriptors or _all_descriptors())
    safe_pool: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for d in pool:
        rej = reject_disallowed_phase33_adapter(d)
        if rej["rejected"]:
            rejected.append({"descriptor": d, "reason": rej["reason"]})
        else:
            safe_pool.append(d)
    if not safe_pool:
        return {
            "ok": False, "chosen": None,
            "reason": "no_safe_adapter_in_pool",
            "rejected": rejected,
            "candidate_adapters": [],
            "score_summary": {},
            "phase": _PHASE,
        }
    if preferred_adapter:
        for d in safe_pool:
            if d.get("adapter_type") == preferred_adapter or \
                    d.get("adapter_name") == preferred_adapter:
                return {
                    "ok": True, "chosen": d,
                    "reason": "preferred_adapter_valid",
                    "rejected": rejected,
                    "candidate_adapters":
                        [d2.get("adapter_name") for d2 in safe_pool],
                    "score_summary": {
                        d.get("adapter_name"):
                            score_phase33_adapter_for_request(
                                request, d).get("score", 0.0)},
                    "phase": _PHASE,
                }
        rejected.append({"descriptor": {"adapter_type":
                                          preferred_adapter},
                         "reason": "preferred_not_in_safe_pool"})
    scored = []
    for d in safe_pool:
        s = score_phase33_adapter_for_request(request, d)
        scored.append((d, s.get("score", 0.0), s))
    scored.sort(key=lambda t: t[1], reverse=True)
    top = scored[0]
    return {
        "ok": True, "chosen": top[0],
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


def explain_phase33_selection(choice: Any) -> dict[str, Any]:
    if not isinstance(choice, dict):
        return {"ok": False, "summary": "no_choice_dict"}
    chosen = choice.get("chosen") or {}
    return {
        "ok": bool(choice.get("ok")),
        "summary": (
            f"phase33 selection: adapter="
            f"{chosen.get('adapter_name') or 'none'} "
            f"reason={choice.get('reason') or 'unknown'}"),
        "candidate_adapters": choice.get("candidate_adapters", []),
        "score_summary": choice.get("score_summary", {}),
        "phase": _PHASE,
    }


def write_phase33_selection_policy_report(
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
    "get_phase33_selection_policy",
    "score_phase33_adapter_for_request",
    "choose_phase33_adapter",
    "explain_phase33_selection",
    "reject_disallowed_phase33_adapter",
    "write_phase33_selection_policy_report",
]
