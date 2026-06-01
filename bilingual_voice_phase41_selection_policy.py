"""Phase 41 - Five-Adapter Selection Policy."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_phase41_adapter_interface as p41i
import bilingual_voice_dummy_metadata_adapter as dma
import bilingual_segment_metadata_adapter as bsma
import bilingual_prosody_density_metadata_adapter as pdma
import bilingual_safety_redaction_trace_adapter as srta
import bilingual_memory_continuity_audit_adapter as mcaa


_PHASE = "phase41.selection_policy.v1"


_MIXED_MODES = {
    "mixed", "mixed_en_ru",
    "english_with_russian_terms", "russian_with_english_terms",
}


def get_phase41_selection_policy() -> dict[str, Any]:
    return {
        "version": _PHASE,
        "allowed_adapter_types":
            list(p41i.ALLOWED_ADAPTER_TYPES),
        "rules": [
            "If preferred_adapter is valid, use it.",
            "Memory-state present / continuity preference / "
            "memory summary / correction-history metadata / "
            "conversation-style drift metadata prefer "
            "memory_continuity_audit_metadata_adapter.",
            "Safety warnings / redactions / recognition-only / "
            "do_not_use_unprompted / voice-safe replacements / "
            "vulgar-offensive blocks prefer "
            "safety_redaction_trace_metadata_adapter unless "
            "memory audit is explicitly preferred.",
            "Mixed EN/RU or code-switch boundaries prefer "
            "bilingual_segment_metadata_adapter unless safety "
            "or memory audit is more important.",
            "High prosody density prefers "
            "prosody_density_metadata_adapter unless safety or "
            "memory audit is more important.",
            "Simple single-language payload with no memory "
            "state may use dummy_metadata_adapter.",
            "Reject any adapter outside allowed five.",
            "Reject any adapter with execution flags.",
        ],
        "notes": [
            "Selection never enables audio/TTS/subprocess.",
            "All five adapters return metadata only.",
        ],
    }


def reject_disallowed_phase41_adapter(
    adapter_descriptor: Any,
) -> dict[str, Any]:
    if not isinstance(adapter_descriptor, dict):
        return {"rejected": True, "reason": "descriptor_not_dict",
                "phase": _PHASE}
    at = str(adapter_descriptor.get("adapter_type") or "")
    if at not in p41i.ALLOWED_ADAPTER_TYPES:
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


def _memory_signal(request: dict[str, Any]) -> dict[str, Any]:
    vms = request.get("voice_memory_state") or {}
    if not isinstance(vms, dict):
        vms = {}
    present = bool(vms)
    fields = sorted(list(vms.keys()))
    drift = bool(vms.get("recent_drift_signal")) or \
        bool(vms.get("user_preference_drift"))
    corrections = int(vms.get(
        "correction_pattern_count") or 0)
    signal = 0
    if present:
        signal += 3
    if drift:
        signal += 2
    if corrections >= 1:
        signal += min(3, corrections)
    if vms.get("voice_style_continuity") and \
            str(vms.get("voice_style_continuity")) != "stable":
        signal += 2
    if vms.get("preferred_language_mode"):
        signal += 1
    return {
        "present": present,
        "drift": drift,
        "corrections": corrections,
        "fields": fields,
        "total_signal": signal,
    }


def _safety_signal(request: dict[str, Any]) -> dict[str, Any]:
    safety = request.get("safety_summary") or {}
    spoken = request.get("spoken_render_payload") or {}
    spoken_safety = spoken.get("safety_summary") or {}
    merged = {**(safety if isinstance(safety, dict) else {}),
              **(spoken_safety if isinstance(
                  spoken_safety, dict) else {})}
    warn = bool(merged.get("unsafe") or merged.get("high_risk")
                 or merged.get("blocked")
                 or merged.get("unsafe_leakage_detected"))
    replacements = int(merged.get("replacements_count") or 0)
    risks = (len(merged.get("risks") or [])
              if isinstance(merged.get("risks"), list) else 0)
    redactions = srta.summarize_redaction_decisions(
        spoken)["redaction_decision_count"]
    rec_only = srta.count_recognition_only_blocks(spoken)
    dnu = srta.count_do_not_use_unprompted_blocks(spoken)
    vob = srta.count_vulgar_offensive_blocks(spoken)
    vsr = srta.count_voice_safe_replacements(spoken)
    total = ((5 if warn else 0) + replacements + risks
             + redactions + rec_only + dnu + vob + vsr)
    return {
        "warn": warn,
        "total_signal": total,
        "recognition_only": rec_only,
        "do_not_use_unprompted": dnu,
        "vulgar_offensive_blocks": vob,
        "voice_safe_replacements": vsr,
    }


def _prosody_signal(payload: dict[str, Any]) -> dict[str, Any]:
    pros = payload.get("prosody") or {}
    if not isinstance(pros, dict):
        return {"total": 0}
    pause = sum(1 for k in pros if "pause" in str(k).lower()
                 or "break" in str(k).lower())
    emp = sum(1 for k in pros if "emphasis" in str(k).lower()
               or "stress" in str(k).lower()
               or "accent" in str(k).lower())
    tone = sum(1 for k in pros if "tone" in str(k).lower()
                or "pitch" in str(k).lower()
                or "intonation" in str(k).lower())
    return {"pause": pause, "emphasis": emp, "tone": tone,
             "total": pause + emp + tone}


def score_phase41_adapter_for_request(
    request: Any,
    adapter_descriptor: Any,
) -> dict[str, Any]:
    rej = reject_disallowed_phase41_adapter(adapter_descriptor)
    if rej["rejected"]:
        return {"score": 0.0, "ok": False,
                "reason": rej["reason"], "phase": _PHASE}
    req = request if isinstance(request, dict) else {}
    spoken = req.get("spoken_render_payload") or {}
    mode = str(req.get("language_mode") or
                spoken.get("language_mode") or "").lower()
    segs = spoken.get("segments") or []
    csb = spoken.get("code_switch_boundaries") or []
    memory = _memory_signal(req)
    safety = _safety_signal(req)
    pros = _prosody_signal(spoken)
    at = adapter_descriptor.get("adapter_type") or ""
    score = 0.5
    reasons: list[str] = []
    if at == "memory_continuity_audit_metadata_adapter":
        if memory["present"]:
            score += 0.4
            reasons.append("memory_state_present")
        if memory["drift"]:
            score += 0.2
            reasons.append("drift_signal_present")
        if memory["corrections"] >= 1:
            score += 0.1
            reasons.append("recent_corrections_present")
        if memory["total_signal"] >= 5:
            score += 0.05
            reasons.append("rich_memory_signal")
    elif at == "safety_redaction_trace_metadata_adapter":
        if safety["warn"]:
            score += 0.4
            reasons.append("safety_warning_present")
        if safety["total_signal"] >= 3:
            score += 0.3
            reasons.append("redaction_metadata_present")
        if safety["recognition_only"] >= 1 or \
                safety["do_not_use_unprompted"] >= 1:
            score += 0.1
            reasons.append("recognition_only_or_dnu_block")
        if safety["vulgar_offensive_blocks"] >= 1 or \
                safety["voice_safe_replacements"] >= 1:
            score += 0.05
            reasons.append("vulgar_or_voice_safe_block")
    elif at == "bilingual_segment_metadata_adapter":
        if mode in _MIXED_MODES:
            score += 0.4
            reasons.append("mixed_language_mode")
        if csb:
            score += 0.1
            reasons.append("code_switch_boundaries_present")
        if isinstance(segs, list) and len(segs) > 1:
            score += 0.05
            reasons.append("multi_segment_payload")
        if safety["warn"] and safety["total_signal"] < 3:
            score += 0.05
            reasons.append("mild_safety_warning")
    elif at == "prosody_density_metadata_adapter":
        if pros["total"] >= 3:
            score += 0.4
            reasons.append("high_prosody_density")
        if pros.get("pause", 0) >= 1 and pros.get(
                "emphasis", 0) >= 1:
            score += 0.1
            reasons.append("multiple_marker_kinds")
        if csb and pros["total"] >= 2:
            score += 0.05
            reasons.append("code_switch_plus_prosody")
    elif at == "dummy_metadata_adapter":
        if (mode not in _MIXED_MODES and not csb
                and pros["total"] == 0 and not safety["warn"]
                and safety["total_signal"] == 0
                and not memory["present"]):
            score += 0.3
            reasons.append("simple_single_language_payload")
        if isinstance(segs, list) and len(segs) <= 1 \
                and pros["total"] == 0 \
                and safety["total_signal"] == 0 \
                and not memory["present"]:
            score += 0.05
            reasons.append("single_segment_payload")
        if safety["warn"] or pros["total"] >= 3 \
                or safety["total_signal"] >= 3 \
                or memory["present"]:
            score -= 0.2
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
        srta.get_safety_redaction_trace_adapter_descriptor(),
        mcaa.get_memory_continuity_audit_adapter_descriptor(),
    ]


def choose_phase41_adapter(
    request: Any,
    available_descriptors: Optional[list[dict[str, Any]]] = None,
    preferred_adapter: Optional[str] = None,
) -> dict[str, Any]:
    pool = list(available_descriptors or _all_descriptors())
    safe_pool: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for d in pool:
        rej = reject_disallowed_phase41_adapter(d)
        if rej["rejected"]:
            rejected.append({"descriptor": d,
                              "reason": rej["reason"]})
        else:
            safe_pool.append(d)
    if not safe_pool:
        return {
            "ok": False, "chosen": None,
            "reason": "no_safe_adapter_in_pool",
            "rejected": rejected,
            "candidate_adapters": [],
            "score_summary": {}, "phase": _PHASE,
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
                        [d2.get("adapter_name")
                         for d2 in safe_pool],
                    "score_summary": {
                        d.get("adapter_name"):
                            score_phase41_adapter_for_request(
                                request, d).get("score", 0.0)},
                    "phase": _PHASE,
                }
        rejected.append({
            "descriptor": {"adapter_type":
                            preferred_adapter},
            "reason": "preferred_not_in_safe_pool",
        })
    scored = []
    for d in safe_pool:
        s = score_phase41_adapter_for_request(request, d)
        scored.append((d, s.get("score", 0.0), s))
    scored.sort(key=lambda t: t[1], reverse=True)
    top = scored[0]
    return {
        "ok": True, "chosen": top[0],
        "reason": ("highest_score:" +
                    ",".join(top[2].get("reasons", []) or
                              ["default"])),
        "rejected": rejected,
        "candidate_adapters":
            [d.get("adapter_name") for d, _, _ in scored],
        "score_summary":
            {d.get("adapter_name"): score
             for d, score, _ in scored},
        "phase": _PHASE,
    }


def explain_phase41_selection(choice: Any) -> dict[str, Any]:
    if not isinstance(choice, dict):
        return {"ok": False, "summary": "no_choice_dict"}
    chosen = choice.get("chosen") or {}
    return {
        "ok": bool(choice.get("ok")),
        "summary": (
            f"phase41 selection: adapter="
            f"{chosen.get('adapter_name') or 'none'} "
            f"reason={choice.get('reason') or 'unknown'}"),
        "candidate_adapters":
            choice.get("candidate_adapters", []),
        "score_summary": choice.get("score_summary", {}),
        "phase": _PHASE,
    }


def write_phase41_selection_policy_report(
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
    "get_phase41_selection_policy",
    "score_phase41_adapter_for_request",
    "choose_phase41_adapter",
    "explain_phase41_selection",
    "reject_disallowed_phase41_adapter",
    "write_phase41_selection_policy_report",
]
