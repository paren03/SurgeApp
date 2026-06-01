"""Phase 27 — Voice-Render Adapter Policy.

Choose the safest future renderer strategy for a spoken-render payload
WITHOUT invoking any renderer. Dry-run only.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_adapter_contract as vac


_POLICY_VERSION = "phase27.policy.v1"


def get_adapter_selection_policy() -> dict[str, Any]:
    return {
        "version": _POLICY_VERSION,
        "prefer_dry_run_renderer": True,
        "reject_non_dry_run": True,
        "reject_subprocess_required": True,
        "reject_network_required": True,
        "reject_voice_clone_required": True,
        "reject_audio_file_required": True,
        "prefer_code_switching_for_mixed": True,
        "prefer_pronunciation_for_ru_or_mixed": True,
        "prefer_prosody_when_available": True,
        "fail_closed_on_unsupported": True,
        "supported_languages_required": ["en", "ru", "mixed"],
    }


_MIXED_MODES = {
    "mixed", "mixed_en_ru", "english_with_russian_terms",
    "russian_with_english_terms",
}
_RU_MODES = {"russian", "russian_only", "russian_with_english_terms"}


def _is_mixed_payload(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    mode = str(payload.get("language_mode") or "").lower()
    if mode in _MIXED_MODES:
        return True
    boundaries = payload.get("code_switch_boundaries") or []
    return isinstance(boundaries, list) and len(boundaries) > 0


def _is_ru_payload(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    mode = str(payload.get("language_mode") or "").lower()
    return mode in _RU_MODES


def _payload_text_len(payload: dict[str, Any]) -> int:
    if not isinstance(payload, dict):
        return 0
    txt = payload.get("voice_safe_text") or payload.get("normalized_text") \
        or ""
    return len(str(txt))


def _payload_segment_count(payload: dict[str, Any]) -> int:
    if not isinstance(payload, dict):
        return 0
    segs = payload.get("segments") or []
    return len(segs) if isinstance(segs, list) else 0


def _payload_languages(payload: dict[str, Any]) -> set[str]:
    """Return the set of languages the payload claims. Includes literal
    'unknown' if the payload's mode or any segment language is not
    one of en/ru/mixed; the policy uses that to reject the payload as
    unsupported."""
    out: set[str] = set()
    if not isinstance(payload, dict):
        return out
    mode = str(payload.get("language_mode") or "").lower()
    if mode in _MIXED_MODES:
        out.update({"en", "ru", "mixed"})
    elif "english" in mode or mode == "en" or mode.startswith("en_"):
        out.add("en")
    elif "russian" in mode or mode == "ru" or mode.startswith("ru_"):
        out.add("ru")
    elif mode:
        out.add("unknown")
    for seg in (payload.get("segments") or []):
        if isinstance(seg, dict):
            lang = str(seg.get("language") or "").lower()
            if lang in ("en", "ru", "mixed"):
                out.add(lang)
            elif lang and lang not in ("und", ""):
                out.add("unknown")
    return out


def score_adapter_compatibility(
    payload: dict[str, Any],
    adapter_descriptor: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(adapter_descriptor, dict):
        return {"score": 0.0, "compatible": False,
                "reasons": ["descriptor_not_dict"]}
    val = vac.validate_voice_adapter_descriptor(adapter_descriptor)
    if not val["ok"]:
        return {"score": 0.0, "compatible": False,
                "reasons": ["descriptor_invalid"] + val["reasons"]}
    reasons: list[str] = []
    score = 0.5  # baseline for any valid dry-run descriptor

    # Language coverage
    supports = {str(x).lower() for x in adapter_descriptor.get(
        "supports_languages") or []}
    needed = _payload_languages(payload)
    if "unknown" in needed:
        reasons.append("language_not_supported")
        return {"score": 0.0, "compatible": False, "reasons": reasons}
    if needed and not (needed & supports) and "mixed" not in supports:
        reasons.append("language_not_supported")
        return {"score": 0.0, "compatible": False, "reasons": reasons}
    if needed and needed.issubset(supports | {"mixed"}):
        score += 0.1

    # Mixed / code-switch preference
    if _is_mixed_payload(payload):
        if adapter_descriptor.get("supports_code_switching"):
            score += 0.15
        else:
            reasons.append("missing_code_switching_for_mixed")

    # Russian or mixed → pronunciation hints
    if _is_ru_payload(payload) or _is_mixed_payload(payload):
        if adapter_descriptor.get("supports_pronunciation_hints"):
            score += 0.1
        else:
            reasons.append("missing_pronunciation_hints_for_ru_or_mixed")

    # Prosody preference
    if adapter_descriptor.get("supports_prosody"):
        score += 0.1

    # Segments preference
    if adapter_descriptor.get("supports_segments"):
        score += 0.05

    # Text / segment caps
    txt_len = _payload_text_len(payload)
    if txt_len > int(adapter_descriptor.get("max_text_chars",
                                            vac.HARD_TEXT_CHAR_CAP)):
        reasons.append("payload_exceeds_max_text_chars")
        return {"score": 0.0, "compatible": False, "reasons": reasons}
    seg_count = _payload_segment_count(payload)
    if seg_count > int(adapter_descriptor.get("max_segments",
                                              vac.HARD_SEGMENT_CAP)):
        reasons.append("payload_exceeds_max_segments")
        return {"score": 0.0, "compatible": False, "reasons": reasons}

    # Safety summary
    safety = (payload.get("safety_summary") if isinstance(payload, dict)
              else None) or {}
    if safety.get("blocked") or safety.get("unsafe"):
        reasons.append("payload_unsafe")
        return {"score": 0.0, "compatible": False, "reasons": reasons}

    score = max(0.0, min(1.0, score))
    return {"score": score, "compatible": True, "reasons": reasons}


def enforce_adapter_safety_policy(
    adapter_descriptor: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    if not isinstance(adapter_descriptor, dict):
        return {"allowed": False, "reasons": ["descriptor_not_dict"]}
    if adapter_descriptor.get("dry_run") is not True:
        reasons.append("non_dry_run_blocked")
    fra = {str(x).lower() for x in adapter_descriptor.get(
        "forbidden_runtime_actions") or []}
    required_blocks = {
        "audio_generation", "tts_invocation", "voice_cloning",
        "subprocess_execution", "powershell_invocation",
        "network_call", "audio_file_write",
    }
    missing = sorted(required_blocks - fra)
    if missing:
        reasons.append("forbidden_actions_not_blocked:" + ",".join(missing))
    if isinstance(payload, dict):
        safety = payload.get("safety_summary") or {}
        if safety.get("blocked") or safety.get("unsafe"):
            reasons.append("payload_unsafe")
    return {"allowed": not reasons, "reasons": reasons}


def reject_runtime_execution_attempt(
    adapter_descriptor: dict[str, Any],
) -> dict[str, Any]:
    """Always returns a rejection plan in Phase 27 — runtime execution
    of any adapter is out of scope."""
    return {
        "rejected": True,
        "reason": "phase27_runtime_execution_not_permitted",
        "adapter_name": (adapter_descriptor or {}).get(
            "adapter_name", "unknown"),
        "policy_version": _POLICY_VERSION,
        "advice": "All Phase 27 adapter paths are dry-run only. "
                  "Execution is deferred to a future operator-gated phase.",
    }


def choose_adapter_for_payload(
    payload: dict[str, Any],
    available_adapters: Optional[list[dict[str, Any]]] = None,
    voice_memory_state: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    pool: list[dict[str, Any]] = list(available_adapters or [])
    if not pool:
        # Built-in fallback: dry-run basic.
        pool = [vac.create_voice_adapter_descriptor(
            "dry_run_basic", "dry_run_renderer",
            capabilities={
                "supports_languages": ["en", "ru", "mixed"],
                "supports_code_switching": True,
                "supports_segments": True,
                "supports_prosody": True,
                "supports_pronunciation_hints": True,
            })]
    candidates: list[dict[str, Any]] = []
    for desc in pool:
        safety = enforce_adapter_safety_policy(desc, payload)
        if not safety["allowed"]:
            continue
        score_info = score_adapter_compatibility(payload, desc)
        if not score_info["compatible"]:
            continue
        candidates.append({
            "descriptor": desc,
            "score": score_info["score"],
            "compatibility_reasons": score_info["reasons"],
            "safety_reasons": safety["reasons"],
        })
    if not candidates:
        return {
            "chosen": None,
            "score": 0.0,
            "compatibility_reasons": ["no_compatible_adapter"],
            "safety_reasons": [],
            "policy_version": _POLICY_VERSION,
            "candidates_considered": len(pool),
            "advice": ("No compatible dry-run adapter for this payload. "
                       "Refusing to fall back to runtime execution."),
        }
    candidates.sort(key=lambda c: c["score"], reverse=True)
    top = candidates[0]
    return {
        "chosen": top["descriptor"],
        "score": top["score"],
        "compatibility_reasons": top["compatibility_reasons"],
        "safety_reasons": top["safety_reasons"],
        "policy_version": _POLICY_VERSION,
        "candidates_considered": len(pool),
    }


def explain_adapter_choice(choice: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(choice, dict):
        return {"ok": False, "summary": "no_choice_dict"}
    chosen = choice.get("chosen")
    if not chosen:
        return {
            "ok": True,
            "summary": "no_adapter_chosen",
            "reasons": choice.get("compatibility_reasons", []),
            "policy_version": choice.get("policy_version"),
        }
    return {
        "ok": True,
        "summary": (f"chose:{chosen.get('adapter_name')}"
                    f" type:{chosen.get('adapter_type')}"
                    f" score:{choice.get('score'):.2f}"),
        "adapter_name": chosen.get("adapter_name"),
        "adapter_type": chosen.get("adapter_type"),
        "score": choice.get("score"),
        "compatibility_reasons": choice.get("compatibility_reasons", []),
        "safety_reasons": choice.get("safety_reasons", []),
        "policy_version": choice.get("policy_version"),
        "dry_run": True,
    }


def write_adapter_policy_report(
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
    "get_adapter_selection_policy",
    "score_adapter_compatibility",
    "choose_adapter_for_payload",
    "explain_adapter_choice",
    "enforce_adapter_safety_policy",
    "reject_runtime_execution_attempt",
    "write_adapter_policy_report",
]
