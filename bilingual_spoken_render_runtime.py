"""Phase 25 - Bilingual Spoken Render Runtime.

Single standalone entry point for spoken-render payload building. NOT
integrated. No audio, no TTS, no subprocess.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import bilingual_voice_style_runtime as vsr
import bilingual_language_mode_detector as lmd
import bilingual_voice_text_normalizer as vtn
import bilingual_prosody_markup as pmk
import bilingual_pronunciation_hinting as pph
import bilingual_spoken_safety_redactor as ssr
import bilingual_spoken_render_contract as src
import bilingual_voice_renderer_interface as vri


_DEFAULT_LIMIT = 25
_HARD_LIMIT = 100


def _clamp(n: Optional[int]) -> int:
    if n is None:
        return _DEFAULT_LIMIT
    try:
        v = int(n)
    except Exception:
        return _DEFAULT_LIMIT
    return max(1, min(v, _HARD_LIMIT))


def build_voice_safe_render_payload(text: str,
                                    language_mode: str = "mixed_en_ru",
                                    conversation_mode: str = "conversation",
                                    is_user_prompted: bool = False
                                    ) -> dict[str, Any]:
    """Lighter helper: just normalize + redact + segment + payload."""
    norm = vtn.normalize_for_spoken_render(
        text, language_mode=language_mode,
        conversation_mode=conversation_mode,
        is_user_prompted=is_user_prompted)
    red = ssr.redact_for_spoken_voice(
        norm["text"], language_mode=language_mode,
        conversation_mode=conversation_mode,
        is_user_prompted=is_user_prompted)
    prosody = pmk.create_prosody_plan(
        red["voice_safe_text"], language_mode=language_mode,
        conversation_mode=conversation_mode)
    pron_hints = pph.detect_pronunciation_sensitive_terms(
        red["voice_safe_text"], language_mode)
    payload = src.create_spoken_render_payload(
        text=norm["text"],
        language_mode=language_mode,
        voice_safe_text=red["voice_safe_text"],
        segments=prosody["segments"],
        prosody={"emotional_tone": prosody.get("emotional_tone")},
        code_switch_boundaries=prosody["code_switch_boundaries"],
        pronunciation_notes=pron_hints,
        safety_summary=red["safety_summary"],
        conversation_mode=conversation_mode,
        renderer_constraints={"audio_in_phase": False,
                              "tts_in_phase": False,
                              "subprocess_in_phase": False})
    val = src.validate_spoken_render_payload(payload)
    return {"ok": val["ok"], "payload": payload, "validation": val,
            "normalization": norm, "redaction": red,
            "prosody_validation": prosody.get("validation")}


def build_spoken_render_payload(
    user_text: str,
    draft_response_text: str = "",
    conversation_state: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    user_preference: Optional[str] = None,
    is_user_prompted: bool = False,
    limit: int = _DEFAULT_LIMIT,
    link_db_path: Optional[str] = None,
) -> dict[str, Any]:
    cap = _clamp(limit)
    # Voice-style planning from Phase 24
    vs_plan = vsr.get_bilingual_voice_style_plan(
        user_text, conversation_state=conversation_state,
        conversation_mode=conversation_mode,
        user_preference=user_preference,
        is_user_prompted=is_user_prompted, limit=cap,
        link_db_path=link_db_path)
    language_detection = {
        "detected_mode": vs_plan["detected_language_mode"],
        "chosen_spoken_mode": vs_plan["chosen_spoken_mode"],
    }
    spoken_mode = vs_plan["chosen_spoken_mode"]
    # Use draft if provided, else use the skeleton's spoken stubs.
    if draft_response_text:
        text_for_render = draft_response_text
    else:
        steps = (vs_plan.get("demo_response_skeleton", {}) or {}
                 ).get("skeleton_steps", [])
        text_for_render = " ".join(s.get("text", "") for s in steps).strip()
        if not text_for_render:
            text_for_render = user_text
    # Build voice-safe payload over the chosen text.
    safe_pack = build_voice_safe_render_payload(
        text_for_render, language_mode=spoken_mode,
        conversation_mode=conversation_mode,
        is_user_prompted=is_user_prompted)
    payload = safe_pack["payload"]
    # Attach style plan back-references in the payload metadata.
    payload["metadata"]["style_plan_ref"] = "phase24_voice_style"
    payload["metadata"]["personality_profile_ref"] = "luna_bilingual_v1"
    payload["turn_strategy"] = vs_plan.get("turn_strategy")
    # Renderer request, dry_run only.
    rr = vri.create_renderer_request_from_payload(payload)
    rr_val = vri.validate_renderer_request(rr)
    sim = vri.simulate_renderer_acceptance(payload)
    # Pronunciation hint subgroup helpers
    p_en = pph.create_english_pronunciation_hints(payload["segments"])
    p_ru = pph.create_russian_pronunciation_hints(payload["segments"])
    p_cs = pph.create_code_switch_pronunciation_hints(payload["segments"])
    return {
        "ok": safe_pack["ok"] and rr_val["ok"],
        "language_detection": language_detection,
        "voice_style_plan": {
            "detected_language_mode": vs_plan["detected_language_mode"],
            "chosen_spoken_mode": spoken_mode,
            "spoken_register": vs_plan["spoken_register"],
            "code_switch_density": vs_plan["code_switch_density"],
        },
        "normalized_text": safe_pack["normalization"]["text"],
        "voice_safe_text": payload["voice_safe_text"],
        "segments": payload["segments"],
        "prosody_plan": payload["prosody"],
        "pronunciation_hints": {
            "english_hints": p_en,
            "russian_hints": p_ru,
            "code_switch_hints": p_cs,
            "sensitive_terms": payload["pronunciation_notes"],
        },
        "code_switch_boundaries": payload["code_switch_boundaries"],
        "safety_summary": payload["safety_summary"],
        "renderer_request_dry_run": rr,
        "validation": {
            "payload": safe_pack["validation"],
            "renderer_request": rr_val,
            "simulated_acceptance": sim,
        },
        "gap_notes": vs_plan.get("gap_notes"),
    }


def validate_and_prepare_renderer_request(
    payload: dict[str, Any],
    renderer_name: str = "unbound_future_renderer",
) -> dict[str, Any]:
    request = vri.create_renderer_request_from_payload(
        payload, renderer_name=renderer_name)
    val = vri.validate_renderer_request(request)
    return {"ok": val["ok"], "request": request, "validation": val}


def demo_spoken_render_payloads(limit: int = 12) -> dict[str, Any]:
    cap = max(1, min(int(limit), 20))
    scenarios = [
        ("Hello! Can you explain a lighthouse?", "conversation", None),
        ("Привет! Расскажи мне про маяк.", "conversation", None),
        ("Hello, я инженер and I work hard.", "conversation", None),
        ("Translate 'verse' to Russian.", "translation_help", None),
        ("Let's practice Russian together.", "bilingual_practice",
         "russian"),
        ("Explain vectors precisely.", "teacher", None),
        ("idk", "conversation", None),
        ("I'm feeling tired today.", "warm_friend", None),
        ("Build me a function that adds two numbers.", "coding", None),
        ("Just chat with me casually.", "warm_friend", "mix"),
        ("Skazhi mne po-russki, kak dela.", "conversation", None),
        ("Show me NASA, FAA, and CNN.", "teacher", None),
    ][:cap]
    out: list[dict[str, Any]] = []
    for text, mode, pref in scenarios:
        payload_full = build_spoken_render_payload(
            text, conversation_mode=mode, user_preference=pref, limit=5)
        out.append({
            "user_text": text,
            "conversation_mode": mode,
            "preference": pref,
            "detected": payload_full["language_detection"]["detected_mode"],
            "spoken": payload_full["language_detection"]["chosen_spoken_mode"],
            "n_segments": len(payload_full["segments"]),
            "renderer_dry_run": payload_full["renderer_request_dry_run"]["dry_run"],
            "validation_ok": payload_full["validation"]["payload"]["ok"],
        })
    return {"ok": True, "count": len(out), "scenarios": out}


def write_spoken_render_runtime_report(report: dict[str, Any],
                                       output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "build_spoken_render_payload",
    "build_voice_safe_render_payload",
    "validate_and_prepare_renderer_request",
    "demo_spoken_render_payloads",
    "write_spoken_render_runtime_report",
]
