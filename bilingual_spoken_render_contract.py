"""Phase 25 - Bilingual Spoken Render Contract.

Defines the canonical JSON-serializable payload that future Luna voice / TTS
systems will consume. Pure data + validators. No audio, no TTS, no
subprocess.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, Optional


SUPPORTED_LANGUAGE_MODES = (
    "english_only",
    "russian_only",
    "mixed_en_ru",
    "english_with_russian_terms",
    "russian_with_english_terms",
    "code_switch_sentence_level",
    "code_switch_phrase_level",
    "code_switch_word_level",
)


SUPPORTED_SEGMENT_TYPES = (
    "sentence",
    "phrase",
    "term",
    "word",
    "pause",
    "repair_phrase",
    "clarification",
    "emphasis",
    "safety_redaction",
)


SUPPORTED_PROSODY_FIELDS = (
    "pause_after_ms",
    "emphasis",
    "pace",
    "tone",
    "warmth",
    "confidence",
    "repair_softness",
    "code_switch_boundary",
    "pronunciation_attention",
)


REQUIRED_PAYLOAD_FIELDS = (
    "render_id",
    "created_at",
    "language_mode",
    "conversation_mode",
    "raw_text",
    "voice_safe_text",
    "segments",
    "prosody",
    "code_switch_boundaries",
    "pronunciation_notes",
    "safety_summary",
    "renderer_constraints",
    "metadata",
)


REQUIRED_SEGMENT_FIELDS = (
    "segment_id",
    "text",
    "language",
    "segment_type",
    "start_index",
    "end_index",
)


HARD_SEGMENT_CAP = 200
HARD_TEXT_CHAR_CAP_DEFAULT = 10_000


def get_spoken_render_schema() -> dict[str, Any]:
    return {
        "version": "phase25.v1",
        "required_payload_fields": list(REQUIRED_PAYLOAD_FIELDS),
        "required_segment_fields": list(REQUIRED_SEGMENT_FIELDS),
        "supported_language_modes": list(SUPPORTED_LANGUAGE_MODES),
        "supported_segment_types": list(SUPPORTED_SEGMENT_TYPES),
        "supported_prosody_fields": list(SUPPORTED_PROSODY_FIELDS),
        "hard_segment_cap": HARD_SEGMENT_CAP,
        "hard_text_char_cap_default": HARD_TEXT_CHAR_CAP_DEFAULT,
        "disallowed": [
            "audio_bytes", "audio_url", "audio_path", "wav_bytes",
            "mp3_bytes", "tts_model", "voice_clone_ref",
        ],
        "notes": [
            "Payload is JSON-serializable. No audio data. No file paths.",
            "Renderer is unbound; future adapters consume this contract.",
            "Fail-closed if unsafe text is present at validation time.",
        ],
    }


def get_required_payload_fields() -> list[str]:
    return list(REQUIRED_PAYLOAD_FIELDS)


def get_supported_language_modes() -> list[str]:
    return list(SUPPORTED_LANGUAGE_MODES)


def get_supported_segment_types() -> list[str]:
    return list(SUPPORTED_SEGMENT_TYPES)


def get_supported_prosody_fields() -> list[str]:
    return list(SUPPORTED_PROSODY_FIELDS)


def _new_render_id() -> str:
    return f"render_{int(time.time())}_{uuid.uuid4().hex[:10]}"


def _new_segment_id(idx: int) -> str:
    return f"seg_{idx:04d}_{uuid.uuid4().hex[:6]}"


def _enforce_text_cap(text: str, cap: int) -> tuple[str, bool]:
    if text is None:
        return "", False
    s = str(text)
    if len(s) <= cap:
        return s, False
    return s[:cap], True


def _normalize_segment(seg: dict[str, Any], idx: int) -> dict[str, Any]:
    out: dict[str, Any] = dict(seg) if isinstance(seg, dict) else {}
    out.setdefault("segment_id", _new_segment_id(idx))
    out.setdefault("text", "")
    out.setdefault("language", "und")
    out.setdefault("segment_type", "phrase")
    out.setdefault("start_index", 0)
    out.setdefault("end_index", len(str(out.get("text", ""))))
    out.setdefault("emphasis", "normal")
    out.setdefault("pause_after_ms", 0)
    out.setdefault("pace", "normal")
    out.setdefault("tone", "steady")
    out.setdefault("register", "standard")
    out.setdefault("safety_flags", [])
    out.setdefault("pronunciation_hint", "")
    out.setdefault("notes", "")
    return out


def create_spoken_render_payload(
    text: str,
    language_mode: str,
    voice_style_plan: Optional[dict[str, Any]] = None,
    segments: Optional[list[dict[str, Any]]] = None,
    safety_summary: Optional[dict[str, Any]] = None,
    metadata: Optional[dict[str, Any]] = None,
    conversation_mode: str = "conversation",
    voice_safe_text: Optional[str] = None,
    prosody: Optional[dict[str, Any]] = None,
    code_switch_boundaries: Optional[list[dict[str, Any]]] = None,
    pronunciation_notes: Optional[list[dict[str, Any]]] = None,
    renderer_constraints: Optional[dict[str, Any]] = None,
    text_char_cap: int = HARD_TEXT_CHAR_CAP_DEFAULT,
) -> dict[str, Any]:
    raw, truncated = _enforce_text_cap(text or "", text_char_cap)
    vst = voice_safe_text if voice_safe_text is not None else raw
    vst, vst_truncated = _enforce_text_cap(vst, text_char_cap)
    segs = list(segments or [])[:HARD_SEGMENT_CAP]
    norm_segs = [_normalize_segment(s, i) for i, s in enumerate(segs)]
    boundaries = list(code_switch_boundaries or [])[:HARD_SEGMENT_CAP]
    pron_notes = list(pronunciation_notes or [])[:HARD_SEGMENT_CAP]
    md = dict(metadata or {})
    if truncated:
        md["raw_text_truncated_to_cap"] = text_char_cap
    if vst_truncated:
        md["voice_safe_text_truncated_to_cap"] = text_char_cap
    md.setdefault("phase", "phase25")
    md.setdefault("renderer_bound", False)
    return {
        "render_id": _new_render_id(),
        "created_at": time.time(),
        "language_mode": str(language_mode),
        "conversation_mode": str(conversation_mode),
        "raw_text": raw,
        "voice_safe_text": vst,
        "segments": norm_segs,
        "prosody": dict(prosody or {}),
        "code_switch_boundaries": boundaries,
        "pronunciation_notes": pron_notes,
        "safety_summary": dict(safety_summary or {}),
        "personality_profile_ref": (voice_style_plan or {}).get(
            "personality_profile_ref"),
        "style_plan_ref": (voice_style_plan or {}).get("plan_id"),
        "turn_strategy": (voice_style_plan or {}).get("turn_strategy"),
        "renderer_constraints": dict(renderer_constraints or {}),
        "metadata": md,
    }


def _is_disallowed_field(k: str) -> bool:
    return k in {"audio_bytes", "audio_url", "audio_path", "wav_bytes",
                  "mp3_bytes", "tts_model", "voice_clone_ref"}


def validate_spoken_render_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"ok": False, "reason": "payload_not_dict"}
    missing = [f for f in REQUIRED_PAYLOAD_FIELDS if f not in payload]
    if missing:
        return {"ok": False, "reason": "missing_required",
                "missing": missing}
    lm = payload.get("language_mode")
    if lm not in SUPPORTED_LANGUAGE_MODES:
        return {"ok": False,
                "reason": f"unsupported_language_mode: {lm!r}"}
    segs = payload.get("segments") or []
    if not isinstance(segs, list):
        return {"ok": False, "reason": "segments_not_list"}
    if len(segs) > HARD_SEGMENT_CAP:
        return {"ok": False, "reason": "segment_cap_exceeded",
                "limit": HARD_SEGMENT_CAP, "actual": len(segs)}
    for i, s in enumerate(segs):
        if not isinstance(s, dict):
            return {"ok": False, "reason": f"segment_{i}_not_dict"}
        s_missing = [f for f in REQUIRED_SEGMENT_FIELDS if f not in s]
        if s_missing:
            return {"ok": False,
                    "reason": f"segment_{i}_missing_required",
                    "missing": s_missing}
        if s["segment_type"] not in SUPPORTED_SEGMENT_TYPES:
            return {"ok": False,
                    "reason": f"segment_{i}_unsupported_segment_type",
                    "segment_type": s["segment_type"]}
    # Disallowed-field scan (top-level keys + metadata)
    for k in payload.keys():
        if _is_disallowed_field(k):
            return {"ok": False, "reason": f"disallowed_field: {k!r}"}
    md = payload.get("metadata") or {}
    if isinstance(md, dict):
        for k in md.keys():
            if _is_disallowed_field(k):
                return {"ok": False,
                        "reason": f"disallowed_metadata_field: {k!r}"}
    # JSON-serializable
    try:
        json.dumps(payload, ensure_ascii=False, default=str)
    except Exception as e:
        return {"ok": False, "reason": f"not_json_serializable: {e}"}
    # Fail-closed if safety_summary signals unsafe leakage
    sas = payload.get("safety_summary") or {}
    if isinstance(sas, dict) and sas.get("unsafe_leakage_detected"):
        return {"ok": False,
                "reason": "unsafe_text_present_in_payload",
                "safety_summary": sas}
    return {"ok": True, "n_segments": len(segs),
            "raw_text_chars": len(payload.get("raw_text") or "")}


def normalize_render_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"ok": False, "reason": "payload_not_dict"}
    p = dict(payload)
    p.setdefault("render_id", _new_render_id())
    p.setdefault("created_at", time.time())
    p.setdefault("conversation_mode", "conversation")
    p.setdefault("raw_text", "")
    p.setdefault("voice_safe_text", p["raw_text"])
    p.setdefault("segments", [])
    p["segments"] = [_normalize_segment(s, i)
                      for i, s in enumerate(p["segments"][:HARD_SEGMENT_CAP])]
    p.setdefault("prosody", {})
    p.setdefault("code_switch_boundaries", [])
    p.setdefault("pronunciation_notes", [])
    p.setdefault("safety_summary", {})
    p.setdefault("renderer_constraints", {})
    p.setdefault("metadata", {})
    return {"ok": True, "payload": p}


def write_spoken_render_contract_report(report: dict[str, Any],
                                        output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "SUPPORTED_LANGUAGE_MODES", "SUPPORTED_SEGMENT_TYPES",
    "SUPPORTED_PROSODY_FIELDS", "REQUIRED_PAYLOAD_FIELDS",
    "REQUIRED_SEGMENT_FIELDS", "HARD_SEGMENT_CAP",
    "HARD_TEXT_CHAR_CAP_DEFAULT",
    "get_spoken_render_schema", "get_required_payload_fields",
    "get_supported_language_modes", "get_supported_segment_types",
    "get_supported_prosody_fields",
    "create_spoken_render_payload",
    "validate_spoken_render_payload",
    "normalize_render_payload",
    "write_spoken_render_contract_report",
]
