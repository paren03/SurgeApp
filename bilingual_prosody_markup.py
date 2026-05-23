"""Phase 25 - Bilingual Prosody Markup.

Generates inspectable prosody plans for the spoken-render payload. No SSML
dependency, no audio.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional


_HARD_SEG_CAP = 200


_PROSODY_FIELDS = (
    "pause_after_ms", "emphasis", "pace", "tone", "warmth",
    "confidence", "repair_softness", "code_switch_boundary",
    "pronunciation_attention",
)


_EMOTIONAL_TONES = (
    "steady", "warm", "curious", "gentle", "thoughtful",
    "encouraging", "concerned", "neutral",
)


def segment_text_for_prosody(text: str,
                             language_mode: str = "mixed_en_ru"
                             ) -> list[dict[str, Any]]:
    """Split by sentence boundaries first; within each sentence detect
    EN/RU runs as separate phrase segments."""
    s = text or ""
    sentences = [t for t in re.split(r"(?<=[.!?])\s+|\n+", s) if t.strip()]
    out: list[dict[str, Any]] = []
    seg_idx = 0
    cursor = 0
    for sent in sentences:
        sent_start = s.find(sent, cursor)
        if sent_start < 0:
            sent_start = cursor
        cursor = sent_start + len(sent)
        # Within-sentence: split into runs of EN vs RU vs other.
        run_start = sent_start
        run_lang = None
        run_text: list[str] = []
        for m in re.finditer(r"\S+|\s+", sent):
            tok = m.group(0)
            if tok.strip() == "":
                if run_text:
                    run_text.append(tok)
                continue
            cyr = any("Ѐ" <= c <= "ӿ" for c in tok)
            lat = any(c.isalpha() and c.isascii() for c in tok)
            lang = "ru" if cyr and not lat else \
                   "en" if lat and not cyr else \
                   "mixed_token" if cyr and lat else "und"
            if run_lang is None:
                run_lang = lang
                run_text = [tok]
            elif lang == run_lang or lang == "mixed_token":
                run_text.append(tok)
            else:
                # Flush run.
                phrase = "".join(run_text).strip()
                if phrase:
                    out.append(_make_segment(seg_idx, phrase,
                                              run_lang, sent_start, sent_start + len(phrase)))
                    seg_idx += 1
                    if len(out) >= _HARD_SEG_CAP:
                        return out
                run_lang = lang
                run_text = [tok]
        phrase = "".join(run_text).strip()
        if phrase:
            out.append(_make_segment(seg_idx, phrase, run_lang or "und",
                                      sent_start, sent_start + len(phrase)))
            seg_idx += 1
            if len(out) >= _HARD_SEG_CAP:
                return out
    return out


def _make_segment(idx: int, text: str, lang: str,
                  start: int, end: int) -> dict[str, Any]:
    return {
        "segment_id": f"pseg_{idx:04d}",
        "text": text,
        "language": lang,
        "segment_type": "phrase",
        "start_index": int(start),
        "end_index": int(end),
        "emphasis": "normal",
        "pause_after_ms": 0,
        "pace": "normal",
        "tone": "steady",
        "register": "standard",
        "safety_flags": [],
        "pronunciation_hint": "",
        "notes": "",
    }


def assign_pause_hints(segments: list[dict[str, Any]],
                       conversation_mode: str = "conversation"
                       ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    base = 180 if conversation_mode in ("teacher", "curriculum") else 120
    for i, s in enumerate(segments):
        s2 = dict(s)
        text = (s2.get("text") or "")
        # End-of-sentence punctuation -> longer pause.
        if re.search(r"[.!?]\s*$", text):
            s2["pause_after_ms"] = base + 60
        elif re.search(r"[,;:]\s*$", text):
            s2["pause_after_ms"] = base // 2
        else:
            s2["pause_after_ms"] = base // 3
        out.append(s2)
    return out


def assign_emphasis_hints(segments: list[dict[str, Any]],
                          conversation_mode: str = "conversation"
                          ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in segments:
        s2 = dict(s)
        t = (s2.get("text") or "")
        em = "normal"
        if re.search(r"\*[^*]+\*", t) or t.isupper() and len(t.split()) <= 4:
            em = "strong"
        elif conversation_mode in ("teacher", "curriculum") \
                and len((t.split())) >= 5:
            em = "moderate"
        s2["emphasis"] = em
        out.append(s2)
    return out


def assign_pace_hints(segments: list[dict[str, Any]],
                      conversation_mode: str = "conversation"
                      ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    pace = ("slow" if conversation_mode in ("teacher", "curriculum")
            else "fast" if conversation_mode == "concise"
            else "normal")
    for s in segments:
        s2 = dict(s)
        s2["pace"] = pace
        out.append(s2)
    return out


def assign_tone_hints(segments: list[dict[str, Any]],
                      emotional_tone: str = "steady"
                      ) -> list[dict[str, Any]]:
    tone = emotional_tone if emotional_tone in _EMOTIONAL_TONES else "steady"
    out: list[dict[str, Any]] = []
    for s in segments:
        s2 = dict(s)
        s2["tone"] = tone
        out.append(s2)
    return out


def mark_code_switch_boundaries(segments: list[dict[str, Any]]
                                ) -> list[dict[str, Any]]:
    """Annotate segments where the language differs from the previous
    segment as code-switch boundaries."""
    out: list[dict[str, Any]] = []
    boundaries: list[dict[str, Any]] = []
    last_lang = None
    for s in segments:
        s2 = dict(s)
        cur_lang = s2.get("language", "und")
        is_boundary = (last_lang is not None and cur_lang != last_lang
                        and cur_lang in ("en", "ru")
                        and last_lang in ("en", "ru"))
        s2["code_switch_boundary"] = is_boundary
        if is_boundary:
            boundaries.append({"segment_id": s2.get("segment_id"),
                                "from_lang": last_lang,
                                "to_lang": cur_lang,
                                "start_index": s2.get("start_index"),
                                "end_index": s2.get("end_index")})
        last_lang = cur_lang
        out.append(s2)
    return out, boundaries  # type: ignore[return-value]


def validate_prosody_plan(plan: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan, dict):
        return {"ok": False, "reason": "plan_not_dict"}
    segs = plan.get("segments") or []
    if not isinstance(segs, list):
        return {"ok": False, "reason": "segments_not_list"}
    if len(segs) > _HARD_SEG_CAP:
        return {"ok": False, "reason": "segment_cap_exceeded",
                "limit": _HARD_SEG_CAP}
    for i, s in enumerate(segs):
        if not isinstance(s, dict):
            return {"ok": False, "reason": f"segment_{i}_not_dict"}
        for f in _PROSODY_FIELDS:
            if f == "code_switch_boundary":
                continue
            if f not in s:
                # Plan may not assign every field per segment; allow missing.
                continue
        pa = s.get("pause_after_ms", 0)
        if not isinstance(pa, (int, float)) or pa < 0:
            return {"ok": False,
                    "reason": f"segment_{i}_bad_pause_after_ms"}
    return {"ok": True, "n_segments": len(segs)}


def create_prosody_plan(text: str,
                        language_mode: str = "mixed_en_ru",
                        conversation_mode: str = "conversation",
                        emotional_tone: str = "steady"
                        ) -> dict[str, Any]:
    segs = segment_text_for_prosody(text, language_mode)
    segs = assign_pause_hints(segs, conversation_mode)
    segs = assign_emphasis_hints(segs, conversation_mode)
    segs = assign_pace_hints(segs, conversation_mode)
    segs = assign_tone_hints(segs, emotional_tone)
    segs, boundaries = mark_code_switch_boundaries(segs)
    plan = {
        "language_mode": language_mode,
        "conversation_mode": conversation_mode,
        "emotional_tone": emotional_tone if emotional_tone in _EMOTIONAL_TONES
                          else "steady",
        "segments": segs,
        "code_switch_boundaries": boundaries,
    }
    plan["validation"] = validate_prosody_plan(plan)
    return plan


def write_prosody_report(report: dict[str, Any],
                         output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "create_prosody_plan",
    "segment_text_for_prosody",
    "assign_pause_hints",
    "assign_emphasis_hints",
    "assign_pace_hints",
    "assign_tone_hints",
    "mark_code_switch_boundaries",
    "validate_prosody_plan",
    "write_prosody_report",
]
