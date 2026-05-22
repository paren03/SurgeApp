"""Phase 26 - Bilingual Voice Correction Memory.

Tracks bounded style corrections like "say it less formal" / "use more
Russian words" within a session. Session-only by default. Newer
corrections override older conflicting ones. Safety policy always
overrides.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional


_HARD_CORRECTIONS_CAP = 20
_DEFAULT_ACTIVE_CAP = 10


CORRECTION_TYPES = (
    "more_russian", "more_english", "mix_more", "mix_less",
    "less_formal", "more_formal", "simpler", "more_technical",
    "less_slang", "more_natural", "slower_spoken_style",
    "shorter_answers", "longer_explanations",
    "pronunciation_focus", "grammar_correction_focus",
)


# Per-type regex hint banks (English + Russian + mixed).
_TYPE_HINTS: dict[str, tuple[str, ...]] = {
    "more_russian": (
        r"\b(speak more russian|use more russian|more russian please|"
        r"use russian words|switch to russian)\b",
        r"\b(говори больше по\-?русски|используй больше русских|"
        r"переключись на русский|больше русского)\b",
    ),
    "more_english": (
        r"\b(speak more english|use more english|more english please|"
        r"use english words|switch to english)\b",
        r"\b(говори больше по\-?английски|больше английского|"
        r"переключись на английский)\b",
    ),
    "mix_more": (
        r"\b(mix more|mix both|use both languages|"
        r"more mixing|one word english one word russian)\b",
        r"\b(смешивай больше|на двух языках)\b",
    ),
    "mix_less": (
        r"\b(stop mixing|less mixing|don'?t mix|keep one language)\b",
        r"\b(не смешивай|перестань смешивать|меньше смешивай)\b",
    ),
    "less_formal": (
        r"\b(less formal|more casual|drop the formality|too formal)\b",
        r"\b(попроще|менее формально|разговорнее|без официоза)\b",
    ),
    "more_formal": (
        r"\b(more formal|be professional|business tone)\b",
        r"\b(более формально|официально|по\-деловому)\b",
    ),
    "simpler": (
        r"\b(simpler please|say it simpler|too complex|"
        r"keep it simple|plain language)\b",
        r"\b(проще|по\-простому|объясни попроще)\b",
    ),
    "more_technical": (
        r"\b(more technical|be precise|use technical terms|"
        r"give the formal definition)\b",
        r"\b(техничнее|точнее|формально|термины)\b",
    ),
    "less_slang": (
        r"\b(no slang|less slang|drop the slang|stop using slang)\b",
        r"\b(без сленга|меньше сленга)\b",
    ),
    "more_natural": (
        r"\b(more natural|sound natural|less robotic|"
        r"talk normally)\b",
        r"\b(естественнее|менее роботизированно|говори нормально)\b",
    ),
    "slower_spoken_style": (
        r"\b(speak slower|slow down|slower pace)\b",
        r"\b(помедленнее|потише|медленнее)\b",
    ),
    "shorter_answers": (
        r"\b(shorter answers|keep it short|be concise|tldr|"
        r"too long)\b",
        r"\b(покороче|коротко|кратко)\b",
    ),
    "longer_explanations": (
        r"\b(more detail|explain more|deeper|elaborate|longer answer)\b",
        r"\b(подробнее|глубже|больше деталей)\b",
    ),
    "pronunciation_focus": (
        r"\b(focus on pronunciation|fix my pronunciation|"
        r"check my pronunciation)\b",
        r"\b(произношение|проверь моё произношение|"
        r"объясни произношение)\b",
    ),
    "grammar_correction_focus": (
        r"\b(correct my grammar|fix my grammar|grammar check|"
        r"correct my russian|correct my english)\b",
        r"\b(исправляй грамматику|исправляй мой русский|"
        r"исправляй мой английский)\b",
    ),
}


# Pairs of correction types that conflict; newer overrides older.
_CONFLICTING_PAIRS = (
    ("more_russian", "more_english"),
    ("mix_more", "mix_less"),
    ("less_formal", "more_formal"),
    ("simpler", "more_technical"),
    ("shorter_answers", "longer_explanations"),
)


def classify_correction(text: str) -> dict[str, Any]:
    s = (text or "").lower()
    hits: list[dict[str, Any]] = []
    for ct, patterns in _TYPE_HINTS.items():
        matched = []
        for p in patterns:
            for m in re.finditer(p, s):
                matched.append(m.group(0))
        if matched:
            hits.append({"type": ct, "evidence": matched[:5],
                          "confidence": min(1.0,
                                             0.6 + 0.1 * len(matched))})
    if not hits:
        return {"detected": False, "type": None,
                "confidence": 0.0, "evidence": []}
    # Highest-confidence first
    hits.sort(key=lambda h: h["confidence"], reverse=True)
    top = hits[0]
    return {"detected": True, "type": top["type"],
            "confidence": top["confidence"],
            "evidence": top["evidence"], "all_matches": hits}


def create_correction_record(text: str, correction_type: str,
                              confidence: float = 0.5
                              ) -> dict[str, Any]:
    ct = correction_type if correction_type in CORRECTION_TYPES \
        else "unknown_correction"
    return {
        "type": ct,
        "confidence": max(0.0, min(1.0, float(confidence))),
        "text": (text or "")[:240],
        "ts": time.time(),
    }


def _resolve_conflicts(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Walk older→newer; if a newer record conflicts with an older one,
    drop the older."""
    out: list[dict[str, Any]] = []
    for rec in records:
        rec_type = rec.get("type")
        to_drop_idxs: list[int] = []
        for i, prev in enumerate(out):
            for a, b in _CONFLICTING_PAIRS:
                if (prev.get("type") == a and rec_type == b) \
                        or (prev.get("type") == b and rec_type == a):
                    to_drop_idxs.append(i)
                    break
        for i in reversed(to_drop_idxs):
            del out[i]
        out.append(rec)
    return out[-_HARD_CORRECTIONS_CAP:]


def apply_correction_to_state(state: dict[str, Any],
                                correction_text: str) -> dict[str, Any]:
    if not isinstance(state, dict):
        return state
    cls = classify_correction(correction_text)
    if not cls["detected"]:
        return state
    out = dict(state)
    rec = create_correction_record(correction_text, cls["type"],
                                    cls["confidence"])
    recs = list(out.get("recent_corrections") or [])
    recs.append(rec)
    out["recent_corrections"] = _resolve_conflicts(recs)
    out["updated_at"] = time.time()
    return out


def get_active_corrections(state: dict[str, Any],
                            limit: int = _DEFAULT_ACTIVE_CAP
                            ) -> list[dict[str, Any]]:
    cap = max(1, min(int(limit), _HARD_CORRECTIONS_CAP))
    if not isinstance(state, dict):
        return []
    recs = list(state.get("recent_corrections") or [])
    return _resolve_conflicts(recs)[-cap:]


def expire_old_corrections(state: dict[str, Any],
                            max_items: int = _HARD_CORRECTIONS_CAP
                            ) -> dict[str, Any]:
    cap = max(1, min(int(max_items), _HARD_CORRECTIONS_CAP))
    if not isinstance(state, dict):
        return state
    out = dict(state)
    recs = list(out.get("recent_corrections") or [])
    out["recent_corrections"] = recs[-cap:]
    out["updated_at"] = time.time()
    return out


def summarize_corrections(state: dict[str, Any]) -> dict[str, Any]:
    actives = get_active_corrections(state)
    by_type: dict[str, int] = {}
    for r in actives:
        t = r.get("type")
        if t:
            by_type[t] = by_type.get(t, 0) + 1
    return {"ok": True, "n_active": len(actives), "by_type": by_type,
            "latest": actives[-3:]}


def write_correction_memory_report(report: dict[str, Any],
                                    output_path: str | Path) -> str:
    p = Path(output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = dict(report)
    body["written_at"] = time.time()
    p.write_text(json.dumps(body, ensure_ascii=False, indent=2,
                            default=str), encoding="utf-8")
    return str(p)


__all__ = [
    "CORRECTION_TYPES",
    "classify_correction",
    "create_correction_record",
    "apply_correction_to_state",
    "get_active_corrections",
    "expire_old_corrections",
    "summarize_corrections",
    "write_correction_memory_report",
]
