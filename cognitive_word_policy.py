"""Luna word-use policy.

Decides whether a word fits Luna's current speaking mode. Pure logic, no I/O,
no side effects. Standalone — does NOT touch Program S.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

MODES = (
    "normal",
    "voice_conversation",
    "teacher",
    "technical",
    "curriculum",
    "coding",
    "professional",
)

LEVELS = ("plain", "everyday", "intermediate", "advanced", "rare", "specialized")
_LEVEL_RANK = {lvl: i for i, lvl in enumerate(LEVELS)}

_MODE_MAX_LEVEL = {
    "normal":             "everyday",
    "voice_conversation": "everyday",
    "teacher":            "intermediate",
    "technical":          "specialized",
    "curriculum":         "advanced",
    "coding":             "specialized",
    "professional":       "advanced",
}

_MODE_RARE_BUDGET = {
    "normal":             1,
    "voice_conversation": 0,
    "teacher":            2,
    "technical":          6,
    "curriculum":         4,
    "coding":             6,
    "professional":       3,
}

_AWKWARD_FOR_VOICE = {
    "aforementioned", "heretofore", "notwithstanding", "viz", "i.e.", "e.g.",
    "qua", "ergo", "henceforth", "hitherto",
}

DECISION_CONTEXTS: tuple[str, ...] = (
    "recognition",      # Luna recognizes the word internally — never blocked
    "explanation",      # Luna shows the user a definition — almost never blocked
    "suggestion",       # Luna picks vocabulary hints to draw from — gated
    "response_wording", # Luna actually uses the word in output — strictest
)

_INFORMAL_MODES = {"voice_conversation", "warm_friend", "informal", "concise"}
_VOICE_MODES = {"voice_conversation", "warm_friend"}
_STRICT_MODES = {"normal", "teacher", "curriculum", "professional", "voice_conversation"}


def _norm_set(items) -> set[str]:
    if not items:
        return set()
    if isinstance(items, (list, tuple, set, frozenset)):
        return {str(s).strip().lower() for s in items if str(s).strip()}
    if isinstance(items, str):
        return {items.strip().lower()}
    return set()


def _normalize_mode(mode: Optional[str]) -> str:
    if not isinstance(mode, str):
        return "normal"
    m = mode.strip().lower().replace("-", "_")
    return m if m in MODES else "normal"


def _normalize_level(level: Optional[str]) -> str:
    if not isinstance(level, str):
        return "plain"
    lv = level.strip().lower()
    return lv if lv in _LEVEL_RANK else "plain"


@dataclass
class WordDecision:
    word: str
    allowed: bool
    reason: str
    suggested_level: str
    mode: str


def is_word_allowed(
    word: str,
    word_level: str = "plain",
    mode: str = "normal",
    frequency_score: float = 0.0,
    safety_tags=None,
    register_tags=None,
    is_user_prompted: bool = False,
    decision_context: str = "suggestion",
) -> WordDecision:
    """Return a structured decision for a single word.

    `decision_context` ∈ DECISION_CONTEXTS controls strictness:
      - recognition: always allowed (Luna may know the word internally).
      - explanation: allowed unless safety blocks it from being shown at all
        (today no tag fully blocks explanation; recognition_only is OK to
        explain — the gate is on Luna *using* it).
      - suggestion: this is what `get_context_words` does; strict.
      - response_wording: strictest — even an explicit user prompt cannot
        unlock recognition_only.
    """
    m = _normalize_mode(mode)
    lvl = _normalize_level(word_level)
    max_lvl = _MODE_MAX_LEVEL[m]
    word_norm = (word or "").strip().lower()
    saf = _norm_set(safety_tags)
    reg = _norm_set(register_tags)
    ctx = decision_context if decision_context in DECISION_CONTEXTS else "suggestion"

    if ctx == "recognition":
        return WordDecision(word_norm, True, "recognition_always_allowed", max_lvl, m)

    if "recognition_only" in saf or "recognition_only" in reg:
        if ctx == "explanation":
            return WordDecision(word_norm, True, "recognition_only_explainable", max_lvl, m)
        return WordDecision(word_norm, False, "recognition_only_blocked", max_lvl, m)

    if "do_not_use_unprompted" in saf or "do_not_use_unprompted" in reg:
        if not is_user_prompted:
            return WordDecision(word_norm, False, "do_not_use_unprompted_blocked", max_lvl, m)

    if "vulgar" in saf or "offensive" in saf or "vulgar" in reg or "offensive" in reg:
        if m in _STRICT_MODES or m == "curriculum":
            return WordDecision(word_norm, False, "vulgar_offensive_blocked_in_mode", max_lvl, m)
        if not is_user_prompted:
            return WordDecision(word_norm, False, "vulgar_offensive_requires_prompt", max_lvl, m)

    if ({"slang", "street", "regional"} & reg) and m not in _INFORMAL_MODES and not is_user_prompted:
        return WordDecision(word_norm, False,
                            "slang_street_regional_requires_informal_mode_or_prompt",
                            max_lvl, m)

    if m in _VOICE_MODES and word_norm in _AWKWARD_FOR_VOICE:
        return WordDecision(word_norm, False, "awkward_in_voice", max_lvl, m)

    if _LEVEL_RANK[lvl] > _LEVEL_RANK[max_lvl]:
        return WordDecision(
            word_norm, False,
            f"level_{lvl}_exceeds_mode_max_{max_lvl}",
            max_lvl, m,
        )

    if m in ("normal", "voice_conversation") and frequency_score < 0.0:
        return WordDecision(word_norm, False, "negative_frequency_in_casual_mode", max_lvl, m)

    return WordDecision(word_norm, True, "ok", max_lvl, m)


def filter_words(
    candidates: Iterable[dict],
    mode: str = "normal",
    is_user_prompted: bool = False,
    decision_context: str = "suggestion",
) -> list[dict]:
    """Filter a list of word dicts (as returned by the store) by mode + safety policy."""
    out: list[dict] = []
    for c in candidates or []:
        if not isinstance(c, dict):
            continue
        d = is_word_allowed(
            c.get("word", ""),
            word_level=c.get("word_level", "plain"),
            mode=mode,
            frequency_score=float(c.get("frequency_score", 0.0)),
            safety_tags=c.get("safety_tags"),
            register_tags=c.get("register_tags"),
            is_user_prompted=is_user_prompted,
            decision_context=decision_context,
        )
        if d.allowed:
            out.append(c)
    return out


def enforce_rare_budget(
    candidates: Iterable[dict],
    mode: str = "normal",
) -> list[dict]:
    """Cap the number of rare/specialized words returned, per-mode budget."""
    m = _normalize_mode(mode)
    budget = _MODE_RARE_BUDGET[m]
    out: list[dict] = []
    rare_used = 0
    for c in candidates or []:
        if not isinstance(c, dict):
            continue
        lvl = _normalize_level(c.get("word_level", "plain"))
        if _LEVEL_RANK[lvl] >= _LEVEL_RANK["rare"]:
            if rare_used >= budget:
                continue
            rare_used += 1
        out.append(c)
    return out


def apply_policy(
    candidates: Iterable[dict],
    mode: str = "normal",
    is_user_prompted: bool = False,
    decision_context: str = "suggestion",
) -> list[dict]:
    """Full policy pass: per-word allow + global rare-budget cap."""
    filtered = filter_words(
        candidates, mode=mode,
        is_user_prompted=is_user_prompted,
        decision_context=decision_context,
    )
    return enforce_rare_budget(filtered, mode=mode)


def mode_summary(mode: str = "normal") -> dict:
    m = _normalize_mode(mode)
    return {
        "mode": m,
        "max_level": _MODE_MAX_LEVEL[m],
        "rare_budget": _MODE_RARE_BUDGET[m],
        "valid_modes": list(MODES),
    }
