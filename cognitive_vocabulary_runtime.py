"""Luna vocabulary runtime — bounded, gated, side-effect free.

Public surface for downstream callers (Luna only — never Program S):

    get_context_words(prompt, mode, limit)
    explain_word(word)
    find_better_word(word, tone, difficulty)
    find_related_terms(topic, limit)
    classify_word_level(word)
    get_optional_vocabulary_context(prompt, mode, limit)

Hard guarantees:
- Feature flag LUNA_VOCABULARY_RUNTIME controls everything.
- If the flag is unset or "0", `get_optional_vocabulary_context` returns {}.
- No background threads, no daemons, no recursion.
- Optional deps (wordfreq, nltk WordNet) are best-effort; missing => graceful.
- Every query is bounded; full DB is never loaded into memory.
"""

from __future__ import annotations

import os
import re
from typing import Any, Optional

from cognitive_lexicon_store import (
    DEFAULT_LIMIT,
    HARD_MAX_LIMIT,
    get_related_words,
    init_db,
    lookup_word,
    search_contains,
    search_prefix,
)
from cognitive_word_policy import (
    LEVELS,
    apply_policy,
    is_word_allowed,
    mode_summary,
)

FEATURE_FLAG = "LUNA_VOCABULARY_RUNTIME"

_WORD_RE = re.compile(r"[A-Za-z][A-Za-z'-]{1,31}")
_STOPWORDS = frozenset({
    "the", "a", "an", "and", "or", "but", "if", "then", "of", "to", "in", "on",
    "for", "with", "as", "is", "are", "was", "were", "be", "been", "being",
    "this", "that", "these", "those", "it", "its", "i", "you", "he", "she",
    "we", "they", "me", "him", "her", "them", "my", "your", "our", "their",
    "do", "does", "did", "doing", "have", "has", "had", "having",
    "not", "no", "yes", "so", "very", "too", "than", "such", "also",
    "from", "by", "at", "into", "out", "up", "down", "over", "under",
})


def _flag_enabled() -> bool:
    v = os.environ.get(FEATURE_FLAG, "")
    return v.strip() in ("1", "true", "yes", "on")


def _clamp(limit: Optional[int]) -> int:
    if limit is None:
        return DEFAULT_LIMIT
    try:
        n = int(limit)
    except (TypeError, ValueError):
        return DEFAULT_LIMIT
    if n <= 0:
        return DEFAULT_LIMIT
    return min(n, HARD_MAX_LIMIT)


def _tokenize(text: str, cap: int = 64) -> list[str]:
    if not isinstance(text, str) or not text.strip():
        return []
    out: list[str] = []
    seen: set[str] = set()
    for m in _WORD_RE.finditer(text):
        w = m.group(0).lower()
        if w in _STOPWORDS or w in seen or len(w) < 2:
            continue
        seen.add(w)
        out.append(w)
        if len(out) >= cap:
            break
    return out


_WORDFREQ_OK: Optional[bool] = None
_WORDNET_OK: Optional[bool] = None


def _wordfreq():
    global _WORDFREQ_OK
    try:
        import wordfreq
        _WORDFREQ_OK = True
        return wordfreq
    except Exception:
        _WORDFREQ_OK = False
        return None


def _wordnet():
    global _WORDNET_OK
    try:
        from nltk.corpus import wordnet
        _ = wordnet.synsets("test")
        _WORDNET_OK = True
        return wordnet
    except Exception:
        _WORDNET_OK = False
        return None


def dependency_status() -> dict[str, Any]:
    return {
        "wordfreq": _wordfreq() is not None,
        "wordnet": _wordnet() is not None,
        "install_hint": "pip install wordfreq nltk  &&  python -m nltk.downloader wordnet",
    }


def classify_word_level(word: str) -> str:
    """Return one of cognitive_word_policy.LEVELS for `word`."""
    if not isinstance(word, str) or not word.strip():
        return "plain"
    w = word.strip().lower()
    row = lookup_word(w)
    if row and row.get("word_level") in LEVELS:
        return row["word_level"]

    wf = _wordfreq()
    if wf is not None:
        try:
            zipf = wf.zipf_frequency(w, "en")
        except Exception:
            zipf = 0.0
        if zipf >= 5.5:
            return "plain"
        if zipf >= 4.5:
            return "everyday"
        if zipf >= 3.5:
            return "intermediate"
        if zipf >= 2.5:
            return "advanced"
        if zipf > 0.0:
            return "rare"
        return "specialized"

    if len(w) <= 5:
        return "plain"
    if len(w) <= 8:
        return "everyday"
    if len(w) <= 11:
        return "intermediate"
    if len(w) <= 14:
        return "advanced"
    return "rare"


def explain_word(word: str) -> dict[str, Any]:
    """Definition + synonyms + level. DB first, WordNet fallback, never crash."""
    if not isinstance(word, str) or not word.strip():
        return {"word": "", "found": False, "definition": "", "synonyms": [],
                "examples": [], "level": "plain", "source": "empty"}
    w = word.strip().lower()
    row = lookup_word(w)
    if row:
        return {
            "word": w,
            "found": True,
            "definition": row.get("definition", ""),
            "synonyms": (row.get("synonyms") or [])[:HARD_MAX_LIMIT],
            "examples": (row.get("examples") or [])[:10],
            "level": row.get("word_level", "plain"),
            "source": row.get("source", "db"),
        }

    wn = _wordnet()
    if wn is not None:
        try:
            syns = wn.synsets(w)[:5]
            if syns:
                first = syns[0]
                definition = first.definition() or ""
                examples = list(first.examples())[:3]
                lemma_names: list[str] = []
                for s in syns:
                    for lemma in s.lemmas():
                        name = lemma.name().replace("_", " ").lower()
                        if name != w and name not in lemma_names:
                            lemma_names.append(name)
                    if len(lemma_names) >= 20:
                        break
                return {
                    "word": w,
                    "found": True,
                    "definition": definition,
                    "synonyms": lemma_names[:20],
                    "examples": examples,
                    "level": classify_word_level(w),
                    "source": "wordnet",
                }
        except Exception:
            pass

    return {
        "word": w,
        "found": False,
        "definition": "",
        "synonyms": [],
        "examples": [],
        "level": classify_word_level(w),
        "source": "unknown",
    }


def find_related_terms(topic: str, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    """Find terms related to a topic via tag/prefix/contains, capped to limit."""
    if not isinstance(topic, str) or not topic.strip():
        return []
    n = _clamp(limit)
    t = topic.strip().lower()

    seen: set[str] = set()
    out: list[dict[str, Any]] = []

    for r in get_related_words(t, limit=n):
        if r["word"] not in seen:
            seen.add(r["word"])
            out.append(r)
            if len(out) >= n:
                return out[:n]

    for r in search_prefix(t, limit=n):
        if r["word"] not in seen:
            seen.add(r["word"])
            out.append(r)
            if len(out) >= n:
                return out[:n]

    for r in search_contains(t, limit=n):
        if r["word"] not in seen:
            seen.add(r["word"])
            out.append(r)
            if len(out) >= n:
                return out[:n]

    wn = _wordnet()
    if wn is not None and len(out) < n:
        try:
            for syn in wn.synsets(t)[:5]:
                for lemma in syn.lemmas():
                    name = lemma.name().replace("_", " ").lower()
                    if name and name != t and name not in seen:
                        seen.add(name)
                        out.append({
                            "word": name, "definition": syn.definition() or "",
                            "synonyms": [], "examples": [], "tags": [],
                            "source": "wordnet", "language": "en",
                            "frequency_score": 0.0,
                            "word_level": classify_word_level(name),
                            "created_at": 0.0, "updated_at": 0.0,
                        })
                        if len(out) >= n:
                            return out[:n]
        except Exception:
            pass

    return out[:n]


def find_better_word(
    word: str,
    tone: str = "normal",
    difficulty: str = "plain",
) -> Optional[dict[str, Any]]:
    """Suggest a substitute that satisfies tone (mode) and difficulty (level)."""
    if not isinstance(word, str) or not word.strip():
        return None
    w = word.strip().lower()

    target_level = difficulty if difficulty in LEVELS else "plain"
    candidates: list[dict[str, Any]] = list(get_related_words(w, limit=HARD_MAX_LIMIT))

    if not candidates:
        info = explain_word(w)
        for syn in info.get("synonyms", []):
            row = lookup_word(syn)
            if row:
                candidates.append(row)
            else:
                candidates.append({
                    "word": syn, "definition": "", "synonyms": [], "examples": [],
                    "tags": [], "source": "explain", "language": "en",
                    "frequency_score": 0.0, "word_level": classify_word_level(syn),
                    "created_at": 0.0, "updated_at": 0.0,
                })

    allowed = apply_policy(candidates, mode=tone)
    if not allowed:
        return None

    exact = [c for c in allowed if c.get("word_level") == target_level]
    pool = exact or allowed
    pool.sort(key=lambda c: float(c.get("frequency_score", 0.0)), reverse=True)
    return pool[0] if pool else None


def get_context_words(
    prompt: str,
    mode: str = "normal",
    limit: int = DEFAULT_LIMIT,
    is_user_prompted: bool = False,
) -> list[dict[str, Any]]:
    """Return mode-appropriate vocabulary hints for tokens found in `prompt`."""
    n = _clamp(limit)
    tokens = _tokenize(prompt, cap=min(n * 2, 64))
    if not tokens:
        return []

    found: list[dict[str, Any]] = []
    seen: set[str] = set()
    for tok in tokens:
        row = lookup_word(tok)
        if row and row["word"] not in seen:
            d = is_word_allowed(
                row["word"], row.get("word_level", "plain"),
                mode=mode, frequency_score=float(row.get("frequency_score", 0.0)),
                safety_tags=row.get("safety_tags"),
                register_tags=row.get("register_tags"),
                is_user_prompted=is_user_prompted,
                decision_context="suggestion",
            )
            if d.allowed:
                seen.add(row["word"])
                found.append(row)
                if len(found) >= n:
                    return found[:n]

    if len(found) < n:
        for tok in tokens:
            for r in search_prefix(tok, limit=min(n, 10)):
                if r["word"] in seen:
                    continue
                d = is_word_allowed(
                    r["word"], r.get("word_level", "plain"),
                    mode=mode, frequency_score=float(r.get("frequency_score", 0.0)),
                    safety_tags=r.get("safety_tags"),
                    register_tags=r.get("register_tags"),
                    is_user_prompted=is_user_prompted,
                    decision_context="suggestion",
                )
                if d.allowed:
                    seen.add(r["word"])
                    found.append(r)
                    if len(found) >= n:
                        return found[:n]
            if len(found) >= n:
                break

    return apply_policy(found, mode=mode,
                       is_user_prompted=is_user_prompted,
                       decision_context="suggestion")[:n]


def get_optional_vocabulary_context(
    prompt: str,
    mode: str = "normal",
    limit: int = DEFAULT_LIMIT,
    is_user_prompted: bool = False,
) -> dict[str, Any]:
    """The ONE public function callers should embed.

    Returns {} when the feature flag is disabled. Never crashes, never blocks,
    never returns more than `limit` items, never loads the full DB.
    """
    if not _flag_enabled():
        return {}
    n = _clamp(limit)
    try:
        init_db()
    except Exception as e:
        return {"enabled": True, "error": f"init_failed: {e}", "context": []}

    try:
        words = get_context_words(prompt, mode=mode, limit=n,
                                  is_user_prompted=is_user_prompted)
    except Exception as e:
        return {"enabled": True, "error": f"context_failed: {e}", "context": []}

    compact = [
        {
            "word": w.get("word", ""),
            "definition": (w.get("definition") or "")[:240],
            "level": w.get("word_level", "plain"),
            "tags": (w.get("tags") or [])[:5],
        }
        for w in words[:n]
    ]
    return {
        "enabled": True,
        "mode": mode_summary(mode)["mode"],
        "count": len(compact),
        "limit": n,
        "context": compact,
    }


__all__ = [
    "FEATURE_FLAG",
    "classify_word_level",
    "dependency_status",
    "explain_word",
    "find_better_word",
    "find_related_terms",
    "get_context_words",
    "get_optional_vocabulary_context",
]
