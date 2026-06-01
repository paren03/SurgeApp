"""Read-only bilingual dictionary lookup over the 1M+1M bilingual_links.sqlite.

Grounds Luna's replies in Serge's own curated EN+RU vocabulary: given the user's
message, look up its content words (Latin -> english_words, Cyrillic ->
russian_words) by INDEXED exact match and return compact {word, pos, definition}
entries for injection into the main-brain prompt.

STRICTLY READ-ONLY (sqlite URI ?mode=ro) — it physically cannot mutate the
~1.05 GB production DB. NEVER raises. Bounded (max_terms). Connection cached +
lock-guarded for fast reuse. Flag-gated
(``cognitive_bilingual_vocab_lookup_enabled``, default True).
"""
from __future__ import annotations

import importlib
import os
import re
import sqlite3
import threading
from typing import Any, Dict, List, Optional

DB_PATH = r"D:\SurgeApp\bilingual_links.sqlite"
_DEF_MAX_TERMS = 5
_DEF_TRUNC = 160
_MIN_WORD_LEN = 3
_MAX_CANDIDATES = 12

_CYR = re.compile(r"[Ѐ-ӿ]")
_TOKEN = re.compile(r"[A-Za-zЀ-ӿ]{%d,}" % _MIN_WORD_LEN)

# Common function words to skip (focus lookups on content words). Lowercased.
_STOPWORDS = {
    # EN
    "the", "and", "but", "for", "with", "about", "are", "was", "were", "you",
    "your", "this", "that", "what", "how", "why", "when", "tell", "can",
    "will", "would", "should", "please", "hey", "hello", "have", "has", "had",
    "not", "now", "they", "them", "from", "into", "out", "over", "just",
    "like", "want", "need", "get", "got", "let", "make", "made", "give",
    # RU function words
    "что", "как", "это", "так", "вот", "там", "тут", "его", "она", "они",
    "был", "была", "были", "для", "про", "под", "над", "без", "при", "или",
}


def _safe(modname: str):
    try:
        return importlib.import_module(modname)
    except Exception:  # noqa: BLE001
        return None


def is_enabled() -> bool:
    ff = _safe("luna_modules.cognitive_feature_flags")
    if ff is None:
        return True
    try:
        return bool(ff.read_flags().get(
            "cognitive_bilingual_vocab_lookup_enabled", True))
    except Exception:  # noqa: BLE001
        return True


_CONN_LOCK = threading.Lock()
_CONN: List[Optional[sqlite3.Connection]] = [None]


def _get_conn() -> Optional[sqlite3.Connection]:
    """Cached READ-ONLY connection (reused across turns). NEVER raises."""
    if _CONN[0] is not None:
        return _CONN[0]
    try:
        if not os.path.isfile(DB_PATH):
            return None
        uri = "file:" + DB_PATH.replace("\\", "/") + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0,
                               check_same_thread=False)
        conn.row_factory = sqlite3.Row
        _CONN[0] = conn
        return conn
    except Exception:  # noqa: BLE001
        return None


def _candidates(text: str) -> List[str]:
    out: List[str] = []
    for t in _TOKEN.findall(text or ""):
        tl = t.lower()
        if tl in _STOPWORDS or tl in out:
            continue
        out.append(tl)
        if len(out) >= _MAX_CANDIDATES:
            break
    return out


def lookup(text: str, *, max_terms: int = _DEF_MAX_TERMS
           ) -> List[Dict[str, Any]]:
    """Up to max_terms {word, pos, definition, lang} entries for the content
    words in `text`. READ-ONLY. NEVER raises; returns [] on any issue."""
    if not text or not is_enabled():
        return []
    cands = _candidates(text)
    if not cands:
        return []
    out: List[Dict[str, Any]] = []
    try:
        with _CONN_LOCK:
            conn = _get_conn()
            if conn is None:
                return []
            cur = conn.cursor()
            for tok in cands:
                if len(out) >= max_terms:
                    break
                is_ru = bool(_CYR.search(tok))
                tbl = "russian_words" if is_ru else "english_words"
                row = None
                for cand in (tok, tok.capitalize()):
                    try:
                        row = cur.execute(
                            f"SELECT word, definition, pos FROM {tbl} "
                            f"WHERE word=? LIMIT 1", (cand,)).fetchone()
                    except Exception:  # noqa: BLE001
                        row = None
                    if row is not None:
                        break
                if row is None:
                    continue
                defn = (row["definition"] or "").strip().replace("\n", " ")
                if not defn:
                    continue
                if len(defn) > _DEF_TRUNC:
                    defn = defn[:_DEF_TRUNC].rstrip() + "…"
                out.append({"word": row["word"], "pos": (row["pos"] or ""),
                            "definition": defn,
                            "lang": "ru" if is_ru else "en"})
    except Exception:  # noqa: BLE001
        return out
    return out


def as_prompt_block(text: str, *, max_terms: int = _DEF_MAX_TERMS) -> str:
    """Compact prompt block of relevant dictionary entries, or '' if none."""
    hits = lookup(text, max_terms=max_terms)
    if not hits:
        return ""
    lines = []
    for h in hits:
        pos = f" ({h['pos']})" if h.get("pos") else ""
        lines.append(f"- {h['word']}{pos}: {h['definition']}")
    return ("Reference vocabulary from your bilingual dictionary "
            "(use only if relevant; do not force it):\n" + "\n".join(lines))


def report() -> Dict[str, Any]:
    return {"available": os.path.isfile(DB_PATH), "enabled": is_enabled(),
            "db_path": DB_PATH, "max_terms_default": _DEF_MAX_TERMS}
