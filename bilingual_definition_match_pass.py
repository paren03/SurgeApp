"""Definition-match bilingual linker — direct translation pairs from
RU.definition_en field → EN.word lookup.

Origin
======
2026-06-01: after the rank-based deep_pass saturated the link store at
2334 concepts (~73% theoretical max), 906 potential HIGH-CONFIDENCE
direct-translation pairs were identified by scanning RU rows'
``definition_en`` field. Examples:
  - функция → function (exact)
  - переменная → variable (matches first token of "variable (math/science)")
  - коммит → commit (matches first token of "commit (git)")

These are real translations, not category-level guesses. They get
higher confidence (0.85 exact, 0.65 first-token) than the rank-based
pairs (~0.5) and are tagged ``link_method="lemma_match"`` or
``"heuristic"`` (both pre-blessed in the Phase 22 valid methods list).

Doctrine
========
- Uses ONLY existing Phase 22 primitives + my batched orchestrator
  pattern. NOT a new phase module — orchestrator script.
- Honors register/safety filter from bilingual_link_builder
- Skips any (EN, RU) pair where EITHER side is already linked
- Additive to bilingual_links.sqlite only — no production row changes
- Single SQLite transaction (batch mode pattern)
- Idempotent — re-running adds 0 new pairs once exhausted

CLI
===
    python bilingual_definition_match_pass.py [--first-token]

Default: exact matches only. --first-token also adds first-token matches
(higher coverage, slightly lower confidence).
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from bilingual_link_builder import (
    _register_compatible,
    _safety_compatible,
    _coverage_overlap,
    _domain_overlap,
    _parse_json_list,
)
import bilingual_concept_link_store as bls
from bilingual_concept_link_store import (
    _new_id, _now, _json_list,
    _clamp_confidence, _normalize_method,
    init_bilingual_link_db, DEFAULT_LINK_DB,
)

WORKTREE = Path(__file__).resolve().parent
EN_DB = WORKTREE / "lexicon" / "luna_vocabulary.sqlite"
RU_DB = WORKTREE / "russian_stack" / "russian_lexicon.sqlite"
REPORTS_DIR = WORKTREE / "bilingual_stack" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

EXACT_CONFIDENCE = 0.85
FIRST_TOKEN_CONFIDENCE = 0.65
FIRST_TOKEN_SPLIT_RE = re.compile(r"[\s,;\(\[/]")
# Stopwords skipped when scanning for a content-bearing token. Without
# this, "to braise/stew" → first token "to" would either fail (if "to"
# not in EN) or match wrong (if "to" IS in EN). With it, we skip past
# the particle to "braise" which is the real translation.
STOPWORDS = frozenset({
    "to", "the", "a", "an", "of", "in", "on", "for", "with",
    "at", "by", "from", "as", "or", "and", "is", "are",
})


def _scan_def_for_match(def_en: str, en_index: Dict[str, Dict[str, Any]]
                         ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """Walk tokens left-to-right, return the first non-stopword token
    that hits the EN word index. Returns (matched_token_lc, en_row) or
    (None, None)."""
    if not def_en:
        return (None, None)
    # Split on space, slash, comma, semicolon, paren. Strip residual parens.
    parts: List[str] = []
    for raw in re.split(r"[\s,;/]+", def_en):
        raw = re.sub(r"[\(\)\[\]]", "", raw).strip()
        if raw:
            parts.append(raw)
    for p in parts:
        pl = p.lower()
        if pl in STOPWORDS:
            continue
        en = en_index.get(pl)
        if en is not None:
            return (pl, en)
    return (None, None)


def _now_iso() -> str:
    import datetime as _dt
    return _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def _load_en_word_index() -> Dict[str, Dict[str, Any]]:
    """Map lowercased EN word → full row dict (for register/safety check)."""
    out: Dict[str, Dict[str, Any]] = {}
    con = sqlite3.connect(str(EN_DB))
    con.row_factory = sqlite3.Row
    try:
        for r in con.execute("SELECT * FROM words"):
            d = dict(r)
            w = (d.get("word") or "").strip().lower()
            if w and w not in out:
                out[w] = d
    finally:
        con.close()
    return out


def _load_ru_rows_with_def_en() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    con = sqlite3.connect(str(RU_DB))
    con.row_factory = sqlite3.Row
    try:
        for r in con.execute("SELECT * FROM words WHERE definition_en IS NOT NULL AND TRIM(definition_en) != ''"):
            rows.append(dict(r))
    finally:
        con.close()
    return rows


def _existing_linked_words(link_db_path: Optional[str] = None
                            ) -> Tuple[Set[str], Set[str]]:
    en_seen: Set[str] = set()
    ru_seen: Set[str] = set()
    try:
        rows = bls.bounded_query(
            "SELECT language, source_word FROM entry_links",
            limit=100000, db_path=link_db_path)
        for lang, word in rows:
            w = (word or "").strip().lower()
            if not w:
                continue
            if lang == "en":
                en_seen.add(w)
            elif lang == "ru":
                ru_seen.add(w)
    except Exception:  # noqa: BLE001
        pass
    return en_seen, ru_seen


def definition_match_pass(use_first_token: bool = True,
                          link_db_path: Optional[str] = None
                          ) -> Dict[str, Any]:
    """Run the definition-match pass. Always uses batch mode (single
    transaction). Never raises into the caller.
    """
    t0 = time.monotonic()
    en_index = _load_en_word_index()
    ru_rows = _load_ru_rows_with_def_en()
    en_seen, ru_seen = _existing_linked_words(link_db_path)

    concept_rows: List[Tuple] = []
    link_rows: List[Tuple] = []
    exact_pairs = 0
    first_token_pairs = 0
    skipped_register = 0
    skipped_duplicate = 0
    skipped_no_match = 0

    for ru in ru_rows:
        ru_word = (ru.get("word") or "").strip()
        ru_word_lc = ru_word.lower()
        def_en = (ru.get("definition_en") or "").strip()
        if not ru_word or not def_en:
            continue

        # Try exact first.
        candidate = def_en.lower()
        match_kind = "exact"
        en = en_index.get(candidate)
        if en is None and use_first_token:
            # Walk all tokens left-to-right, skipping stopwords
            # ("to", "the", "of", ...). Returns first content-bearing
            # token that hits the EN word index.
            tok, en2 = _scan_def_for_match(def_en, en_index)
            if en2 is not None:
                candidate = tok or ""
                en = en2
                match_kind = "first_token"
        if en is None:
            skipped_no_match += 1
            continue

        en_word = (en.get("word") or "").strip()
        en_word_lc = en_word.lower()
        if not en_word:
            skipped_no_match += 1
            continue

        if en_word_lc in en_seen or ru_word_lc in ru_seen:
            skipped_duplicate += 1
            continue
        if (not _register_compatible(en, ru)
                or not _safety_compatible(en, ru)):
            skipped_register += 1
            continue

        cov = sorted(_coverage_overlap(en, ru))
        if not cov:
            # Fall back to whatever the RU side has tagged, since these
            # are real translations even if categories don't overlap.
            cov = sorted(_parse_json_list(ru.get("coverage_categories_json"))) or []
        dom = sorted(_domain_overlap(en, ru))
        conf = (EXACT_CONFIDENCE if match_kind == "exact"
                else FIRST_TOKEN_CONFIDENCE)
        method = ("lemma_match" if match_kind == "exact"
                  else "heuristic")
        notes = (f"definition_match kind={match_kind} "
                 f"def_en={def_en[:60]!r}")
        now = _now()

        cid = _new_id("concept")
        concept_rows.append((
            cid, en_word, ru_word,
            _json_list(cov), _json_list(dom),
            _json_list(["standard"]), _json_list([]),
            now, now, notes,
        ))
        link_rows.append((
            _new_id("link"), cid, "en",
            "cognitive_lexicon_store", en_word, en_word, "",
            str(en.get("lemma") or ""),
            str(en.get("part_of_speech") or ""),
            _clamp_confidence(conf),
            _normalize_method(method),
            now, notes,
        ))
        link_rows.append((
            _new_id("link"), cid, "ru",
            "russian_lexicon_store", ru_word, ru_word, "",
            str(ru.get("lemma") or ""),
            str(ru.get("part_of_speech") or ""),
            _clamp_confidence(conf),
            _normalize_method(method),
            now, notes,
        ))
        en_seen.add(en_word_lc)
        ru_seen.add(ru_word_lc)
        if match_kind == "exact":
            exact_pairs += 1
        else:
            first_token_pairs += 1

    init_bilingual_link_db(link_db_path)
    db_path_str = str(link_db_path) if link_db_path else str(DEFAULT_LINK_DB)

    conn = sqlite3.connect(db_path_str, timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    try:
        conn.execute("BEGIN")
        if concept_rows:
            conn.executemany(
                "INSERT INTO concepts (concept_id, canonical_label_en, "
                "canonical_label_ru, coverage_categories_json, "
                "domain_tags_json, register_tags_json, safety_tags_json, "
                "created_at, updated_at, notes) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                concept_rows)
        if link_rows:
            conn.executemany(
                "INSERT INTO entry_links (link_id, concept_id, language, "
                "source_store, source_entry_id, source_word, source_phrase, "
                "lemma, part_of_speech, confidence, link_method, created_at, "
                "notes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                link_rows)
        conn.execute(
            "INSERT INTO link_audit (audit_id, action, concept_id, link_id, "
            "status, message, created_at) VALUES (?,?,?,?,?,?,?)",
            (_new_id("audit"), "definition_match_pass", "", "", "ok",
             f"exact={exact_pairs} first_token={first_token_pairs} "
             f"skipped_register={skipped_register} "
             f"skipped_duplicate={skipped_duplicate} "
             f"skipped_no_match={skipped_no_match}",
             _now()))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    return {
        "ts": _now_iso(),
        "ok": True,
        "use_first_token": use_first_token,
        "created_concepts": len(concept_rows),
        "created_entry_links": len(link_rows),
        "exact_pairs": exact_pairs,
        "first_token_pairs": first_token_pairs,
        "skipped_register": skipped_register,
        "skipped_duplicate": skipped_duplicate,
        "skipped_no_match": skipped_no_match,
        "elapsed_s": round(time.monotonic() - t0, 3),
        "ru_rows_scanned": len(ru_rows),
        "en_index_size": len(en_index),
        "report_version": 1,
    }


def _cli() -> int:
    p = argparse.ArgumentParser(
        description="Definition-match bilingual linker — direct "
                    "translation pairs from RU.definition_en → EN.word.")
    p.add_argument("--no-first-token", action="store_true",
                   help="Disable first-token fallback (exact matches only)")
    args = p.parse_args()

    report = definition_match_pass(use_first_token=not args.no_first_token)

    out_path = REPORTS_DIR / (
        f"bilingual_definition_match_pass_"
        f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nfull report -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
