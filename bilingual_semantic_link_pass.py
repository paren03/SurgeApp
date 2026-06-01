"""Semantic-similarity bilingual linker — sentence-transformers based.

Origin
======
2026-06-01: after 4 passes (category-rank batched + definition-match
exact + first-token + stopword-aware), the bilingual link store was at
3899 concepts / 7798 entry_links. Remaining unlinked: 423 EN words
+ 213 RU words.

These remaining rows are MISSED by all prior heuristics — meaning:
- No category-rank pairing landed them (their category was already
  saturated by higher-frequency peers)
- Their RU.definition_en didn't literally match any EN word
- Stopword-aware token scanning still didn't find a hit

A multilingual sentence-transformer can catch these via vector
similarity: e.g., RU "балон" (def_en: "cylinder / tank for gas")
semantically matches EN "tank" even though the RU side wasn't a
direct lexical hit.

Model
=====
`paraphrase-multilingual-MiniLM-L12-v2` — 384-dim, ~120 MB on disk,
fast CPU inference. Trained on 50+ languages including Russian.
Alternative for higher quality: `paraphrase-multilingual-mpnet-base-v2`
(768-dim, ~1.1 GB, slower).

Doctrine
========
- Pure orchestrator. Uses bilingual_concept_link_store primitives
  (create_concept, add_entry_link) + my batched-write pattern. Phase
  48 freeze respected.
- Threshold-gated: only writes pairs with cosine ≥ THRESHOLD (default
  0.70 — conservative; below this is mostly false positives)
- Confidence = max(0.55, cosine * 0.7) — lower than exact (0.85) or
  first-token (0.65); these are semantic guesses, not direct translations
- link_method = "heuristic" (Phase 22 valid methods list)
- Respects register/safety filters
- Skips pairs where EITHER side is already linked (loaded from DB)
- Production EN/RU rows untouched

CLI
===
    python bilingual_semantic_link_pass.py [--threshold 0.70]
        [--model paraphrase-multilingual-MiniLM-L12-v2] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bilingual_link_builder import (
    _register_compatible,
    _safety_compatible,
    _coverage_overlap,
    _domain_overlap,
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

DEFAULT_THRESHOLD = 0.70
DEFAULT_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"


def _now_iso() -> str:
    import datetime as _dt
    return _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def _load_unlinked_en(link_db_path: Optional[str]) -> List[Dict[str, Any]]:
    """All EN rows whose word doesn't appear in any entry_links row."""
    link = sqlite3.connect(
        str(link_db_path) if link_db_path else str(DEFAULT_LINK_DB))
    linked = {(r[0] or "").strip().lower()
              for r in link.execute(
                  "SELECT source_word FROM entry_links WHERE language='en'")}
    link.close()
    con = sqlite3.connect(str(EN_DB))
    con.row_factory = sqlite3.Row
    out: List[Dict[str, Any]] = []
    try:
        for r in con.execute("SELECT * FROM words"):
            d = dict(r)
            w = (d.get("word") or "").strip()
            if w and w.lower() not in linked:
                out.append(d)
    finally:
        con.close()
    return out


def _load_unlinked_ru(link_db_path: Optional[str]) -> List[Dict[str, Any]]:
    link = sqlite3.connect(
        str(link_db_path) if link_db_path else str(DEFAULT_LINK_DB))
    linked = {(r[0] or "").strip().lower()
              for r in link.execute(
                  "SELECT source_word FROM entry_links WHERE language='ru'")}
    link.close()
    con = sqlite3.connect(str(RU_DB))
    con.row_factory = sqlite3.Row
    out: List[Dict[str, Any]] = []
    try:
        for r in con.execute("SELECT * FROM words"):
            d = dict(r)
            w = (d.get("word") or "").strip()
            if w and w.lower() not in linked:
                out.append(d)
    finally:
        con.close()
    return out


def _make_en_text(row: Dict[str, Any]) -> str:
    """Source text fed to encoder for an EN row."""
    parts = [str(row.get("word") or "")]
    defn = str(row.get("definition") or "").strip()
    if defn:
        parts.append(defn[:200])
    return ". ".join(p for p in parts if p)


def _make_ru_text(row: Dict[str, Any]) -> str:
    """Source text fed to encoder for an RU row. Combines RU word +
    its declared English definition (which is a strong signal even if
    not a direct lookup match)."""
    parts = []
    w = str(row.get("word") or "").strip()
    if w:
        parts.append(w)
    def_en = str(row.get("definition_en") or "").strip()
    if def_en:
        parts.append(def_en[:200])
    def_ru = str(row.get("definition_ru") or "").strip()
    if def_ru:
        parts.append(def_ru[:200])
    return ". ".join(parts)


def semantic_pass(threshold: float = DEFAULT_THRESHOLD,
                  model_name: str = DEFAULT_MODEL,
                  link_db_path: Optional[str] = None,
                  dry_run: bool = False) -> Dict[str, Any]:
    """Run the semantic-similarity pass. Always batched write."""
    t0 = time.monotonic()
    en_rows = _load_unlinked_en(link_db_path)
    ru_rows = _load_unlinked_ru(link_db_path)
    out: Dict[str, Any] = {
        "ts": _now_iso(),
        "threshold": threshold,
        "model": model_name,
        "en_unlinked_count": len(en_rows),
        "ru_unlinked_count": len(ru_rows),
        "report_version": 1,
    }
    if not en_rows or not ru_rows:
        out["ok"] = True
        out["created_concepts"] = 0
        out["created_entry_links"] = 0
        out["note"] = "no_unlinked_rows_in_one_or_both_sides"
        out["elapsed_s"] = round(time.monotonic() - t0, 3)
        return out

    # Lazy-import sentence_transformers + numpy here so the import cost
    # is paid only when this orchestrator actually runs.
    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except Exception as exc:  # noqa: BLE001
        out["ok"] = False
        out["error"] = f"import_failed: {type(exc).__name__}: {exc}"
        out["elapsed_s"] = round(time.monotonic() - t0, 3)
        return out

    print(f"  loading model {model_name} ...", flush=True)
    t_model = time.monotonic()
    model = SentenceTransformer(model_name)
    out["model_load_s"] = round(time.monotonic() - t_model, 2)

    en_texts = [_make_en_text(r) for r in en_rows]
    ru_texts = [_make_ru_text(r) for r in ru_rows]

    print(f"  embedding {len(en_texts)} EN + {len(ru_texts)} RU rows ...",
          flush=True)
    t_emb = time.monotonic()
    en_emb = model.encode(en_texts, convert_to_numpy=True,
                          show_progress_bar=False, batch_size=32,
                          normalize_embeddings=True)
    ru_emb = model.encode(ru_texts, convert_to_numpy=True,
                          show_progress_bar=False, batch_size=32,
                          normalize_embeddings=True)
    out["embed_s"] = round(time.monotonic() - t_emb, 2)

    # Cosine similarity (since both already L2-normalized).
    sim = ru_emb @ en_emb.T  # (ru, en) matrix
    best_en_idx = sim.argmax(axis=1)
    best_sim = sim.max(axis=1)

    # Take pairs with sim >= threshold. Greedy assignment: highest-sim
    # pair first; each EN word used at most once (en_seen tracks).
    en_seen: set = set()
    pair_candidates: List[Tuple[float, int, int]] = []  # (sim, ru_idx, en_idx)
    for ru_idx, (en_idx, s) in enumerate(zip(best_en_idx, best_sim)):
        if s < threshold:
            continue
        pair_candidates.append((float(s), int(ru_idx), int(en_idx)))
    pair_candidates.sort(key=lambda x: -x[0])  # high sim first

    concept_rows: List[Tuple] = []
    link_rows: List[Tuple] = []
    register_skip = 0
    duplicate_en_skip = 0

    for sim_val, ru_idx, en_idx in pair_candidates:
        e = en_rows[en_idx]
        r = ru_rows[ru_idx]
        e_word = (e.get("word") or "").strip()
        r_word = (r.get("word") or "").strip()
        if not e_word or not r_word:
            continue
        if e_word.lower() in en_seen:
            duplicate_en_skip += 1
            continue
        if (not _register_compatible(e, r)
                or not _safety_compatible(e, r)):
            register_skip += 1
            continue

        cov = sorted(_coverage_overlap(e, r))
        dom = sorted(_domain_overlap(e, r))
        # Confidence floor 0.55, max 0.80 (lower than direct lemma_match).
        conf = _clamp_confidence(max(0.55, min(0.80, sim_val * 0.85)))
        notes = (f"semantic_match sim={sim_val:.3f} "
                 f"def_en={(r.get('definition_en') or '')[:50]!r}")
        now = _now()

        cid = _new_id("concept")
        concept_rows.append((
            cid, e_word, r_word,
            _json_list(cov), _json_list(dom),
            _json_list(["standard"]), _json_list([]),
            now, now, notes,
        ))
        link_rows.append((
            _new_id("link"), cid, "en",
            "cognitive_lexicon_store", e_word, e_word, "",
            str(e.get("lemma") or ""),
            str(e.get("part_of_speech") or ""),
            conf, _normalize_method("heuristic"),
            now, notes,
        ))
        link_rows.append((
            _new_id("link"), cid, "ru",
            "russian_lexicon_store", r_word, r_word, "",
            str(r.get("lemma") or ""),
            str(r.get("part_of_speech") or ""),
            conf, _normalize_method("heuristic"),
            now, notes,
        ))
        en_seen.add(e_word.lower())

    if dry_run:
        out["ok"] = True
        out["dry_run"] = True
        out["created_concepts"] = 0
        out["would_create_concepts"] = len(concept_rows)
        out["would_create_entry_links"] = len(link_rows)
        out["candidates_above_threshold"] = len(pair_candidates)
        out["register_skip"] = register_skip
        out["duplicate_en_skip"] = duplicate_en_skip
        out["elapsed_s"] = round(time.monotonic() - t0, 3)
        return out

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
            (_new_id("audit"), "semantic_link_pass", "", "", "ok",
             f"model={model_name} threshold={threshold} "
             f"created_concepts={len(concept_rows)} "
             f"candidates={len(pair_candidates)} "
             f"register_skip={register_skip} "
             f"duplicate_en_skip={duplicate_en_skip}",
             _now()))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    out["ok"] = True
    out["created_concepts"] = len(concept_rows)
    out["created_entry_links"] = len(link_rows)
    out["candidates_above_threshold"] = len(pair_candidates)
    out["register_skip"] = register_skip
    out["duplicate_en_skip"] = duplicate_en_skip
    out["elapsed_s"] = round(time.monotonic() - t0, 3)
    return out


def _cli() -> int:
    p = argparse.ArgumentParser(
        description="Semantic-similarity bilingual linker using a "
                    "multilingual sentence-transformer.")
    p.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                   help=f"Cosine threshold (default {DEFAULT_THRESHOLD})")
    p.add_argument("--model", default=DEFAULT_MODEL,
                   help=f"Sentence-transformer model (default {DEFAULT_MODEL})")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    report = semantic_pass(threshold=args.threshold,
                           model_name=args.model,
                           dry_run=args.dry_run)
    suffix = "_dryrun" if args.dry_run else ""
    out_path = REPORTS_DIR / (
        f"bilingual_semantic_link_pass{suffix}_"
        f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nfull report -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
