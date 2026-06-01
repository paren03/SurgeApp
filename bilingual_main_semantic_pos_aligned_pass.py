"""POS-aligned semantic-similarity bilingual linker at SAMPLE scale.

Improvement over `bilingual_main_semantic_sample_pass.py`:
the previous version paired any RU with any EN whose cosine >=
threshold. At 0.65 that produced noise like `on ↔ работать` (preposition
↔ verb) and `mann ↔ дятел` (name ↔ noun, no relation).

This pass:
  1. Canonicalizes POS strings on both sides (`adj`/`adjective` →
     `adj_class`; `pron`/`pronoun` → `pronoun_class`; etc.) — main has
     36 distinct EN POS values and 30 RU values that collapse to 8
     canonical classes.
  2. Restricts pairing to SAME canonical POS class. A noun can only
     pair with a noun, verb with verb, etc.
  3. Raises threshold to 0.75 by default (was 0.65) for better quality.
  4. Larger sample (25k × 25k by default).

Yield is expected LOWER count but HIGHER quality than the unfiltered
0.65 pass. Pairs still carry sim in notes for retro-filtering.

Doctrine
========
Same as the base orchestrator:
  - Main DB opened READ-ONLY (URI mode=ro).
  - Writes only to bilingual_concept_links.sqlite.
  - Dedupes on insert against existing entry_links (idempotent re-run).
  - Confidence = clamp(0.6, cos * 0.95, 0.85) — slightly higher floor
    because POS-aligned matches are inherently better signal.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import bilingual_main_adapter as _mainadapt
import bilingual_concept_links_db as _cdb

MAIN_DB = _mainadapt.MAIN_DB_PATH
CONCEPT_DB = _cdb.CONCEPT_DB_PATH
REPORTS_DIR = Path(__file__).resolve().parent / "memory" / "bilingual_main_reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_THRESHOLD = 0.75
DEFAULT_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"

# Canonical POS class map. Keys are lowercased POS strings as seen in
# main. Anything unmapped falls through to "other" (which will only
# pair with other "other" — likely empty pool, so effectively excluded).
POS_CLASS_MAP: Dict[str, str] = {
    "noun":          "noun",
    "name":          "noun",
    "verb":          "verb",
    "participle":    "verb",
    "adjective":     "adj",
    "adj":           "adj",
    "adverb":        "adv",
    "adv":           "adv",
    "pronoun":       "pronoun",
    "pron":          "pronoun",
    "preposition":   "prep",
    "prep":          "prep",
    "interjection":  "intj",
    "intj":          "intj",
    "numeral":       "num",
    "num":           "num",
    "phrase":        "phrase",
    "prep_phrase":   "phrase",
    "proverb":       "phrase",
    "determiner":    "det",
    "det":           "det",
    "contraction":   "other",
    "prefix":        "other",
    "suffix":        "other",
    "unknown":       "other",
    "":              "other",
    "(none)":        "other",
}


def _pos_class(pos: str) -> str:
    return POS_CLASS_MAP.get((pos or "").strip().lower(), "other")


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_unlinked_sample_with_pos(
    language: str, sample_size: int, existing_linked: set[str]
) -> List[Dict[str, Any]]:
    table = "english_words" if language == "en" else "russian_words"
    conn = sqlite3.connect(
        f"file:{MAIN_DB.as_posix()}?mode=ro", uri=True, timeout=15.0)
    conn.row_factory = sqlite3.Row
    out: List[Dict[str, Any]] = []
    try:
        # Pull strictly content-bearing POS rows (skip "other" class entirely
        # so the sample budget is spent on alignable pairs).
        sql_pos_filter = ("WHERE pos IS NOT NULL AND pos != '' "
                          "AND LOWER(pos) NOT IN "
                          "('contraction','prefix','suffix','unknown','other')")
        cursor = conn.execute(
            f"SELECT * FROM {table} {sql_pos_filter} ORDER BY id")
        for r in cursor:
            d = dict(r)
            w = (d.get("word") or "").strip()
            if not w:
                continue
            if w.lower() in existing_linked:
                continue
            pc = _pos_class(d.get("pos") or "")
            if pc == "other":
                continue
            d["_pos_class"] = pc
            out.append(d)
            if len(out) >= sample_size:
                break
    finally:
        conn.close()
    return out


def _en_text(r: Dict[str, Any]) -> str:
    parts = [str(r.get("word") or "")]
    defn = str(r.get("definition") or "").strip()
    if defn:
        parts.append(defn[:200])
    return ". ".join(p for p in parts if p)


def _ru_text(r: Dict[str, Any]) -> str:
    parts = [str(r.get("word") or "")]
    defn = str(r.get("definition") or "").strip()
    if defn:
        parts.append(defn[:200])
    return ". ".join(p for p in parts if p)


def pos_aligned_semantic_pass(
    en_sample: int = 25000,
    ru_sample: int = 25000,
    threshold: float = DEFAULT_THRESHOLD,
    model_name: str = DEFAULT_MODEL,
) -> Dict[str, Any]:
    t0 = time.monotonic()
    report: Dict[str, Any] = {
        "ts": _now_iso(),
        "pass": "main_semantic_pos_aligned",
        "main_db": str(MAIN_DB),
        "concept_db": str(CONCEPT_DB),
        "en_sample": en_sample,
        "ru_sample": ru_sample,
        "threshold": threshold,
        "model": model_name,
        "report_version": 1,
    }
    print("loading existing linked-words set...", flush=True)
    en_seen, ru_seen = _cdb.existing_linked_words()
    report["pre_en_seen"] = len(en_seen)
    report["pre_ru_seen"] = len(ru_seen)

    print(f"sampling {en_sample:,} POS-tagged unlinked EN rows...", flush=True)
    t_load = time.monotonic()
    en_rows = _load_unlinked_sample_with_pos("en", en_sample, en_seen)
    report["en_sampled"] = len(en_rows)
    print(f"  {len(en_rows):,} EN rows in {time.monotonic()-t_load:.1f}s",
          flush=True)

    print(f"sampling {ru_sample:,} POS-tagged unlinked RU rows...", flush=True)
    t_load = time.monotonic()
    ru_rows = _load_unlinked_sample_with_pos("ru", ru_sample, ru_seen)
    report["ru_sampled"] = len(ru_rows)
    print(f"  {len(ru_rows):,} RU rows in {time.monotonic()-t_load:.1f}s",
          flush=True)

    # Group EN by POS class so per-RU lookup is restricted.
    import collections
    en_by_class: Dict[str, List[int]] = collections.defaultdict(list)
    for i, r in enumerate(en_rows):
        en_by_class[r["_pos_class"]].append(i)
    ru_by_class: Dict[str, List[int]] = collections.defaultdict(list)
    for i, r in enumerate(ru_rows):
        ru_by_class[r["_pos_class"]].append(i)
    report["en_class_sizes"] = {k: len(v) for k, v in en_by_class.items()}
    report["ru_class_sizes"] = {k: len(v) for k, v in ru_by_class.items()}

    if not en_rows or not ru_rows:
        report["ok"] = True
        report["created_concepts"] = 0
        report["note"] = "no_samples"
        report["elapsed_s"] = round(time.monotonic() - t0, 2)
        return report

    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except Exception as exc:  # noqa: BLE001
        report["ok"] = False
        report["error"] = f"import_failed: {type(exc).__name__}: {exc}"
        report["elapsed_s"] = round(time.monotonic() - t0, 2)
        return report

    print(f"loading model {model_name}...", flush=True)
    t_model = time.monotonic()
    model = SentenceTransformer(model_name)
    report["model_load_s"] = round(time.monotonic() - t_model, 2)

    en_texts = [_en_text(r) for r in en_rows]
    ru_texts = [_ru_text(r) for r in ru_rows]
    print(f"embedding {len(en_texts):,} EN + {len(ru_texts):,} RU rows...",
          flush=True)
    t_emb = time.monotonic()
    en_emb = model.encode(en_texts, convert_to_numpy=True,
                           show_progress_bar=False, batch_size=64,
                           normalize_embeddings=True)
    ru_emb = model.encode(ru_texts, convert_to_numpy=True,
                           show_progress_bar=False, batch_size=64,
                           normalize_embeddings=True)
    report["embed_s"] = round(time.monotonic() - t_emb, 2)

    # Per-class cosine search: for each POS class, compute sim only
    # between rows in that class. Saves both compute and noise.
    print("computing per-class similarity + matching...", flush=True)
    t_sim = time.monotonic()
    pair_candidates: List[Tuple[float, int, int]] = []
    for pos_class, ru_idxs in ru_by_class.items():
        en_idxs = en_by_class.get(pos_class) or []
        if not en_idxs or not ru_idxs:
            continue
        ru_block = ru_emb[ru_idxs]
        en_block = en_emb[en_idxs]
        sim = ru_block @ en_block.T  # (|ru_idxs|, |en_idxs|)
        best_local = sim.argmax(axis=1)
        best_sim = sim.max(axis=1)
        for local_ru, (local_en, s) in enumerate(zip(best_local, best_sim)):
            s_f = float(s)
            if s_f < threshold:
                continue
            global_ru = ru_idxs[local_ru]
            global_en = en_idxs[int(local_en)]
            pair_candidates.append((s_f, global_ru, global_en))
    report["sim_compute_s"] = round(time.monotonic() - t_sim, 2)
    pair_candidates.sort(key=lambda x: -x[0])
    report["candidates_above_threshold"] = len(pair_candidates)

    # Greedy: highest-sim first, each EN used once.
    concept_rows: List[Tuple] = []
    link_rows: List[Tuple] = []
    used_en: set[int] = set()
    for sim_val, ru_idx, en_idx in pair_candidates:
        if en_idx in used_en:
            continue
        en = en_rows[en_idx]
        r = ru_rows[ru_idx]
        e_word = (en.get("word") or "").strip()
        r_word = (r.get("word") or "").strip()
        if not e_word or not r_word:
            continue
        cid = _cdb._new_id("concept")
        now = _now_iso()
        try:
            en_cov = set(json.loads(en.get("coverage_categories") or "[]"))
        except Exception:  # noqa: BLE001
            en_cov = set()
        try:
            ru_cov = set(json.loads(r.get("coverage_categories") or "[]"))
        except Exception:  # noqa: BLE001
            ru_cov = set()
        coverage = sorted(en_cov | ru_cov)
        registers = sorted(
            {en.get("register") or "", r.get("register") or ""} - {""})
        conf = _cdb.clamp_confidence(max(0.6, min(0.85, sim_val * 0.95)))
        notes = (f"main_semantic_pos_aligned sim={sim_val:.3f} "
                 f"pos_class={en['_pos_class']} "
                 f"en_id={en.get('id')} ru_id={r.get('id')}")
        concept_rows.append((
            cid, e_word, r_word,
            _cdb.json_list(coverage),
            _cdb.json_list(coverage),
            _cdb.json_list(registers or ["standard"]),
            "[]",
            now, now, notes,
        ))
        link_rows.append((
            _cdb._new_id("link"), cid, "en",
            "main_english_words", str(en.get("id")), e_word, "",
            "", en.get("pos") or "",
            conf, "heuristic", now, notes,
        ))
        link_rows.append((
            _cdb._new_id("link"), cid, "ru",
            "main_russian_words", str(r.get("id")), r_word, "",
            "", r.get("pos") or "",
            conf, "heuristic", now, notes,
        ))
        used_en.add(en_idx)
    report["queued_concepts"] = len(concept_rows)

    if concept_rows:
        t_ins = time.monotonic()
        ins = _cdb.insert_concept_batch(
            concept_rows, link_rows,
            audit_message=(f"semantic_pos_aligned en_sample={en_sample} "
                            f"ru_sample={ru_sample} "
                            f"threshold={threshold} "
                            f"created={len(concept_rows)}"))
        report["insert"] = ins
        report["insert_s"] = round(time.monotonic() - t_ins, 2)
    else:
        report["insert_skipped"] = "no_pairs_above_threshold"

    report["ok"] = True
    report["elapsed_s"] = round(time.monotonic() - t0, 2)
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--en-sample", type=int, default=25000)
    p.add_argument("--ru-sample", type=int, default=25000)
    p.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    args = p.parse_args()
    report = pos_aligned_semantic_pass(
        en_sample=args.en_sample,
        ru_sample=args.ru_sample,
        threshold=args.threshold)
    out_path = REPORTS_DIR / (
        f"bilingual_main_semantic_pos_aligned_"
        f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json")
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nreport -> {out_path}")
